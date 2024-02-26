# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from collections import defaultdict
import copy
import logging
import stat
import struct
from tempfile import SpooledTemporaryFile
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Protocol,
    Set,
    cast,
)
import urllib.parse

from dulwich.errors import NotGitRepository
from dulwich.objects import S_IFGITLINK, Commit, ShaFile, Tree
from dulwich.pack import Pack, PackData, PackIndex, load_pack_index_file
import requests
from tenacity.before_sleep import before_sleep_log

from swh.core.retry import http_retry
from swh.loader.git.utils import HexBytes, PackWriter

if TYPE_CHECKING:
    from .loader import RepoRepresentation

logger = logging.getLogger(__name__)
fetch_pack_logger = logger.getChild("fetch_pack")


class BytesWriter(Protocol):
    def write(self, data: bytes):
        ...


def requests_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Inject User-Agent header in the requests kwargs"""
    ret = copy.deepcopy(kwargs)
    ret.setdefault("headers", {}).update(
        {"User-Agent": "Software Heritage dumb Git loader"}
    )
    ret.setdefault("timeout", (120, 60))
    return ret


@http_retry(
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def check_protocol(repo_url: str, requests_extra_kwargs: Dict[str, Any] = {}) -> bool:
    """Checks if a git repository can be cloned using the dumb protocol.

    Args:
        repo_url: Base URL of a git repository
        requests_extra_kwargs: extra keyword arguments to be passed to requests,
          e.g. `timeout`, `verify`.

    Returns:
        Whether the dumb protocol is supported.

    """
    if not repo_url.startswith("http"):
        return False
    url = urllib.parse.urljoin(
        repo_url.rstrip("/") + "/", "info/refs?service=git-upload-pack"
    )
    logger.debug("Fetching %s", url)
    response = requests.get(url, **requests_kwargs(requests_extra_kwargs))
    response.raise_for_status()
    content_type = response.headers.get("Content-Type")
    return (
        response.status_code
        in (
            200,
            304,
        )
        # header is not mandatory in protocol specification
        and (content_type is None or not content_type.startswith("application/x-git-"))
    )


class GitObjectsFetcher:
    """Git objects fetcher using dumb HTTP protocol.

    Fetches a set of git objects for a repository according to its archival
    state by Software Heritage and provides iterators on them.

    Args:
        repo_url: Base URL of a git repository
        base_repo: State of repository archived by Software Heritage
        requests_extra_kwargs: extra keyword arguments to be passed to requests,
          e.g. `timeout`, `verify`.
    """

    def __init__(
        self,
        repo_url: str,
        base_repo: RepoRepresentation,
        pack_size_limit: int,
        requests_extra_kwargs: Dict[str, Any] = {},
    ):
        self._session = requests.Session()
        self.requests_extra_kwargs = requests_extra_kwargs
        self.repo_url = repo_url
        self.base_repo = base_repo
        self.pack_size_limit = pack_size_limit
        self.objects: Dict[bytes, Set[bytes]] = defaultdict(set)
        self.refs = self._get_refs()
        self.head = self._get_head() if self.refs else {}
        self.packs = self._get_packs()

    def fetch_object_ids(self) -> None:
        """Fetches identifiers of git objects to load into the archive."""
        wants = self.base_repo.determine_wants(self.refs)

        # process refs
        commit_objects = []
        for ref in wants:
            ref_object = self._get_git_object(ref)
            if ref_object.type_num == Commit.type_num:
                commit_objects.append(cast(Commit, ref_object))
                self.objects[b"commit"].add(ref)
            else:
                self.objects[b"tag"].add(ref)

        # perform DFS on commits graph
        while commit_objects:
            commit = commit_objects.pop()
            # fetch tree and blob ids recursively
            self._fetch_tree_objects(commit.tree)
            for parent in commit.parents:
                if (
                    # commit not already seen in the current load
                    parent not in self.objects[b"commit"]
                    # commit not already archived by a previous load
                    and parent not in self.base_repo.local_heads
                ):
                    commit_objects.append(cast(Commit, self._get_git_object(parent)))
                    self.objects[b"commit"].add(parent)

    def iter_objects(self, object_type: bytes) -> Iterable[ShaFile]:
        """Returns a generator on fetched git objects per type.

        Args:
            object_type: Git object type, either b"blob", b"commit", b"tag" or b"tree"

        Returns:
            A generator fetching git objects on the fly.
        """
        return map(self._get_git_object, self.objects[object_type])

    @http_retry(
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _http_get(self, path: str) -> SpooledTemporaryFile:
        url = urllib.parse.urljoin(self.repo_url.rstrip("/") + "/", path)
        logger.debug("Fetching %s", url)
        response = self._session.get(
            url, stream=True, **requests_kwargs(self.requests_extra_kwargs)
        )
        response.raise_for_status()
        buffer = SpooledTemporaryFile(max_size=100 * 1024 * 1024)
        bytes_writer: BytesWriter = buffer
        if path.startswith("objects/pack/") and path.endswith(".pack"):
            bytes_writer = PackWriter(
                pack_buffer=buffer,
                size_limit=self.pack_size_limit,
                origin_url=self.repo_url,
                fetch_pack_logger=fetch_pack_logger,
            )
        for chunk in response.iter_content(chunk_size=10 * 1024 * 1024):
            bytes_writer.write(chunk)
        buffer.flush()
        buffer.seek(0)
        return buffer

    def _get_refs(self) -> Dict[bytes, HexBytes]:
        refs = {}
        refs_resp_bytes = self._http_get("info/refs")
        for ref_line in refs_resp_bytes.readlines():
            ref_target, ref_name = ref_line.replace(b"\n", b"").split(b"\t")
            refs[ref_name] = ref_target
        return refs

    def _get_head(self) -> Dict[bytes, HexBytes]:
        head_resp_bytes = self._http_get("HEAD")
        head_split = head_resp_bytes.readline().replace(b"\n", b"").split(b" ")
        head_target = head_split[1] if len(head_split) > 1 else head_split[0]
        # handle HEAD legacy format containing a commit id instead of a ref name
        for ref_name, ret_target in self.refs.items():
            if ret_target == head_target:
                head_target = ref_name
                break
        return {b"HEAD": head_target}

    def _get_pack_data(self, pack_name: str) -> Callable[[], PackData]:
        def _pack_data() -> PackData:
            pack_data_bytes = self._http_get(f"objects/pack/{pack_name}")
            return PackData(pack_name, file=pack_data_bytes)

        return _pack_data

    def _get_pack_idx(self, pack_idx_name: str) -> Callable[[], PackIndex]:
        def _pack_idx() -> PackIndex:
            pack_idx_bytes = self._http_get(f"objects/pack/{pack_idx_name}")
            return load_pack_index_file(pack_idx_name, pack_idx_bytes)

        return _pack_idx

    def _get_packs(self) -> List[Pack]:
        packs = []
        packs_info_bytes = self._http_get("objects/info/packs")
        packs_info = packs_info_bytes.read().decode()
        for pack_info in packs_info.split("\n"):
            if pack_info:
                pack_name = pack_info.split(" ")[1]
                pack_idx_name = pack_name.replace(".pack", ".idx")
                # pack index and data file will be lazily fetched when required
                packs.append(
                    Pack.from_lazy_objects(
                        self._get_pack_data(pack_name),
                        self._get_pack_idx(pack_idx_name),
                    )
                )
        return packs

    def _get_git_object(self, sha: bytes) -> ShaFile:
        # try to get the object from a pack file first to avoid flooding
        # git server with numerous HTTP requests
        for pack in list(self.packs):
            try:
                if sha in pack:
                    return pack[sha]
            except (NotGitRepository, struct.error, requests.HTTPError) as e:
                if (
                    isinstance(e, requests.HTTPError)
                    and e.response is not None
                    and e.response.status_code != 404
                ):
                    raise
                # missing or invalid pack index/content, remove it from global packs list
                logger.debug("A pack file is missing or its content is invalid")
                self.packs.remove(pack)
        # fetch it from objects/ directory otherwise
        sha_hex = sha.decode()
        object_path = f"objects/{sha_hex[:2]}/{sha_hex[2:]}"
        return ShaFile.from_file(self._http_get(object_path))

    def _fetch_tree_objects(self, sha: bytes) -> None:
        if sha not in self.objects[b"tree"]:
            tree = cast(Tree, self._get_git_object(sha))
            self.objects[b"tree"].add(sha)
            for item in tree.items():
                if item.mode == S_IFGITLINK:
                    # skip submodules as objects are not stored in repository
                    continue
                if item.mode & stat.S_IFDIR:
                    self._fetch_tree_objects(item.sha)
                else:
                    self.objects[b"blob"].add(item.sha)
