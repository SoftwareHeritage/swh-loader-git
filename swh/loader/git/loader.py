# Copyright (C) 2016-2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
from dataclasses import dataclass
import datetime
import json
import logging
import os
import pickle
from tempfile import SpooledTemporaryFile
import time
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
)

import dulwich.client
from dulwich.object_store import ObjectStoreGraphWalker
from dulwich.objects import Blob, Commit, ShaFile, Tag, Tree
from dulwich.pack import PackData, PackInflater
import urllib3.util

from swh.core.statsd import Statsd
from swh.loader.exception import NotFound
from swh.loader.git.utils import raise_not_found_repository
from swh.model import hashutil
from swh.model.git_objects import (
    content_git_object,
    directory_git_object,
    release_git_object,
    revision_git_object,
)
from swh.model.model import (
    BaseContent,
    Content,
    Directory,
    RawExtrinsicMetadata,
    Release,
    Revision,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
)
from swh.model.swhids import ExtendedObjectType
from swh.storage.algos.directory import directory_get
from swh.storage.algos.snapshot import snapshot_get_latest
from swh.storage.interface import StorageInterface

from . import converters, dumb, utils
from .base import BaseGitLoader
from .utils import LOGGING_INTERVAL, HexBytes, PackWriter

logger = logging.getLogger(__name__)
heads_logger = logger.getChild("refs")
remote_logger = logger.getChild("remote")
fetch_pack_logger = logger.getChild("fetch_pack")


def split_lines_and_remainder(buf: bytes) -> Tuple[List[bytes], bytes]:
    """Get newline-terminated (``b"\\r"`` or ``b"\\n"``) lines from `buf`,
    and the beginning of the last line if it isn't terminated."""

    lines = buf.splitlines(keepends=True)
    if not lines:
        return [], b""

    if buf.endswith((b"\r", b"\n")):
        # The buffer ended with a newline, everything can be sent as lines
        return lines, b""
    else:
        # The buffer didn't end with a newline, we need to keep the
        # last bit as the beginning of the next line
        return lines[:-1], lines[-1]


class RepoRepresentation:
    """Repository representation for a Software Heritage origin."""

    def __init__(
        self,
        storage,
        base_snapshots: Optional[List[Snapshot]] = None,
        incremental: bool = True,
        statsd: Optional[Statsd] = None,
    ):
        self.storage = storage
        self.incremental = incremental
        self.statsd = statsd

        if base_snapshots and incremental:
            self.base_snapshots: List[Snapshot] = base_snapshots
        else:
            self.base_snapshots = []

        # Cache existing heads
        self.local_heads: Set[HexBytes] = set()
        heads_logger.debug("Heads known in the archive:")
        for base_snapshot in self.base_snapshots:
            for branch_name, branch in base_snapshot.branches.items():
                if not branch or branch.target_type == SnapshotTargetType.ALIAS:
                    continue
                heads_logger.debug("    %r: %s", branch_name, branch.target.hex())
                self.local_heads.add(HexBytes(hashutil.hash_to_bytehex(branch.target)))

    def graph_walker(self) -> ObjectStoreGraphWalker:
        return ObjectStoreGraphWalker(self.local_heads, get_parents=lambda commit: [])

    def determine_wants(self, refs: Dict[bytes, HexBytes]) -> List[HexBytes]:
        """Get the list of bytehex sha1s that the git loader should fetch.

        This compares the remote refs sent by the server with the base snapshot
        provided by the loader.

        """
        if not refs:
            return []

        if heads_logger.isEnabledFor(logging.DEBUG):
            heads_logger.debug("Heads returned by the git remote:")
            for name, value in refs.items():
                heads_logger.debug("    %r: %s", name, value.decode())

        # Get the remote heads that we want to fetch
        remote_heads: Set[HexBytes] = set()
        for ref_name, ref_target in refs.items():
            if utils.ignore_branch_name(ref_name):
                continue
            remote_heads.add(ref_target)

        logger.debug("local_heads_count=%s", len(self.local_heads))
        logger.debug("remote_heads_count=%s", len(remote_heads))
        wanted_refs = list(remote_heads - self.local_heads)

        logger.debug("wanted_refs_count=%s", len(wanted_refs))
        if self.statsd is not None:
            self.statsd.histogram(
                "git_ignored_refs_percent",
                len(remote_heads - set(refs.values())) / len(refs),
                tags={},
            )
            self.statsd.histogram(
                "git_known_refs_percent",
                len(self.local_heads & remote_heads) / len(remote_heads),
                tags={},
            )
        return wanted_refs


@dataclass
class FetchPackReturn:
    remote_refs: Dict[bytes, HexBytes]
    symbolic_refs: Dict[bytes, HexBytes]
    pack_buffer: SpooledTemporaryFile
    pack_size: int


class GitLoader(BaseGitLoader):
    """A bulk loader for a git repository

    Emits the following statsd stats:

    * increments ``swh_loader_git``
    * histogram ``swh_loader_git_ignored_refs_percent`` is the ratio of refs ignored
      over all refs of the remote repository
    * histogram ``swh_loader_git_known_refs_percent`` is the ratio of (non-ignored)
      remote heads that are already local over all non-ignored remote heads

    All three are tagged with ``{{"incremental": "<incremental_mode>"}}`` where
    ``incremental_mode`` is one of:

    * ``from_same_origin`` when the origin was already loaded
    * ``from_parent_origin`` when the origin was not already loaded,
      but it was detected as a forge-fork of an origin that was already loaded
    * ``no_previous_snapshot`` when the origin was not already loaded,
      and it was detected as a forge-fork of origins that were not already loaded either
    * ``no_parent_origin`` when the origin was no already loaded, and it was not
      detected as a forge-fork of any other origin
    * ``disabled`` when incremental loading is disabled by configuration
    """

    visit_type = "git"

    def __init__(
        self,
        storage: StorageInterface,
        url: str,
        incremental: bool = True,
        repo_representation: Type[RepoRepresentation] = RepoRepresentation,
        pack_size_bytes: int = 4 * 1024 * 1024 * 1024,
        temp_file_cutoff: int = 100 * 1024 * 1024,
        connect_timeout: float = 120,
        read_timeout: float = 60,
        verify_certs: bool = True,
        urllib3_extra_kwargs: Dict[str, Any] = {},
        requests_extra_kwargs: Dict[str, Any] = {},
        **kwargs: Any,
    ):
        """Initialize the bulk updater.

        Args:
            repo_representation: swh's repository representation
            which is in charge of filtering between known and remote
            data.
            ...

            incremental: If True, the default, this starts from the last known snapshot
                (if any) references. Otherwise, this loads the full repository.

        """
        super().__init__(storage=storage, origin_url=url, **kwargs)
        self.incremental = incremental
        self.repo_representation = repo_representation
        self.pack_size_bytes = pack_size_bytes
        self.temp_file_cutoff = temp_file_cutoff
        # state initialized in fetch_data
        self.remote_refs: Dict[bytes, HexBytes] = {}
        self.symbolic_refs: Dict[bytes, HexBytes] = {}
        self.ref_object_types: Dict[bytes, Optional[SnapshotTargetType]] = {}
        self.ext_refs: Dict[bytes, Optional[Tuple[int, bytes]]] = {}
        self.repo_pack_size_bytes = 0
        self.urllib3_extra_kwargs = urllib3_extra_kwargs
        self.urllib3_extra_kwargs["timeout"] = urllib3.util.Timeout(
            connect=connect_timeout, read=read_timeout
        )
        self.requests_extra_kwargs = requests_extra_kwargs
        self.requests_extra_kwargs["timeout"] = (connect_timeout, read_timeout)

        if not verify_certs:
            self.urllib3_extra_kwargs["cert_reqs"] = "CERT_NONE"
            self.requests_extra_kwargs["verify"] = False

    def fetch_pack_from_origin(
        self,
        origin_url: str,
        base_repo: RepoRepresentation,
        do_activity: Callable[[bytes], None],
    ) -> FetchPackReturn:
        """Fetch a pack from the origin"""

        pack_buffer = SpooledTemporaryFile(max_size=self.temp_file_cutoff)
        transport_url = origin_url

        logger.debug("Transport url to communicate with server: %s", transport_url)

        transport_kwargs: Dict[str, Any] = {"thin_packs": False}

        if transport_url.startswith(("http://", "https://")):
            # Inject urllib3 kwargs into the pool manager
            transport_kwargs["pool_manager"] = dulwich.client.default_urllib3_manager(
                config=None,
                **self.urllib3_extra_kwargs,
            )

        client, path = dulwich.client.get_transport_and_path(
            location=transport_url,
            config=None,
            operation="pull",
            **transport_kwargs,
        )

        logger.debug("Client %s to fetch pack at %s", client, path)

        pack_writer = PackWriter(
            pack_buffer=pack_buffer,
            size_limit=self.pack_size_bytes,
            origin_url=origin_url,
            fetch_pack_logger=fetch_pack_logger,
        )

        pack_result = client.fetch_pack(
            path,
            base_repo.determine_wants,
            base_repo.graph_walker(),
            pack_writer.write,
            progress=do_activity,
        )

        remote_refs = pack_result.refs or {}
        symbolic_refs = pack_result.symrefs or {}

        pack_buffer.flush()
        pack_size = pack_buffer.tell()
        pack_buffer.seek(0)

        logger.debug("fetched_pack_size=%s", pack_size)

        # check if repository only supports git dumb transfer protocol,
        # fetched pack file will be empty in that case as dulwich do
        # not support it and do not fetch any refs
        self.dumb = transport_url.startswith("http") and getattr(client, "dumb", False)

        return FetchPackReturn(
            remote_refs=utils.filter_refs(remote_refs),
            symbolic_refs=utils.filter_refs(symbolic_refs),
            pack_buffer=pack_buffer,
            pack_size=pack_size,
        )

    def get_full_snapshot(self, origin_url) -> Optional[Snapshot]:
        return snapshot_get_latest(
            self.storage,
            origin_url,
            visit_type=self.visit_type,
        )

    def load_metadata_objects(
        self, metadata_objects: List[RawExtrinsicMetadata]
    ) -> None:
        for metadata in metadata_objects:
            if (
                metadata.target.object_type == ExtendedObjectType.ORIGIN
                and metadata.format == "application/vnd.github.v3+json"
            ):
                try:
                    json_metadata = json.loads(metadata.metadata)
                    self.repo_pack_size_bytes = json_metadata.get("size", 0) * 1024
                except json.JSONDecodeError:
                    logger.warning(
                        "JSON metadata for origin %s could not be parsed.",
                        self.origin.url,
                    )
        super().load_metadata_objects(metadata_objects)

    def prepare(self) -> None:
        assert self.origin is not None

        self.prev_snapshot = Snapshot(branches={})
        """Last snapshot of this origin if any; empty snapshot otherwise"""
        self.base_snapshots = []
        """Last snapshot of this origin and all its parents, if any."""

        self.statsd.constant_tags["incremental_enabled"] = self.incremental
        self.statsd.constant_tags["has_parent_origins"] = bool(self.parent_origins)

        # May be set to True later
        self.statsd.constant_tags["has_parent_snapshot"] = False

        if self.incremental:
            prev_snapshot = self.get_full_snapshot(self.origin.url)
            self.statsd.constant_tags["has_previous_snapshot"] = bool(prev_snapshot)
            if prev_snapshot:
                self.prev_snapshot = prev_snapshot
                self.base_snapshots.append(prev_snapshot)

            if self.parent_origins is not None:
                # If this origin is a forge fork, load incrementally from the
                # origins it was forked from
                for parent_origin in self.parent_origins:
                    parent_snapshot = self.get_full_snapshot(parent_origin.url)
                    if parent_snapshot is not None:
                        self.statsd.constant_tags["has_parent_snapshot"] = True
                        self.base_snapshots.append(parent_snapshot)

        # Increments a metric with full name 'swh_loader_git'; which is useful to
        # count how many runs of the loader are with each incremental mode
        self.statsd.increment("git_total", tags={})

    def fetch_data(self) -> bool:
        assert self.origin is not None

        if not self.base_snapshots and self.repo_pack_size_bytes > self.pack_size_bytes:
            raise IOError(
                f"Pack file too big for repository {self.origin.url}, "
                f"limit is {self.pack_size_bytes} bytes, "
                f"current size is {self.repo_pack_size_bytes}"
            )

        base_repo = self.repo_representation(
            storage=self.storage,
            base_snapshots=self.base_snapshots,
            incremental=self.incremental,
            statsd=self.statsd,
        )

        # Remote logging utilities

        # Number of lines (ending with a carriage return) elided when debug
        # logging is not enabled
        remote_lines_elided = 0

        # Timestamp where the last elision was logged
        last_elision_logged = time.monotonic()

        def maybe_log_elision(force: bool = False):
            nonlocal remote_lines_elided
            nonlocal last_elision_logged

            if remote_lines_elided and (
                force
                # Always log at least every LOGGING_INTERVAL
                or time.monotonic() > last_elision_logged + LOGGING_INTERVAL
            ):
                remote_logger.info(
                    "%s remote line%s elided",
                    remote_lines_elided,
                    "s" if remote_lines_elided > 1 else "",
                )
                remote_lines_elided = 0
                last_elision_logged = time.monotonic()

        def log_remote_message(line: bytes):
            nonlocal remote_lines_elided

            do_debug = remote_logger.isEnabledFor(logging.DEBUG)

            if not line.endswith(b"\n"):
                # This is a verbose line, ending with a carriage return only
                if do_debug:
                    if stripped := line.strip():
                        remote_logger.debug(
                            "remote: %s", stripped.decode("utf-8", "backslashreplace")
                        )
                else:
                    remote_lines_elided += 1
                    maybe_log_elision()
            else:
                # This is the last line in the current section, we will always log it
                maybe_log_elision(force=True)
                if stripped := line.strip():
                    remote_logger.info(
                        "remote: %s", stripped.decode("utf-8", "backslashreplace")
                    )

        # This buffer keeps the end of what do_remote has received, across
        # calls, if it happens to be unterminated
        next_line_buf = b""

        def do_remote(msg: bytes) -> None:
            nonlocal next_line_buf

            lines, next_line_buf = split_lines_and_remainder(next_line_buf + msg)

            for line in lines:
                log_remote_message(line)

        try:
            with raise_not_found_repository():
                fetch_info = self.fetch_pack_from_origin(
                    self.origin.url, base_repo, do_remote
                )
        except NotFound:
            # NotFound inherits from ValueError and should not be caught
            # by the next exception handler
            raise
        except (AttributeError, NotImplementedError, ValueError):
            # with old dulwich versions, those exceptions types can be raised
            # by the fetch_pack operation when encountering a repository with
            # dumb transfer protocol so we check if the repository supports it
            # here to continue the loading if it is the case
            self.dumb = dumb.check_protocol(self.origin.url, self.requests_extra_kwargs)
            if not self.dumb:
                raise
        else:
            # Always log what remains in the next_line_buf, if it's not empty
            maybe_log_elision(force=True)
            log_remote_message(next_line_buf)

        logger.debug(
            "Protocol used for communication: %s", "dumb" if self.dumb else "smart"
        )
        if self.dumb:
            self.dumb_fetcher = dumb.GitObjectsFetcher(
                repo_url=self.origin.url,
                base_repo=base_repo,
                pack_size_limit=self.pack_size_bytes,
                requests_extra_kwargs=self.requests_extra_kwargs,
            )
            self.dumb_fetcher.fetch_object_ids()
            self.remote_refs = utils.filter_refs(self.dumb_fetcher.refs)
            self.symbolic_refs = utils.filter_refs(self.dumb_fetcher.head)
        else:
            self.pack_buffer = fetch_info.pack_buffer
            self.pack_size = fetch_info.pack_size
            self.remote_refs = fetch_info.remote_refs
            self.symbolic_refs = fetch_info.symbolic_refs

        self.ref_object_types = {sha1: None for sha1 in self.remote_refs.values()}

        logger.info(
            "Listed %d refs for repo %s",
            len(self.remote_refs),
            self.origin.url,
            extra={
                "swh_type": "git_repo_list_refs",
                "swh_repo": self.origin.url,
                "swh_num_refs": len(self.remote_refs),
            },
        )

        # No more data to fetch
        return False

    def save_data(self) -> None:
        """Store a pack for archival"""
        assert isinstance(self.visit_date, datetime.datetime)

        write_size = 8192
        pack_dir = self.get_save_data_path()

        pack_name = "%s.pack" % self.visit_date.isoformat()
        refs_name = "%s.refs" % self.visit_date.isoformat()

        with open(os.path.join(pack_dir, pack_name), "xb") as f:
            self.pack_buffer.seek(0)
            while True:
                r = self.pack_buffer.read(write_size)
                if not r:
                    break
                f.write(r)

        self.pack_buffer.seek(0)

        with open(os.path.join(pack_dir, refs_name), "xb") as f:
            pickle.dump(self.remote_refs, f)

    def _resolve_ext_ref(self, sha1: bytes) -> Tuple[int, bytes]:
        """Resolve external references to git objects a pack file might contain
        by getting associated git manifests from the archive.
        """
        storage = self.storage
        ext_refs = self.ext_refs
        statsd_metric = "swh_loader_git_external_reference_fetch_total"

        def set_ext_ref(type_num, manifest, swh_type):
            ext_refs[sha1] = (type_num, manifest.split(b"\x00", maxsplit=1)[1])
            self.statsd.increment(
                statsd_metric,
                tags={"type": swh_type, "result": "found"},
            )
            self.log.debug(
                "External reference %s of type %s in the pack file resolved from the archive",
                hashutil.hash_to_hex(sha1),
                swh_type,
            )

        if sha1 not in ext_refs:
            cnts = storage.content_find({"sha1_git": sha1})
            if cnts and cnts[0] is not None:
                cnt = cnts[0]
                d = cnt.to_dict()
                d["data"] = storage.content_get_data(cnt.sha1)
                cnt = Content.from_dict(d)
                cnt.check()
                set_ext_ref(Blob.type_num, content_git_object(cnt), "content")
        if sha1 not in ext_refs:
            dir = directory_get(storage, sha1)
            if dir is not None:
                dir.check()
                set_ext_ref(Tree.type_num, directory_git_object(dir), "directory")
        if sha1 not in ext_refs:
            rev = storage.revision_get([sha1], ignore_displayname=True)[0]
            if rev is not None:
                rev.check()
                set_ext_ref(Commit.type_num, revision_git_object(rev), "revision")
        if sha1 not in ext_refs:
            rel = storage.release_get([sha1], ignore_displayname=True)[0]
            if rel is not None:
                rel.check()
                set_ext_ref(Tag.type_num, release_git_object(rel), "release")

        if sha1 not in ext_refs:
            self.statsd.increment(
                statsd_metric,
                tags={"type": "unknown", "result": "not_found"},
            )
            self.log.debug(
                "External reference %s in the pack file could not be resolved from the archive",
                hashutil.hash_to_hex(sha1),
            )
            ext_refs[sha1] = None

        ext_ref = ext_refs[sha1]
        if ext_ref is None:
            # dulwich catches this exception but checks for pending objects in the pack once
            # all ref chains have been walked
            raise KeyError(
                f"Object with sha1_git {hashutil.hash_to_hex(sha1)} not found in the archive"
            )
        return ext_ref

    def iter_objects(self, object_type: bytes) -> Iterator[ShaFile]:
        """Read all the objects of type `object_type` from the packfile"""
        if self.dumb:
            yield from self.dumb_fetcher.iter_objects(object_type)
        elif self.pack_size > 0:
            self.pack_buffer.seek(0)
            count = 0
            for obj in PackInflater.for_pack_data(
                PackData.from_file(
                    self.pack_buffer,
                    self.pack_size,
                ),
                resolve_ext_ref=self._resolve_ext_ref,
            ):
                if obj.type_name != object_type:
                    continue
                yield obj
                count += 1
            logger.debug("packfile_read_count_%s=%s", object_type.decode(), count)

    def get_contents(self) -> Iterable[BaseContent]:
        """Format the blobs from the git repository as swh contents"""
        for raw_obj in self.iter_objects(b"blob"):
            if raw_obj.id in self.ref_object_types:
                self.ref_object_types[raw_obj.id] = SnapshotTargetType.CONTENT

            yield converters.dulwich_blob_to_content(
                raw_obj, max_content_size=self.max_content_size
            )

    def get_directories(self) -> Iterable[Directory]:
        """Format the trees as swh directories"""
        for raw_obj in self.iter_objects(b"tree"):
            if raw_obj.id in self.ref_object_types:
                self.ref_object_types[raw_obj.id] = SnapshotTargetType.DIRECTORY

            yield converters.dulwich_tree_to_directory(raw_obj)

    def get_revisions(self) -> Iterable[Revision]:
        """Format commits as swh revisions"""
        for raw_obj in self.iter_objects(b"commit"):
            if raw_obj.id in self.ref_object_types:
                self.ref_object_types[raw_obj.id] = SnapshotTargetType.REVISION

            yield converters.dulwich_commit_to_revision(raw_obj)

    def get_releases(self) -> Iterable[Release]:
        """Retrieve all the release objects from the git repository"""
        for raw_obj in self.iter_objects(b"tag"):
            if raw_obj.id in self.ref_object_types:
                self.ref_object_types[raw_obj.id] = SnapshotTargetType.RELEASE

            yield converters.dulwich_tag_to_release(raw_obj)

    def get_snapshot(self) -> Snapshot:
        """Get the snapshot for the current visit.

        The main complexity of this function is mapping target objects to their
        types, as the `refs` dictionaries returned by the git server only give
        us the identifiers for the target objects, and not their types.

        The loader itself only knows the types of the objects that it has
        fetched from the server (as it has parsed them while loading them to
        the archive). As we only fetched an increment between the previous
        snapshot and the current state of the server, we are missing the type
        information for the objects that would already have been referenced by
        the previous snapshot, and that the git server didn't send us. We infer
        the type of these objects from the previous snapshot.

        """
        branches: Dict[bytes, Optional[SnapshotBranch]] = {}

        unfetched_refs: Dict[bytes, bytes] = {}

        # Retrieve types from the objects loaded by the current loader
        for ref_name, ref_object in self.remote_refs.items():
            if ref_name in self.symbolic_refs:
                continue
            target = hashutil.hash_to_bytes(ref_object.decode())
            target_type = self.ref_object_types.get(ref_object)
            if target_type:
                branches[ref_name] = SnapshotBranch(
                    target=target, target_type=target_type
                )
            else:
                # The object pointed at by this ref was not fetched, supposedly
                # because it existed in the base snapshot. We record it here,
                # and we can get it from the base snapshot later.
                unfetched_refs[ref_name] = target

        dangling_branches = {}
        # Handle symbolic references as alias branches
        for ref_name, target in self.symbolic_refs.items():
            branches[ref_name] = SnapshotBranch(
                target_type=SnapshotTargetType.ALIAS,
                target=target,
            )
            if target not in branches and target not in unfetched_refs:
                # This handles the case where the pointer is "dangling".
                # There's a chance that a further symbolic reference
                # override this default value, which is totally fine.
                dangling_branches[target] = ref_name
                branches[target] = None

        if unfetched_refs:
            # Handle inference of object types from the contents of the
            # previous snapshot
            unknown_objects = {}

            base_snapshot_reverse_branches = {
                branch.target: branch
                for base_snapshot in reversed(self.base_snapshots)
                for branch in base_snapshot.branches.values()
                if branch and branch.target_type != SnapshotTargetType.ALIAS
            }
            assert all(
                base_snapshot_reverse_branches[branch.target] == branch
                for branch in self.prev_snapshot.branches.values()
                if branch and branch.target_type != SnapshotTargetType.ALIAS
            ), "base_snapshot_reverse_branches is not a superset of prev_snapshot"

            for ref_name, target in unfetched_refs.items():
                branch = base_snapshot_reverse_branches.get(target)
                branches[ref_name] = branch
                if not branch:
                    unknown_objects[ref_name] = target

            if unknown_objects and self.base_snapshots:
                # The remote has sent us a partial packfile. It will have skipped
                # objects that it knows are ancestors of the heads we have sent as
                # known. We can look these objects up in the archive, as they should
                # have had all their ancestors loaded when the previous snapshot was
                # loaded.
                refs_for_target = defaultdict(list)
                for ref_name, target in unknown_objects.items():
                    refs_for_target[target].append(ref_name)

                targets_unknown = set(refs_for_target)

                for method, target_type in (
                    (self.storage.revision_missing, SnapshotTargetType.REVISION),
                    (self.storage.release_missing, SnapshotTargetType.RELEASE),
                    (self.storage.directory_missing, SnapshotTargetType.DIRECTORY),
                    (
                        self.storage.content_missing_per_sha1_git,
                        SnapshotTargetType.CONTENT,
                    ),
                ):
                    missing = set(method(list(targets_unknown)))
                    known = targets_unknown - missing

                    for target in known:
                        for ref_name in refs_for_target[target]:
                            logger.debug(
                                "Inferred type %s for branch %r pointing at unfetched %s",
                                target_type.name,
                                ref_name,
                                hashutil.hash_to_hex(target),
                                extra={
                                    "swh_type": "swh_loader_git_inferred_target_type"
                                },
                            )
                            branches[ref_name] = SnapshotBranch(
                                target=target, target_type=target_type
                            )
                            del unknown_objects[ref_name]

                    targets_unknown = missing
                    if not targets_unknown:
                        break

            if unknown_objects:
                # This object was referenced by the server; We did not fetch
                # it, and we do not know it from the previous snapshot. This is
                # likely a bug in the loader.
                raise RuntimeError(
                    "Unknown objects referenced by remote refs: %s"
                    % (
                        ", ".join(
                            f"{name!r}: {hashutil.hash_to_hex(obj)}"
                            for name, obj in unknown_objects.items()
                        )
                    )
                )

        utils.warn_dangling_branches(
            branches, dangling_branches, logger, self.origin.url
        )

        self.snapshot = Snapshot(branches=branches)
        return self.snapshot

    def load_status(self) -> Dict[str, Any]:
        """The load was eventful if the current snapshot is different to
        the one we retrieved at the beginning of the run"""
        eventful = False
        if self.prev_snapshot and self.snapshot:
            eventful = self.snapshot.id != self.prev_snapshot.id
        elif self.snapshot:
            eventful = bool(self.snapshot.branches)

        return {"status": ("eventful" if eventful else "uneventful")}


if __name__ == "__main__":
    import click

    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s %(process)d %(message)s"
    )

    from deprecated import deprecated

    @deprecated(version="1.1", reason="Use `swh loader run git --help` instead")
    @click.command()
    @click.option("--origin-url", help="Origin url", required=True)
    @click.option("--base-url", default=None, help="Optional Base url")
    @click.option(
        "--ignore-history/--no-ignore-history",
        help="Ignore the repository history",
        default=False,
    )
    def main(origin_url: str, incremental: bool) -> Dict[str, Any]:
        from swh.storage import get_storage

        storage = get_storage(cls="memory")
        loader = GitLoader(
            storage,
            origin_url,
            incremental=incremental,
        )
        return loader.load()

    main()
