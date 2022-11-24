# Copyright (C) 2016-2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
from dataclasses import dataclass
import datetime
import logging
import os
import pickle
import sys
from tempfile import SpooledTemporaryFile
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
from dulwich.errors import GitProtocolError, NotGitRepository
from dulwich.object_store import ObjectStoreGraphWalker
from dulwich.objects import Blob, Commit, ShaFile, Tag, Tree
from dulwich.pack import PackData, PackInflater

from swh.core.statsd import Statsd
from swh.loader.exception import NotFound
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
    Release,
    Revision,
    Snapshot,
    SnapshotBranch,
    TargetType,
)
from swh.storage.algos.directory import directory_get
from swh.storage.algos.snapshot import snapshot_get_latest
from swh.storage.interface import StorageInterface

from . import converters, dumb, utils
from .base import BaseGitLoader
from .utils import HexBytes

logger = logging.getLogger(__name__)
heads_logger = logger.getChild("refs")


DEFAULT_NUMBER_OF_HEADS_PER_PACKFILE = 200


def do_print_progress(msg: bytes) -> None:
    sys.stderr.buffer.write(msg)
    sys.stderr.flush()


class BaseRepoRepresentation:
    """Repository representation for a Software Heritage origin."""

    def __init__(
        self,
        storage,
        base_snapshots: Optional[List[Snapshot]] = None,
        incremental: bool = True,
        statsd: Optional[Statsd] = None,
        **kwargs: Any,  # extra kwargs are just ignored
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
                if not branch or branch.target_type == TargetType.ALIAS:
                    continue
                heads_logger.debug("    %r: %s", branch_name, branch.target.hex())
                self.local_heads.add(HexBytes(hashutil.hash_to_bytehex(branch.target)))

        self.walker = ObjectStoreGraphWalker(self.local_heads, lambda commit: [])
        self.target_to_refs: defaultdict = defaultdict(list)

    def compute_wanted_refs(self, refs: Dict[bytes, HexBytes]) -> List[HexBytes]:
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

        # specific set of objects to sort
        tag_names = set()
        branch_names = set()
        # remote heads is just all refs without order
        remote_heads = set()
        for ref_name, ref_target in refs.items():
            # Ignore either usual branch to ignore or known references
            if utils.ignore_branch_name(ref_name) or ref_target in self.local_heads:
                continue
            # Then we'll sort out the tags from the branches
            if ref_name.startswith(b"refs/tags/"):
                tag_names.add(ref_name)
            else:
                branch_names.add(ref_name)

            remote_heads.add(ref_target)
            self.target_to_refs[ref_target].append(ref_name)

        logger.debug("local_heads_count=%s", len(self.local_heads))
        logger.debug("remote_heads_count=%s", len(remote_heads))

        # Then we sort the refs (by tags then by branches) so it's mostly ingested in
        # lexicographic order (provided there is some consistency there)
        tags = [refs[ref_name] for ref_name in sorted(tag_names)]
        branches = [refs[ref_name] for ref_name in sorted(branch_names)]
        # The wanted refs is the concatenation first tags then branches references
        wanted_refs = tags + branches

        if heads_logger.isEnabledFor(logging.DEBUG):
            heads_logger.debug("Ordered wanted heads returned by the git remote:")
            for ref_target in wanted_refs:
                heads_logger.debug(
                    "    %r: %s", self.target_to_refs[ref_target], ref_target.decode()
                )

        logger.debug("wanted_refs_count=%s", len(wanted_refs))
        if self.statsd is not None:
            self.statsd.histogram(
                "git_ignored_refs_percent",
                len(remote_heads - set(refs.values())) / len(refs),
                tags={},
            )
            git_known_refs_percent = (
                len(self.local_heads & remote_heads) / len(remote_heads)
                if remote_heads
                else 0
            )
            self.statsd.histogram(
                "git_known_refs_percent",
                git_known_refs_percent,
                tags={},
            )
        return wanted_refs

    def continue_fetch_refs(self) -> bool:
        """Determine whether we are done fetching all refs."""
        return False

    def determine_wants(
        self, refs: Dict[bytes, HexBytes], depth=None
    ) -> List[HexBytes]:
        """Get the list of bytehex sha1s that the git loader should fetch.

        This compares the remote refs sent by the server with the base snapshot
        provided by the loader.

        """
        raise NotImplementedError

    def ref_names(self, ref: HexBytes) -> List[bytes]:
        """Given a reference, return the reference names"""
        return self.target_to_refs.get(ref, [])


class RepoRepresentation(BaseRepoRepresentation):
    """A RepoRepresentation object able to provide all refs to fetch.

    Internally, this computes the full list of wanted_refs and returns it the first time
    :meth:`determine_wants` method is called. It's expected to be called once. The
    caller has then all the necessary refs to retrieve the packfile.

    """

    def determine_wants(
        self, refs: Dict[bytes, HexBytes], depth=None
    ) -> List[HexBytes]:
        """Get the list of bytehex sha1s that the git loader should fetch.

        This compares the remote refs sent by the server with the base snapshot
        provided by the loader.

        """
        return self.compute_wanted_refs(refs)


class RepoRepresentationPaginated(BaseRepoRepresentation):
    """A RepoRepresentation objects able to provide interval of refs to fetch.

    Internally, this computes the full list of wanted_refs but then provide interval of
    number_of_heads_per_packfile refs each time :meth:`determine_wants` method is
    called. This expects the caller to call the :meth:`continue_fetch_refs` method to
    determine if more refs are needed to be fetched or not.

    """

    def __init__(
        self,
        *args,
        number_of_heads_per_packfile=DEFAULT_NUMBER_OF_HEADS_PER_PACKFILE,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        # Pagination index
        self.index: int = 0
        self.number_of_heads_per_packfile = number_of_heads_per_packfile
        self.wanted_refs: Optional[List[HexBytes]] = None
        self.asked_refs: List[HexBytes] = []

    def continue_fetch_refs(self) -> bool:
        """Determine whether we need to fetch other refs or not."""
        return self.wanted_refs is None or self.index < len(self.wanted_refs)

    def determine_wants(
        self, refs: Dict[bytes, HexBytes], depth=None
    ) -> List[HexBytes]:
        """Get the list of bytehex sha1s that the git loader should fetch.

        This compares the remote refs sent by the server with the base snapshot
        provided by the loader.

        """
        # First time around, we'll initialize all the wanted refs
        if not self.wanted_refs:
            self.wanted_refs = self.compute_wanted_refs(refs)

        # If empty, then we are done
        if not self.wanted_refs:
            return []

        # We have all wanted refs but we are ingesting them one interval of
        # number_of_heads_per_packfile refs at a time
        start = self.index
        self.index += self.number_of_heads_per_packfile

        assert self.wanted_refs
        asked_refs = self.wanted_refs[start : min(self.index, len(self.wanted_refs))]
        if heads_logger.isEnabledFor(logging.DEBUG):
            heads_logger.debug("Asked remote heads:")
            for value in asked_refs:
                heads_logger.debug("    %s", value.decode())

        if start > 0:
            # Previous refs was already walked so we can remove them from the next walk
            # iteration to avoid processing them again
            self.walker.heads.update(self.asked_refs)
        self.asked_refs = asked_refs
        logger.debug("asked_refs_count=%s", len(asked_refs))
        return asked_refs


@dataclass
class FetchPackReturn:
    remote_refs: Dict[bytes, HexBytes]
    symbolic_refs: Dict[bytes, HexBytes]
    pack_buffer: SpooledTemporaryFile
    pack_size: int
    continue_fetch_refs: bool
    """Determine whether we still have to fetch remaining references."""


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
        pack_size_bytes: int = 4 * 1024 * 1024 * 1024,
        temp_file_cutoff: int = 100 * 1024 * 1024,
        fetch_multiple_packfiles: bool = True,
        number_of_heads_per_packfile: int = DEFAULT_NUMBER_OF_HEADS_PER_PACKFILE,
        dumb: bool = False,
        **kwargs: Any,
    ):
        """Initialize the bulk updater.

        Args:
            incremental: If True, the default, this starts from the last known snapshot
                (if any) references. Otherwise, this loads the full repository.
            fetch_multiple_packfiles: If True, and the protocol used to read git refs is
              the smart protocol, this ingests the repository using (internally)
              multiple packfiles (creating partial incremental snapshots along the way).
              When False, this uses the existing ingestion policy of retrieving one
              packfile to ingest.
            number_of_heads_per_packfile: When fetch_multiple_packfiles is used, this
              splits packfiles per a given number of head references (there is no
              guarantee on the packfile size though as the refs fetched are just the
              head).

        """
        super().__init__(storage=storage, origin_url=url, **kwargs)
        # check if repository only supports git dumb transfer protocol,
        # fetched pack file will be empty in that case as dulwich do
        # not support it and do not fetch any refs
        logger.debug("Transport url to communicate with server: %s", url)
        self.client, self.path = dulwich.client.get_transport_and_path(
            url, thin_packs=False
        )
        self.dumb = dumb or (
            url.startswith("http") and getattr(self.client, "dumb", False)
        )
        logger.debug(
            "Client %s to fetch pack at %s with protocol %s",
            self.client,
            self.path,
            "dumb" if self.dumb else "smart",
        )
        self.incremental = incremental
        self.pack_size_bytes = pack_size_bytes
        self.temp_file_cutoff = temp_file_cutoff
        # state initialized in fetch_data
        self.remote_refs: Dict[bytes, HexBytes] = {}
        self.symbolic_refs: Dict[bytes, HexBytes] = {}
        self.ref_object_types: Dict[bytes, Optional[TargetType]] = {}
        self.ext_refs: Dict[bytes, Optional[Tuple[int, bytes]]] = {}
        self.configure_packfile_fetching_policy(
            fetch_multiple_packfiles, number_of_heads_per_packfile
        )

    def configure_packfile_fetching_policy(
        self, fetch_multiple_packfiles: bool, number_of_heads_per_packfile: int
    ):
        """Configure the packfile fetching policy. The default is to fetch one packfile
        to ingest everything unknown out of it. When fetch_multiple_packfiles is True
        (and the ingestion passes through the smart protocol), the ingestion uses
        packfiles (with a given number_of_heads_per_packfile). After each packfile is
        loaded, a 'partial' (because incomplete) and 'incremental' (as in gathering seen
        refs) so far snapshot is created (incremental).

        """
        # will create partial snapshot alongside fetching multiple packfiles (when the
        # transfer protocol is not the 'dumb' one)
        self.number_of_heads_per_packfile: Optional[int] = None
        self.repo_representation: Type[BaseRepoRepresentation] = RepoRepresentation

        if self.dumb:  # Ignore fetching multiple packfiles
            self.fetch_multiple_packfiles = self.create_partial_snapshot = False
        else:
            self.fetch_multiple_packfiles = fetch_multiple_packfiles
            self.create_partial_snapshot = fetch_multiple_packfiles

            if self.fetch_multiple_packfiles:
                self.number_of_heads_per_packfile = number_of_heads_per_packfile
                self.repo_representation = RepoRepresentationPaginated
                logger.debug(
                    "Client %s configured to create partial snapshot and fetch %s per packfile",
                    self.client,
                    self.number_of_heads_per_packfile,
                )

    def fetch_pack_from_origin(
        self,
        origin_url: str,
        do_activity: Callable[[bytes], None],
    ) -> FetchPackReturn:
        """Fetch a pack from the origin"""

        pack_buffer = SpooledTemporaryFile(max_size=self.temp_file_cutoff)
        size_limit = self.pack_size_bytes

        def do_pack(data: bytes) -> None:
            cur_size = pack_buffer.tell()
            would_write = len(data)
            if cur_size + would_write > size_limit:
                raise IOError(
                    f"Pack file too big for repository {origin_url}, "
                    f"number_of_heads_per_packfile is {size_limit} bytes, "
                    f"current size is {cur_size}, "
                    f"would write {would_write}"
                )

            pack_buffer.write(data)

        pack_result = self.client.fetch_pack(
            self.path,
            self.base_repo.determine_wants,
            self.base_repo.walker,
            do_pack,
            progress=do_activity,
        )

        pack_buffer.flush()
        pack_size = pack_buffer.tell()
        pack_buffer.seek(0)

        logger.debug("fetched_pack_size=%s", pack_size)

        symbolic_refs = pack_result.symrefs or {}
        if not self.fetch_multiple_packfiles:
            # No incremeental fetch, so all refs from the packfile will be ingested, we
            # take them all as is
            remote_refs = pack_result.refs or {}
        else:
            # Refs will be fetched incrementally in multiple packfiles
            # self.client.fetch_pack still returns all refs so we filter only on the
            # refs that will be currently ingested. The loader will still create a
            # partial snapshot will all the refs seen so far.
            remote_refs = {}

            assert isinstance(self.base_repo, RepoRepresentationPaginated)

            # Retrieve what's currently
            for ref in list(self.base_repo.asked_refs):
                ref_names = self.base_repo.ref_names(ref)
                for ref_name in ref_names:
                    if ref_name in pack_result.refs:
                        remote_refs[ref_name] = pack_result.refs[ref_name]

        return FetchPackReturn(
            remote_refs=remote_refs,
            symbolic_refs=symbolic_refs,
            pack_buffer=pack_buffer,
            pack_size=pack_size,
            continue_fetch_refs=self.base_repo.continue_fetch_refs(),
        )

    def get_full_snapshot(self, origin_url) -> Optional[Snapshot]:
        return snapshot_get_latest(self.storage, origin_url)

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

        self.base_repo = self.repo_representation(
            storage=self.storage,
            base_snapshots=self.base_snapshots,
            incremental=self.incremental,
            statsd=self.statsd,
            # Only used when self.repo_representation is RepoRepresentationpaginated,
            # ignored otherwise
            number_of_heads_per_packfile=self.number_of_heads_per_packfile,
        )

    def fetch_data(self) -> bool:
        continue_fetch_refs = False
        assert self.origin is not None

        try:
            fetch_info = self.fetch_pack_from_origin(self.origin.url, do_print_progress)
            continue_fetch_refs = fetch_info.continue_fetch_refs
        except (dulwich.client.HTTPUnauthorized, NotGitRepository) as e:
            raise NotFound(e)
        except GitProtocolError as e:
            # unfortunately, that kind of error is not specific to a not found
            # scenario... It depends on the value of message within the exception.
            for msg in [
                "Repository unavailable",  # e.g DMCA takedown
                "Repository not found",
                "unexpected http resp 401",
            ]:
                if msg in e.args[0]:
                    raise NotFound(e)
            # otherwise transmit the error
            raise
        except (AttributeError, NotImplementedError, ValueError):
            # with old dulwich versions, those exceptions types can be raised
            # by the fetch_pack operation when encountering a repository with
            # dumb transfer protocol so we check if the repository supports it
            # here to continue the loading if it is the case
            self.dumb = dumb.check_protocol(self.origin.url)
            if not self.dumb:
                raise

        if self.dumb:
            self.dumb_fetcher = dumb.GitObjectsFetcher(self.origin.url, self.base_repo)
            self.dumb_fetcher.fetch_object_ids()
            remote_refs = self.dumb_fetcher.refs
            symbolic_refs = self.dumb_fetcher.head
        else:
            self.pack_buffer = fetch_info.pack_buffer
            self.pack_size = fetch_info.pack_size
            remote_refs = fetch_info.remote_refs
            symbolic_refs = fetch_info.symbolic_refs

        # So the partial snapshot and the final ones creates the full branches
        self.remote_refs.update(utils.filter_refs(remote_refs))
        self.symbolic_refs.update(utils.filter_refs(symbolic_refs))

        logger.debug("incremental_remote_refs_len: %s", len(self.remote_refs))
        logger.debug("incremental_remote_symbolic_len: %s", len(self.symbolic_refs))

        for sha1 in self.remote_refs.values():
            if sha1 in self.ref_object_types:
                continue
            self.ref_object_types[sha1] = None

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

        # If we fetch multiple packfiles and we have more data to fetch, we'll create a
        # partial snapshot
        self.with_partial_snapshot = (
            self.create_partial_snapshot and continue_fetch_refs
        )
        return continue_fetch_refs

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

    def build_partial_snapshot(self) -> Optional[Snapshot]:
        # Current implementation makes it a simple call to existing :meth:`get_snapshot`
        assert self.with_partial_snapshot is True
        snapshot = self.get_snapshot()
        # Update summary stats (from base git loader)
        self.counts["snapshot"] += 1
        # HACK: This call should be happening on its own in the loader core but to
        # update stats as expected by the base git loader and not complicating further
        # the code we are calling it here.
        self.storage_summary.update(self.storage.snapshot_add([snapshot]))

        logger.debug("partial snapshot number of branches: %s", len(snapshot.branches))
        return snapshot

    def store_data(self):
        """Override the default implementation so we make sure to close the pack_buffer
        if we use one in between loop (dumb loader does not actually one for example).

        """
        super().store_data(with_final_snapshot=not self.with_partial_snapshot)

        if not self.dumb:
            self.pack_buffer.close()

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
                self.ref_object_types[raw_obj.id] = TargetType.CONTENT

            yield converters.dulwich_blob_to_content(
                raw_obj, max_content_size=self.max_content_size
            )

    def get_directories(self) -> Iterable[Directory]:
        """Format the trees as swh directories"""
        for raw_obj in self.iter_objects(b"tree"):
            if raw_obj.id in self.ref_object_types:
                self.ref_object_types[raw_obj.id] = TargetType.DIRECTORY

            yield converters.dulwich_tree_to_directory(raw_obj)

    def get_revisions(self) -> Iterable[Revision]:
        """Format commits as swh revisions"""
        for raw_obj in self.iter_objects(b"commit"):
            if raw_obj.id in self.ref_object_types:
                self.ref_object_types[raw_obj.id] = TargetType.REVISION

            yield converters.dulwich_commit_to_revision(raw_obj)

    def get_releases(self) -> Iterable[Release]:
        """Retrieve all the release objects from the git repository"""
        for raw_obj in self.iter_objects(b"tag"):
            if raw_obj.id in self.ref_object_types:
                self.ref_object_types[raw_obj.id] = TargetType.RELEASE

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
                target_type=TargetType.ALIAS,
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
                if branch and branch.target_type != TargetType.ALIAS
            }
            assert all(
                base_snapshot_reverse_branches[branch.target] == branch
                for branch in self.prev_snapshot.branches.values()
                if branch and branch.target_type != TargetType.ALIAS
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
                    (self.storage.revision_missing, TargetType.REVISION),
                    (self.storage.release_missing, TargetType.RELEASE),
                    (self.storage.directory_missing, TargetType.DIRECTORY),
                    (self.storage.content_missing_per_sha1_git, TargetType.CONTENT),
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

            if unknown_objects and not self.create_partial_snapshot:
                # Let's warn about dangling object when loading full packfiles. The
                # following objects were referenced by the server but we did not fetch
                # it, and we do not know it from the previous snapshot.
                logger.warning(
                    "Unknown objects referenced by remote refs: %s",
                    (
                        ", ".join(
                            f"{name!r}: {hashutil.hash_to_hex(obj)}"
                            for name, obj in unknown_objects.items()
                        )
                    ),
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
