# Copyright (C) 2016-2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from dataclasses import dataclass
import datetime
import logging
import os
import pickle
import sys
from tempfile import SpooledTemporaryFile
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Set, Type

import dulwich.client
from dulwich.errors import GitProtocolError, NotGitRepository
from dulwich.object_store import ObjectStoreGraphWalker
from dulwich.objects import ShaFile
from dulwich.pack import PackData, PackInflater

from swh.core.statsd import Statsd
from swh.loader.exception import NotFound
from swh.model import hashutil
from swh.model.model import (
    BaseContent,
    Directory,
    Release,
    Revision,
    Snapshot,
    SnapshotBranch,
    TargetType,
)
from swh.storage.algos.snapshot import snapshot_get_latest
from swh.storage.interface import StorageInterface

from . import converters, dumb, utils
from .base import BaseGitLoader
from .utils import HexBytes

logger = logging.getLogger(__name__)


class RepoRepresentation:
    """Repository representation for a Software Heritage origin."""

    def __init__(
        self,
        storage,
        base_snapshots: List[Snapshot] = None,
        incremental: bool = True,
        statsd: Statsd = None,
    ):
        self.storage = storage
        self.incremental = incremental
        self.statsd = statsd

        if base_snapshots and incremental:
            self.base_snapshots: List[Snapshot] = base_snapshots
        else:
            self.base_snapshots = []

        self.heads: Set[HexBytes] = set()

    def get_parents(self, commit: bytes) -> List[bytes]:
        """This method should return the list of known parents"""
        return []

    def graph_walker(self) -> ObjectStoreGraphWalker:
        return ObjectStoreGraphWalker(self.heads, self.get_parents)

    def determine_wants(self, refs: Dict[bytes, HexBytes]) -> List[HexBytes]:
        """Get the list of bytehex sha1s that the git loader should fetch.

        This compares the remote refs sent by the server with the base snapshot
        provided by the loader.

        """
        if not refs:
            return []

        # Cache existing heads
        local_heads: Set[HexBytes] = set()
        for base_snapshot in self.base_snapshots:
            for branch_name, branch in base_snapshot.branches.items():
                if not branch or branch.target_type == TargetType.ALIAS:
                    continue
                local_heads.add(HexBytes(hashutil.hash_to_bytehex(branch.target)))

        self.heads = local_heads

        # Get the remote heads that we want to fetch
        remote_heads: Set[HexBytes] = set()
        for ref_name, ref_target in refs.items():
            if utils.ignore_branch_name(ref_name):
                continue
            remote_heads.add(ref_target)

        logger.debug("local_heads_count=%s", len(local_heads))
        logger.debug("remote_heads_count=%s", len(remote_heads))
        wanted_refs = list(remote_heads - local_heads)
        logger.debug("wanted_refs_count=%s", len(wanted_refs))
        if self.statsd is not None:
            self.statsd.histogram(
                "git_ignored_refs_percent",
                len(remote_heads - set(refs.values())) / len(refs),
                tags={},
            )
            self.statsd.histogram(
                "git_known_refs_percent",
                len(local_heads & remote_heads) / len(remote_heads),
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
        self.ref_object_types: Dict[bytes, Optional[TargetType]] = {}

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

        client, path = dulwich.client.get_transport_and_path(
            transport_url, thin_packs=False
        )

        logger.debug("Client %s to fetch pack at %s", client, path)

        size_limit = self.pack_size_bytes

        def do_pack(data: bytes) -> None:
            cur_size = pack_buffer.tell()
            would_write = len(data)
            if cur_size + would_write > size_limit:
                raise IOError(
                    f"Pack file too big for repository {origin_url}, "
                    f"limit is {size_limit} bytes, current size is {cur_size}, "
                    f"would write {would_write}"
                )

            pack_buffer.write(data)

        pack_result = client.fetch_pack(
            path,
            base_repo.determine_wants,
            base_repo.graph_walker(),
            do_pack,
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

    def fetch_data(self) -> bool:
        assert self.origin is not None

        base_repo = self.repo_representation(
            storage=self.storage,
            base_snapshots=self.base_snapshots,
            incremental=self.incremental,
            statsd=self.statsd,
        )

        def do_progress(msg: bytes) -> None:
            sys.stderr.buffer.write(msg)
            sys.stderr.flush()

        try:
            fetch_info = self.fetch_pack_from_origin(
                self.origin.url, base_repo, do_progress
            )
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

        logger.debug(
            "Protocol used for communication: %s", "dumb" if self.dumb else "smart"
        )
        if self.dumb:
            self.dumb_fetcher = dumb.GitObjectsFetcher(self.origin.url, base_repo)
            self.dumb_fetcher.fetch_object_ids()
            self.remote_refs = utils.filter_refs(self.dumb_fetcher.refs)  # type: ignore
            self.symbolic_refs = self.dumb_fetcher.head
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

    def iter_objects(self, object_type: bytes) -> Iterator[ShaFile]:
        """Read all the objects of type `object_type` from the packfile"""
        if self.dumb:
            yield from self.dumb_fetcher.iter_objects(object_type)
        else:
            self.pack_buffer.seek(0)
            count = 0
            for obj in PackInflater.for_pack_data(
                PackData.from_file(self.pack_buffer, self.pack_size)
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

            if unknown_objects:
                # This object was referenced by the server; We did not fetch
                # it, and we do not know it from the previous snapshot. This is
                # likely a bug in the loader.
                raise RuntimeError(
                    "Unknown objects referenced by remote refs: %s"
                    % (
                        ", ".join(
                            f"{name.decode()}: {hashutil.hash_to_hex(obj)}"
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
