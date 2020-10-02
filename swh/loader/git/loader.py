# Copyright (C) 2016-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
from dataclasses import dataclass
import datetime
from io import BytesIO
import logging
import os
import pickle
import sys
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Type

import dulwich.client
from dulwich.object_store import ObjectStoreGraphWalker
from dulwich.pack import PackData, PackInflater

from swh.core.config import merge_configs
from swh.loader.core.loader import DVCSLoader
from swh.model import hashutil
from swh.model.model import (
    BaseContent,
    Directory,
    Origin,
    Release,
    Revision,
    Sha1Git,
    Snapshot,
    SnapshotBranch,
    TargetType,
)
from swh.storage.algos.snapshot import snapshot_get_latest

from . import converters, utils


class RepoRepresentation:
    """Repository representation for a Software Heritage origin."""

    def __init__(
        self, storage, base_snapshot: Optional[Snapshot] = None, ignore_history=False
    ):
        self.storage = storage
        self.ignore_history = ignore_history

        if base_snapshot and not ignore_history:
            self.base_snapshot: Snapshot = base_snapshot
        else:
            self.base_snapshot = Snapshot(branches={})

        self.heads: Set[bytes] = set()

    def get_parents(self, commit: bytes) -> List[bytes]:
        """This method should return the list of known parents"""
        return []

    def graph_walker(self) -> ObjectStoreGraphWalker:
        return ObjectStoreGraphWalker(self.heads, self.get_parents)

    def determine_wants(self, refs: Dict[bytes, bytes]) -> List[bytes]:
        """Get the list of bytehex sha1s that the git loader should fetch.

        This compares the remote refs sent by the server with the base snapshot
        provided by the loader.

        """
        if not refs:
            return []

        # Cache existing heads
        local_heads: Set[bytes] = set()
        for branch_name, branch in self.base_snapshot.branches.items():
            if not branch or branch.target_type == TargetType.ALIAS:
                continue
            local_heads.add(hashutil.hash_to_hex(branch.target).encode())

        self.heads = local_heads

        # Get the remote heads that we want to fetch
        remote_heads: Set[bytes] = set()
        for ref_name, ref_target in refs.items():
            if utils.ignore_branch_name(ref_name):
                continue
            remote_heads.add(ref_target)

        return list(remote_heads - local_heads)


@dataclass
class FetchPackReturn:
    remote_refs: Dict[bytes, bytes]
    symbolic_refs: Dict[bytes, bytes]
    pack_buffer: BytesIO
    pack_size: int


DEFAULT_CONFIG: Dict[str, Any] = {
    "pack_size_bytes": 4 * 1024 * 1024 * 1024,
}


class GitLoader(DVCSLoader):
    """A bulk loader for a git repository"""

    visit_type = "git"

    def __init__(
        self,
        url: str,
        base_url: Optional[str] = None,
        ignore_history: bool = False,
        repo_representation: Type[RepoRepresentation] = RepoRepresentation,
        config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the bulk updater.

        Args:
            repo_representation: swh's repository representation
            which is in charge of filtering between known and remote
            data.

        """
        super().__init__(logging_class="swh.loader.git.BulkLoader", config=config)
        self.config = merge_configs(DEFAULT_CONFIG, self.config)
        self.origin_url = url
        self.base_url = base_url
        self.ignore_history = ignore_history
        self.repo_representation = repo_representation

        # state initialized in fetch_data
        self.remote_refs: Dict[bytes, bytes] = {}
        self.symbolic_refs: Dict[bytes, bytes] = {}

    def fetch_pack_from_origin(
        self,
        origin_url: str,
        base_snapshot: Optional[Snapshot],
        do_activity: Callable[[bytes], None],
    ) -> FetchPackReturn:
        """Fetch a pack from the origin"""
        pack_buffer = BytesIO()

        base_repo = self.repo_representation(
            storage=self.storage,
            base_snapshot=base_snapshot,
            ignore_history=self.ignore_history,
        )

        client, path = dulwich.client.get_transport_and_path(
            origin_url, thin_packs=False
        )

        size_limit = self.config["pack_size_bytes"]

        def do_pack(data: bytes) -> None:
            cur_size = pack_buffer.tell()
            would_write = len(data)
            if cur_size + would_write > size_limit:
                raise IOError(
                    "Pack file too big for repository %s, "
                    "limit is %d bytes, current size is %d, "
                    "would write %d" % (origin_url, size_limit, cur_size, would_write)
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

        return FetchPackReturn(
            remote_refs=utils.filter_refs(remote_refs),
            symbolic_refs=utils.filter_refs(symbolic_refs),
            pack_buffer=pack_buffer,
            pack_size=pack_size,
        )

    def list_pack(
        self, pack_data, pack_size
    ) -> Tuple[Dict[bytes, bytes], Dict[bytes, Set[bytes]]]:
        id_to_type = {}
        type_to_ids = defaultdict(set)

        inflater = self.get_inflater()

        for obj in inflater:
            type, id = obj.type_name, obj.id
            id_to_type[id] = type
            type_to_ids[type].add(id)

        return id_to_type, type_to_ids

    def prepare_origin_visit(self, *args, **kwargs) -> None:
        self.visit_date = datetime.datetime.now(tz=datetime.timezone.utc)
        self.origin = Origin(url=self.origin_url)

    def get_full_snapshot(self, origin_url) -> Optional[Snapshot]:
        return snapshot_get_latest(self.storage, origin_url)

    def prepare(self, *args, **kwargs) -> None:
        assert self.origin is not None

        prev_snapshot: Optional[Snapshot] = None

        if not self.ignore_history:
            prev_snapshot = self.get_full_snapshot(self.origin.url)

        if self.base_url and prev_snapshot is None:
            base_origin = list(self.storage.origin_get([self.base_url]))[0]
            if base_origin:
                prev_snapshot = self.get_full_snapshot(base_origin.url)

        if prev_snapshot is not None:
            self.base_snapshot = prev_snapshot
        else:
            self.base_snapshot = Snapshot(branches={})

    def fetch_data(self) -> bool:
        assert self.origin is not None

        def do_progress(msg: bytes) -> None:
            sys.stderr.buffer.write(msg)
            sys.stderr.flush()

        fetch_info = self.fetch_pack_from_origin(
            self.origin.url, self.base_snapshot, do_progress
        )

        self.pack_buffer = fetch_info.pack_buffer
        self.pack_size = fetch_info.pack_size

        self.remote_refs = fetch_info.remote_refs
        self.symbolic_refs = fetch_info.symbolic_refs

        self.log.info(
            "Listed %d refs for repo %s" % (len(self.remote_refs), self.origin.url),
            extra={
                "swh_type": "git_repo_list_refs",
                "swh_repo": self.origin.url,
                "swh_num_refs": len(self.remote_refs),
            },
        )

        # We want to load the repository, walk all the objects
        id_to_type, type_to_ids = self.list_pack(self.pack_buffer, self.pack_size)

        self.id_to_type = id_to_type
        self.type_to_ids = type_to_ids

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

    def get_inflater(self) -> PackInflater:
        """Reset the pack buffer and get an object inflater from it"""
        self.pack_buffer.seek(0)
        return PackInflater.for_pack_data(
            PackData.from_file(self.pack_buffer, self.pack_size)
        )

    def has_contents(self) -> bool:
        return bool(self.type_to_ids[b"blob"])

    def get_content_ids(self) -> Iterable[Dict[str, Any]]:
        """Get the content identifiers from the git repository"""
        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b"blob":
                continue

            yield converters.dulwich_blob_to_content_id(raw_obj)

    def get_contents(self) -> Iterable[BaseContent]:
        """Format the blobs from the git repository as swh contents"""
        missing_contents = set(
            self.storage.content_missing(list(self.get_content_ids()), "sha1_git")
        )

        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b"blob":
                continue

            if raw_obj.sha().digest() not in missing_contents:
                continue

            yield converters.dulwich_blob_to_content(
                raw_obj, max_content_size=self.max_content_size
            )

    def has_directories(self) -> bool:
        return bool(self.type_to_ids[b"tree"])

    def get_directory_ids(self) -> Iterable[Sha1Git]:
        """Get the directory identifiers from the git repository"""
        return (hashutil.hash_to_bytes(id.decode()) for id in self.type_to_ids[b"tree"])

    def get_directories(self) -> Iterable[Directory]:
        """Format the trees as swh directories"""
        missing_dirs = set(
            self.storage.directory_missing(sorted(self.get_directory_ids()))
        )

        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b"tree":
                continue

            if raw_obj.sha().digest() not in missing_dirs:
                continue

            yield converters.dulwich_tree_to_directory(raw_obj, log=self.log)

    def has_revisions(self) -> bool:
        return bool(self.type_to_ids[b"commit"])

    def get_revision_ids(self) -> Iterable[Sha1Git]:
        """Get the revision identifiers from the git repository"""
        return (
            hashutil.hash_to_bytes(id.decode()) for id in self.type_to_ids[b"commit"]
        )

    def get_revisions(self) -> Iterable[Revision]:
        """Format commits as swh revisions"""
        missing_revs = set(
            self.storage.revision_missing(sorted(self.get_revision_ids()))
        )

        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b"commit":
                continue

            if raw_obj.sha().digest() not in missing_revs:
                continue

            yield converters.dulwich_commit_to_revision(raw_obj, log=self.log)

    def has_releases(self) -> bool:
        return bool(self.type_to_ids[b"tag"])

    def get_release_ids(self) -> Iterable[Sha1Git]:
        """Get the release identifiers from the git repository"""
        return (hashutil.hash_to_bytes(id.decode()) for id in self.type_to_ids[b"tag"])

    def get_releases(self) -> Iterable[Release]:
        """Retrieve all the release objects from the git repository"""
        missing_rels = set(self.storage.release_missing(sorted(self.get_release_ids())))

        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b"tag":
                continue

            if raw_obj.sha().digest() not in missing_rels:
                continue

            yield converters.dulwich_tag_to_release(raw_obj, log=self.log)

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
            object_type = self.id_to_type.get(ref_object)
            if object_type:
                branches[ref_name] = SnapshotBranch(
                    target=target,
                    target_type=converters.DULWICH_TARGET_TYPES[object_type],
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
                target_type=TargetType.ALIAS, target=target,
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
                for branch in self.base_snapshot.branches.values()
                if branch and branch.target_type != TargetType.ALIAS
            }

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
            branches, dangling_branches, self.log, self.origin_url
        )

        self.snapshot = Snapshot(branches=branches)
        return self.snapshot

    def get_fetch_history_result(self) -> Dict[str, int]:
        return {
            "contents": len(self.type_to_ids[b"blob"]),
            "directories": len(self.type_to_ids[b"tree"]),
            "revisions": len(self.type_to_ids[b"commit"]),
            "releases": len(self.type_to_ids[b"tag"]),
        }

    def load_status(self) -> Dict[str, Any]:
        """The load was eventful if the current snapshot is different to
           the one we retrieved at the beginning of the run"""
        eventful = False

        if self.base_snapshot and self.snapshot:
            eventful = self.snapshot.id != self.base_snapshot.id
        elif self.snapshot:
            eventful = bool(self.snapshot.branches)

        return {"status": ("eventful" if eventful else "uneventful")}


if __name__ == "__main__":
    import click

    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s %(process)d %(message)s"
    )

    @click.command()
    @click.option("--origin-url", help="Origin url", required=True)
    @click.option("--base-url", default=None, help="Optional Base url")
    @click.option(
        "--ignore-history/--no-ignore-history",
        help="Ignore the repository history",
        default=False,
    )
    def main(origin_url: str, base_url: str, ignore_history: bool) -> Dict[str, Any]:
        loader = GitLoader(
            origin_url, base_url=base_url, ignore_history=ignore_history,
        )
        return loader.load()

    main()
