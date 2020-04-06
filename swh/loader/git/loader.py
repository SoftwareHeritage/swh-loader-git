# Copyright (C) 2016-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import datetime
from io import BytesIO
import logging
import os
import pickle
import sys
from typing import Any, Dict, Iterable, List, Optional, Union

import dulwich.client
from dulwich.object_store import ObjectStoreGraphWalker
from dulwich.pack import PackData, PackInflater

from swh.model import hashutil
from swh.model.model import (
    BaseContent, Directory, Origin, Revision,
    Release, Snapshot, SnapshotBranch, TargetType, Sha1Git)
from swh.loader.core.loader import DVCSLoader
from swh.storage.algos.snapshot import snapshot_get_all_branches

from . import converters


class RepoRepresentation:
    """Repository representation for a Software Heritage origin."""
    def __init__(self, storage, base_snapshot=None,
                 ignore_history=False):
        self.storage = storage

        self._parents_cache = {}
        self._type_cache: Dict[bytes, TargetType] = {}

        self.ignore_history = ignore_history

        if base_snapshot and not ignore_history:
            self.heads = set(self._cache_heads(base_snapshot))
        else:
            self.heads = set()

    def _fill_parents_cache(self, commits):
        """When querying for a commit's parents, we fill the cache to a depth of 1000
        commits."""
        root_revs = self._encode_for_storage(commits)
        for rev, parents in self.storage.revision_shortlog(root_revs, 1000):
            rev_id = hashutil.hash_to_bytehex(rev)
            if rev_id not in self._parents_cache:
                self._parents_cache[rev_id] = [
                    hashutil.hash_to_bytehex(parent) for parent in parents
                ]
        for rev in commits:
            if rev not in self._parents_cache:
                self._parents_cache[rev] = []

    def _cache_heads(self, base_snapshot):
        """Return all the known head commits for the given snapshot"""
        _git_types = list(converters.DULWICH_TARGET_TYPES.values())

        if not base_snapshot:
            return []

        snapshot_targets = set()
        for branch in base_snapshot.branches.values():
            if branch and branch.target_type in _git_types:
                snapshot_targets.add(branch.target)

        decoded_targets = self._decode_from_storage(snapshot_targets)

        for id, objs in self.get_stored_objects(decoded_targets).items():
            if not objs:
                logging.warn('Missing head: %s' % hashutil.hash_to_hex(id))
                return []

        return decoded_targets

    def get_parents(self, commit):
        """Bogus method to prevent expensive recursion, at the expense of less
        efficient downloading"""
        return []

    def get_heads(self):
        return self.heads

    @staticmethod
    def _encode_for_storage(objects):
        return [hashutil.bytehex_to_hash(object) for object in objects]

    @staticmethod
    def _decode_from_storage(objects):
        return set(hashutil.hash_to_bytehex(object) for object in objects)

    def graph_walker(self):
        return ObjectStoreGraphWalker(self.get_heads(), self.get_parents)

    @staticmethod
    def filter_unwanted_refs(refs):
        """Filter the unwanted references from refs"""
        ret = {}
        for ref, val in refs.items():
            if ref.endswith(b'^{}'):
                # Peeled refs make the git protocol explode
                continue
            elif ref.startswith(b'refs/pull/') and ref.endswith(b'/merge'):
                # We filter-out auto-merged GitHub pull requests
                continue
            else:
                ret[ref] = val

        return ret

    def determine_wants(self, refs):
        """Filter the remote references to figure out which ones
        Software Heritage needs.
        """
        if not refs:
            return []

        # Find what objects Software Heritage has
        refs = self.find_remote_ref_types_in_swh(refs)

        # Cache the objects found in swh as existing heads
        for target in refs.values():
            if target['target_type'] is not None:
                self.heads.add(target['target'])

        ret = set()
        for target in self.filter_unwanted_refs(refs).values():
            if target['target_type'] is None:
                # The target doesn't exist in Software Heritage, let's retrieve
                # it.
                ret.add(target['target'])

        return list(ret)

    def _get_stored_objects_batch(
            self, query
            ) -> Dict[bytes, List[Dict[str, Union[bytes, TargetType]]]]:
        results = self.storage.object_find_by_sha1_git(
            self._encode_for_storage(query)
        )
        ret: Dict[bytes, List[Dict[str, Union[bytes, TargetType]]]] = {}
        for (id, objects) in results.items():
            assert id not in ret
            ret[id] = [
                {
                    'sha1_git': obj['sha1_git'],
                    'type': TargetType(obj['type']),
                }
                for obj in objects
            ]
        return ret

    def get_stored_objects(
            self, objects
            ) -> Dict[bytes, List[Dict[str, Union[bytes, TargetType]]]]:
        """Find which of these objects were stored in the archive.

        Do the request in packets to avoid a server timeout.
        """
        if self.ignore_history:
            return {}

        packet_size = 1000

        ret: Dict[bytes, List[Dict[str, Union[bytes, TargetType]]]] = {}
        query = []
        for object in objects:
            query.append(object)
            if len(query) >= packet_size:
                ret.update(self._get_stored_objects_batch(query))
                query = []
        if query:
            ret.update(self._get_stored_objects_batch(query))
        return ret

    def find_remote_ref_types_in_swh(
            self, remote_refs) -> Dict[bytes, Dict[str, Any]]:
        """Parse the remote refs information and list the objects that exist in
        Software Heritage.

        Returns:
            dict whose keys are branch names, and values are tuples
            `(target, target_type)`.
        """

        all_objs = set(remote_refs.values()) - set(self._type_cache)
        type_by_id: Dict[bytes, TargetType] = {}

        for id, objs in self.get_stored_objects(all_objs).items():
            id = hashutil.hash_to_bytehex(id)
            if objs:
                type_ = objs[0]['type']
                assert isinstance(type_, TargetType)
                type_by_id[id] = type_

        self._type_cache.update(type_by_id)

        ret = {}
        for ref, id in remote_refs.items():
            ret[ref] = {
                'target': id,
                'target_type': self._type_cache.get(id),
            }
        return ret


class GitLoader(DVCSLoader):
    """A bulk loader for a git repository"""
    CONFIG_BASE_FILENAME = 'loader/git'

    ADDITIONAL_CONFIG = {
        'pack_size_bytes': ('int', 4 * 1024 * 1024 * 1024),
    }

    visit_type = 'git'

    def __init__(self, url, base_url=None, ignore_history=False,
                 repo_representation=RepoRepresentation, config=None):
        """Initialize the bulk updater.

        Args:
            repo_representation: swh's repository representation
            which is in charge of filtering between known and remote
            data.

        """
        super().__init__(logging_class='swh.loader.git.BulkLoader',
                         config=config)
        self.origin_url = url
        self.base_url = base_url
        self.ignore_history = ignore_history
        self.repo_representation = repo_representation

    def fetch_pack_from_origin(self, origin_url,
                               base_snapshot, do_activity):
        """Fetch a pack from the origin"""
        pack_buffer = BytesIO()

        base_repo = self.repo_representation(
            storage=self.storage,
            base_snapshot=base_snapshot,
            ignore_history=self.ignore_history,
        )

        client, path = dulwich.client.get_transport_and_path(origin_url,
                                                             thin_packs=False)

        size_limit = self.config['pack_size_bytes']

        def do_pack(data):
            cur_size = pack_buffer.tell()
            would_write = len(data)
            if cur_size + would_write > size_limit:
                raise IOError('Pack file too big for repository %s, '
                              'limit is %d bytes, current size is %d, '
                              'would write %d' %
                              (origin_url, size_limit, cur_size, would_write))

            pack_buffer.write(data)

        pack_result = client.fetch_pack(path,
                                        base_repo.determine_wants,
                                        base_repo.graph_walker(),
                                        do_pack,
                                        progress=do_activity)

        remote_refs = pack_result.refs
        symbolic_refs = pack_result.symrefs

        if remote_refs:
            local_refs = base_repo.find_remote_ref_types_in_swh(remote_refs)
        else:
            local_refs = remote_refs = {}

        pack_buffer.flush()
        pack_size = pack_buffer.tell()
        pack_buffer.seek(0)

        return {
            'remote_refs': base_repo.filter_unwanted_refs(remote_refs),
            'local_refs': local_refs,
            'symbolic_refs': symbolic_refs,
            'pack_buffer': pack_buffer,
            'pack_size': pack_size,
        }

    def list_pack(self, pack_data, pack_size):
        id_to_type = {}
        type_to_ids = defaultdict(set)

        inflater = self.get_inflater()

        for obj in inflater:
            type, id = obj.type_name, obj.id
            id_to_type[id] = type
            type_to_ids[type].add(id)

        return id_to_type, type_to_ids

    def prepare_origin_visit(self, *args, **kwargs):
        self.visit_date = datetime.datetime.now(tz=datetime.timezone.utc)
        self.origin = Origin(url=self.origin_url)

    def get_full_snapshot(self, origin_url) -> Optional[Snapshot]:
        visit = self.storage.origin_visit_get_latest(
            origin_url, require_snapshot=True)
        if visit and visit['snapshot']:
            snapshot = snapshot_get_all_branches(
                self.storage, visit['snapshot'])
        else:
            snapshot = None
        if snapshot is None:
            return None
        return Snapshot.from_dict(snapshot)

    def prepare(self, *args, **kwargs):
        base_origin_url = origin_url = self.origin.url

        prev_snapshot = None

        if not self.ignore_history:
            prev_snapshot = self.get_full_snapshot(origin_url)

        if self.base_url and not prev_snapshot:
            base_origin = Origin(url=self.base_url)
            base_origin = self.storage.origin_get(base_origin)
            if base_origin:
                base_origin_url = base_origin['url']
                prev_snapshot = self.get_full_snapshot(base_origin_url)

        self.base_snapshot = prev_snapshot
        self.base_origin_url = base_origin_url

    def fetch_data(self):
        def do_progress(msg):
            sys.stderr.buffer.write(msg)
            sys.stderr.flush()

        fetch_info = self.fetch_pack_from_origin(
            self.origin.url, self.base_snapshot,
            do_progress)

        self.pack_buffer = fetch_info['pack_buffer']
        self.pack_size = fetch_info['pack_size']

        self.remote_refs = fetch_info['remote_refs']
        self.local_refs = fetch_info['local_refs']
        self.symbolic_refs = fetch_info['symbolic_refs']

        origin_url = self.origin.url

        self.log.info('Listed %d refs for repo %s' % (
            len(self.remote_refs), origin_url), extra={
                'swh_type': 'git_repo_list_refs',
                'swh_repo': origin_url,
                'swh_num_refs': len(self.remote_refs),
            })

        # We want to load the repository, walk all the objects
        id_to_type, type_to_ids = self.list_pack(self.pack_buffer,
                                                 self.pack_size)

        self.id_to_type = id_to_type
        self.type_to_ids = type_to_ids

    def save_data(self):
        """Store a pack for archival"""

        write_size = 8192
        pack_dir = self.get_save_data_path()

        pack_name = "%s.pack" % self.visit_date.isoformat()
        refs_name = "%s.refs" % self.visit_date.isoformat()

        with open(os.path.join(pack_dir, pack_name), 'xb') as f:
            self.pack_buffer.seek(0)
            while True:
                r = self.pack_buffer.read(write_size)
                if not r:
                    break
                f.write(r)

        self.pack_buffer.seek(0)

        with open(os.path.join(pack_dir, refs_name), 'xb') as f:
            pickle.dump(self.remote_refs, f)

    def get_inflater(self):
        """Reset the pack buffer and get an object inflater from it"""
        self.pack_buffer.seek(0)
        return PackInflater.for_pack_data(
            PackData.from_file(self.pack_buffer, self.pack_size))

    def has_contents(self):
        return bool(self.type_to_ids[b'blob'])

    def get_content_ids(self) -> Iterable[Dict[str, Any]]:
        """Get the content identifiers from the git repository"""
        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b'blob':
                continue

            yield converters.dulwich_blob_to_content_id(raw_obj)

    def get_contents(self) -> Iterable[BaseContent]:
        """Format the blobs from the git repository as swh contents"""
        missing_contents = set(self.storage.content_missing(
            self.get_content_ids(), 'sha1_git'))

        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b'blob':
                continue

            if raw_obj.sha().digest() not in missing_contents:
                continue

            yield converters.dulwich_blob_to_content(
                raw_obj, max_content_size=self.max_content_size)

    def has_directories(self) -> bool:
        return bool(self.type_to_ids[b'tree'])

    def get_directory_ids(self) -> Iterable[Sha1Git]:
        """Get the directory identifiers from the git repository"""
        return (hashutil.hash_to_bytes(id.decode())
                for id in self.type_to_ids[b'tree'])

    def get_directories(self) -> Iterable[Directory]:
        """Format the trees as swh directories"""
        missing_dirs = set(self.storage.directory_missing(
            sorted(self.get_directory_ids())))

        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b'tree':
                continue

            if raw_obj.sha().digest() not in missing_dirs:
                continue

            yield converters.dulwich_tree_to_directory(raw_obj, log=self.log)

    def has_revisions(self) -> bool:
        return bool(self.type_to_ids[b'commit'])

    def get_revision_ids(self) -> Iterable[Sha1Git]:
        """Get the revision identifiers from the git repository"""
        return (hashutil.hash_to_bytes(id.decode())
                for id in self.type_to_ids[b'commit'])

    def get_revisions(self) -> Iterable[Revision]:
        """Format commits as swh revisions"""
        missing_revs = set(self.storage.revision_missing(
            sorted(self.get_revision_ids())))

        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b'commit':
                continue

            if raw_obj.sha().digest() not in missing_revs:
                continue

            yield converters.dulwich_commit_to_revision(raw_obj, log=self.log)

    def has_releases(self) -> bool:
        return bool(self.type_to_ids[b'tag'])

    def get_release_ids(self) -> Iterable[Sha1Git]:
        """Get the release identifiers from the git repository"""
        return (hashutil.hash_to_bytes(id.decode())
                for id in self.type_to_ids[b'tag'])

    def get_releases(self) -> Iterable[Release]:
        """Retrieve all the release objects from the git repository"""
        missing_rels = set(self.storage.release_missing(
            sorted(self.get_release_ids())))

        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b'tag':
                continue

            if raw_obj.sha().digest() not in missing_rels:
                continue

            yield converters.dulwich_tag_to_release(raw_obj, log=self.log)

    def get_snapshot(self) -> Snapshot:
        branches: Dict[bytes, Optional[SnapshotBranch]] = {}

        for ref in self.remote_refs:
            ret_ref = self.local_refs[ref].copy()
            if not ret_ref['target_type']:
                target_type = self.id_to_type[ret_ref['target']]
                ret_ref['target_type'] = \
                    converters.DULWICH_TARGET_TYPES[target_type]

            ret_ref['target'] = hashutil.bytehex_to_hash(ret_ref['target'])

            branches[ref] = SnapshotBranch(
                target_type=ret_ref['target_type'],
                target=ret_ref['target'],
            )

        for ref, target in self.symbolic_refs.items():
            branches[ref] = SnapshotBranch(
                target_type=TargetType.ALIAS,
                target=target,
            )

        self.snapshot = Snapshot(branches=branches)
        return self.snapshot

    def get_fetch_history_result(self) -> Dict[str, int]:
        return {
            'contents': len(self.type_to_ids[b'blob']),
            'directories': len(self.type_to_ids[b'tree']),
            'revisions': len(self.type_to_ids[b'commit']),
            'releases': len(self.type_to_ids[b'tag']),
        }

    def load_status(self) -> Dict[str, Any]:
        """The load was eventful if the current snapshot is different to
           the one we retrieved at the beginning of the run"""
        eventful = False

        if self.base_snapshot:
            eventful = self.snapshot.id != self.base_snapshot.id
        else:
            eventful = bool(self.snapshot.branches)

        return {'status': ('eventful' if eventful else 'uneventful')}


if __name__ == '__main__':
    import click

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(process)d %(message)s'
    )

    @click.command()
    @click.option('--origin-url', help='Origin url', required=True)
    @click.option('--base-url', default=None, help='Optional Base url')
    @click.option('--ignore-history/--no-ignore-history',
                  help='Ignore the repository history', default=False)
    def main(origin_url, base_url, ignore_history):
        loader = GitLoader(
            origin_url,
            base_url=base_url,
            ignore_history=ignore_history,
        )
        return loader.load()

    main()
