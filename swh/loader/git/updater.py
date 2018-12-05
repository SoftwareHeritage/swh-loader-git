# Copyright (C) 2016-2018 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import dulwich.client
import logging
import os
import pickle
import sys

from collections import defaultdict
from io import BytesIO
from dulwich.object_store import ObjectStoreGraphWalker
from dulwich.pack import PackData, PackInflater

from swh.model import hashutil
from swh.loader.core.loader import UnbufferedLoader
from swh.storage.algos.snapshot import snapshot_get_all_branches
from . import converters


class SWHRepoRepresentation:
    """Repository representation for a Software Heritage origin."""
    def __init__(self, storage, origin_id, base_snapshot=None,
                 ignore_history=False):
        self.storage = storage

        self._parents_cache = {}
        self._type_cache = {}

        self.ignore_history = ignore_history

        if origin_id and not ignore_history:
            self.heads = set(self._cache_heads(origin_id, base_snapshot))
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

    def _cache_heads(self, origin_id, base_snapshot):
        """Return all the known head commits for `origin_id`"""
        _git_types = ['content', 'directory', 'revision', 'release']

        if not base_snapshot:
            return []

        snapshot_targets = set()
        for target in base_snapshot['branches'].values():
            if target and target['target_type'] in _git_types:
                snapshot_targets.add(target['target'])

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

    def get_stored_objects(self, objects):
        """Find which of these objects were stored in the archive.

        Do the request in packets to avoid a server timeout.
        """
        if self.ignore_history:
            return {}

        packet_size = 1000

        ret = {}
        query = []
        for object in objects:
            query.append(object)
            if len(query) >= packet_size:
                ret.update(
                    self.storage.object_find_by_sha1_git(
                        self._encode_for_storage(query)
                    )
                )
                query = []
        if query:
            ret.update(
                self.storage.object_find_by_sha1_git(
                    self._encode_for_storage(query)
                )
            )
        return ret

    def find_remote_ref_types_in_swh(self, remote_refs):
        """Parse the remote refs information and list the objects that exist in
        Software Heritage.
        """

        all_objs = set(remote_refs.values()) - set(self._type_cache)
        type_by_id = {}

        for id, objs in self.get_stored_objects(all_objs).items():
            id = hashutil.hash_to_bytehex(id)
            if objs:
                type_by_id[id] = objs[0]['type']

        self._type_cache.update(type_by_id)

        ret = {}
        for ref, id in remote_refs.items():
            ret[ref] = {
                'target': id,
                'target_type': self._type_cache.get(id),
            }
        return ret


class BulkUpdater(UnbufferedLoader):
    """A bulk loader for a git repository"""
    CONFIG_BASE_FILENAME = 'loader/git-updater'

    ADDITIONAL_CONFIG = {
        'pack_size_bytes': ('int', 4 * 1024 * 1024 * 1024),
    }

    def __init__(self, repo_representation=SWHRepoRepresentation, config=None):
        """Initialize the bulk updater.

        Args:
            repo_representation: swh's repository representation
            which is in charge of filtering between known and remote
            data.

        """
        super().__init__(logging_class='swh.loader.git.BulkLoader',
                         config=config)
        self.repo_representation = repo_representation

    def fetch_pack_from_origin(self, origin_url, base_origin_id,
                               base_snapshot, do_activity):
        """Fetch a pack from the origin"""
        pack_buffer = BytesIO()

        base_repo = self.repo_representation(
            storage=self.storage,
            origin_id=base_origin_id,
            base_snapshot=base_snapshot,
            ignore_history=self.ignore_history,
        )

        client, path = dulwich.client.get_transport_and_path(origin_url,
                                                             thin_packs=False)

        size_limit = self.config['pack_size_bytes']

        def do_pack(data,
                    pack_buffer=pack_buffer,
                    limit=size_limit,
                    origin_url=origin_url):
            cur_size = pack_buffer.tell()
            would_write = len(data)
            if cur_size + would_write > limit:
                raise IOError('Pack file too big for repository %s, '
                              'limit is %d bytes, current size is %d, '
                              'would write %d' %
                              (origin_url, limit, cur_size, would_write))

            pack_buffer.write(data)

        remote_refs = client.fetch_pack(path,
                                        base_repo.determine_wants,
                                        base_repo.graph_walker(),
                                        do_pack,
                                        progress=do_activity).refs

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

    def prepare_origin_visit(self, origin_url, **kwargs):
        self.visit_date = datetime.datetime.now(tz=datetime.timezone.utc)
        self.origin = converters.origin_url_to_origin(origin_url)

    def get_full_snapshot(self, origin_id):
        prev_snapshot = self.storage.snapshot_get_latest(origin_id)
        if prev_snapshot and prev_snapshot.pop('next_branch', None):
            return snapshot_get_all_branches(self.storage, prev_snapshot['id'])

        return prev_snapshot

    def prepare(self, origin_url, base_url=None, ignore_history=False):
        base_origin_id = origin_id = self.origin_id

        prev_snapshot = None

        if not ignore_history:
            prev_snapshot = self.get_full_snapshot(origin_id)

        if base_url and not prev_snapshot:
            base_origin = converters.origin_url_to_origin(base_url)
            base_origin = self.storage.origin_get(base_origin)
            if base_origin:
                base_origin_id = base_origin['id']
                prev_snapshot = self.get_full_snapshot(base_origin_id)

        self.base_snapshot = prev_snapshot
        self.base_origin_id = base_origin_id
        self.ignore_history = ignore_history

    def fetch_data(self):
        def do_progress(msg):
            sys.stderr.buffer.write(msg)
            sys.stderr.flush()

        fetch_info = self.fetch_pack_from_origin(
            self.origin['url'], self.base_origin_id, self.base_snapshot,
            do_progress)

        self.pack_buffer = fetch_info['pack_buffer']
        self.pack_size = fetch_info['pack_size']

        self.remote_refs = fetch_info['remote_refs']
        self.local_refs = fetch_info['local_refs']

        origin_url = self.origin['url']

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

    def get_content_ids(self):
        """Get the content identifiers from the git repository"""
        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b'blob':
                continue

            yield converters.dulwich_blob_to_content_id(raw_obj)

    def get_contents(self):
        """Format the blobs from the git repository as swh contents"""
        max_content_size = self.config['content_size_limit']

        missing_contents = set(self.storage.content_missing(
            self.get_content_ids(), 'sha1_git'))

        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b'blob':
                continue

            if raw_obj.sha().digest() not in missing_contents:
                continue

            yield converters.dulwich_blob_to_content(
                raw_obj, log=self.log, max_content_size=max_content_size,
                origin_id=self.origin_id)

    def has_directories(self):
        return bool(self.type_to_ids[b'tree'])

    def get_directory_ids(self):
        """Get the directory identifiers from the git repository"""
        return (hashutil.hash_to_bytes(id.decode())
                for id in self.type_to_ids[b'tree'])

    def get_directories(self):
        """Format the trees as swh directories"""
        missing_dirs = set(self.storage.directory_missing(
            sorted(self.get_directory_ids())))

        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b'tree':
                continue

            if raw_obj.sha().digest() not in missing_dirs:
                continue

            yield converters.dulwich_tree_to_directory(raw_obj, log=self.log)

    def has_revisions(self):
        return bool(self.type_to_ids[b'commit'])

    def get_revision_ids(self):
        """Get the revision identifiers from the git repository"""
        return (hashutil.hash_to_bytes(id.decode())
                for id in self.type_to_ids[b'commit'])

    def get_revisions(self):
        """Format commits as swh revisions"""
        missing_revs = set(self.storage.revision_missing(
            sorted(self.get_revision_ids())))

        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b'commit':
                continue

            if raw_obj.sha().digest() not in missing_revs:
                continue

            yield converters.dulwich_commit_to_revision(raw_obj, log=self.log)

    def has_releases(self):
        return bool(self.type_to_ids[b'tag'])

    def get_release_ids(self):
        """Get the release identifiers from the git repository"""
        return (hashutil.hash_to_bytes(id.decode())
                for id in self.type_to_ids[b'tag'])

    def get_releases(self):
        """Retrieve all the release objects from the git repository"""
        missing_rels = set(self.storage.release_missing(
            sorted(self.get_release_ids())))

        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b'tag':
                continue

            if raw_obj.sha().digest() not in missing_rels:
                continue

            yield converters.dulwich_tag_to_release(raw_obj, log=self.log)

    def get_snapshot(self):
        branches = {}

        for ref in self.remote_refs:
            ret_ref = self.local_refs[ref].copy()
            if not ret_ref['target_type']:
                target_type = self.id_to_type[ret_ref['target']]
                ret_ref['target_type'] = converters.DULWICH_TYPES[target_type]

            ret_ref['target'] = hashutil.bytehex_to_hash(ret_ref['target'])

            branches[ref] = ret_ref

        self.snapshot = converters.branches_to_snapshot(branches)
        return self.snapshot

    def get_fetch_history_result(self):
        return {
            'contents': len(self.type_to_ids[b'blob']),
            'directories': len(self.type_to_ids[b'tree']),
            'revisions': len(self.type_to_ids[b'commit']),
            'releases': len(self.type_to_ids[b'tag']),
        }

    def load_status(self):
        """The load was eventful if the current snapshot is different to
           the one we retrieved at the beginning of the run"""
        eventful = False

        if self.base_snapshot:
            eventful = self.snapshot['id'] != self.base_snapshot['id']
        else:
            eventful = bool(self.snapshot['branches'])

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
        return BulkUpdater().load(
            origin_url,
            base_url=base_url,
            ignore_history=ignore_history,
        )

    main()
