# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from io import BytesIO
import logging
import sys

from collections import defaultdict
import dulwich.client
from dulwich.object_store import ObjectStoreGraphWalker
from dulwich.pack import PackData, PackInflater
from urllib.parse import urlparse

from swh.core import config, hashutil
from swh.storage import get_storage

from . import converters


class BulkUpdater(config.SWHConfig):
    """A bulk loader for a git repository"""
    CONFIG_BASE_FILENAME = 'updater.ini'

    DEFAULT_CONFIG = {
        'storage_class': ('str', 'remote_storage'),
        'storage_args': ('list[str]', ['http://localhost:5000/']),

        'send_contents': ('bool', True),
        'send_directories': ('bool', True),
        'send_revisions': ('bool', True),
        'send_releases': ('bool', True),
        'send_occurrences': ('bool', True),

        'content_packet_size': ('int', 10000),
        'content_packet_size_bytes': ('int', 1024 * 1024 * 1024),
        'directory_packet_size': ('int', 25000),
        'revision_packet_size': ('int', 100000),
        'release_packet_size': ('int', 100000),
        'occurrence_packet_size': ('int', 100000),
    }

    def __init__(self, config):
        self.config = config
        self.storage = get_storage(config['storage_class'],
                                   config['storage_args'])
        self.log = logging.getLogger('swh.loader.git.BulkLoader')


class SWHRepoRepresentation:
    """Repository representation for a Software Heritage origin."""
    def __init__(self, storage, origin_url):
        self.storage = storage

        self._parents_cache = {}
        self._refs_cache = None

        origin = storage.origin_get({'url': origin_url, 'type': 'git'})
        if origin:
            origin_id = origin['id']
            self.heads = self._cache_heads(origin_id)
        else:
            self.heads = []

    def _fill_parents_cache(self, commit):
        """When querying for a commit's parents, we fill the cache to a depth of 100
        commits."""
        root_rev = hashutil.hex_to_hash(commit.decode())
        for rev, parents in self.storage.revision_shortlog([root_rev], 100):
            rev_id = hashutil.hash_to_bytehex(rev)
            if rev_id not in self._parents_cache:
                self._parents_cache[rev_id] = [
                    hashutil.hash_to_bytehex(parent) for parent in parents
                ]

    def _cache_heads(self, origin_id):
        """Return all the known head commits for `origin_id`"""
        return [
            hashutil.hash_to_bytehex(revision['id'])
            for revision in self.storage.revision_get_by(
                    origin_id, branch_name=None, timestamp=None, limit=None)
        ]

    def get_parents(self, commit):
        """get the parent commits for `commit`"""
        if commit not in self._parents_cache:
            self._fill_parents_cache(commit)
        return self._parents_cache.get(commit, [])

    def get_heads(self):
        return self.heads

    @staticmethod
    def _encode_for_storage(objects):
        return [hashutil.hex_to_hash(object.decode()) for object in objects]

    @staticmethod
    def _decode_from_storage(objects):
        return set(hashutil.hash_to_hex(object).encode() for object in objects)

    def get_stored_commits(self, commits):
        return commits - self._decode_from_storage(
            self.storage.revision_missing(
                self._encode_for_storage(commits)
            )
        )

    def get_stored_tags(self, tags):
        return tags - self._decode_from_storage(
            self.storage.release_missing(
                self._encode_for_storage(tags)
            )
        )

    def get_stored_trees(self, trees):
        return trees - self._decode_from_storage(
            self.storage.directory_missing(
                self._encode_for_storage(trees)
            )
        )

    def get_stored_blobs(self, blobs):
        return set()

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
        refs = self.parse_refs(refs)
        ret = set()
        for ref, target in self.filter_unwanted_refs(refs).items():
            if target['target_type'] is None:
                # The target doesn't exist in Software Heritage
                ret.add(target['target'])

        return list(ret)

    def parse_refs(self, remote_refs):
        """Parse the remote refs information and list the objects that exist in
        Software Heritage"""
        if self._refs_cache is not None:
            return self._refs_cache

        all_objs = set(remote_refs.values())
        type_by_id = defaultdict(None)

        tags = self.get_stored_tags(all_objs)
        all_objs -= tags
        for tag in tags:
            type_by_id[tag] = 'release'

        commits = self.get_stored_commits(all_objs)
        all_objs -= commits
        for commit in commits:
            type_by_id[commit] = 'revision'

        trees = self.get_stored_trees(all_objs)
        all_objs -= trees
        for tree in trees:
            type_by_id[tree] = 'directory'

        blobs = self.get_stored_blobs(all_objs)
        all_objs -= blobs
        for blob in blobs:
            type_by_id[blob] = 'content'

        ret = {}
        for ref, id in remote_refs.items():
            ret[ref] = {
                'target': id,
                'target_type': type_by_id.get(id),
            }

        self._refs_cache = ret

        return ret


def fetch_pack_from_origin(storage, origin_url, base_url, activity_buffer):
    """Fetch a pack from the origin"""
    pack_buffer = BytesIO()

    base_repo = SWHRepoRepresentation(storage, base_url)

    parsed_uri = urlparse(origin_url)

    path = parsed_uri.path
    if not path.endswith('.git'):
        path += '.git'

    client = dulwich.client.TCPGitClient(parsed_uri.netloc, thin_packs=False)

    def do_pack(data, pack_buffer=pack_buffer):
        pack_buffer.write(data)

    def do_activity(data, activity_buffer=activity_buffer):
        activity_buffer.write(data)
        activity_buffer.flush()

    remote_refs = client.fetch_pack(path.encode('ascii'),
                                    base_repo.determine_wants,
                                    base_repo.graph_walker(),
                                    do_pack,
                                    progress=do_activity)

    local_refs = base_repo.parse_refs(remote_refs)

    pack_buffer.flush()
    pack_size = pack_buffer.tell()
    pack_buffer.seek(0)
    return {
        'remote_refs': base_repo.filter_unwanted_refs(remote_refs),
        'local_refs': local_refs,
        'pack_buffer': pack_buffer,
        'pack_size': pack_size,
    }


def refs_to_occurrences(remote_refs, local_refs, types_per_id, origin_id,
                        timestamp):
    """Merge references remote and local"""
    ret = {}
    for ref, data in remote_refs.items():
        ret[ref] = local_refs[ref].copy()
        ret[ref].update({
            'branch': ref,
            'origin': origin_id,
            'validity': timestamp,
        })
        if not ret[ref]['target_type']:
            target_type = types_per_id[remote_refs[ref]]
            ret[ref]['target_type'] = converters.DULWICH_TYPES[target_type]

    return ret


if __name__ == '__main__':
    config = BulkUpdater.parse_config_file(
            base_filename='updater.ini'
        )

    bulkupdater = BulkUpdater(config)

    origin_url = sys.argv[1]
    base_url = origin_url
    if len(sys.argv) > 2:
        base_url = sys.argv[2]

    fetch_info = fetch_pack_from_origin(
        bulkupdater.storage, origin_url, base_url, sys.stderr.buffer)

    pack_data = PackInflater.for_pack_data(
        PackData.from_file(fetch_info['pack_buffer'], fetch_info['pack_size']))

    objs_per_type = defaultdict(list)
    types_per_id = {}
    for obj in pack_data:
        obj_type = obj.type_name
        types_per_id[obj.id] = obj_type
        conv = converters.DULWICH_CONVERTERS[obj_type]
        objs_per_type[obj_type].append(conv(obj))
        print([(k, len(l)) for k, l in sorted(objs_per_type.items())],
              file=sys.stderr, end='\r')

    remote_refs = fetch_info['remote_refs']
    local_refs = fetch_info['local_refs']

    origin_id = 42
    now = datetime.datetime.now()

    occurrences = refs_to_occurrences(
        remote_refs, local_refs, types_per_id, origin_id, now)

    for branch in occurrences:
        print(occurrences[branch])
    objs_per_type[b'refs'] = remote_refs

    print({k: len(l) for k, l in objs_per_type.items()})
