# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

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

        origin = storage.origin_get({'url': origin_url, 'type': 'git'})
        if origin:
            origin_id = origin['id']
            self.heads = self._cache_heads(origin_id)
            self.tags = self._cache_tags(origin_id)
        else:
            self.heads = []
            self.tags = []

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

    def _cache_tags(self, origin_id):
        """Return all the tag objects pointing to heads of `origin_id`"""
        return [
            hashutil.hash_to_bytehex(release['id'])
            for release in self.storage.release_get_by(origin_id)
        ]

    def get_parents(self, commit):
        """get the parent commits for `commit`"""
        if commit not in self._parents_cache:
            self._fill_parents_cache(commit)
        return self._parents_cache.get(commit, [])

    def get_heads(self):
        return self.heads

    def get_tags(self):
        return self.tags

    def graph_walker(self):
        return ObjectStoreGraphWalker(self.get_heads(), self.get_parents)

    def determine_wants(self, refs):
        all_objs = set()
        objs_by_id = defaultdict(list)
        for ref, id in refs.items():
            objs_by_id[id].append(ref)
            if ref.endswith(b'^{}'):
                continue
            if ref.startswith(b'refs/tags/'):
                all_objs.add(id)
            if ref.startswith(b'refs/pull/'):
                if not ref.endswith(b'/merge'):
                    all_objs.add(id)
                continue
            if not ref.startswith(b'refs/pull/'):
                all_objs.add(id)
                continue

        ret = list(all_objs - set(self.get_heads()) - set(self.get_tags()))
        return ret


def fetch_pack_from_origin(storage, origin_url, base_url, pack_buffer,
                           activity_buffer):

    base_repo = SWHRepoRepresentation(storage, base_url)

    parsed_uri = urlparse(origin_url)

    path = parsed_uri.path
    if not path.endswith('.git'):
        path += '.git'

    client = dulwich.client.TCPGitClient(parsed_uri.netloc, thin_packs=False)

    pack = client.fetch_pack(path.encode('ascii'),
                             base_repo.determine_wants,
                             base_repo.graph_walker(),
                             pack_buffer.write,
                             progress=activity_buffer.write)

    return pack

if __name__ == '__main__':
    config = BulkUpdater.parse_config_file(
            base_filename='updater.ini'
        )

    bulkupdater = BulkUpdater(config)

    origin_url = sys.argv[1]
    base_url = origin_url
    if len(sys.argv) > 2:
        base_url = sys.argv[2]

    pack = BytesIO()
    refs = fetch_pack_from_origin(
        bulkupdater.storage, origin_url, base_url, pack, sys.stderr.buffer)

    pack_size = pack.tell()
    pack.seek(0)
    pack_data = PackInflater.for_pack_data(PackData.from_file(pack, pack_size))
    objs_per_type = defaultdict(list)
    for obj in pack_data:
        obj_type = obj.type_name
        conv = converters.DULWICH_CONVERTERS[obj_type]
        objs_per_type[obj_type].append(conv(obj))

    print({k: len(l) for k, l in objs_per_type.items()})
    print(len(refs))
