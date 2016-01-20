# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import dulwich.client
import logging
import sys

from collections import defaultdict
from dulwich.object_store import ObjectStoreGraphWalker
from urllib.parse import urlparse

from swh.core import config, hashutil
from swh.storage import get_storage


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
        origin = storage.origin_get({'url': origin_url, 'type': 'git'})
        if origin:
            origin_id = origin['id']

            self.parents = self._cache_parents(origin_id)
            self.heads = self._cache_heads(origin_id)
            self.tags = self._cache_tags(origin_id)
        else:
            raise ValueError('Unexpected error, the origin %s was not found.'
                             % origin_url)

    def _cache_parents(self, origin_id):
        """Return an id -> parent_ids mapping for the repository at
           `origin_id`"""
        occurrences = self.storage.occurrence_get(origin_id)
        root_revisions = (occ['revision'] for occ in occurrences)

        ret_parents = defaultdict(list)
        for revision in self.storage.revision_log(root_revisions):
            rev_id = hashutil.hash_to_bytehex(revision['id'])
            for parent in revision['parents']:
                parent_id = hashutil.hash_to_bytehex(parent)
                ret_parents[rev_id].append(parent_id)

        return ret_parents

    def _cache_heads(self, origin_id):
        """Return all the known head commits for `origin_id`"""
        for revision in self.storage.revision_get_by(origin_id,
                                                     branch_name=None,
                                                     timestamp=None,
                                                     limit=None):
            yield hashutil.hash_to_bytehex(revision['id'])

    def _cache_tags(self, origin_id):
        """Return all the tag objects pointing to heads of `origin_id`"""
        for release in self.storage.release_get_by(origin_id):
            yield hashutil.hash_to_bytehex(release['id'])

    def get_parents(self, commit):
        """get the parent commits for `commit`"""
        print('########################### Request commit %s' % commit)
        return self.parents[commit]

    def get_heads(self):
        print('########################### Request heads!')
        return self.heads

    def get_tags(self):
        print('########################### Request tags!')
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


def fetch_pack_from_origin(storage, origin_url, buf):

    def report_activity(arg):
        print('########################### Report activity %s!' % arg)
        # sys.stderr.buffer.write(arg)
        # sys.stderr.buffer.flush()

    repo = SWHRepoRepresentation(storage, origin_url)

    parsed_uri = urlparse(origin_url)

    path = parsed_uri.path
    if not path.endswith('.git'):
        path += '.git'

    client = dulwich.client.TCPGitClient(parsed_uri.netloc, thin_packs=False)

    pack = client.fetch_pack(path.encode('ascii'),
                             repo.determine_wants,
                             repo.graph_walker(),
                             buf.write,
                             progress=report_activity)

    # refs = client.get_refs(path.encode('ascii'))
    # print(refs)

    return pack

if __name__ == '__main__':
    config = BulkUpdater.parse_config_file(
            base_filename='updater.ini'
        )

    bulkupdater = BulkUpdater(config)

    origin_url = sys.argv[1]
    pack = fetch_pack_from_origin(bulkupdater.storage,
                                  origin_url,
                                  sys.stdout.buffer)
    print(pack)
