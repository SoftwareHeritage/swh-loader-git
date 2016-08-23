# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import datetime

import dulwich.repo

from swh.core import hashutil

from . import base, converters


class GitLoader(base.BaseLoader):
    """Load a git repository from a directory.
    """

    CONFIG_BASE_FILENAME = 'loader/git-loader.ini'

    def prepare(self, origin_url, directory, fetch_date):
        self.origin_url = origin_url
        self.repo = dulwich.repo.Repo(directory)
        self.fetch_date = fetch_date

    def get_origin(self):
        """Get the origin that is currently being loaded"""
        return converters.origin_url_to_origin(self.origin_url)

    def iter_objects(self):
        object_store = self.repo.object_store

        for pack in object_store.packs:
            objs = list(pack.index.iterentries())
            objs.sort(key=lambda x: x[1])
            for sha, offset, crc32 in objs:
                yield hashutil.hash_to_bytehex(sha)

        yield from object_store._iter_loose_objects()
        yield from object_store._iter_alternate_objects()

    def fetch_data(self):
        """Fetch the data from the data source"""
        type_to_ids = defaultdict(list)
        for oid in self.iter_objects():
            type_name = self.repo[oid].type_name
            type_to_ids[type_name].append(oid)

        self.type_to_ids = type_to_ids

    def has_contents(self):
        """Checks whether we need to load contents"""
        return bool(self.type_to_ids[b'blob'])

    def get_contents(self):
        """Get the contents that need to be loaded"""
        max_content_size = self.config['content_size_limit']

        for oid in self.type_to_ids[b'blob']:
            yield converters.dulwich_blob_to_content(
                self.repo[oid], log=self.log,
                max_content_size=max_content_size,
                origin_id=self.origin_id)

    def has_directories(self):
        """Checks whether we need to load directories"""
        return bool(self.type_to_ids[b'tree'])

    def get_directories(self):
        """Get the directories that need to be loaded"""
        for oid in self.type_to_ids[b'tree']:
            yield converters.dulwich_tree_to_directory(
                self.repo[oid], log=self.log)

    def has_revisions(self):
        """Checks whether we need to load revisions"""
        return bool(self.type_to_ids[b'commit'])

    def get_revisions(self):
        """Get the revisions that need to be loaded"""
        for oid in self.type_to_ids[b'commit']:
            yield converters.dulwich_commit_to_revision(
                self.repo[oid], log=self.log)

    def has_releases(self):
        """Checks whether we need to load releases"""
        return bool(self.type_to_ids[b'tag'])

    def get_releases(self):
        """Get the releases that need to be loaded"""
        for oid in self.type_to_ids[b'tag']:
            yield converters.dulwich_tag_to_release(
                self.repo[oid], log=self.log)

    def has_occurrences(self):
        """Checks whether we need to load occurrences"""
        return True

    def get_occurrences(self):
        """Get the occurrences that need to be loaded"""
        repo = self.repo
        origin_id = self.origin_id
        visit = self.visit

        for ref, target in repo.refs.as_dict().items():
            target_type_name = repo[target].type_name
            target_type = converters.DULWICH_TYPES[target_type_name]
            yield {
                'branch': ref,
                'origin': origin_id,
                'target': hashutil.bytehex_to_hash(target),
                'target_type': target_type,
                'visit': visit,
            }

    def get_fetch_history_result(self):
        """Return the data to store in fetch_history for the current loader"""
        return {
            'contents': len(self.type_to_ids[b'blob']),
            'directories': len(self.type_to_ids[b'tree']),
            'revisions': len(self.type_to_ids[b'commit']),
            'releases': len(self.type_to_ids[b'tag']),
            'occurrences': len(self.repo.refs.allkeys()),
        }

    def eventful(self):
        """Whether the load was eventful"""
        return True

if __name__ == '__main__':
    import logging
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(process)d %(message)s'
    )
    loader = GitLoader()

    origin_url = sys.argv[1]
    directory = sys.argv[2]
    fetch_date = datetime.datetime.now(tz=datetime.timezone.utc)

    print(loader.load(origin_url, directory, fetch_date))
