# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.storage import get_storage

from typing import Sequence, Dict


class ProxyStorage:
    def __init__(self, **storage):
        self.storage = get_storage(**storage)
        self.contents_seen = set()
        self.directories_seen = set()
        self.revisions_seen = set()


    def _filter_missing_contents(
            self, content_hashes: Sequence[bytes]) -> Sequence[bytes]:
        """Return only the content keys missing from swh

        Args:
            content_hashes: List of sha256 to check for existence in swh
                storage

        """
        missing_hashes = []
        for hash in content_hashes:
            if hash in self.contents_seen:
                continue
            self.contents_seen.add(hash)
            missing_hashes.append({'sha256': hash})

        return list(self.storage.content_missing(
            missing_hashes,
            key_hash='sha256',
        ))

    def content_add(self, content: Sequence[Dict]) -> Dict:
        contents = list(content)
        missing_hashes = self._filter_missing_contents(
            c['sha256'] for c in contents
        )
        return self.storage.content_add(
            x for x in contents if x['sha256'] in missing_hashes
        )

    def _filter_missing_ids(
            self,
            object_type: str,
            ids: Sequence[bytes]) -> Sequence[bytes]:
        """Filter missing ids from the storage for a given object type.

        Args:
            object_type: object type to use {revision, directory}
            ids: List of object_type ids

        Returns:
            Missing ids from the storage for object_type

        """
        missing_ids = []
        for id in ids:
            if id in self.directories_seen:
                continue
            self.directories_seen.add(id)
            missing_ids.append(id)

        fn_by_object_type = {
            'revision': self.storage.revision_missing,
            'directory': self.storage.directory_missing,
        }
        fn = fn_by_object_type[object_type]

        return list(fn(missing_ids))

    def directory_add(self, directories: Sequence[Dict]) -> Dict:
        directories = list(directories)
        missing_ids = self._filter_missing_ids(
            'directory',
            (d['id'] for d in directories)
        )
        return self.storage.directory_add(
            d for d in directories if d['id'] in missing_ids
        )

    def revision_add(self, revisions):
        revisions = list(revisions)
        missing_ids = self._filter_missing_ids(
            'revision',
            (d['id'] for d in revisions)
        )
        return self.storage.revision_add(
            r for r in revisions if r['id'] in missing_ids
        )

    def __getattr__(self, key):
        return getattr(self.storage, key)
