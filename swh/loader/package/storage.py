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

    def origin_add(self, origins):
        return self.storage.origin_add(origins)

    def origin_visit_add(self, origin, date, type=None):
        return self.storage.origin_visit_add(origin, date, type=type)

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

    def _filter_missing_directories(
            self, directory_ids: Sequence[bytes]) -> Sequence[bytes]:
        """Return only the content keys missing from swh

        Args:
            directory_ids: List of directory ids

        """
        missing_ids = []
        for id in directory_ids:
            if id in self.directories_seen:
                continue
            self.directories_seen.add(id)
            missing_ids.append(id)

        return list(self.storage.directory_missing(missing_ids))

    def directory_add(self, directories: Sequence[Dict]) -> Dict:
        directories = list(directories)
        missing_ids = self._filter_missing_directories(
            d['id'] for d in directories
        )
        return self.storage.directory_add(
            d for d in directories if d['id'] in missing_ids
        )

    def _filter_missing_revisions(
            self, revision_ids: Sequence[bytes]) -> Sequence[bytes]:
        """Return only the content keys missing from swh

        Args:
            revision_ids: List of revisions ids

        """
        missing_ids = []
        for id in revision_ids:
            if id in self.revisions_seen:
                continue
            self.revisions_seen.add(id)
            missing_ids.append(id)

        return list(self.storage.revision_missing(missing_ids))

    def revision_add(self, revisions):
        revisions = list(revisions)
        missing_ids = self._filter_missing_revisions(
            d['id'] for d in revisions
        )
        return self.storage.revision_add(
            r for r in revisions if r['id'] in missing_ids
        )

    def snapshot_add(self, snapshots):
        return self.storage.snapshot_add(snapshots)

    def origin_visit_update(self, origin, visit_id, status=None,
                            metadata=None, snapshot=None):
        return self.storage.origin_visit_update(
            origin, visit_id, status=status,
            metadata=metadata, snapshot=snapshot
        )

    def stat_counters(self):
        return self.storage.stat_counters()

    def origin_visit_get(self, origin, last_visit=None, limit=None):
        return self.storage.origin_visit_get(
            origin, last_visit=last_visit, limit=limit)

    def content_missing_per_sha1(self, contents):
        yield from self.storage.content_missing_per_sha1(contents)

    def directory_missing(self, directories):
        return self.storage.directory_missing(directories)

    def revision_missing(self, revisions):
        return self.storage.revision_missing(revisions)

    def snapshot_get(self, snapshot_id):
        return self.storage.snapshot_get(snapshot_id)

    def content_get(self, content):
        return self.storage.content_get(content)
