# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.storage import get_storage


class ProxyStorage:
    def __init__(self, **storage):
        self.storage = get_storage(**storage)

    def origin_add(self, origins):
        return self.storage.origin_add(origins)

    def origin_visit_add(self, origin, date, type=None):
        return self.storage.origin_visit_add(origin, date, type=type)

    def content_add(self, content):
        return self.storage.content_add(content)

    def directory_add(self, directories):
        return self.storage.directory_add(directories)

    def revision_add(self, revisions):
        return self.storage.revision_add(revisions)

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
        return self.storage.content_missing_per_sha1(contents)

    def directory_missing(self, directories):
        return self.storage.directory_missing(directories)

    def revision_missing(self, revisions):
        return self.storage.revision_missing(revisions)

    def snapshot_get(self, snapshot_id):
        return self.storage.snapshot_get(snapshot_id)

    def content_get(self, content):
        return self.storage.content_get(content)
