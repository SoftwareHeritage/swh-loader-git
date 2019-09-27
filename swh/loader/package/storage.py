# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Optional, Sequence, Dict, Set
from functools import partial
from collections import deque

from swh.core.utils import grouper
from swh.storage import get_storage


class BufferingProxyStorage:
    """Storage implementation in charge of accumulating objects prior to
       discussing with the "main" storage.

    """
    def __init__(self, **storage):
        self.storage = get_storage(**storage)

        self._max_size = {
            'content': self.config.get('content_packet_size', 10000),
            'directory': self.config.get('directory_packet_size', 25000),
            'revision': self.config.get('directory_packet_size', 100000),
        }
        self.object_types = ['content', 'directory', 'revision']
        self._objects = {k: deque() for k in self.object_types}

    def flush(self, object_types: Optional[Sequence[str]] = None) -> Dict:
        if object_types is None:
            object_types = self.object_types
        summary = {}
        for object_type in object_types:
            q = self._objects[object_type]
            for objs in grouper(q, n=self._max_size[object_type]):
                add_fn = getattr(self.storage, '%s_add' % object_type)
                s = add_fn(objs)
                summary = {k: v + summary.get(k, 0)
                           for k, v in s.items()}

        return summary

    def object_add(self, objects: Sequence[Dict], *, object_type: str) -> Dict:
        q = self._objects[object_type]
        max_size = self._max_size[object_type]
        q.extend(objects)
        if len(q) > max_size:
            return self.flush()

        return {}

    def __getattr__(self, key):
        if key.endswith('_add'):
            object_type = key.split('_')[0]
            if object_type in self.object_types:
                return partial(
                    self.object_add, object_type=object_type
                )
        return getattr(self.storage, key)


class FilteringProxyStorage:
    """Storage implementation in charge of filtering existing objects prior to
       calling the storage api for ingestion.

    """
    def __init__(self, **storage):
        self.storage = get_storage(**storage)
        self.objects_seen = {
            'content': set(),    # set of content hashes (sha256) seen
            'directory': set(),
            'revision': set(),
        }

    def content_add(self, content: Sequence[Dict]) -> Dict:
        contents = list(content)
        contents_to_add = self._filter_missing_contents(contents)
        return self.storage.content_add(
            x for x in contents if x['sha256'] in contents_to_add
        )

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

    def _filter_missing_contents(
            self, content_hashes: Sequence[Dict]) -> Set[bytes]:
        """Return only the content keys missing from swh

        Args:
            content_hashes: List of sha256 to check for existence in swh
                storage

        """
        objects_seen = self.objects_seen['content']
        missing_hashes = []
        for hashes in content_hashes:
            if hashes['sha256'] in objects_seen:
                continue
            objects_seen.add(hashes['sha256'])
            missing_hashes.append(hashes)

        return set(self.storage.content_missing(
            missing_hashes,
            key_hash='sha256',
        ))

    def _filter_missing_ids(
            self,
            object_type: str,
            ids: Sequence[bytes]) -> Set[bytes]:
        """Filter missing ids from the storage for a given object type.

        Args:
            object_type: object type to use {revision, directory}
            ids: List of object_type ids

        Returns:
            Missing ids from the storage for object_type

        """
        objects_seen = self.objects_seen[object_type]
        missing_ids = []
        for id in ids:
            if id in objects_seen:
                continue
            objects_seen.add(id)
            missing_ids.append(id)

        fn_by_object_type = {
            'revision': self.storage.revision_missing,
            'directory': self.storage.directory_missing,
        }

        fn = fn_by_object_type[object_type]
        return set(fn(missing_ids))

    def __getattr__(self, key):
        return getattr(self.storage, key)
