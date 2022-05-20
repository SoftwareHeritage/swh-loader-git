# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Iterable

from swh.loader.core.loader import BaseLoader
from swh.model.model import (
    BaseContent,
    Content,
    Directory,
    Release,
    Revision,
    SkippedContent,
    Snapshot,
)


class BaseGitLoader(BaseLoader):
    """This base class is a pattern for both git loaders

    Those loaders are able to load all the data in one go.
    """

    def cleanup(self) -> None:
        """Clean up an eventual state installed for computations."""
        pass

    def has_contents(self) -> bool:
        """Checks whether we need to load contents"""
        return True

    def get_contents(self) -> Iterable[BaseContent]:
        """Get the contents that need to be loaded"""
        raise NotImplementedError

    def has_directories(self) -> bool:
        """Checks whether we need to load directories"""
        return True

    def get_directories(self) -> Iterable[Directory]:
        """Get the directories that need to be loaded"""
        raise NotImplementedError

    def has_revisions(self) -> bool:
        """Checks whether we need to load revisions"""
        return True

    def get_revisions(self) -> Iterable[Revision]:
        """Get the revisions that need to be loaded"""
        raise NotImplementedError

    def has_releases(self) -> bool:
        """Checks whether we need to load releases"""
        return True

    def get_releases(self) -> Iterable[Release]:
        """Get the releases that need to be loaded"""
        raise NotImplementedError

    def get_snapshot(self) -> Snapshot:
        """Get the snapshot that needs to be loaded"""
        raise NotImplementedError

    def eventful(self) -> bool:
        """Whether the load was eventful"""
        raise NotImplementedError

    def store_data(self) -> None:
        assert self.origin
        if self.save_data_path:
            self.save_data()

        if self.has_contents():
            for obj in self.get_contents():
                if isinstance(obj, Content):
                    self.storage.content_add([obj])
                elif isinstance(obj, SkippedContent):
                    self.storage.skipped_content_add([obj])
                else:
                    raise TypeError(f"Unexpected content type: {obj}")
        if self.has_directories():
            for directory in self.get_directories():
                self.storage.directory_add([directory])
        if self.has_revisions():
            for revision in self.get_revisions():
                self.storage.revision_add([revision])
        if self.has_releases():
            for release in self.get_releases():
                self.storage.release_add([release])
        snapshot = self.get_snapshot()
        self.storage.snapshot_add([snapshot])
        self.flush()
        self.loaded_snapshot_id = snapshot.id
