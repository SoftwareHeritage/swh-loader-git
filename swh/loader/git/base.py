# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import logging
import time
from typing import Dict, Iterable

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

logger = logging.getLogger(__name__)

# Print a log message every LOGGING_INTERVAL
LOGGING_INTERVAL = 180


class BaseGitLoader(BaseLoader):
    """This base class is a pattern for both git loaders

    Those loaders are able to load all the data in one go.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.next_log_after = time.monotonic() + LOGGING_INTERVAL

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

    def maybe_log(self, msg: str, *args, level=logging.INFO, force=False, **kwargs):
        """Only log if ``LOGGING_INTERVAL`` has elapsed since the last log line was printed.

        Arguments are identical to those of ``logging.Logger.log``, except if the log format
        arguments are callable, the call only happens if the log is actually
        being printed.
        """
        if time.monotonic() < self.next_log_after and not force:
            return

        if logger.isEnabledFor(level):
            logger.log(
                level,
                msg,
                *(arg() if callable(arg) else arg for arg in args),
                **kwargs,
            )
            self.next_log_after = time.monotonic() + LOGGING_INTERVAL

    def store_data(self) -> None:
        assert self.origin
        if self.save_data_path:
            self.save_data()

        counts: Dict[str, int] = collections.defaultdict(int)
        storage_summary: Dict[str, int] = collections.Counter()

        def sum_counts():
            return sum(counts.values())

        def sum_storage():
            return sum(storage_summary[f"{object_type}:add"] for object_type in counts)

        def maybe_log_summary(msg, force=False):
            self.maybe_log(
                msg + ": processed %s objects, %s are new",
                sum_counts,
                sum_storage,
                force=force,
            )

        if self.has_contents():
            for obj in self.get_contents():
                if isinstance(obj, Content):
                    counts["content"] += 1
                    storage_summary.update(self.storage.content_add([obj]))
                elif isinstance(obj, SkippedContent):
                    counts["skipped_content"] += 1
                    storage_summary.update(self.storage.skipped_content_add([obj]))
                else:
                    raise TypeError(f"Unexpected content type: {obj}")

                maybe_log_summary("In contents")

            maybe_log_summary("After contents", force=True)

        if self.has_directories():
            for directory in self.get_directories():
                counts["directory"] += 1
                storage_summary.update(self.storage.directory_add([directory]))
                maybe_log_summary("In directories")

            maybe_log_summary("After directories", force=True)

        if self.has_revisions():
            for revision in self.get_revisions():
                counts["revision"] += 1
                storage_summary.update(self.storage.revision_add([revision]))
                maybe_log_summary("In revisions")

            maybe_log_summary("After revisions", force=True)

        if self.has_releases():
            for release in self.get_releases():
                counts["release"] += 1
                storage_summary.update(self.storage.release_add([release]))
                maybe_log_summary("In releases")

        snapshot = self.get_snapshot()
        counts["snapshot"] += 1
        storage_summary.update(self.storage.snapshot_add([snapshot]))

        storage_summary.update(self.flush())
        self.loaded_snapshot_id = snapshot.id

        for object_type, total in counts.items():
            filtered = total - storage_summary[f"{object_type}:add"]
            assert 0 <= filtered <= total, (filtered, total)

            if total == 0:
                # No need to send it
                continue

            # cannot use self.statsd_average, because this is a weighted average
            tags = {"object_type": object_type}

            # unweighted average
            self.statsd.histogram(
                "filtered_objects_percent", filtered / total, tags=tags
            )

            # average weighted by total
            self.statsd.increment("filtered_objects_total_sum", filtered, tags=tags)
            self.statsd.increment("filtered_objects_total_count", total, tags=tags)

        maybe_log_summary("After snapshot", force=True)
