# Copyright (C) 2015-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import contextlib
import json
import logging
import random
import time
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from swh.loader.core.loader import BaseLoader
from swh.model.model import (
    BaseContent,
    BaseModel,
    Content,
    Directory,
    Release,
    Revision,
    SkippedContent,
    Snapshot,
)

stats_logger = logging.getLogger(f"{__name__}.stats")
logger = logging.getLogger(__name__)

# Print a log message every LOGGING_INTERVAL
LOGGING_INTERVAL = 180

SwhObject = Union[BaseContent, Directory, Revision, Release]

SwhObjectType = TypeVar("SwhObjectType")


class BaseGitLoader(BaseLoader):
    """This base class is a pattern for both git loaders

    Those loaders are able to load all the data in one go.


    Args:
        store_order: One of ``as_origin`` (store them in the same order as the packfile
            sent by the remote), ``by_type_layers`` (same, but loads all contents,
            then all directories, then all revisions, then all releases)
    """

    def __init__(
        self,
        *args,
        store_order: Literal["as_origin", "by_type_layers"] = "by_type_layers",
        **kwargs,
    ) -> None:

        known_lister_prefixes = kwargs.pop(
            "known_lister_prefixes", {"https://github.com/": ("github", "github")}
        )
        super().__init__(*args, **kwargs)

        self.credentials = self.extract_credentials(
            known_lister_prefixes=known_lister_prefixes
        )

        self.next_log_after = time.monotonic() + LOGGING_INTERVAL
        self.store_order = store_order
        self.stats: Dict[str, Any] = {
            "filtered_objects": {},
            "external_references": {"found": 0, "not_found": 0},
            "counts": [],
            "timings": [],
            "config": {"store_order": store_order},
            "task": {
                "origin_url": self.origin.url,
                "lister_name": self.lister_name,
                "lister_instance_name": self.lister_instance_name,
                "username": None,
            },
        }

    def extract_credentials(
        self, known_lister_prefixes: Dict[str, Tuple[str, str]]
    ) -> List[Dict[str, str]]:
        """Extract credentials from the loader keyword arguments"""
        # TODO: move this to swh.loader.core.metadata_fetchers.get_fetchers_for_lister
        if not self.lister_name or not self.lister_instance_name:
            for prefix in known_lister_prefixes:
                if self.origin.url.startswith(prefix):
                    self.lister_name, self.lister_instance_name = known_lister_prefixes[
                        prefix
                    ]

        if (
            not self.lister_name
            or not self.lister_instance_name
            or not self.metadata_fetcher_credentials
        ):
            return []

        if (
            self.lister_name in self.metadata_fetcher_credentials
            and self.lister_instance_name
            in self.metadata_fetcher_credentials[self.lister_name]
        ):
            creds = self.metadata_fetcher_credentials[self.lister_name][
                self.lister_instance_name
            ]
            # Copy and shuffle the list
            return random.sample(creds, k=len(creds))

        return []

    def cleanup(self) -> None:
        """Clean up an eventual state installed for computations."""
        stats_logger.info(
            "%s",
            json.dumps(
                {"stats": self.stats, "tags": self.statsd.constant_tags}, indent="\t"
            ),
        )

    def get_objects(self, object_type: Optional[bytes] = None) -> Iterable[SwhObject]:
        raise NotImplementedError

    def has_contents(self) -> bool:
        """Checks whether we need to load contents"""
        return True

    def get_contents(self) -> Iterable[BaseContent]:
        """Get the contents that need to be loaded"""
        return cast(Iterable[BaseContent], self.get_objects(b"blob"))

    def has_directories(self) -> bool:
        """Checks whether we need to load directories"""
        return True

    def get_directories(self) -> Iterable[Directory]:
        """Get the directories that need to be loaded"""
        return cast(Iterable[Directory], self.get_objects(b"tree"))

    def has_revisions(self) -> bool:
        """Checks whether we need to load revisions"""
        return True

    def get_revisions(self) -> Iterable[Revision]:
        """Get the revisions that need to be loaded"""
        return cast(Iterable[Revision], self.get_objects(b"commit"))

    def has_releases(self) -> bool:
        """Checks whether we need to load releases"""
        return True

    def get_releases(self) -> Iterable[Release]:
        """Get the releases that need to be loaded"""
        return cast(Iterable[Release], self.get_objects(b"tag"))

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
        object_counts: Dict[str, int] = collections.defaultdict(int)
        storage_summary: Dict[str, int] = collections.Counter()

        def sum_object_counts():
            return sum(object_counts.values())

        def sum_storage():
            return sum(
                storage_summary[f"{object_type}:add"] for object_type in object_counts
            )

        def maybe_log_summary(msg, force=False):
            self.maybe_log(
                msg + ": processed %s objects, %s are new",
                sum_object_counts,
                sum_storage,
                force=force,
            )

        def add_content(obj: Content):
            object_counts["content"] += 1
            counts["content_bytes"] += obj.length
            storage_summary.update(self.storage.content_add([obj]))

        def add_skipped_content(obj: SkippedContent):
            object_counts["skipped_content"] += 1
            if obj.length is not None:
                counts["skipped_content_bytes"] += obj.length
            storage_summary.update(self.storage.skipped_content_add([obj]))

        def add_directory(directory: Directory):
            object_counts["directory"] += 1
            counts["directory_entries"] += len(directory.entries)
            storage_summary.update(self.storage.directory_add([directory]))

        def add_revision(revision: Revision):
            object_counts["revision"] += 1
            counts["revision_parents"] += len(revision.parents)
            if revision.message is not None:
                counts["revision_message_bytes"] += len(revision.message)
            storage_summary.update(self.storage.revision_add([revision]))

        def add_release(release: Release):
            object_counts["release"] += 1
            if release.message is not None:
                counts["release_message_bytes"] += len(release.message)
            storage_summary.update(self.storage.release_add([release]))

        if self.store_order == "as_origin":
            adders: Dict[Type[BaseModel], Callable] = {
                Content: add_content,
                SkippedContent: add_skipped_content,
                Directory: add_directory,
                Revision: add_revision,
                Release: add_release,
            }
            for i, obj in enumerate(self.get_objects()):
                if i % 1_000 == 0:
                    maybe_log_summary("In objects")
                try:
                    adder = adders[type(obj)]
                except KeyError:
                    raise TypeError(f"Unknown object type: {type(obj)!r}") from None
                adder(obj)

            storage_summary.update(self.flush())
        elif self.store_order == "by_type_layers":
            if self.has_contents():
                for obj in self.get_contents():
                    if isinstance(obj, Content):
                        add_content(obj)
                    elif isinstance(obj, SkippedContent):
                        add_skipped_content(obj)
                    else:
                        raise TypeError(f"Unexpected content type: {obj}")

                    maybe_log_summary("In contents")

                storage_summary.update(self.flush())
                maybe_log_summary("After contents", force=True)

            if self.has_directories():
                for directory in self.get_directories():
                    add_directory(directory)
                    maybe_log_summary("In directories")

                storage_summary.update(self.flush())
                maybe_log_summary("After directories", force=True)

            if self.has_revisions():
                for revision in self.get_revisions():
                    add_revision(revision)
                    maybe_log_summary("In revisions")

                storage_summary.update(self.flush())
                maybe_log_summary("After revisions", force=True)

            if self.has_releases():
                for release in self.get_releases():
                    add_release(release)
                    maybe_log_summary("In releases")

                storage_summary.update(self.flush())
                maybe_log_summary("After releases", force=True)
        else:
            raise ValueError(f"Unknown store_order: {self.store_order}")

        snapshot = self.get_snapshot()
        object_counts["snapshot"] += 1
        counts["snapshot_branches"] += len(snapshot.branches)
        storage_summary.update(self.storage.snapshot_add([snapshot]))

        storage_summary.update(self.flush())
        self.loaded_snapshot_id = snapshot.id

        for object_type, total in object_counts.items():
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
            self.stats["filtered_objects"][object_type] = {
                "filtered": filtered,
                "total": total,
            }

        for counter, total in counts.items():
            self.statsd_average(counter, total)

        self.stats["storage_summary"] = storage_summary

        maybe_log_summary("After snapshot", force=True)

    def statsd_timed(self, name: str, tags: Dict[str, Any] = {}) -> ContextManager:
        super_ctx_manager = super().statsd_timed(name, tags)

        @contextlib.contextmanager
        def ctx_manager():
            with super_ctx_manager:
                yield super_ctx_manager
            self.statsd_timing(name, super_ctx_manager.elapsed, tags)

        return ctx_manager()

    def statsd_timing(self, name: str, value: float, tags: Dict[str, Any] = {}) -> None:
        self.stats["timings"].append({"name": name, "tags": tags, "elapsed": value})
        super().statsd_timing(name, value, tags)

    def statsd_average(
        self, name: str, value: Union[int, float], tags: Dict[str, Any] = {}
    ) -> None:
        self.stats["counts"].append({"name": name, "tags": tags, "value": value})
        super().statsd_average(name, value, tags)

    def statsd_increment(
        self, name: str, value: Union[int, float], tags: Dict[str, Any] = {}
    ) -> None:
        self.stats["counts"].append({"name": name, "tags": tags, "value": value})
        self.statsd.increment(name, value, tags=tags)
