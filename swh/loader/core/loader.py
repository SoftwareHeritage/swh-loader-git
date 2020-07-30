# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import hashlib
import logging
import os

from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Iterable, Optional, Tuple

from swh.core import config
from swh.model.model import (
    BaseContent,
    Content,
    SkippedContent,
    Directory,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Revision,
    Release,
    Sha1Git,
    Snapshot,
)
from swh.storage import get_storage
from swh.storage.utils import now


class BaseLoader(config.SWHConfig, metaclass=ABCMeta):
    """Mixin base class for loader.

    To use this class, you must:

    - inherit from this class
    - and implement the @abstractmethod methods:

      - :func:`prepare`: First step executed by the loader to prepare some
        state needed by the `func`:load method.

      - :func:`get_origin`: Retrieve the origin that is currently being loaded.

      - :func:`fetch_data`: Fetch the data is actually the method to implement
        to compute data to inject in swh (through the store_data method)

      - :func:`store_data`: Store data fetched.

      - :func:`visit_status`: Explicit status of the visit ('partial' or
        'full')

      - :func:`load_status`: Explicit status of the loading, for use by the
        scheduler (eventful/uneventful/temporary failure/permanent failure).

      - :func:`cleanup`: Last step executed by the loader.

    The entry point for the resulting loader is :func:`load`.

    You can take a look at some example classes:

    - :class:`BaseSvnLoader`

    """

    CONFIG_BASE_FILENAME = None  # type: Optional[str]

    DEFAULT_CONFIG = {
        "storage": ("dict", {"cls": "remote", "url": "http://localhost:5002/",}),
        "max_content_size": ("int", 100 * 1024 * 1024),
        "save_data": ("bool", False),
        "save_data_path": ("str", ""),
    }  # type: Dict[str, Tuple[str, Any]]

    ADDITIONAL_CONFIG = {}  # type: Dict[str, Tuple[str, Any]]

    def __init__(
        self, logging_class: Optional[str] = None, config: Dict[str, Any] = {}
    ):
        if config:
            self.config = config
        else:
            self.config = self.parse_config_file(
                additional_configs=[self.ADDITIONAL_CONFIG]
            )

        self.storage = get_storage(**self.config["storage"])

        if logging_class is None:
            logging_class = "%s.%s" % (
                self.__class__.__module__,
                self.__class__.__name__,
            )
        self.log = logging.getLogger(logging_class)

        _log = logging.getLogger("requests.packages.urllib3.connectionpool")
        _log.setLevel(logging.WARN)

        self.max_content_size = self.config["max_content_size"]

        # possibly overridden in self.prepare method
        self.visit_date: Optional[datetime.datetime] = None

        self.origin: Optional[Origin] = None

        if not hasattr(self, "visit_type"):
            self.visit_type: Optional[str] = None

        self.origin_metadata: Dict[str, Any] = {}

        self.loaded_snapshot_id: Optional[Sha1Git] = None

        # Make sure the config is sane
        save_data = self.config.get("save_data")
        if save_data:
            path = self.config["save_data_path"]
            os.stat(path)
            if not os.access(path, os.R_OK | os.W_OK):
                raise PermissionError("Permission denied: %r" % path)

    def save_data(self) -> None:
        """Save the data associated to the current load"""
        raise NotImplementedError

    def get_save_data_path(self) -> str:
        """The path to which we archive the loader's raw data"""
        if not hasattr(self, "__save_data_path"):
            year = str(self.visit_date.year)  # type: ignore

            assert self.origin
            url = self.origin.url.encode("utf-8")
            origin_url_hash = hashlib.sha1(url).hexdigest()

            path = "%s/sha1:%s/%s/%s" % (
                self.config["save_data_path"],
                origin_url_hash[0:2],
                origin_url_hash,
                year,
            )

            os.makedirs(path, exist_ok=True)
            self.__save_data_path = path

        return self.__save_data_path

    def flush(self) -> None:
        """Flush any potential buffered data not sent to swh-storage.

        """
        self.storage.flush()

    @abstractmethod
    def cleanup(self) -> None:
        """Last step executed by the loader.

        """
        pass

    @abstractmethod
    def prepare_origin_visit(self, *args, **kwargs) -> None:
        """First step executed by the loader to prepare origin and visit
           references. Set/update self.origin, and
           optionally self.origin_url, self.visit_date.

        """
        pass

    def _store_origin_visit(self) -> None:
        """Store origin and visit references. Sets the self.visit references.

        """
        assert self.origin
        self.storage.origin_add([self.origin])

        if not self.visit_date:  # now as default visit_date if not provided
            self.visit_date = datetime.datetime.now(tz=datetime.timezone.utc)
        assert isinstance(self.visit_date, datetime.datetime)
        assert isinstance(self.visit_type, str)
        self.visit = list(
            self.storage.origin_visit_add(
                [
                    OriginVisit(
                        origin=self.origin.url,
                        date=self.visit_date,
                        type=self.visit_type,
                    )
                ]
            )
        )[0]

    @abstractmethod
    def prepare(self, *args, **kwargs) -> None:
        """Second step executed by the loader to prepare some state needed by
           the loader.

        """
        pass

    def get_origin(self) -> Origin:
        """Get the origin that is currently being loaded.
        self.origin should be set in :func:`prepare_origin`

        Returns:
          dict: an origin ready to be sent to storage by
          :func:`origin_add`.
        """
        assert self.origin
        return self.origin

    @abstractmethod
    def fetch_data(self) -> bool:
        """Fetch the data from the source the loader is currently loading
           (ex: git/hg/svn/... repository).

        Returns:
            a value that is interpreted as a boolean. If True, fetch_data needs
            to be called again to complete loading.

        """
        pass

    @abstractmethod
    def store_data(self):
        """Store fetched data in the database.

        Should call the :func:`maybe_load_xyz` methods, which handle the
        bundles sent to storage, rather than send directly.
        """
        pass

    def store_metadata(self) -> None:
        """Store fetched metadata in the database.

        For more information, see implementation in :class:`DepositLoader`.
        """
        pass

    def load_status(self) -> Dict[str, str]:
        """Detailed loading status.

        Defaults to logging an eventful load.

        Returns: a dictionary that is eventually passed back as the task's
          result to the scheduler, allowing tuning of the task recurrence
          mechanism.
        """
        return {
            "status": "eventful",
        }

    def post_load(self, success: bool = True) -> None:
        """Permit the loader to do some additional actions according to status
        after the loading is done. The flag success indicates the
        loading's status.

        Defaults to doing nothing.

        This is up to the implementer of this method to make sure this
        does not break.

        Args:
            success (bool): the success status of the loading

        """
        pass

    def visit_status(self) -> str:
        """Detailed visit status.

        Defaults to logging a full visit.
        """
        return "full"

    def pre_cleanup(self) -> None:
        """As a first step, will try and check for dangling data to cleanup.
        This should do its best to avoid raising issues.

        """
        pass

    def load(self, *args, **kwargs) -> Dict[str, str]:
        r"""Loading logic for the loader to follow:

        - 1. Call :meth:`prepare_origin_visit` to prepare the
             origin and visit we will associate loading data to
        - 2. Store the actual ``origin_visit`` to storage
        - 3. Call :meth:`prepare` to prepare any eventual state
        - 4. Call :meth:`get_origin` to get the origin we work with and store

        - while True:

          - 5. Call :meth:`fetch_data` to fetch the data to store
          - 6. Call :meth:`store_data` to store the data

        - 7. Call :meth:`cleanup` to clean up any eventual state put in place
             in :meth:`prepare` method.

        """
        try:
            self.pre_cleanup()
        except Exception:
            msg = "Cleaning up dangling data failed! Continue loading."
            self.log.warning(msg)

        self.prepare_origin_visit(*args, **kwargs)
        self._store_origin_visit()

        assert self.origin
        assert self.visit.visit

        try:
            self.prepare(*args, **kwargs)

            while True:
                more_data_to_fetch = self.fetch_data()
                self.store_data()
                if not more_data_to_fetch:
                    break

            self.store_metadata()
            visit_status = OriginVisitStatus(
                origin=self.origin.url,
                visit=self.visit.visit,
                date=now(),
                status=self.visit_status(),
                snapshot=self.loaded_snapshot_id,
            )
            self.storage.origin_visit_status_add([visit_status])
            self.post_load()
        except Exception:
            self.log.exception(
                "Loading failure, updating to `partial` status",
                extra={"swh_task_args": args, "swh_task_kwargs": kwargs,},
            )
            visit_status = OriginVisitStatus(
                origin=self.origin.url,
                visit=self.visit.visit,
                date=now(),
                status="partial",
                snapshot=self.loaded_snapshot_id,
            )
            self.storage.origin_visit_status_add([visit_status])
            self.post_load(success=False)
            return {"status": "failed"}
        finally:
            self.flush()
            self.cleanup()

        return self.load_status()


class DVCSLoader(BaseLoader):
    """This base class is a pattern for dvcs loaders (e.g. git, mercurial).

    Those loaders are able to load all the data in one go. For example, the
    loader defined in swh-loader-git :class:`BulkUpdater`.

    For other loaders (stateful one, (e.g :class:`SWHSvnLoader`),
    inherit directly from :class:`BaseLoader`.

    """

    ADDITIONAL_CONFIG = {}  # type: Dict[str, Tuple[str, Any]]

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
        if self.config.get("save_data"):
            self.save_data()

        if self.has_contents():
            contents = []
            skipped_contents = []
            for obj in self.get_contents():
                if isinstance(obj, Content):
                    contents.append(obj)
                elif isinstance(obj, SkippedContent):
                    skipped_contents.append(obj)
                else:
                    raise TypeError(f"Unexpected content type: {obj}")
            self.storage.skipped_content_add(skipped_contents)
            self.storage.content_add(contents)
        if self.has_directories():
            self.storage.directory_add(list(self.get_directories()))
        if self.has_revisions():
            self.storage.revision_add(list(self.get_revisions()))
        if self.has_releases():
            self.storage.release_add(list(self.get_releases()))
        snapshot = self.get_snapshot()
        self.storage.snapshot_add([snapshot])
        self.flush()
        self.loaded_snapshot_id = snapshot.id
