# Copyright (C) 2015-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import hashlib
import logging
import os
from typing import Any, Dict, Iterable, Optional

from swh.core.config import load_from_envvar
from swh.loader.exception import NotFound
from swh.model.model import (
    BaseContent,
    Content,
    Directory,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Release,
    Revision,
    Sha1Git,
    SkippedContent,
    Snapshot,
)
from swh.storage import get_storage
from swh.storage.interface import StorageInterface
from swh.storage.utils import now

DEFAULT_CONFIG: Dict[str, Any] = {
    "max_content_size": 100 * 1024 * 1024,
}


class BaseLoader:
    """Base class for (D)VCS loaders (e.g Svn, Git, Mercurial, ...) or PackageLoader (e.g
    PyPI, Npm, CRAN, ...)

    A loader retrieves origin information (git/mercurial/svn repositories, pypi/npm/...
    package artifacts), ingests the contents/directories/revisions/releases/snapshot
    read from those artifacts and send them to the archive through the storage backend.

    The main entry point for the loader is the :func:`load` function.

    2 static methods (:func:`from_config`, :func:`from_configfile`) centralizes and
    eases the loader instantiation from either configuration dict or configuration file.

    Some class examples:

    - :class:`SvnLoader`
    - :class:`GitLoader`
    - :class:`PyPILoader`
    - :class:`NpmLoader`

    """

    def __init__(
        self,
        storage: StorageInterface,
        logging_class: Optional[str] = None,
        save_data_path: Optional[str] = None,
        max_content_size: Optional[int] = None,
    ):
        super().__init__()
        self.storage = storage
        self.max_content_size = int(max_content_size) if max_content_size else None

        if logging_class is None:
            logging_class = "%s.%s" % (
                self.__class__.__module__,
                self.__class__.__name__,
            )
        self.log = logging.getLogger(logging_class)

        _log = logging.getLogger("requests.packages.urllib3.connectionpool")
        _log.setLevel(logging.WARN)

        # possibly overridden in self.prepare method
        self.visit_date: Optional[datetime.datetime] = None
        self.origin: Optional[Origin] = None

        if not hasattr(self, "visit_type"):
            self.visit_type: Optional[str] = None

        self.origin_metadata: Dict[str, Any] = {}
        self.loaded_snapshot_id: Optional[Sha1Git] = None

        if save_data_path:
            path = save_data_path
            os.stat(path)
            if not os.access(path, os.R_OK | os.W_OK):
                raise PermissionError("Permission denied: %r" % path)

        self.save_data_path = save_data_path

    @classmethod
    def from_config(cls, storage: Dict[str, Any], **config: Any):
        """Instantiate a loader from a configuration dict.

        This is basically a backwards-compatibility shim for the CLI.

        Args:
          storage: instantiation config for the storage
          config: the configuration dict for the loader, with the following keys:
            - credentials (optional): credentials list for the scheduler
            - any other kwargs passed to the loader.

        Returns:
          the instantiated loader
        """
        # Drop the legacy config keys which aren't used for this generation of loader.
        for legacy_key in ("storage", "celery"):
            config.pop(legacy_key, None)

        # Instantiate the storage
        storage_instance = get_storage(**storage)
        return cls(storage=storage_instance, **config)

    @classmethod
    def from_configfile(cls, **kwargs: Any):
        """Instantiate a loader from the configuration loaded from the
        SWH_CONFIG_FILENAME envvar, with potential extra keyword arguments if their
        value is not None.

        Args:
            kwargs: kwargs passed to the loader instantiation

        """
        config = dict(load_from_envvar(DEFAULT_CONFIG))
        config.update({k: v for k, v in kwargs.items() if v is not None})
        return cls.from_config(**config)

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
                self.save_data_path,
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

    def cleanup(self) -> None:
        """Last step executed by the loader.

        """
        raise NotImplementedError

    def prepare_origin_visit(self) -> None:
        """First step executed by the loader to prepare origin and visit
           references. Set/update self.origin, and
           optionally self.origin_url, self.visit_date.

        """
        raise NotImplementedError

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

    def prepare(self) -> None:
        """Second step executed by the loader to prepare some state needed by
           the loader.

        Raises
           NotFound exception if the origin to ingest is not found.

        """
        raise NotImplementedError

    def get_origin(self) -> Origin:
        """Get the origin that is currently being loaded.
        self.origin should be set in :func:`prepare_origin`

        Returns:
          dict: an origin ready to be sent to storage by
          :func:`origin_add`.
        """
        assert self.origin
        return self.origin

    def fetch_data(self) -> bool:
        """Fetch the data from the source the loader is currently loading
           (ex: git/hg/svn/... repository).

        Returns:
            a value that is interpreted as a boolean. If True, fetch_data needs
            to be called again to complete loading.

        """
        raise NotImplementedError

    def store_data(self):
        """Store fetched data in the database.

        Should call the :func:`maybe_load_xyz` methods, which handle the
        bundles sent to storage, rather than send directly.
        """
        raise NotImplementedError

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

    def load(self) -> Dict[str, str]:
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

        self.prepare_origin_visit()
        self._store_origin_visit()

        assert (
            self.origin
        ), "The method `prepare_origin_visit` call should set the origin (Origin)"
        assert (
            self.visit.visit
        ), "The method `_store_origin_visit` should set the visit (OriginVisit)"
        self.log.info(
            "Load origin '%s' with type '%s'", self.origin.url, self.visit.type
        )

        try:
            self.prepare()

            while True:
                more_data_to_fetch = self.fetch_data()
                self.store_data()
                if not more_data_to_fetch:
                    break

            self.store_metadata()
            visit_status = OriginVisitStatus(
                origin=self.origin.url,
                visit=self.visit.visit,
                type=self.visit_type,
                date=now(),
                status=self.visit_status(),
                snapshot=self.loaded_snapshot_id,
            )
            self.storage.origin_visit_status_add([visit_status])
            self.post_load()
        except Exception as e:
            if isinstance(e, NotFound):
                status = "not_found"
                task_status = "uneventful"
            else:
                status = "partial" if self.loaded_snapshot_id else "failed"
                task_status = "failed"

            self.log.exception(
                "Loading failure, updating to `%s` status",
                status,
                extra={
                    "swh_task_args": [],
                    "swh_task_kwargs": {
                        "origin": self.origin.url
                    },
                },
            )
            visit_status = OriginVisitStatus(
                origin=self.origin.url,
                visit=self.visit.visit,
                type=self.visit_type,
                date=now(),
                status=status,
                snapshot=self.loaded_snapshot_id,
            )
            self.storage.origin_visit_status_add([visit_status])
            self.post_load(success=False)
            return {"status": task_status}
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
