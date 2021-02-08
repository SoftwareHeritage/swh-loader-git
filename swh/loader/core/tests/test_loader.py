# Copyright (C) 2018-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import hashlib
import logging

from swh.loader.core.loader import BaseLoader, DVCSLoader
from swh.loader.exception import NotFound
from swh.loader.tests import assert_last_visit_matches
from swh.model.hashutil import hash_to_bytes
from swh.model.model import Origin, OriginVisit, Snapshot

ORIGIN = Origin(url="some-url")


class DummyLoader:
    """Base Loader to overload and simplify the base class (technical: to avoid repetition
       in other *Loader classes)"""

    def cleanup(self):
        pass

    def prepare(self, *args, **kwargs):
        pass

    def fetch_data(self):
        pass

    def get_snapshot_id(self):
        return None

    def prepare_origin_visit(self, *args, **kwargs):
        self.origin = ORIGIN
        self.origin_url = ORIGIN.url
        self.visit_date = datetime.datetime.now(tz=datetime.timezone.utc)
        self.visit_type = "git"
        self.storage.origin_add([ORIGIN])
        visit = OriginVisit(
            origin=self.origin_url, date=self.visit_date, type=self.visit_type,
        )
        self.visit = self.storage.origin_visit_add([visit])[0]


class DummyDVCSLoader(DummyLoader, DVCSLoader):
    """DVCS Loader that does nothing in regards to DAG objects.

    """

    def get_contents(self):
        return []

    def get_directories(self):
        return []

    def get_revisions(self):
        return []

    def get_releases(self):
        return []

    def get_snapshot(self):
        return Snapshot(branches={})

    def eventful(self):
        return False


class DummyBaseLoader(DummyLoader, BaseLoader):
    """Buffered loader will send new data when threshold is reached

    """

    def store_data(self):
        pass


def test_base_loader(swh_storage):
    loader = DummyBaseLoader(swh_storage)
    result = loader.load()
    assert result == {"status": "eventful"}


def test_base_loader_with_config(swh_storage):
    loader = DummyBaseLoader(swh_storage, "logger-name")
    result = loader.load()
    assert result == {"status": "eventful"}


def test_dvcs_loader(swh_storage):
    loader = DummyDVCSLoader(swh_storage)
    result = loader.load()
    assert result == {"status": "eventful"}


def test_dvcs_loader_with_config(swh_storage):
    loader = DummyDVCSLoader(swh_storage, "another-logger")
    result = loader.load()
    assert result == {"status": "eventful"}


def test_loader_logger_default_name(swh_storage):
    loader = DummyBaseLoader(swh_storage)
    assert isinstance(loader.log, logging.Logger)
    assert loader.log.name == "swh.loader.core.tests.test_loader.DummyBaseLoader"

    loader = DummyDVCSLoader(swh_storage)
    assert isinstance(loader.log, logging.Logger)
    assert loader.log.name == "swh.loader.core.tests.test_loader.DummyDVCSLoader"


def test_loader_logger_with_name(swh_storage):
    loader = DummyBaseLoader(swh_storage, "some.logger.name")
    assert isinstance(loader.log, logging.Logger)
    assert loader.log.name == "some.logger.name"


def test_loader_save_data_path(swh_storage, tmp_path):
    loader = DummyBaseLoader(swh_storage, "some.logger.name.1", save_data_path=tmp_path)
    url = "http://bitbucket.org/something"
    loader.origin = Origin(url=url)
    loader.visit_date = datetime.datetime(year=2019, month=10, day=1)

    hash_url = hashlib.sha1(url.encode("utf-8")).hexdigest()
    expected_save_path = "%s/sha1:%s/%s/2019" % (str(tmp_path), hash_url[0:2], hash_url)

    save_path = loader.get_save_data_path()
    assert save_path == expected_save_path


def _check_load_failure(caplog, loader, exc_class, exc_text, status="partial"):
    """Check whether a failed load properly logged its exception, and that the
    snapshot didn't get referenced in storage"""
    assert isinstance(loader, DVCSLoader)  # was implicit so far
    for record in caplog.records:
        if record.levelname != "ERROR":
            continue
        assert "Loading failure" in record.message
        assert record.exc_info
        exc = record.exc_info[1]
        assert isinstance(exc, exc_class)
        assert exc_text in exc.args[0]

    # Check that the get_snapshot operation would have succeeded
    assert loader.get_snapshot() is not None

    # And confirm that the visit doesn't reference a snapshot
    visit = assert_last_visit_matches(loader.storage, ORIGIN.url, status)
    if status != "partial":
        assert visit.snapshot is None
        # But that the snapshot didn't get loaded
        assert loader.loaded_snapshot_id is None


class DummyDVCSLoaderExc(DummyDVCSLoader):
    """A loader which raises an exception when loading some contents"""

    def get_contents(self):
        raise RuntimeError("Failed to get contents!")


def test_dvcs_loader_exc_partial_visit(swh_storage, caplog):
    logger_name = "dvcsloaderexc"
    caplog.set_level(logging.ERROR, logger=logger_name)

    loader = DummyDVCSLoaderExc(swh_storage, logging_class=logger_name)
    # fake the loading ending up in a snapshot
    loader.loaded_snapshot_id = hash_to_bytes(
        "9e4dd2b40d1b46b70917c0949aa2195c823a648e"
    )
    result = loader.load()

    # loading failed
    assert result == {"status": "failed"}

    # still resulted in a partial visit with a snapshot (somehow)
    _check_load_failure(
        caplog, loader, RuntimeError, "Failed to get contents!",
    )


class BrokenStorageProxy:
    def __init__(self, storage):
        self.storage = storage

    def __getattr__(self, attr):
        return getattr(self.storage, attr)

    def snapshot_add(self, snapshots):
        raise RuntimeError("Failed to add snapshot!")


class DummyDVCSLoaderStorageExc(DummyDVCSLoader):
    """A loader which raises an exception when loading some contents"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage = BrokenStorageProxy(self.storage)


def test_dvcs_loader_storage_exc_failed_visit(swh_storage, caplog):
    logger_name = "dvcsloaderexc"
    caplog.set_level(logging.ERROR, logger=logger_name)

    loader = DummyDVCSLoaderStorageExc(swh_storage, logging_class=logger_name)
    result = loader.load()

    assert result == {"status": "failed"}

    _check_load_failure(
        caplog, loader, RuntimeError, "Failed to add snapshot!", status="failed"
    )


class DummyDVCSLoaderNotFound(DummyDVCSLoader, BaseLoader):
    """A loader which raises a not_found exception during the prepare method call

    """

    def prepare(*args, **kwargs):
        raise NotFound("Unknown origin!")

    def load_status(self):
        return {
            "status": "uneventful",
        }


def test_loader_not_found(swh_storage, caplog):
    loader = DummyDVCSLoaderNotFound(swh_storage)
    result = loader.load()

    assert result == {"status": "uneventful"}

    _check_load_failure(caplog, loader, NotFound, "Unknown origin!", status="not_found")
