# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import hashlib
import logging

from swh.model.model import Origin, OriginVisit, Snapshot

from swh.loader.core.loader import BaseLoader, DVCSLoader
from swh.loader.tests import assert_last_visit_matches


ORIGIN = Origin(url="some-url")


class DummyLoader:
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
    """Unbuffered loader will send directly to storage new data

    """

    def parse_config_file(self, *args, **kwargs):
        return {
            "max_content_size": 100 * 1024 * 1024,
            "storage": {
                "cls": "pipeline",
                "steps": [{"cls": "retry",}, {"cls": "filter",}, {"cls": "memory",},],
            },
        }

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

    def parse_config_file(self, *args, **kwargs):
        return {
            "max_content_size": 100 * 1024 * 1024,
            "storage": {
                "cls": "pipeline",
                "steps": [
                    {"cls": "retry",},
                    {"cls": "filter",},
                    {
                        "cls": "buffer",
                        "min_batch_size": {
                            "content": 2,
                            "content_bytes": 8,
                            "directory": 2,
                            "revision": 2,
                            "release": 2,
                        },
                    },
                    {"cls": "memory",},
                ],
            },
        }

    def store_data(self):
        pass


def test_base_loader():
    loader = DummyBaseLoader()
    result = loader.load()

    assert result == {"status": "eventful"}


def test_dvcs_loader():
    loader = DummyDVCSLoader()
    result = loader.load()
    assert result == {"status": "eventful"}


def test_loader_logger_default_name():
    loader = DummyBaseLoader()
    assert isinstance(loader.log, logging.Logger)
    assert loader.log.name == "swh.loader.core.tests.test_loader.DummyBaseLoader"

    loader = DummyDVCSLoader()
    assert isinstance(loader.log, logging.Logger)
    assert loader.log.name == "swh.loader.core.tests.test_loader.DummyDVCSLoader"


def test_loader_logger_with_name():
    loader = DummyBaseLoader("some.logger.name")
    assert isinstance(loader.log, logging.Logger)
    assert loader.log.name == "some.logger.name"


def test_loader_save_data_path(tmp_path):
    loader = DummyBaseLoader("some.logger.name.1")
    url = "http://bitbucket.org/something"
    loader.origin = Origin(url=url)
    loader.visit_date = datetime.datetime(year=2019, month=10, day=1)
    loader.config = {
        "save_data_path": tmp_path,
    }

    hash_url = hashlib.sha1(url.encode("utf-8")).hexdigest()
    expected_save_path = "%s/sha1:%s/%s/2019" % (str(tmp_path), hash_url[0:2], hash_url)

    save_path = loader.get_save_data_path()
    assert save_path == expected_save_path


def _check_load_failure(caplog, loader, exc_class, exc_text):
    """Check whether a failed load properly logged its exception, and that the
    snapshot didn't get referenced in storage"""
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

    # But that the snapshot didn't get loaded
    assert loader.loaded_snapshot_id is None

    # And confirm that the visit doesn't reference a snapshot
    visit = assert_last_visit_matches(loader.storage, ORIGIN.url, status="partial")
    assert visit.snapshot is None


class DummyDVCSLoaderExc(DummyDVCSLoader):
    """A loader which raises an exception when loading some contents"""

    def get_contents(self):
        raise RuntimeError("Failed to get contents!")


def test_dvcs_loader_exc_partial_visit(caplog):
    logger_name = "dvcsloaderexc"
    caplog.set_level(logging.ERROR, logger=logger_name)

    loader = DummyDVCSLoaderExc(logging_class=logger_name)
    result = loader.load()

    assert result == {"status": "failed"}

    _check_load_failure(caplog, loader, RuntimeError, "Failed to get contents!")


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


def test_dvcs_loader_storage_exc_partial_visit(caplog):
    logger_name = "dvcsloaderexc"
    caplog.set_level(logging.ERROR, logger=logger_name)

    loader = DummyDVCSLoaderStorageExc(logging_class=logger_name)
    result = loader.load()

    assert result == {"status": "failed"}

    _check_load_failure(caplog, loader, RuntimeError, "Failed to add snapshot!")
