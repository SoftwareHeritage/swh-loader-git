# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import hashlib
import logging
import pytest

from swh.model.model import Origin

from swh.loader.core.loader import BaseLoader, DVCSLoader


class DummyLoader:
    def cleanup(self):
        pass

    def prepare(self, *args, **kwargs):
        pass

    def fetch_data(self):
        pass

    def store_data(self):
        pass

    def prepare_origin_visit(self, *args, **kwargs):
        origin = Origin(url='some-url')
        self.origin = origin
        self.origin_url = origin.url
        self.visit_date = datetime.datetime.utcnow()
        self.visit_type = 'git'
        origin_url = self.storage.origin_add_one(origin)
        self.visit = self.storage.origin_visit_add(
            origin_url, self.visit_date, self.visit_type)


class DummyDVCSLoader(DummyLoader, DVCSLoader):
    """Unbuffered loader will send directly to storage new data

    """
    def parse_config_file(self, *args, **kwargs):
        return {
            'max_content_size': 100 * 1024 * 1024,
            'storage': {
                'cls': 'pipeline',
                'steps': [
                    {
                        'cls': 'retry',
                    },
                    {
                        'cls': 'filter',
                    },
                    {
                        'cls': 'memory',
                    },
                ]
            },
        }


class DummyBaseLoader(DummyLoader, BaseLoader):
    """Buffered loader will send new data when threshold is reached

    """
    def parse_config_file(self, *args, **kwargs):
        return {
            'max_content_size': 100 * 1024 * 1024,
            'storage': {
                'cls': 'pipeline',
                'steps': [
                    {
                        'cls': 'retry',
                    },
                    {
                        'cls': 'filter',
                    },
                    {
                        'cls': 'buffer',
                        'min_batch_size': {
                            'content': 2,
                            'content_bytes': 8,
                            'directory': 2,
                            'revision': 2,
                            'release': 2,
                        },
                    },
                    {
                        'cls': 'memory',
                    },
                ]
            },
        }


def test_base_loader():
    loader = DummyBaseLoader()
    result = loader.load()

    assert result == {'status': 'eventful'}


def test_dvcs_loader():
    loader = DummyDVCSLoader()
    result = loader.load()
    assert result == {'status': 'eventful'}


def test_loader_logger_default_name():
    loader = DummyBaseLoader()
    assert isinstance(loader.log, logging.Logger)
    assert loader.log.name == \
        'swh.loader.core.tests.test_loader.DummyBaseLoader'

    loader = DummyDVCSLoader()
    assert isinstance(loader.log, logging.Logger)
    assert loader.log.name == \
        'swh.loader.core.tests.test_loader.DummyDVCSLoader'


def test_loader_logger_with_name():
    loader = DummyBaseLoader('some.logger.name')
    assert isinstance(loader.log, logging.Logger)
    assert loader.log.name == \
        'some.logger.name'


@pytest.mark.fs
def test_loader_save_data_path(tmp_path):
    loader = DummyBaseLoader('some.logger.name.1')
    url = 'http://bitbucket.org/something'
    loader.origin = Origin(url=url)
    loader.visit_date = datetime.datetime(year=2019, month=10, day=1)
    loader.config = {
        'save_data_path': tmp_path,
    }

    hash_url = hashlib.sha1(url.encode('utf-8')).hexdigest()
    expected_save_path = '%s/sha1:%s/%s/2019' % (
        str(tmp_path), hash_url[0:2], hash_url
    )

    save_path = loader.get_save_data_path()
    assert save_path == expected_save_path
