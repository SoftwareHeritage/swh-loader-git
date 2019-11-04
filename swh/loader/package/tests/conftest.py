# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest
import yaml

from swh.storage.tests.conftest import * # noqa
from swh.scheduler.tests.conftest import *  # noqa


@pytest.fixture
def swh_config(monkeypatch, swh_storage_postgresql, tmp_path):
    storage_config = {
        'storage': {
            'cls': 'local',
            'args': {
                'db': swh_storage_postgresql.dsn,
                'objstorage': {
                    'cls': 'memory',
                    'args': {}
                },
            },
        },
        'url': 'https://deposit.softwareheritage.org/1/private',
    }

    conffile = os.path.join(str(tmp_path), 'loader.yml')
    with open(conffile, 'w') as f:
        f.write(yaml.dump(storage_config))
    monkeypatch.setenv('SWH_CONFIG_FILENAME', conffile)
    return conffile


@pytest.fixture(autouse=True, scope='session')
def swh_proxy():
    """Automatically inject this fixture in all tests to ensure no outside
       connection takes place.

    """
    os.environ['http_proxy'] = 'http://localhost:999'
    os.environ['https_proxy'] = 'http://localhost:999'


@pytest.fixture(scope='session')  # type: ignore  # expected redefinition
def celery_includes():
    return [
        'swh.loader.package.tasks',
    ]
