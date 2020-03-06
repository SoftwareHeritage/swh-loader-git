# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest
import yaml

from typing import Any, Dict

from swh.storage.tests.conftest import * # noqa
from swh.scheduler.tests.conftest import *  # noqa


@pytest.fixture
def swh_loader_config(swh_storage_postgresql) -> Dict[str, Any]:
    return {
        'storage': {
            'cls': 'pipeline',
            'steps': [
                {'cls': 'retry'},
                {'cls': 'filter'},
                {'cls': 'buffer'},
                {
                    'cls': 'local',
                    'args': {
                        'db': swh_storage_postgresql.dsn,
                        'objstorage': {
                            'cls': 'memory',
                            'args': {}
                        },
                    }
                }
            ]
        },
        'deposit': {
            'url': 'https://deposit.softwareheritage.org/1/private',
            'auth': {
                'username': 'user',
                'password': 'pass',
            }
        },
    }


@pytest.fixture
def swh_config(swh_loader_config, monkeypatch, tmp_path):
    conffile = os.path.join(str(tmp_path), 'loader.yml')
    with open(conffile, 'w') as f:
        f.write(yaml.dump(swh_loader_config))
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
        'swh.loader.package.archive.tasks',
        'swh.loader.package.cran.tasks',
        'swh.loader.package.debian.tasks',
        'swh.loader.package.deposit.tasks',
        'swh.loader.package.npm.tasks',
        'swh.loader.package.pypi.tasks',
        'swh.loader.package.functional.tasks',
    ]
