# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest


@pytest.fixture
def swh_config(monkeypatch, datadir):
    conffile = os.path.join(datadir, 'loader.yml')
    monkeypatch.setenv('SWH_CONFIG_FILENAME', conffile)
    return conffile


@pytest.fixture(autouse=True, scope='session')
def swh_proxy():
    """Automatically inject this fixture in all tests to ensure no outside
       connection takes place.

    """
    os.environ['http_proxy'] = 'http://localhost:999'
    os.environ['https_proxy'] = 'http://localhost:999'
