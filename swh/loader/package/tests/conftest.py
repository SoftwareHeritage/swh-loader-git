# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import re

from functools import partial
from os import path

from swh.core.pytest_plugin import get_response_cb


@pytest.fixture
def swh_config(monkeypatch, datadir):
    conffile = path.join(datadir, 'loader.yml')
    monkeypatch.setenv('SWH_CONFIG_FILENAME', conffile)
    return conffile


@pytest.fixture
def requests_mock_http_datadir(requests_mock_datadir, datadir):
    # hack: main fixture does not support http query yet
    requests_mock_datadir.get(re.compile('http://'), body=partial(
        get_response_cb, datadir=datadir))

    return requests_mock_datadir
