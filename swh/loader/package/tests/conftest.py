import os
import re
import pytest

from .common import DATADIR, get_response_cb


@pytest.fixture
def swh_config(monkeypatch):
    conffile = os.path.join(DATADIR, 'loader.yml')
    monkeypatch.setenv('SWH_CONFIG_FILENAME', conffile)
    return conffile


@pytest.fixture
def local_get(requests_mock):
    requests_mock.get(re.compile('https://'), body=get_response_cb)
    return requests_mock
