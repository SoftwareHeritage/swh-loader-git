import os
import pytest

from .common import DATADIR


@pytest.fixture
def swh_config(monkeypatch):
    conffile = os.path.join(DATADIR, 'loader.yml')
    monkeypatch.setenv('SWH_CONFIG_FILENAME', conffile)
    return conffile
