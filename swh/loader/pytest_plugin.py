# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Any, Dict

import pytest
import yaml


@pytest.fixture
def swh_storage_backend_config(swh_storage_postgresql) -> Dict[str, Any]:
    return {
        "cls": "retry",
        "storage": {
            "cls": "filter",
            "storage": {
                "cls": "buffer",
                "storage": {
                    "cls": "local",
                    "db": swh_storage_postgresql.dsn,
                    "objstorage": {"cls": "memory"},
                },
            },
        },
    }


@pytest.fixture
def swh_loader_config(swh_storage_backend_config) -> Dict[str, Any]:
    return {
        "storage": swh_storage_backend_config,
    }


@pytest.fixture
def swh_config(swh_loader_config, monkeypatch, tmp_path) -> str:
    conffile = os.path.join(str(tmp_path), "loader.yml")
    with open(conffile, "w") as f:
        f.write(yaml.dump(swh_loader_config))
        monkeypatch.setenv("SWH_CONFIG_FILENAME", conffile)
    return conffile


@pytest.fixture(autouse=True, scope="session")
def swh_proxy():
    """Automatically inject this fixture in all tests to ensure no outside
       connection takes place.

    """
    os.environ["http_proxy"] = "http://localhost:999"
    os.environ["https_proxy"] = "http://localhost:999"
