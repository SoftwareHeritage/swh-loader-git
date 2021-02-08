# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
from typing import Any, Dict

import pytest

from swh.loader.package.deposit.loader import ApiClient


@pytest.fixture
def swh_loader_config(swh_loader_config) -> Dict[str, Any]:
    config = copy.deepcopy(swh_loader_config)
    config.update(
        {
            "deposit": {
                "url": "https://deposit.softwareheritage.org/1/private",
                "auth": {"username": "user", "password": "pass",},
            },
        }
    )
    return config


@pytest.fixture
def deposit_client(swh_loader_config):
    return ApiClient(**swh_loader_config["deposit"])
