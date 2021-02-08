# Copyright (C) 2019-2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict

import pytest


@pytest.fixture
def swh_loader_config() -> Dict[str, Any]:
    return {
        "storage": {"cls": "memory",},
        "deposit": {
            "url": "https://deposit.softwareheritage.org/1/private",
            "auth": {"username": "user", "password": "pass",},
        },
    }
