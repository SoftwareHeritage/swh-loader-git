# Copyright (C) 2018-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict
import uuid

import pytest

from swh.scheduler.model import Lister

NAMESPACE = "swh.loader.git"


@pytest.fixture
def git_lister():
    return Lister(name="git-lister", instance_name="example", id=uuid.uuid4())


@pytest.fixture
def swh_storage_backend_config(swh_storage_backend_config):
    """Basic pg storage configuration with no journal collaborator
    (to avoid pulling optional dependency on clients of this fixture)

    """
    return {
        "cls": "filter",
        "storage": {
            "cls": "buffer",
            "min_batch_size": {
                "content": 10,
                "content_bytes": 100 * 1024 * 1024,
                "directory": 10,
                "revision": 10,
                "release": 10,
            },
            "storage": swh_storage_backend_config,
        },
    }


@pytest.fixture
def swh_loader_config(swh_storage_backend_config) -> Dict[str, Any]:
    return {
        "storage": swh_storage_backend_config,
        "max_content_size": 100 * 1024 * 1024,
        "save_data_path": None,
    }
