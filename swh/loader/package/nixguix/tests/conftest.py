# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict

import pytest


@pytest.fixture
def swh_loader_config(swh_storage_backend_config) -> Dict[str, Any]:
    # nixguix loader needs a pg-storage backend because some tests share data
    return {
        "storage": swh_storage_backend_config,
        "unsupported_file_extensions": [
            "patch",
            "iso",
            "whl",
            "gem",
            "pom",
            "msi",
            "pod",
            "png",
            "rock",
            "ttf",
            "jar",
            "c",
            "el",
            "rpm",
            "diff",
        ],
    }
