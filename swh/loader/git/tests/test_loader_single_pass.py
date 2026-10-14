# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from .test_loader import TestGitLoader as _TestGitLoader


@pytest.fixture
def extra_loader_arguments():
    return {"store_order": "as_origin"}


class TestGitLoaderSinglePass(_TestGitLoader):
    """Same as :class:``TestGitLoader``, but configures ``store_order = "as_origin"``"""

    def test_load_visit_without_snapshot_so_status_failed(self):
        self.loader.get_objects = None

        super().test_load_visit_without_snapshot_so_status_failed()

    def test_metrics(self, mocker):
        super().test_metrics(
            mocker, object_type_order=["revision", "directory", "content", "snapshot"]
        )

    def test_metrics_filtered(self, mocker):
        super().test_metrics_filtered(
            mocker, object_type_order=["revision", "directory", "content", "snapshot"]
        )
