# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import call

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
        total_sum_name = "filtered_objects_total_sum"
        total_count_name = "filtered_objects_total_count"
        percent_name = "filtered_objects_percent"
        super().test_metrics(
            mocker,
            expected_statsd_calls=[
                call(percent_name, "h", 0.0, {"object_type": "revision"}, 1),
                call(total_sum_name, "c", 0, {"object_type": "revision"}, 1),
                call(total_count_name, "c", 7, {"object_type": "revision"}, 1),
                call(percent_name, "h", 0.0, {"object_type": "directory"}, 1),
                call(total_sum_name, "c", 0, {"object_type": "directory"}, 1),
                call(total_count_name, "c", 7, {"object_type": "directory"}, 1),
                call(percent_name, "h", 0.0, {"object_type": "content"}, 1),
                call(total_sum_name, "c", 0, {"object_type": "content"}, 1),
                call(total_count_name, "c", 4, {"object_type": "content"}, 1),
                call(percent_name, "h", 0.0, {"object_type": "snapshot"}, 1),
                call(total_sum_name, "c", 0, {"object_type": "snapshot"}, 1),
                call(total_count_name, "c", 1, {"object_type": "snapshot"}, 1),
            ],
        )

    def test_metrics_filtered(self, mocker):
        total_sum_name = "filtered_objects_total_sum"
        total_count_name = "filtered_objects_total_count"
        percent_name = "filtered_objects_percent"
        super().test_metrics_filtered(
            mocker,
            expected_statsd_calls=[
                call(percent_name, "h", 2 / 7, {"object_type": "revision"}, 1),
                call(total_sum_name, "c", 2, {"object_type": "revision"}, 1),
                call(total_count_name, "c", 7, {"object_type": "revision"}, 1),
                call(percent_name, "h", 3 / 7, {"object_type": "directory"}, 1),
                call(total_sum_name, "c", 3, {"object_type": "directory"}, 1),
                call(total_count_name, "c", 7, {"object_type": "directory"}, 1),
                call(percent_name, "h", 1 / 4, {"object_type": "content"}, 1),
                call(total_sum_name, "c", 1, {"object_type": "content"}, 1),
                call(total_count_name, "c", 4, {"object_type": "content"}, 1),
                call(percent_name, "h", 0.0, {"object_type": "snapshot"}, 1),
                call(total_sum_name, "c", 0, {"object_type": "snapshot"}, 1),
                call(total_count_name, "c", 1, {"object_type": "snapshot"}, 1),
            ],
        )
