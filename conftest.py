# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

pytest_plugins = [
    "swh.scheduler.pytest_plugin",
    "swh.storage.pytest_plugin",
    "swh.loader.pytest_plugin",
]


@pytest.fixture(scope="session")
def swh_scheduler_celery_includes(swh_scheduler_celery_includes):
    return swh_scheduler_celery_includes + [
        "swh.loader.package.archive.tasks",
        "swh.loader.package.cran.tasks",
        "swh.loader.package.debian.tasks",
        "swh.loader.package.deposit.tasks",
        "swh.loader.package.npm.tasks",
        "swh.loader.package.pypi.tasks",
        "swh.loader.package.nixguix.tasks",
    ]
