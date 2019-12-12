# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.scheduler.tests.conftest import *  # noqa


@pytest.fixture(scope='session')  # type: ignore  # expected redefinition
def celery_includes():
    return [
        'swh.loader.git.tasks',
    ]
