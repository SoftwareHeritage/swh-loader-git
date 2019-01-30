import pytest

from swh.scheduler.tests.conftest import *  # noqa


@pytest.fixture(scope='session')
def celery_includes():
    return [
        'swh.loader.git.tasks',
    ]
