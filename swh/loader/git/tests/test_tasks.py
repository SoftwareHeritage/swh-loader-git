# Copyright (C) 2018-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import uuid

import pytest

from swh.scheduler.model import ListedOrigin, Lister
from swh.scheduler.utils import create_origin_task_dict


@pytest.fixture(autouse=True)
def celery_worker_and_swh_config(swh_scheduler_celery_worker, swh_config):
    pass


@pytest.fixture
def git_lister():
    return Lister(name="git-lister", instance_name="example", id=uuid.uuid4())


@pytest.fixture
def git_listed_origin(git_lister):
    return ListedOrigin(
        lister_id=git_lister.id, url="https://git.example.org/repo", visit_type="git"
    )


def test_git_loader(
    mocker,
    swh_scheduler_celery_app,
):
    mock_loader = mocker.patch("swh.loader.git.loader.GitLoader.load")
    mock_loader.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.git.tasks.UpdateGitRepository",
        kwargs={"url": "origin_url"},
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}
    mock_loader.assert_called_once_with()


def test_git_loader_for_listed_origin(
    mocker,
    swh_scheduler_celery_app,
    git_lister,
    git_listed_origin,
):
    mock_loader = mocker.patch("swh.loader.git.loader.GitLoader.load")
    mock_loader.return_value = {"status": "eventful"}

    task_dict = create_origin_task_dict(git_listed_origin, git_lister)

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.git.tasks.UpdateGitRepository",
        kwargs=task_dict["arguments"]["kwargs"],
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}
    mock_loader.assert_called_once_with()


def test_git_loader_from_disk(
    mocker,
    swh_scheduler_celery_app,
):
    mock_loader = mocker.patch("swh.loader.git.from_disk.GitLoaderFromDisk.load")
    mock_loader.return_value = {"status": "uneventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.git.tasks.LoadDiskGitRepository",
        kwargs={"url": "origin_url2", "directory": "/some/repo", "visit_date": "now"},
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "uneventful"}
    mock_loader.assert_called_once_with()


def test_git_loader_from_disk_for_listed_origin(
    mocker,
    swh_scheduler_celery_app,
    git_lister,
    git_listed_origin,
):
    mock_loader = mocker.patch("swh.loader.git.from_disk.GitLoaderFromDisk.load")
    mock_loader.return_value = {"status": "uneventful"}

    git_listed_origin.extra_loader_arguments = {
        "directory": "/some/repo",
    }
    task_dict = create_origin_task_dict(git_listed_origin, git_lister)

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.git.tasks.LoadDiskGitRepository",
        kwargs=task_dict["arguments"]["kwargs"],
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "uneventful"}
    mock_loader.assert_called_once_with()


def test_git_loader_from_archive(
    mocker,
    swh_scheduler_celery_app,
):
    mock_loader = mocker.patch("swh.loader.git.from_disk.GitLoaderFromArchive.load")
    mock_loader.return_value = {"status": "failed"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.git.tasks.UncompressAndLoadDiskGitRepository",
        kwargs={
            "url": "origin_url3",
            "archive_path": "/some/repo",
            "visit_date": "now",
        },
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "failed"}
    mock_loader.assert_called_once_with()


def test_git_loader_from_archive_for_listed_origin(
    mocker,
    swh_scheduler_celery_app,
    git_lister,
    git_listed_origin,
):
    mock_loader = mocker.patch("swh.loader.git.from_disk.GitLoaderFromArchive.load")
    mock_loader.return_value = {"status": "failed"}

    git_listed_origin.extra_loader_arguments = {
        "archive_path": "/some/repo",
    }
    task_dict = create_origin_task_dict(git_listed_origin, git_lister)

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.git.tasks.UncompressAndLoadDiskGitRepository",
        kwargs=task_dict["arguments"]["kwargs"],
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "failed"}
    mock_loader.assert_called_once_with()
