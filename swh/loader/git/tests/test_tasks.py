# Copyright (C) 2018-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import uuid

import pytest

from swh.scheduler.model import ListedOrigin, Lister

NAMESPACE = "swh.loader.git"


@pytest.fixture
def git_lister():
    return Lister(name="git-lister", instance_name="example", id=uuid.uuid4())


@pytest.fixture
def git_listed_origin(git_lister):
    return ListedOrigin(
        lister_id=git_lister.id, url="https://git.example.org/repo", visit_type="git"
    )


def test_git_loader_for_listed_origin(
    loading_task_creation_for_listed_origin_test,
    git_lister,
    git_listed_origin,
):
    loading_task_creation_for_listed_origin_test(
        loader_class_name=f"{NAMESPACE}.loader.GitLoader",
        task_function_name=f"{NAMESPACE}.tasks.UpdateGitRepository",
        lister=git_lister,
        listed_origin=git_listed_origin,
    )


@pytest.mark.parametrize(
    "extra_loader_arguments",
    [
        {
            "directory": "/some/repo",
        },
        {
            "directory": "/some/repo",
            "visit_date": "now",
        },
    ],
)
def test_git_loader_from_disk_for_listed_origin(
    loading_task_creation_for_listed_origin_test,
    git_lister,
    git_listed_origin,
    extra_loader_arguments,
):

    git_listed_origin.extra_loader_arguments = extra_loader_arguments

    loading_task_creation_for_listed_origin_test(
        loader_class_name=f"{NAMESPACE}.from_disk.GitLoaderFromDisk",
        task_function_name=f"{NAMESPACE}.tasks.LoadDiskGitRepository",
        lister=git_lister,
        listed_origin=git_listed_origin,
    )


@pytest.mark.parametrize(
    "extra_loader_arguments",
    [
        {
            "archive_path": "/some/repo",
        },
        {
            "archive_path": "/some/repo",
            "visit_date": "now",
        },
    ],
)
def test_git_loader_from_archive_for_listed_origin(
    loading_task_creation_for_listed_origin_test,
    git_lister,
    git_listed_origin,
    extra_loader_arguments,
):

    git_listed_origin.extra_loader_arguments = extra_loader_arguments

    loading_task_creation_for_listed_origin_test(
        loader_class_name=f"{NAMESPACE}.from_disk.GitLoaderFromArchive",
        task_function_name=f"{NAMESPACE}.tasks.UncompressAndLoadDiskGitRepository",
        lister=git_lister,
        listed_origin=git_listed_origin,
    )
