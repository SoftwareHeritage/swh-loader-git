# Copyright (C) 2018-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import pytest

from swh.loader.tests import assert_module_tasks_are_scheduler_ready
from swh.scheduler.model import ListedOrigin

from .conftest import NAMESPACE


def test_tasks_loader_visit_type_match_task_name():
    import swh.loader.git

    assert_module_tasks_are_scheduler_ready([swh.loader.git])


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


def test_git_loader_from_archive_for_listed_origin(
    loading_task_creation_for_listed_origin_test,
    git_lister,
    git_listed_origin,
):
    git_listed_origin.extra_loader_arguments = {
        "archive_path": "/some/repo",
    }

    loading_task_creation_for_listed_origin_test(
        loader_class_name=f"{NAMESPACE}.from_disk.GitLoaderFromArchive",
        task_function_name=f"{NAMESPACE}.tasks.UncompressAndLoadDiskGitRepository",
        lister=git_lister,
        listed_origin=git_listed_origin,
    )
