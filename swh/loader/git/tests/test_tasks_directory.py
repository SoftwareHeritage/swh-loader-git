# Copyright (C) 2018-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import pytest

from swh.scheduler.model import ListedOrigin

from .conftest import NAMESPACE


@pytest.fixture
def git_listed_origin(git_lister):
    return ListedOrigin(
        lister_id=git_lister.id,
        url="https://git.example.org/repo",
        visit_type="git-checkout",
    )


@pytest.mark.parametrize(
    "extra_loader_arguments",
    [
        {"checksum_layout": "nar", "checksums": {}, "ref": "master"},
        {"checksum_layout": "standard", "checksums": {}, "ref": "v1.2.0"},
    ],
)
def test_git_directory_loader_for_listed_origin(
    loading_task_creation_for_listed_origin_test,
    git_lister,
    git_listed_origin,
    extra_loader_arguments,
):
    git_listed_origin.extra_loader_arguments = extra_loader_arguments

    loading_task_creation_for_listed_origin_test(
        loader_class_name=f"{NAMESPACE}.directory.GitCheckoutLoader",
        task_function_name=f"{NAMESPACE}.tasks.LoadGitCheckout",
        lister=git_lister,
        listed_origin=git_listed_origin,
    )
