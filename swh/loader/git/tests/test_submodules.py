# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from contextlib import contextmanager
import os
import shutil
from textwrap import dedent

from dulwich.repo import Repo

from swh.loader.git.loader import GitLoader
from swh.loader.tests import get_stats


@contextmanager
def working_directory(path):
    prev_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


def test_loader_load_submodules_recursively(swh_storage, swh_scheduler, tmp_path):
    """Create three sample git repos: main, submodule and subsubmodule:

        * main has a submodule targeting the submodule repo
        * submodule has a submodule targeting the subsubmodule repo
        * subsubmodule has a submodule targeting the main repo (to test cycles handling)

    After loading a repo, submodules loading tasks should have been created in the
    scheduler to archive the associated origins.
    """
    author = b"swh <swh@example.org>"

    main_repo_name = "main"
    main_repo_path = os.path.join(tmp_path, main_repo_name)
    main_repo_url = f"file://{main_repo_path}"

    submodule_repo_name = "submodule"
    submodule_repo_path = os.path.join(tmp_path, submodule_repo_name)
    submodule_repo_url = f"file://{submodule_repo_path}"

    subsubmodule_repo_name = "subsubmodule_repo"
    subsubmodule_repo_path = os.path.join(tmp_path, subsubmodule_repo_name)
    subsubmodule_repo_url = f"file://{subsubmodule_repo_path}"

    # create main repo
    main_repo = Repo.init(main_repo_path, mkdir=True)
    with working_directory(main_repo_path):
        # add foo.sh file
        with open("foo.sh", "wb") as new_file:
            new_file.write(b"#!/bin/bash\n echo foo")
        # add .gitmodules file as created by "git submodule add"
        with open(".gitmodules", "w") as new_file:
            submodules = dedent(
                f"""[submodule "{submodule_repo_name}"]
                    path = {submodule_repo_name}
                    url = {submodule_repo_url}
                """
            )
            new_file.write(submodules)

    # create submodule repo
    submodule_repo = Repo.init(submodule_repo_path, mkdir=True)
    with working_directory(submodule_repo_path):
        # add bar.sh file
        with open("bar.sh", "wb") as new_file:
            new_file.write(b"#!/bin/bash\n echo bar")
        # add .gitmodules file as created by "git submodule add"
        with open(".gitmodules", "w") as new_file:
            submodules = dedent(
                f"""[submodule "{subsubmodule_repo_name}"]
                    path = {subsubmodule_repo_name}
                    url = ../{subsubmodule_repo_name}
                """  # we use a relative URL for the submodule here
            )
            new_file.write(submodules)

    # create subsubmodule repo
    subsubmodule_repo = Repo.init(subsubmodule_repo_path, mkdir=True)
    with working_directory(subsubmodule_repo_path):
        # add baz.sh file
        with open("baz.sh", "wb") as new_file:
            new_file.write(b"#!/bin/bash\n echo baz")

    # stage baz.sh file in subsubmodule repo
    subsubmodule_repo.stage([b"baz.sh"])
    # add commit to subsubmodule repo
    subsubmodule_repo.do_commit(
        message=b"Initial commit", author=author, committer=author
    )

    # copy subsubmodule repo in submodule repo (simulate "git submodule add")
    shutil.copytree(
        subsubmodule_repo_path,
        os.path.join(submodule_repo_path, subsubmodule_repo_name),
    )
    # stage bar.sh file, .gitmodules file and subsubmodule submodule in submodule repo
    submodule_repo.stage([b"bar.sh", b".gitmodules", subsubmodule_repo_name.encode()])
    # add commit to submodule repo
    submodule_repo.do_commit(message=b"Initial commit", author=author, committer=author)

    # copy submodule repo in main repo (simulate "git submodule add")
    shutil.copytree(
        submodule_repo_path, os.path.join(main_repo_path, submodule_repo_name),
    )
    # stage foo.sh file, .gitmodules file and submodule submodule in main repo
    main_repo.stage([b"foo.sh", b".gitmodules", submodule_repo_name.encode()])
    # add commit to main repo
    main_repo.do_commit(message=b"Initial commit", author=author, committer=author)

    # update subsubmodule repo
    with working_directory(subsubmodule_repo_path):
        # add .gitmodules file as created by "git submodule add"
        with open(".gitmodules", "w") as new_file:
            submodules = dedent(
                f"""[submodule "{main_repo_name}"]
                    path = {main_repo_name}
                    url = {main_repo_url}
                """
            )
            new_file.write(submodules)
    # copy main repo in subsubmodule repo (simulate "git submodule add")
    shutil.copytree(
        main_repo_path, os.path.join(subsubmodule_repo_path, main_repo_name)
    )

    # stage .gitmodules file and main submodule in subsubmodule repo
    subsubmodule_repo.stage([b".gitmodules", main_repo_name.encode()])
    # add commit to subsubmodule repo
    subsubmodule_repo.do_commit(
        message=b"Add submodule targeting main repo", author=author, committer=author
    )

    def _load_origin_task_exists(origin_url):
        tasks = [
            dict(row.items())
            for row in swh_scheduler.search_tasks(task_type="load-git")
        ]

        return any(task["arguments"]["kwargs"]["url"] == origin_url for task in tasks)

    # load the main repo
    loader = GitLoader(swh_storage, main_repo_url, scheduler=swh_scheduler)
    assert loader.load() == {"status": "eventful"}

    # a task to load the submodule repo should have been created
    assert _load_origin_task_exists(submodule_repo_url)

    # load the submodule repo (simulate its scheduling)
    loader = GitLoader(swh_storage, submodule_repo_url, scheduler=swh_scheduler)
    assert loader.load() == {"status": "eventful"}

    # a task to load the subsubmodule repo should have been created
    assert _load_origin_task_exists(subsubmodule_repo_url)

    # load the subsubmodule repo (simulate its scheduling)
    loader = GitLoader(swh_storage, subsubmodule_repo_url, scheduler=swh_scheduler)
    assert loader.load() == {"status": "eventful"}

    # a task to load the main repo should not have been created
    assert not _load_origin_task_exists(main_repo_url)

    # check submodules have been loaded
    stats = get_stats(loader.storage)
    assert stats == {
        "content": 6,  # one bash file and one .gitmodules file in each repo
        "directory": 4,  # one directory for main and submodule repo,
        # two for the subsubmodule repo
        "origin": 3,  # three origins
        "origin_visit": 3,  # three visits
        "release": 0,  # no releases
        "revision": 4,  # one revision for main and submodule repo,
        # two for the subsubmodule repo
        "skipped_content": 0,
        "snapshot": 3,  # one snapshot per repo
    }
