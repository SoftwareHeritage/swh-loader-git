# Copyright (C) 2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from os.path import basename, exists, join
from pathlib import Path
from shutil import which
from subprocess import PIPE, CalledProcessError, check_output
import tempfile
from typing import Any, Iterable, Iterator

from swh.loader.core.loader import BaseDirectoryLoader
from swh.loader.exception import NotFound
from swh.loader.git.utils import raise_not_found_repository
from swh.model.from_disk import ignore_empty_directories, ignore_named_directories
from swh.model.model import Snapshot, SnapshotBranch, SnapshotTargetType


def git() -> str:
    """Get the path to the git executable.

    Raises:
        EnvironmentError if no opam executable is found
    """
    ret = which("git")
    if not ret:
        raise EnvironmentError("No git executable found in path {os.environ['PATH']}")

    return ret


def checkout_repository_ref(git_url: str, git_ref: str, target: Path) -> Path:
    """Checkout the reference ``git_ref`` (commit, tag or branch) from git repository
    located at ``git_url``.

    This function can raise for various reasons. This is expected to be caught by the
    main loop in the loader.
    """

    local_name = basename(git_url.rstrip("/"))
    local_path = str(target / local_name)
    os.mkdir(local_path)

    def run_git_cmd(cmd):
        # ensure english output
        env = {"LC_ALL": "C"}
        check_output([git()] + cmd, cwd=local_path, env=env, stderr=PIPE)

    try:
        run_git_cmd(["init", "--initial-branch=main"])
        run_git_cmd(["remote", "add", "origin", git_url])
        try:
            run_git_cmd(["fetch", "--depth", "1", "origin", git_ref])
        except CalledProcessError:
            # shallow fetch failed, retry a full one
            run_git_cmd(["fetch", "-t", "origin"])
            run_git_cmd(["checkout", git_ref])
        else:
            run_git_cmd(["checkout", "FETCH_HEAD"])

    except CalledProcessError as cpe:
        if b"fatal: Could not read from remote repository" in cpe.stderr:
            raise NotFound(f"Repository <{git_url}> not found")
        raise

    return Path(local_path)


def list_git_tree(dirpath: bytes, dirname: bytes, entries: Iterable[Any]) -> bool:
    # def list_git_tree() -> Callable:
    """List a git tree. This ignores any repo_path/.git/* and empty folders. This is a
    filter for :func:`directory_to_objects` to ignore specific directories.

    """
    return ignore_named_directories([b".git"])(
        dirpath, dirname, entries
    ) and ignore_empty_directories(dirpath, dirname, entries)


class GitCheckoutLoader(BaseDirectoryLoader):
    """Git directory loader in charge of ingesting a git tree at a specific commit, tag
    or branch into the swh archive.

    As per the standard git hash computations, this ignores the .git and the empty
    directories.

    The output snapshot is of the form:

    .. code::

       id: <bytes>
       branches:
         HEAD:
           target_type: alias
           target: <git-ref>
         <git-ref>:
           target_type: directory
           target: <directory-id>

    """

    visit_type = "git-checkout"

    def __init__(self, *args, **kwargs):
        self.git_ref = kwargs.pop("ref")
        self.submodules = kwargs.pop("submodules", False)
        # We use a filter which ignore the .git folder and the empty git trees
        super().__init__(*args, path_filter=list_git_tree, **kwargs)

    def fetch_artifact(self) -> Iterator[Path]:
        with raise_not_found_repository():
            with tempfile.TemporaryDirectory() as tmpdir:
                repo_path = checkout_repository_ref(
                    self.origin.url, self.git_ref, target=Path(tmpdir)
                )
                if self.submodules:
                    local_clone = str(repo_path)
                    gitmodules_path = join(local_clone, ".gitmodules")
                    if exists(gitmodules_path):
                        with open(gitmodules_path, "r") as f:
                            gitmodules = f.read()
                        with open(gitmodules_path, "w") as f:
                            # replace no longer working github URLs using TCP protocol
                            f.write(
                                gitmodules.replace(
                                    "git://github.com/", "https://github.com/"
                                )
                            )
                        check_output(
                            [git(), "submodule", "update", "--init", "--recursive"],
                            cwd=local_clone,
                        )
                        # restore original .gitmodules file in case it was modified above
                        check_output([git(), "checkout", "."], cwd=local_clone)
                yield repo_path

    def build_snapshot(self) -> Snapshot:
        """Build snapshot without losing the git reference context."""
        assert self.directory is not None
        branch_name = self.git_ref.encode()
        return Snapshot(
            branches={
                b"HEAD": SnapshotBranch(
                    target_type=SnapshotTargetType.ALIAS,
                    target=branch_name,
                ),
                branch_name: SnapshotBranch(
                    target=self.directory.hash,
                    target_type=SnapshotTargetType.DIRECTORY,
                ),
            }
        )
