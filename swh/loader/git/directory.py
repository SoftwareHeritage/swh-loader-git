# Copyright (C) 2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os.path import basename, exists, join
from pathlib import Path
from shutil import which
from subprocess import CalledProcessError, check_output
import tempfile
from typing import Any, Iterable, Iterator

from swh.loader.core.loader import BaseDirectoryLoader
from swh.loader.exception import NotFound
from swh.loader.git.utils import raise_not_found_repository
from swh.model.from_disk import ignore_empty_directories, ignore_named_directories
from swh.model.model import Snapshot, SnapshotBranch, TargetType


def git() -> str:
    """Get the path to the git executable.

    Raises:
        EnvironmentError if no opam executable is found
    """
    ret = which("git")
    if not ret:
        raise EnvironmentError("No git executable found in path {os.environ['PATH']}")

    return ret


def clone_repository(git_url: str, git_ref: str, target: Path) -> Path:
    """Clone ``git_url`` repository at ``git_ref`` commit, tag or branch.

    This function can raise for various reasons. This is expected to be caught by the
    main loop in the loader.

    """

    local_name = basename(git_url)
    local_clone = str(target / local_name)
    clone_cmd = [
        # treeless clone (except for the head)
        # man git-rev-list > section "--filter": "A treeless clone downloads all
        # reachable commits, then downloads trees and blobs on demand."
        git(),
        "clone",
        "--filter=tree:0",
        git_url,
        local_clone,
    ]
    # Clone
    try:
        check_output(clone_cmd)
    except CalledProcessError:
        raise NotFound(f"Repository <{git_url}> not found")

    # Then checkout the tree at the desired reference
    check_output([git(), "switch", "--detach", git_ref], cwd=local_clone)

    return Path(local_clone)


def list_git_tree(dirpath: str, dirname: str, entries: Iterable[Any]) -> bool:
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
        # We use a filter which ignore the .git folder and the empty git trees
        super().__init__(*args, dir_filter=list_git_tree, **kwargs)

    def fetch_artifact(self) -> Iterator[Path]:
        with raise_not_found_repository():
            with tempfile.TemporaryDirectory() as tmpdir:
                repo_path = clone_repository(
                    self.origin.url, self.git_ref, target=Path(tmpdir)
                )

                yield repo_path

                # if the steps below are executed, it means a directory hash mismatch was
                # found between the one computed from the cloned repository and the expected
                # one provided as loader parameter, retry loading by fetching submodules in
                # case they were used in hash computation

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
                    target_type=TargetType.ALIAS,
                    target=branch_name,
                ),
                branch_name: SnapshotBranch(
                    target=self.directory.hash,
                    target_type=TargetType.DIRECTORY,
                ),
            }
        )
