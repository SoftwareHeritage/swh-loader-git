# Copyright (C) 2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os.path import basename
from pathlib import Path
import string
import tempfile
from typing import Any, Iterable, Iterator

from dulwich.porcelain import checkout_branch, clone

from swh.loader.core.loader import BaseDirectoryLoader
from swh.loader.git.utils import raise_not_found_repository
from swh.model.from_disk import ignore_empty_directories, ignore_named_directories
from swh.model.model import Snapshot, SnapshotBranch, TargetType

# DONE:
# - [x] Exclude the the .git and the empty folders from the hash tree computations.
# - [x] Check depth=1. It fails with NotImplementedError so depth to 0 is fine.

# FIXME:
# - [ ]idea: clone repository at a specific reference like the pip implementation does.


def clone_repository(git_url: str, git_ref: str, target: Path):
    """Clone ``git_url`` repository at ``git_ref`` commit, tag or branch.

    This function can raise for various reasons. This is expected to be caught by the
    main loop in the loader.

    """

    local_name = basename(git_url)
    # Determine whether the ref is a commit or not. That implies different behavior for
    # the cloning step.
    commit_ref = all(c in string.hexdigits for c in git_ref) and len(git_ref) >= 40

    repo = clone(
        source=git_url,
        target=target / local_name,
        branch=git_ref if not commit_ref else None,
        depth=0 if not commit_ref else None,
    )

    if commit_ref:
        checkout_branch(repo, git_ref)

    return repo


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
                repo = clone_repository(
                    self.origin.url, self.git_ref, target=Path(tmpdir)
                )
                yield repo.path

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
