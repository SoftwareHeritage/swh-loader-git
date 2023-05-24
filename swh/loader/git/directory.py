# Copyright (C) 2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path
import tempfile
from typing import Iterator

from swh.loader.core.loader import BaseDirectoryLoader
from swh.loader.git.utils import raise_not_found_repository


def clone_repository(git_url: str, git_ref: str, target: Path):
    """Clone ``git_url`` repository at ``git_ref`` commit, tag or branch.

    This function can raise for various reasons. This is expected to be caught by the
    main loop in the loader.

    """
    from os.path import basename
    import string

    from dulwich.porcelain import checkout_branch, clone

    local_name = basename(git_url)
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


class GitDirectoryLoader(BaseDirectoryLoader):
    """Git directory loader in charge of ingesting a git tree at a specific commit, tag
    or branch into the swh archive.

    The output snapshot is of the form:

    .. code::

       id: <bytes>
       branches:
         HEAD:
           target_type: directory
           target: <directory-id>

    """

    visit_type = "git-checkout"

    def __init__(self, *args, **kwargs):
        self.git_ref = kwargs.pop("ref")
        super().__init__(*args, **kwargs)

    def fetch_artifact(self) -> Iterator[Path]:
        with raise_not_found_repository():
            with tempfile.TemporaryDirectory() as tmpdir:
                repo = clone_repository(
                    self.origin.url, self.git_ref, target=Path(tmpdir)
                )
                yield repo.path
