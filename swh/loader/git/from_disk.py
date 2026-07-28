# Copyright (C) 2015-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import tempfile
from typing import Callable

from dulwich.object_format import DEFAULT_OBJECT_FORMAT
import dulwich.objects
from dulwich.pack import generate_unpacked_objects, write_pack_data
import dulwich.repo

from swh.loader.git import utils
from swh.loader.git.loader import FetchPackReturn, GitLoader, RepoRepresentation

logger = logging.getLogger(__name__)


class GitLoaderFromArchive(GitLoader):
    """Load a git repository from an archive.

    This loader ingests a git repository compressed into an archive.
    The supported archive formats are ``.zip`` and ``.tar.gz``.

    It notably supports the loading of a repository with missing objects
    that can be obtained by using the filter option from the git clone
    command (requires git server to have such feature implemented),
    for instance::

        # clone a repository without fetching blobs not reachable from HEAD
        $ git clone <repo_url> --filter=blob:none

        # clone a repository without fetching trees and blobs not reachable from HEAD
        $ git clone <repo_url> --filter=tree:0

    It can be useful to load such repositories when writing tests that do not need to
    process blob and tree objects so we can produce an archive of smaller size.

    From an input tarball named ``my-git-repo.zip``, the following layout is
    expected in it::

        my-git-repo/
        ├── .git
        │   ├── branches
        │   ├── COMMIT_EDITMSG
        │   ├── config
        │   ├── description
        │   ├── HEAD
        ...

    Nevertheless, the loader is able to ingest tarballs with the following
    layouts too::

        .
        ├── .git
        │   ├── branches
        │   ├── COMMIT_EDITMSG
        │   ├── config
        │   ├── description
        │   ├── HEAD
        ...

    or::

        other-repo-name/
        ├── .git
        │   ├── branches
        │   ├── COMMIT_EDITMSG
        │   ├── config
        │   ├── description
        │   ├── HEAD
        ...

    """

    def __init__(self, *args, archive_path: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.archive_path = archive_path

    def project_name_from_archive(self, archive_path):
        """Compute the project name from the archive's path."""
        archive_name = os.path.basename(archive_path)
        for ext in (".zip", ".tar.gz", ".tgz"):
            if archive_name.lower().endswith(ext):
                archive_name = archive_name[: -len(ext)]
                break
        return archive_name

    def fetch_pack_from_origin(
        self,
        origin_url: str,
        base_repo: RepoRepresentation,
        do_activity: Callable[[bytes], None],
    ):
        with tempfile.TemporaryDirectory() as tmp_dir:
            project_name = self.project_name_from_archive(self.archive_path)
            _, repo_path = utils.init_git_repo_from_archive(
                project_name, self.archive_path, root_temp_dir=tmp_dir
            )
            pack_buffer = tempfile.SpooledTemporaryFile(max_size=self.temp_file_cutoff)
            with dulwich.repo.Repo(repo_path) as repo:
                # create a pack file with all objects referenced in the repository so
                # we can load it even if it has missing objects
                unpacked_objects = generate_unpacked_objects(
                    repo.object_store,
                    list(map(lambda sha: (sha, None), repo.object_store)),
                )
                write_pack_data(
                    pack_buffer.write,
                    records=unpacked_objects,
                    object_format=DEFAULT_OBJECT_FORMAT,
                    num_records=sum(1 for _ in repo.object_store),
                )
                pack_buffer.flush()
                pack_size = pack_buffer.tell()
                pack_buffer.seek(0)

                return FetchPackReturn(
                    remote_refs=utils.filter_refs(repo.refs.as_dict()),
                    symbolic_refs=utils.filter_symbolic_refs(repo.refs.get_symrefs()),
                    pack_buffer=pack_buffer,
                    pack_size=pack_size,
                )
