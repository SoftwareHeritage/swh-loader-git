# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import dateutil.parser

from celery import shared_task

from swh.loader.git.from_disk import GitLoaderFromDisk, GitLoaderFromArchive
from swh.loader.git.loader import GitLoader


@shared_task(name=__name__ + '.UpdateGitRepository')
def load_git(repo_url, base_url=None):
    """Import a git repository from a remote location"""
    loader = GitLoader(repo_url, base_url=base_url)
    return loader.load()


@shared_task(name=__name__ + '.LoadDiskGitRepository')
def load_git_from_dir(origin_url, directory, date):
    """Import a git repository from a local repository

       Import a git repository, cloned in `directory` from `origin_url` at
        `date`.

    """
    visit_date = dateutil.parser.parse(date)
    loader = GitLoaderFromDisk(
        origin_url, directory=directory, visit_date=visit_date)
    return loader.load()


@shared_task(name=__name__ + '.UncompressAndLoadDiskGitRepository')
def load_git_from_zip(origin_url, archive_path, date):
    """Import a git repository from a zip archive

    1. Uncompress an archive repository in a local and temporary folder
    2. Load it through the git disk loader
    3. Clean up the temporary folder
    """
    visit_date = dateutil.parser.parse(date)
    loader = GitLoaderFromArchive(
        origin_url, archive_path=archive_path, visit_date=visit_date)
    return loader.load()
