# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import dateutil.parser

from swh.scheduler.task import Task

from .loader import GitLoader, GitLoaderFromArchive
from .updater import BulkUpdater
from .reader import GitSha1RemoteReaderAndSendToQueue


# TODO: rename to LoadRemoteGitRepository
class UpdateGitRepository(Task):
    """Import a git repository from a remote location"""
    task_queue = 'swh_loader_git'

    def run_task(self, repo_url, base_url=None):
        """Import a git repository"""
        loader = BulkUpdater()
        loader.log = self.log

        return loader.load(repo_url, base_url=base_url)


class LoadDiskGitRepository(Task):
    """Import a git repository from disk"""
    task_queue = 'swh_loader_git_express'

    def run_task(self, origin_url, directory, date):
        """Import a git repository, cloned in `directory` from `origin_url` at
        `date`."""

        loader = GitLoader()
        loader.log = self.log

        return loader.load(origin_url, directory, dateutil.parser.parse(date))


class UncompressAndLoadDiskGitRepository(Task):
    """Import a git repository from a zip archive"""
    task_queue = 'swh_loader_git_archive'

    def run_task(self, origin_url, archive_path, date):
        """1. Uncompress an archive repository in a local and temporary folder
           2. Load it through the git disk loader
           3. Clean up the temporary folder

        """
        loader = GitLoaderFromArchive()
        loader.log = self.log

        return loader.load(
            origin_url, archive_path, dateutil.parser.parse(date))


class ReaderGitRepository(Task):
    task_queue = 'swh_reader_git'

    def run_task(self, repo_url, base_url=None):
        """Read a git repository from a remote location and send sha1 to
        archival.

        """
        loader = GitSha1RemoteReaderAndSendToQueue()
        loader.log = self.log

        return loader.load(repo_url)
