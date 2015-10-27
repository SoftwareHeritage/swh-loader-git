# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import os

from swh.core.scheduling import Task

from .loader import BulkLoader


class LoadGitRepository(Task):
    """Import a git repository to Software Heritage"""

    task_queue = 'swh_loader_git'

    CONFIG_BASE_FILENAME = 'loader/git.ini'
    ADDITIONAL_CONFIG = {}

    def __init__(self):
        self.config = BulkLoader.parse_config_file(
            base_filename=self.CONFIG_BASE_FILENAME,
            additional_configs=[self.ADDITIONAL_CONFIG],
        )

    def run(self, repo_path, origin_url, authority_id, validity):
        """Import a git repository"""
        loader = BulkLoader(self.config)
        loader.log = self.log

        loader.process(repo_path, origin_url, authority_id, validity)


class LoadGitHubRepository(LoadGitRepository):
    """Import a github repository to Software Heritage"""

    task_queue = 'swh_loader_git'

    CONFIG_BASE_FILENAME = 'loader/github.ini'
    ADDITIONAL_CONFIG = {
        'github_basepath': ('str', '/srv/storage/space/data/github'),
        'authority_id': ('str', '5f4d4c51-498a-4e28-88b3-b3e4e8396cba'),
        'default_validity': ('str', '1970-01-01 00:00:00+00'),
    }

    def run(self, repo_fullname):
        authority_id = self.config['authority_id']
        validity = self.config['default_validity']

        repo_path = os.path.join(self.config['github_basepath'],
                                 repo_fullname[0], repo_fullname)

        witness_file = os.path.join(repo_path, 'witness')
        if os.path.exists(witness_file):
            validity_timestamp = os.stat(witness_file).st_mtime
            validity = '%s+00' % datetime.datetime.utcfromtimestamp(
                validity_timestamp)

        origin_url = 'https://github.com/%s' % repo_fullname

        super().run(repo_path, origin_url, authority_id, validity)


class LoadGitHubRepositoryReleases(LoadGitHubRepository):
    """Import a GitHub repository to SoftwareHeritage, only with releases"""

    task_queue = 'swh_loader_git_express'

    def __init__(self):
        super(self.__class__, self).__init__()

        self.config.update({
            'send_contents': False,
            'send_directories': False,
            'send_revisions': False,
            'send_releases': True,
            'send_occurrences': False,
        })


class LoadGitHubRepositoryContents(LoadGitHubRepository):
    """Import a GitHub repository to SoftwareHeritage, only with contents"""

    task_queue = 'swh_loader_git_express'

    def __init__(self):
        super(self.__class__, self).__init__()

        self.config.update({
            'send_contents': True,
            'send_directories': False,
            'send_revisions': False,
            'send_releases': False,
            'send_occurrences': False,
        })
