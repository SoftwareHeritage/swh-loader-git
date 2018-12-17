# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import unittest
from unittest.mock import patch

from swh.loader.git.tasks import (
    UpdateGitRepository, LoadDiskGitRepository,
    UncompressAndLoadDiskGitRepository
)


class TestTasks(unittest.TestCase):
    def test_check_task_name(self):
        task = UpdateGitRepository()
        self.assertEqual(task.task_queue, 'swh_loader_git')

    @patch('swh.loader.git.loader.GitLoader.load')
    def test_task(self, mock_loader):
        mock_loader.return_value = {'status': 'eventful'}
        task = UpdateGitRepository()

        # given
        actual_result = task.run_task('origin_url')

        self.assertEqual(actual_result, {'status': 'eventful'})

        mock_loader.assert_called_once_with('origin_url', base_url=None)


class TestTasks2(unittest.TestCase):
    def test_check_task_name(self):
        task = LoadDiskGitRepository()
        self.assertEqual(task.task_queue, 'swh_loader_git_express')

    @patch('swh.loader.git.from_disk.GitLoaderFromDisk.load')
    def test_task(self, mock_loader):
        mock_loader.return_value = {'status': 'uneventful'}
        task = LoadDiskGitRepository()

        # given
        actual_result = task.run_task('origin_url2', '/some/repo',
                                      '2018-12-10 00:00')

        self.assertEqual(actual_result, {'status': 'uneventful'})

        mock_loader.assert_called_once_with(
            'origin_url2', '/some/repo', datetime.datetime(2018, 12, 10, 0, 0))


class TestTasks3(unittest.TestCase):
    def test_check_task_name(self):
        task = UncompressAndLoadDiskGitRepository()
        self.assertEqual(task.task_queue, 'swh_loader_git_archive')

    @patch('swh.loader.git.from_disk.GitLoaderFromArchive.load')
    def test_task(self, mock_loader):
        mock_loader.return_value = {'status': 'failed'}
        task = UncompressAndLoadDiskGitRepository()

        # given
        actual_result = task.run_task('origin_url3', '/path/repo',
                                      '2017-01-10 00:00')

        self.assertEqual(actual_result, {'status': 'failed'})

        mock_loader.assert_called_once_with(
            'origin_url3', '/path/repo', datetime.datetime(2017, 1, 10, 0, 0))
