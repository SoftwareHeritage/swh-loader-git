# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from unittest.mock import patch


@patch('swh.loader.git.loader.GitLoader.load')
def test_git_loader(mock_loader, swh_app, celery_session_worker):
    mock_loader.return_value = {'status': 'eventful'}

    res = swh_app.send_task(
        'swh.loader.git.tasks.UpdateGitRepository',
        ('origin_url',))
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {'status': 'eventful'}
    mock_loader.assert_called_once_with('origin_url', base_url=None)


@patch('swh.loader.git.from_disk.GitLoaderFromDisk.load')
def test_git_loader_from_disk(mock_loader, swh_app, celery_session_worker):
    mock_loader.return_value = {'status': 'uneventful'}

    res = swh_app.send_task(
        'swh.loader.git.tasks.LoadDiskGitRepository',
        ('origin_url2', '/some/repo', '2018-12-10 00:00'))
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {'status': 'uneventful'}
    mock_loader.assert_called_once_with(
        'origin_url2', '/some/repo', datetime.datetime(2018, 12, 10, 0, 0))


@patch('swh.loader.git.from_disk.GitLoaderFromArchive.load')
def test_git_loader_from_archive(mock_loader, swh_app, celery_session_worker):
    mock_loader.return_value = {'status': 'failed'}

    res = swh_app.send_task(
        'swh.loader.git.tasks.UncompressAndLoadDiskGitRepository',
        ('origin_url3', '/some/repo', '2017-01-10 00:00'))
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {'status': 'failed'}
    mock_loader.assert_called_once_with(
        'origin_url3', '/some/repo', datetime.datetime(2017, 1, 10, 0, 0))
