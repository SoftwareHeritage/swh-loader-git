# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def test_git_loader(mocker, swh_app, celery_session_worker):
    mock_loader = mocker.patch('swh.loader.git.loader.GitLoader.load')
    mock_loader.return_value = {'status': 'eventful'}

    res = swh_app.send_task(
        'swh.loader.git.tasks.UpdateGitRepository',
        kwargs={
            'url': 'origin_url',
        })
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {'status': 'eventful'}
    mock_loader.assert_called_once_with()


def test_git_loader_from_disk(mocker, swh_app, celery_session_worker):
    mock_loader = mocker.patch(
        'swh.loader.git.from_disk.GitLoaderFromDisk.load')
    mock_loader.return_value = {'status': 'uneventful'}

    res = swh_app.send_task(
        'swh.loader.git.tasks.LoadDiskGitRepository',
        kwargs={
            'url': 'origin_url2',
            'directory': '/some/repo',
            'date': '2018-12-10 00:00',
        })
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {'status': 'uneventful'}
    mock_loader.assert_called_once_with()


def test_git_loader_from_archive(mocker, swh_app, celery_session_worker):
    mock_loader = mocker.patch(
        'swh.loader.git.from_disk.GitLoaderFromArchive.load')

    mock_loader.return_value = {'status': 'failed'}

    res = swh_app.send_task(
        'swh.loader.git.tasks.UncompressAndLoadDiskGitRepository',
        kwargs={
            'url': 'origin_url3',
            'archive_path': '/some/repo',
            'date': '2017-01-10 00:00',
        })
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {'status': 'failed'}
    mock_loader.assert_called_once_with()
