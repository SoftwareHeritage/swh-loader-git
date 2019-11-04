# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import patch


@patch('swh.loader.package.archive.ArchiveLoader.load')
def test_gnu_loader(
        mock_loader, swh_app, celery_session_worker, swh_config):
    mock_loader.return_value = {'status': 'eventful'}

    res = swh_app.send_task(
        'swh.loader.package.tasks.LoadArchive',
        (), dict(url='some-url', artifacts=[]))
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {'status': 'eventful'}


@patch('swh.loader.package.debian.DebianLoader.load')
def test_debian_loader(
        mock_loader, swh_app, celery_session_worker, swh_config):
    mock_loader.return_value = {'status': 'eventful'}

    res = swh_app.send_task(
        'swh.loader.package.tasks.LoadDebian',
        (), dict(url='some-url', date='some-date', packages={}))
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {'status': 'eventful'}


@patch('swh.loader.package.deposit.DepositLoader.load')
def test_deposit_loader(
        mock_loader, swh_app, celery_session_worker, swh_config):
    mock_loader.return_value = {'status': 'eventful'}

    res = swh_app.send_task(
        'swh.loader.package.tasks.LoadDeposit',
        (), dict(url='some-url', deposit_id='some-d-id'))
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {'status': 'eventful'}


@patch('swh.loader.package.npm.NpmLoader.load')
def test_npm_loader(
        mock_loader, swh_app, celery_session_worker, swh_config):
    mock_loader.return_value = {'status': 'eventful'}

    res = swh_app.send_task(
        'swh.loader.package.tasks.LoadNpm',
        (), dict(package_name='some-package',
                 package_url='some',
                 package_metadata_url='something'))
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {'status': 'eventful'}


@patch('swh.loader.package.pypi.PyPILoader.load')
def test_pypi_loader(
        mock_loader, swh_app, celery_session_worker, swh_config):
    mock_loader.return_value = {'status': 'eventful'}

    res = swh_app.send_task(
        'swh.loader.package.tasks.LoadPyPI',
        (), dict(url='some-url'))
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {'status': 'eventful'}
