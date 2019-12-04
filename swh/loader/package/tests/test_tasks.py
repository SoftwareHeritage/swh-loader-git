# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import patch


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
