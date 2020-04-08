# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def test_npm_loader(mocker, swh_app, celery_session_worker, swh_config):
    mock_loader = mocker.patch("swh.loader.package.npm.loader.NpmLoader.load")
    mock_loader.return_value = {"status": "eventful"}

    res = swh_app.send_task(
        "swh.loader.package.npm.tasks.LoadNpm",
        kwargs=dict(url="https://www.npmjs.com/package/some-package"),
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}
