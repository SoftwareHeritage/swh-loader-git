# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def test_tasks_nixguix_loader(
    mocker, swh_scheduler_celery_app, swh_scheduler_celery_worker, swh_config
):
    mock_loader = mocker.patch(
        "swh.loader.package.nixguix.loader.NixGuixLoader.from_configfile"
    )
    mock_loader.return_value = mock_loader
    mock_loader.load.return_value = {"status": "eventful"}

    res = swh_scheduler_celery_app.send_task(
        "swh.loader.package.nixguix.tasks.LoadNixguix", kwargs=dict(url="some-url")
    )
    assert res
    res.wait()
    assert res.successful()
    assert mock_loader.called
    assert res.result == {"status": "eventful"}
