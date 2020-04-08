# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def test_nixguix_loader(mocker, swh_app, celery_session_worker, swh_config):
    mock_loader = mocker.patch("swh.loader.package.nixguix.loader.NixGuixLoader.load")
    mock_loader.return_value = {"status": "eventful"}

    mock_retrieve_sources = mocker.patch(
        "swh.loader.package.nixguix.loader.retrieve_sources"
    )
    mock_retrieve_sources.return_value = {
        "version": 1,
        "sources": [],
        "revision": "some-revision",
    }

    res = swh_app.send_task(
        "swh.loader.package.nixguix.tasks.LoadNixguix", kwargs=dict(url="some-url")
    )
    assert res
    res.wait()
    assert res.successful()

    assert res.result == {"status": "eventful"}
