# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import requests


def test_get_response_cb(local_get_visits):
    response = requests.get('https://example.com/file.json')
    assert response.ok
    assert response.json() == {'hello': 'you'}

    response = requests.get('https://example.com/file.json')
    assert response.ok
    assert response.json() == {'hello': 'world'}

    response = requests.get('https://example.com/file.json')
    assert not response.ok
    assert response.status_code == 404
