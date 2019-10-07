# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import requests


# In this arborescence, we override in the local conftest.py module the
# "datadir" fixture to specify where to retrieve the data files from.


def test_local_get_with_datadir_fixture_override(local_get):
    """Override datadir fixture should retrieve data from elsewhere

    """
    response = requests.get('https://example.com/file.json')
    assert response.ok
    assert response.json() == {'welcome': 'you'}
