# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest
import requests


def test_swh_proxy():
    with pytest.raises(requests.exceptions.ProxyError):
        requests.get("https://www.softwareheritage.org")
