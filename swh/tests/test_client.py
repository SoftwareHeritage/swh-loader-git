# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.http import client
from swh.storage import store


class TestClient(unittest.TestCase):
    @istest
    def url(self):
        # when
        s = client.compute_simple_url('http://base-url', '/end')

        # then
        assert s == 'http://base-url/end'

    @istest
    def url_lookup_per_type(self):
        # then
        assert client.url_lookup_per_type == {  store.Type.origin: "/origins/"
                                                , store.Type.content: "/vcs/contents/"
                                                , store.Type.directory: "/vcs/directories/"
                                                , store.Type.revision: "/vcs/revisions/" }

    @istest
    def url_store_per_type(self):
        # then
        assert client.url_store_per_type == {  store.Type.origin: "/origins/"
                                               , store.Type.content: "/vcs/contents/"
                                               , store.Type.directory: "/vcs/directories/"
                                               , store.Type.revision: "/vcs/revisions/"
                                               , store.Type.release: "/vcs/releases/"
                                               , store.Type.occurrence: "/vcs/occurrences/"
                                               }
