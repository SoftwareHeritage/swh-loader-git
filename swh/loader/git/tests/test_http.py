# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.loader.git.client import http
from swh.loader.git.storage import storage


class TestHttp(unittest.TestCase):
    @istest
    def url(self):
        # when
        s = http.compute_simple_url('http://base-url', '/end')

        # then
        assert s == 'http://base-url/end'

    @istest
    def url_lookup_per_type(self):
        # then
        assert http.url_lookup_per_type == { storage.Type.origin: "/vcs/origins/"
                                           , storage.Type.content: "/vcs/contents/"
                                           , storage.Type.directory: "/vcs/directories/"
                                           , storage.Type.revision: "/vcs/revisions/" }

    @istest
    def url_store_per_type(self):
        # then
        assert http.url_store_per_type == { storage.Type.origin: "/vcs/origins/"
                                          , storage.Type.content: "/vcs/contents/"
                                          , storage.Type.directory: "/vcs/directories/"
                                          , storage.Type.revision: "/vcs/revisions/"
                                          , storage.Type.release: "/vcs/releases/"
                                          , storage.Type.occurrence: "/vcs/occurrences/"
                                          , storage.Type.person: "/vcs/persons/"
                                          }
