# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from test_utils import app_client


@attr('slow')
class HomeTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app, _, _ = app_client()

    @istest
    def get_slash(self):
        # when
        rv = self.app.get('/')

        # then
        assert rv.status_code == 200
        assert rv.data == b'Dev SWH API'

    @istest
    def get_404(self):
        # when
        rv = self.app.get('/nowhere')

        # then
        assert rv.status_code == 404

    @istest
    def get_bad_request(self):
        # when
        rv = self.app.get('/vcs/not-a-good-type/1')

        # then
        assert rv.status_code == 400
        assert rv.data == b'Bad request!'
