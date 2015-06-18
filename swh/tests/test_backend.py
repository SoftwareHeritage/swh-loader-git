# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.backend import back

import test_initdb


@attr('slow')
class BackTestCase(unittest.TestCase):

    def setUp(self):
        back.app.config['TESTING'] = True
        self.app = back.app.test_client()
        test_initdb.prepare_db("dbname=swhgitloader-test")

    @istest
    def get_slash(self):
        # given
        rv = self.app.get('/')

        # then
        assert rv.status_code is 200
        assert rv.data == b'Dev SWH API'
