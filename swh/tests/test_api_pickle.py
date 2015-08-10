# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

import time
from datetime import datetime

from test_utils import app_client
from swh.protocols import serial

@attr('slow')
class PickleTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

    @istest
    def post_some_pickle_content(self):
        # when
        # we create it
        body = {'sha1': "123"
                , 'content-sha1': 'content-sha1c46ee476a8be155ab03333333333'
                , 'content-sha256': 'content-sha2566ee476a8be155ab03333333333'
                , 'content': b'tree 37\x00100644 README.md\x00\x8b\xbb\x0b\xcc6\x9a\xb3\xba\xa4#\xda\xcc6\x00a\x81:1\x05\xf3'
                , 'size': '3'
                , 'date-now': time.gmtime()
                , 'date-now2': datetime.utcnow()
                }

        rv = self.app.post('/pickle'
                           , data=serial.dumps(body)
                           , headers={'Content-type': 'application/octet-stream'})

        # then
        print(rv, rv.status_code, rv.data)

        assert rv.status_code == 200
        assert rv.data == b'Received!'
