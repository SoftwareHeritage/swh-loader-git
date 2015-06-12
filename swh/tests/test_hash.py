# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh import hash


@attr('slow')
class TestHash(unittest.TestCase):
    @istest
    def compute_sha1_data(self):
        # given
        # when
        actual_sha1 = hash.hashkey_sha1(b'some data')

        # then
        self.assertEquals(
            actual_sha1.hexdigest(),
            'baf34551fecb48acc3da868eb85e1b6dac9de356',
            "Result should be the result of `echo -n 'some data' | sha1sum`")

    @istest
    def compute_sha1_data2(self):
        # given
        # when
        actual_sha1 = hash.hashkey_sha1(b'some other data')

        # then
        self.assertEquals(
            actual_sha1.hexdigest(),
            '7bd8e7cb8e1e8b7b2e94b472422512935c9d4519',
            """Result should be the result of
               `echo -n 'some other data' | sha1sum`""")

    @istest
    def compute_blob_sha1(self):
        # given
        # when
        sha1 = hash.blob_sha1('some blob data')
        sha1hex = sha1.hexdigest()

        # then
        self.assertEquals(sha1hex,
                          '895018df42621eed0b1e42fd9b8fa12d8f534f38',
                          """Result should be the result of
                             `echo -en 'blob 14\0some blob data' | sha1sum`""")

    @istest
    def compute_blob_sha1_2(self):
        # given
        # when
        sha1 = hash.blob_sha1('some other blob data')
        sha1hex = sha1.hexdigest()

        # then
        self.assertEquals(
            sha1hex,
            '25388c1e2102249d5b8dd33dd9bb4a0e1cc95e62',
            """Result should be the result of
               `echo -en 'blob 20\0some other blob data' | sha1sum`""")
