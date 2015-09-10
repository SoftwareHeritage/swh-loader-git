# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.loader.git import date


class DateTestCase(unittest.TestCase):
    @istest
    def new_swhrepo(self):
        # when
        d0 = date.ts_to_datetime(1434449254, 120)

        assert str(d0) == '2015-06-16 12:07:34+02:00'

        # when
        d1 = date.ts_to_datetime(1434449254, 60)

        assert str(d1) == '2015-06-16 11:07:34+01:00'

        # when
        d2 = date.ts_to_datetime(1434449254, 0)

        assert str(d2) == '2015-06-16 10:07:34+00:00'
