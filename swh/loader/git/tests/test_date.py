# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.loader.git import date


class DateTestCase(unittest.TestCase):
    @istest
    def negative_offsets(self):
        # when
        d0 = date.ts_to_str(1434379797, -120)

        assert d0 == '2015-06-15 12:49:57-02:00'

        # when
        d1 = date.ts_to_str(1434379797, -60)

        assert d1 == '2015-06-15 13:49:57-01:00'

        # when
        d2 = date.ts_to_str(1434379797, -30)

        assert d2 == '2015-06-15 14:19:57-00:30'

        # when
        d3 = date.ts_to_str(1434379797, 30)

        assert d3 == '2015-06-15 15:19:57+00:30'

    @istest
    def positive_offsets(self):
        # when
        d0 = date.ts_to_str(1434449254, 120)

        assert d0 == '2015-06-16 12:07:34+02:00'

        # when
        d1 = date.ts_to_str(1434449254, 60)

        assert d1 == '2015-06-16 11:07:34+01:00'

        # when
        d2 = date.ts_to_str(1434449254, 0)

        assert d2 == '2015-06-16 10:07:34+00:00'

        # when
        d3 = date.ts_to_str(1434449254, -60)

        assert d3 == '2015-06-16 09:07:34-01:00'
