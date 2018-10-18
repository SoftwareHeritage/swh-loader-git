# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from swh.loader.git import utils


class TestUtils(unittest.TestCase):
    def test_check_date_time(self):
        """A long as datetime is fine, date time check does not raise

        """
        for e in range(32, 37):
            ts = 2**e
            utils.check_date_time(ts)

    def test_check_date_time_empty_value(self):
        self.assertIsNone(utils.check_date_time(None))

    def test_check_date_time_raises(self):
        """From a give threshold, check will no longer works.

        """
        exp = 38
        timestamp = 2**exp
        with self.assertRaisesRegex(ValueError, 'is out of range'):
            utils.check_date_time(timestamp)
