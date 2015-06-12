# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import tempfile
import gzip
import os

from nose.tools import istest
from nose.plugins.attrib import attr

from swh import file


@attr('slow')
class TestFile(unittest.TestCase):
    @istest
    def check_folder_path_computation(self):
        # when
        fpath = file.folder_path('prefix-dir', 'aabbccdd0123456789')

        # then
        self.assertEquals(fpath,
                          'prefix-dir/aa/bb/cc/dd',
                          'Depth should be 4 because not specified')

    @istest
    def check_folder_path_computation_with_different_deps(self):
        # when
        fpath = file.folder_path('prefix-dir', 'aabb01234567890', 2)

        # then
        self.assertEquals(fpath,
                          'prefix-dir/aa/bb',
                          'Depth is now 2')

    @istest
    def check_write_data_with_no_compress_flag(self):
        # given
        _, tmpfile = tempfile.mkstemp(prefix='swh-git-loader.',
                                      dir='/tmp')

        # when
        file.write_data('some data to write'.encode('utf-8'), tmpfile)

        # then
        with open(tmpfile, 'r') as f:
            self.assertEquals('some data to write',
                              f.read(),
                              'Data read should be the same!')

        # cleanup
        os.remove(tmpfile)

    @istest
    def check_write_data_with_compress_flag_on(self):
        # given
        _, tmpfile = tempfile.mkstemp(prefix='swh-git-loader.',
                                      dir='/tmp')

        # when
        file.write_data('some data to write compressed'.encode('utf-8'),
                        tmpfile,
                        True)

        # then
        with gzip.open(tmpfile, 'r') as f:
            self.assertEquals('some data to write compressed'.encode('utf-8'),
                              f.read(),
                              'Compressed data read should be the same!')

        # cleanup
        os.remove(tmpfile)
