# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import tempfile
import unittest

from swh.loader.core import converters
from swh.model import from_disk
from swh.model.model import Content, SkippedContent


def tmpfile_with_content(fromdir, contentfile):
    """Create a temporary file with content contentfile in directory fromdir.

    """
    tmpfilepath = tempfile.mktemp(
        suffix='.swh',
        prefix='tmp-file-for-test',
        dir=fromdir)

    with open(tmpfilepath, 'wb') as f:
        f.write(contentfile)

    return tmpfilepath


class TestContentForStorage(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        super().setUpClass()
        self.tmpdir = tempfile.TemporaryDirectory(
            prefix='test-swh-loader-core.'
        )

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_content_for_storage_path(self):
        # given
        data = b'temp file for testing content storage conversion'
        tmpfile = tmpfile_with_content(self.tmpdir.name, data)

        obj = from_disk.Content.from_file(path=os.fsdecode(tmpfile),
                                          save_path=True).get_data()

        expected_content = obj.copy()
        expected_content['data'] = data
        expected_content['status'] = 'visible'
        del expected_content['path']
        del expected_content['perms']
        expected_content = Content.from_dict(expected_content)

        # when
        content = converters.content_for_storage(obj)

        # then
        self.assertEqual(content, expected_content)

    def test_content_for_storage_data(self):
        # given
        data = b'temp file for testing content storage conversion'

        obj = from_disk.Content.from_bytes(data=data, mode=0o100644).get_data()

        expected_content = obj.copy()
        expected_content['status'] = 'visible'
        del expected_content['perms']
        expected_content = Content.from_dict(expected_content)

        # when
        content = converters.content_for_storage(obj)

        # then
        self.assertEqual(content, expected_content)

    def test_content_for_storage_too_long(self):
        # given
        data = b'temp file for testing content storage conversion'

        obj = from_disk.Content.from_bytes(data=data, mode=0o100644).get_data()
        del obj['perms']

        expected_content = obj.copy()
        expected_content.pop('data')
        expected_content['status'] = 'absent'
        expected_content['origin'] = 'http://example.org/'
        expected_content['reason'] = 'Content too large'
        expected_content = SkippedContent.from_dict(expected_content)

        # when
        content = converters.content_for_storage(
            obj, max_content_size=len(data) - 1,
            origin_url=expected_content.origin,
        )

        # then
        self.assertEqual(content, expected_content)
