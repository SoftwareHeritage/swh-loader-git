# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import tempfile
import unittest

from nose.tools import istest

from swh.loader.core import converters
from swh.model import git


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


class TestConverters(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.tmpdir = tempfile.mkdtemp(prefix='test-swh-loader-dir.')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmpdir)
        super().tearDownClass()

    @istest
    def format_to_minutes(self):
        self.assertEquals(converters.format_to_minutes('+0100'), 60)
        self.assertEquals(converters.format_to_minutes('-0200'), -120)
        self.assertEquals(converters.format_to_minutes('+1250'), 12*60+50)
        self.assertEquals(converters.format_to_minutes('+0000'), 0)
        self.assertEquals(converters.format_to_minutes('-0000'), 0)

    @istest
    def annotated_tag_to_release(self):
        # given
        release = {
            'id': '123',
            'target': '456',
            'target_type': 'revision',
            'name': 'some-release',
            'comment': 'some-comment-on-release',
            'date': 1444054085,
            'offset': '-0300',
            'author_name': 'someone',
            'author_email': 'someone@whatelse.eu',
        }

        expected_release = {
            'target': '456',
            'target_type': 'revision',
            'name': b'some-release',
            'message': b'some-comment-on-release',
            'date': {
                'timestamp': 1444054085,
                'offset': -180
            },
            'author': {
                'name': b'someone',
                'email': b'someone@whatelse.eu',
            },
            'synthetic': True,
        }

        # when
        actual_release = converters.annotated_tag_to_release(release)

        # then
        self.assertDictEqual(actual_release, expected_release)

    @istest
    def blob_to_content_visible_data(self):
        # given
        contentfile = b'temp file for testing blob to content conversion'
        tmpfilepath = tmpfile_with_content(self.tmpdir, contentfile)

        obj = {
            'path': tmpfilepath,
            'perms': git.GitPerm.BLOB,
            'type': git.GitType.BLOB,
            'sha1': 'some-sha1',
            'sha256': 'some-sha256',
            'sha1_git': 'some-sha1git',
        }

        expected_blob = {
            'data': contentfile,
            'length': len(contentfile),
            'status': 'visible',
            'sha1': 'some-sha1',
            'sha256': 'some-sha256',
            'sha1_git': 'some-sha1git',
            'perms': git.GitPerm.BLOB.value,
            'type': git.GitType.BLOB.value,
        }

        # when
        actual_blob = converters.blob_to_content(obj)

        # then
        self.assertEqual(actual_blob, expected_blob)

    @istest
    def blob_to_content_link(self):
        # given
        contentfile = b'temp file for testing blob to content conversion'
        tmpfilepath = tmpfile_with_content(self.tmpdir, contentfile)
        tmplinkpath = tempfile.mktemp(dir=self.tmpdir)
        os.symlink(tmpfilepath, tmplinkpath)

        obj = {
            'path': tmplinkpath,
            'perms': git.GitPerm.BLOB,
            'type': git.GitType.BLOB,
            'sha1': 'some-sha1',
            'sha256': 'some-sha256',
            'sha1_git': 'some-sha1git',
        }

        expected_blob = {
            'data': contentfile,
            'length': len(tmpfilepath),
            'status': 'visible',
            'sha1': 'some-sha1',
            'sha256': 'some-sha256',
            'sha1_git': 'some-sha1git',
            'perms': git.GitPerm.BLOB.value,
            'type': git.GitType.BLOB.value,
        }

        # when
        actual_blob = converters.blob_to_content(obj)

        # then
        self.assertEqual(actual_blob, expected_blob)

    @istest
    def blob_to_content_link_with_data_length_populated(self):
        # given
        tmplinkpath = tempfile.mktemp(dir=self.tmpdir)
        obj = {
            'length': 10,  # wrong for test purposes
            'data': 'something wrong',  # again for test purposes
            'path': tmplinkpath,
            'perms': git.GitPerm.BLOB,
            'type': git.GitType.BLOB,
            'sha1': 'some-sha1',
            'sha256': 'some-sha256',
            'sha1_git': 'some-sha1git',
        }

        expected_blob = {
            'length': 10,
            'data': 'something wrong',
            'status': 'visible',
            'sha1': 'some-sha1',
            'sha256': 'some-sha256',
            'sha1_git': 'some-sha1git',
            'perms': git.GitPerm.BLOB.value,
            'type': git.GitType.BLOB.value,
        }

        # when
        actual_blob = converters.blob_to_content(obj)

        # then
        self.assertEqual(actual_blob, expected_blob)

    @istest
    def blob_to_content2_absent_data(self):
        # given
        contentfile = b'temp file for testing blob to content conversion'
        tmpfilepath = tmpfile_with_content(self.tmpdir, contentfile)

        obj = {
            'path': tmpfilepath,
            'perms': git.GitPerm.BLOB,
            'type': git.GitType.BLOB,
            'sha1': 'some-sha1',
            'sha256': 'some-sha256',
            'sha1_git': 'some-sha1git',
        }

        expected_blob = {
            'length': len(contentfile),
            'status': 'absent',
            'sha1': 'some-sha1',
            'sha256': 'some-sha256',
            'sha1_git': 'some-sha1git',
            'perms': git.GitPerm.BLOB.value,
            'type': git.GitType.BLOB.value,
            'reason': 'Content too large',
            'origin': 190
        }

        # when
        actual_blob = converters.blob_to_content(obj, None,
                                                 max_content_size=10,
                                                 origin_id=190)

        # then
        self.assertEqual(actual_blob, expected_blob)

    @istest
    def tree_to_directory_no_entries(self):
        # given
        tree = {
            'path': 'foo',
            'sha1_git': b'tree_sha1_git',
            'children': [{'type': git.GitType.TREE,
                          'perms': git.GitPerm.TREE,
                          'name': 'bar',
                          'sha1_git': b'sha1-target'},
                         {'type': git.GitType.BLOB,
                          'perms': git.GitPerm.BLOB,
                          'name': 'file-foo',
                          'sha1_git': b'file-foo-sha1-target'}]
        }

        expected_directory = {
            'id': b'tree_sha1_git',
            'entries': [{'type': 'dir',
                         'perms': int(git.GitPerm.TREE.value),
                         'name': 'bar',
                         'target': b'sha1-target'},
                        {'type': 'file',
                         'perms': int(git.GitPerm.BLOB.value),
                         'name': 'file-foo',
                         'target': b'file-foo-sha1-target'}]
        }

        # when
        actual_directory = converters.tree_to_directory(tree)

        # then
        self.assertEqual(actual_directory, expected_directory)

    @istest
    def commit_to_revision(self):
        # given
        commit = {
            'sha1_git': 'commit-git-sha1',
            'directory': 'targeted-tree-sha1',
            'author_date': 1444054085,
            'author_offset': '+0000',
            'committer_date': 1444054085,
            'committer_offset': '-0000',
            'type': 'tar',
            'message': 'synthetic-message-input',
            'author_name': 'author-name',
            'author_email': 'author-email',
            'committer_name': 'committer-name',
            'committer_email': 'committer-email',
            'metadata': {'checksums': {'sha1': b'sha1-as-bytes'}},
            'directory': 'targeted-tree-sha1',
        }

        expected_revision = {
            'date': {
                'timestamp': 1444054085,
                'offset': 0,
            },
            'committer_date': {
                'timestamp': 1444054085,
                'offset': 0,
            },
            'type': 'tar',
            'directory': 'targeted-tree-sha1',
            'message': b'synthetic-message-input',
            'author': {
                'name': b'author-name',
                'email': b'author-email',
            },
            'committer': {
                'name': b'committer-name',
                'email': b'committer-email',
            },
            'synthetic': True,
            'metadata': {'checksums': {'sha1': b'sha1-as-bytes'}},
            'parents': [],
        }

        # when
        actual_revision = converters.commit_to_revision(commit)

        # then
        self.assertEquals(actual_revision, expected_revision)

    @istest
    def ref_to_occurrence_1(self):
        # when
        actual_occ = converters.ref_to_occurrence({
            'id': 'some-id',
            'branch': 'some/branch'
        })
        # then
        self.assertEquals(actual_occ, {
            'id': 'some-id',
            'branch': b'some/branch'
        })

    @istest
    def ref_to_occurrence_2(self):
        # when
        actual_occ = converters.ref_to_occurrence({
            'id': 'some-id',
            'branch': b'some/branch'
        })

        # then
        self.assertEquals(actual_occ, {
            'id': 'some-id',
            'branch': b'some/branch'
        })

    @istest
    def shallow_blob(self):
        # when
        actual_blob = converters.shallow_blob({
            'length': 1451,
            'sha1_git':
            b'\xd1\xdd\x9a@\xeb\xf6!\x99\xd4[S\x05\xa8Y\xa3\x80\xa7\xb1;\x9c',
            'name': b'LDPCL',
            'type': b'blob',
            'sha256':
            b'\xe6it!\x99\xb37UT\x8f\x0e\x8f\xd7o\x92"\xce\xa3\x1d\xd2\xe5D>M\xaaj/\x03\x138\xad\x1b',  # noqa
            'perms': b'100644',
            'sha1':
            b'.\x18Y\xd6M\x8c\x9a\xa4\xe1\xf1\xc7\x95\x082\xcf\xc9\xd8\nV)',
            'path':
            b'/tmp/tmp.c86tq5o9.swh.loader/pkg-doc-linux/copyrights/non-free/LDPCL'  # noqa
        })

        # then
        self.assertEqual(actual_blob, {
            'sha1':
            b'.\x18Y\xd6M\x8c\x9a\xa4\xe1\xf1\xc7\x95\x082\xcf\xc9\xd8\nV)',
            'sha1_git':
            b'\xd1\xdd\x9a@\xeb\xf6!\x99\xd4[S\x05\xa8Y\xa3\x80\xa7\xb1;\x9c',
            'sha256':
            b'\xe6it!\x99\xb37UT\x8f\x0e\x8f\xd7o\x92"\xce\xa3\x1d\xd2\xe5D>M\xaaj/\x03\x138\xad\x1b',  # noqa
            'length': 1451,
        })

    @istest
    def shallow_tree(self):
        # when
        actual_shallow_tree = converters.shallow_tree({
            'length': 1451,
            'sha1_git':
            b'tree-id',
            'type': b'tree',
            'sha256':
            b'\xe6it!\x99\xb37UT\x8f\x0e\x8f\xd7o\x92"\xce\xa3\x1d\xd2\xe5D>M\xaaj/\x03\x138\xad\x1b',  # noqa
            'perms': b'100644',
            'sha1':
            b'.\x18Y\xd6M\x8c\x9a\xa4\xe1\xf1\xc7\x95\x082\xcf\xc9\xd8\nV)',
        })

        # then
        self.assertEqual(actual_shallow_tree, b'tree-id')

    @istest
    def shallow_commit(self):
        # when
        actual_shallow_commit = converters.shallow_commit({
            'sha1_git':
            b'\xd1\xdd\x9a@\xeb\xf6!\x99\xd4[S\x05\xa8Y\xa3\x80\xa7\xb1;\x9c',
            'type': b'commit',
            'id': b'let-me-see-some-id',
        })

        # then
        self.assertEqual(actual_shallow_commit, b'let-me-see-some-id')

    @istest
    def shallow_tag(self):
        # when
        actual_shallow_tag = converters.shallow_tag({
            'sha1':
            b'\xd1\xdd\x9a@\xeb\xf6!\x99\xd4[S\x05\xa8Y\xa3\x80\xa7\xb1;\x9c',
            'type': b'tag',
            'id': b'this-is-not-the-id-you-are-looking-for',
        })

        # then
        self.assertEqual(actual_shallow_tag, b'this-is-not-the-id-you-are-looking-for')  # noqa
