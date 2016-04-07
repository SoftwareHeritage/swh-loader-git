# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import subprocess
import tempfile
import unittest
import datetime

from nose.tools import istest
import pygit2

import swh.loader.git.converters as converters
from swh.core.hashutil import hex_to_hash


class TestConverters(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.repo_path = tempfile.mkdtemp()
        cls.repo = pygit2.init_repository(cls.repo_path, bare=True)

        fast_export = os.path.join(os.path.dirname(__file__),
                                   '../../../../..',
                                   'swh-storage-testdata',
                                   'git-repos',
                                   'example-submodule.fast-export.xz')

        xz = subprocess.Popen(
            ['xzcat'],
            stdin=open(fast_export, 'rb'),
            stdout=subprocess.PIPE,
        )

        git = subprocess.Popen(
            ['git', 'fast-import', '--quiet'],
            stdin=xz.stdout,
            cwd=cls.repo_path,
        )

        # flush stdout of xz
        xz.stdout.close()
        git.communicate()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

        shutil.rmtree(cls.repo_path)
        print(cls.repo_path)

    def setUp(self):
        super().setUp()

        self.blob_id = pygit2.Oid(
            hex='28c6f4023d65f74e3b59a2dea3c4277ed9ee07b0')
        self.blob = {
            'sha1_git': self.blob_id.raw,
            'sha1': hex_to_hash('4850a3420a2262ff061cb296fb915430fa92301c'),
            'sha256': hex_to_hash('fee7c8a485a10321ad94b64135073cb5'
                                  '5f22cb9f57fa2417d2adfb09d310adef'),
            'data': (b'[submodule "example-dependency"]\n'
                     b'\tpath = example-dependency\n'
                     b'\turl = https://github.com/githubtraining/'
                     b'example-dependency.git\n'),
            'length': 124,
            'status': 'visible',
            }

        self.blob_hidden = {
            'sha1_git': self.blob_id.raw,
            'length': 124,
            'status': 'absent',
            'reason': 'Content too large',
            'origin': None,
            }

    @istest
    def blob_to_content(self):
        content = converters.blob_to_content(self.blob_id, self.repo)
        self.assertEqual(self.blob, content)

    @istest
    def blob_to_content_absent(self):
        max_length = self.blob['length'] - 1
        content = converters.blob_to_content(self.blob_id, self.repo,
                                             max_content_size=max_length)
        self.assertEqual(self.blob_hidden, content)

    @istest
    def commit_to_revision(self):
        sha1 = '9768d0b576dbaaecd80abedad6dfd0d72f1476da'
        commit = self.repo.revparse_single(sha1)

        # when
        actual_revision = converters.commit_to_revision(commit.id, self.repo)

        offset = datetime.timedelta(minutes=120)
        tzoffset = datetime.timezone(offset)
        expected_revision = {
            'id': hex_to_hash('9768d0b576dbaaecd80abedad6dfd0d72f1476da'),
            'directory': b'\xf0i\\./\xa7\xce\x9dW@#\xc3A7a\xa4s\xe5\x00\xca',
            'type': 'git',
            'committer': {
                'name': b'Stefano Zacchiroli',
                'email': b'zack@upsilon.cc',
            },
            'author': {
                'name': b'Stefano Zacchiroli',
                'email': b'zack@upsilon.cc',
            },
            'committer_date': datetime.datetime(2015, 9, 24, 10, 36, 5,
                                                tzinfo=tzoffset),
            'message': b'add submodule dependency\n',
            'metadata': None,
            'date': datetime.datetime(2015, 9, 24, 10, 36, 5,
                                      tzinfo=tzoffset),
            'parents': [
                b'\xc3\xc5\x88q23`\x9f[\xbb\xb2\xd9\xe7\xf3\xfbJf\x0f?r'
            ],
            'synthetic': False,
        }

        # then
        self.assertEquals(actual_revision, expected_revision)
        self.assertEquals(offset, expected_revision['date'].utcoffset())
        self.assertEquals(offset,
                          expected_revision['committer_date'].utcoffset())

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
    def author_line_to_author(self):
        tests = {
            b'a <b@c.com>': {
                'name': b'a',
                'email': b'b@c.com',
                'fullname': b'a <b@c.com>',
            },
            b'<foo@bar.com>': {
                'name': None,
                'email': b'foo@bar.com',
                'fullname': b'<foo@bar.com>',
            },
            b'malformed <email': {
                'name': b'malformed',
                'email': None,
                'fullname': b'malformed <email'
            },
            b'trailing <sp@c.e> ': {
                'name': b'trailing',
                'email': b'sp@c.e',
                'fullname': b'trailing <sp@c.e> ',
            },
            b'no<sp@c.e>': {
                'name': b'no',
                'email': b'sp@c.e',
                'fullname': b'no<sp@c.e>',
            },
            b' <>': {
                'name': b'',
                'email': b'',
                'fullname': b' <>',
            },
        }

        for author in sorted(tests):
            parsed_author = tests[author]
            self.assertEquals(parsed_author,
                              converters.parse_author(author))
