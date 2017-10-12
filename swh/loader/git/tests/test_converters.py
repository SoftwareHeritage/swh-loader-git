# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import subprocess
import tempfile
import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

import dulwich.repo

import swh.loader.git.converters as converters
from swh.model.hashutil import bytehex_to_hash, hash_to_bytes


@attr('fs')
class TestConverters(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.repo_path = tempfile.mkdtemp()
        cls.repo = dulwich.repo.Repo.init_bare(cls.repo_path)

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

    def setUp(self):
        super().setUp()

        self.blob_id = b'28c6f4023d65f74e3b59a2dea3c4277ed9ee07b0'
        self.blob = {
            'sha1_git': bytehex_to_hash(self.blob_id),
            'sha1': hash_to_bytes('4850a3420a2262ff061cb296fb915430fa92301c'),
            'sha256': hash_to_bytes('fee7c8a485a10321ad94b64135073cb5'
                                    '5f22cb9f57fa2417d2adfb09d310adef'),
            'blake2s256': hash_to_bytes('5d71873f42a137f6d89286e43677721e574'
                                        '1fa05ce4cd5e3c7ea7c44d4c2d10b'),
            'data': (b'[submodule "example-dependency"]\n'
                     b'\tpath = example-dependency\n'
                     b'\turl = https://github.com/githubtraining/'
                     b'example-dependency.git\n'),
            'length': 124,
            'status': 'visible',
        }

        self.blob_hidden = {
            'sha1_git': bytehex_to_hash(self.blob_id),
            'sha1': hash_to_bytes('4850a3420a2262ff061cb296fb915430fa92301c'),
            'sha256': hash_to_bytes('fee7c8a485a10321ad94b64135073cb5'
                                    '5f22cb9f57fa2417d2adfb09d310adef'),
            'blake2s256': hash_to_bytes('5d71873f42a137f6d89286e43677721e574'
                                        '1fa05ce4cd5e3c7ea7c44d4c2d10b'),
            'length': 124,
            'status': 'absent',
            'reason': 'Content too large',
            'origin': None,
        }

    @istest
    def blob_to_content(self):
        content = converters.dulwich_blob_to_content(self.repo[self.blob_id])
        self.assertEqual(self.blob, content)

    @istest
    def blob_to_content_absent(self):
        max_length = self.blob['length'] - 1
        content = converters.dulwich_blob_to_content(
            self.repo[self.blob_id], max_content_size=max_length)
        self.assertEqual(self.blob_hidden, content)

    @istest
    def commit_to_revision(self):
        sha1 = b'9768d0b576dbaaecd80abedad6dfd0d72f1476da'

        revision = converters.dulwich_commit_to_revision(self.repo[sha1])

        expected_revision = {
            'id': hash_to_bytes('9768d0b576dbaaecd80abedad6dfd0d72f1476da'),
            'directory': b'\xf0i\\./\xa7\xce\x9dW@#\xc3A7a\xa4s\xe5\x00\xca',
            'type': 'git',
            'committer': {
                'name': b'Stefano Zacchiroli',
                'fullname': b'Stefano Zacchiroli <zack@upsilon.cc>',
                'email': b'zack@upsilon.cc',
            },
            'author': {
                'name': b'Stefano Zacchiroli',
                'fullname': b'Stefano Zacchiroli <zack@upsilon.cc>',
                'email': b'zack@upsilon.cc',
            },
            'committer_date': {
                'negative_utc': None,
                'timestamp': 1443083765,
                'offset': 120,
            },
            'message': b'add submodule dependency\n',
            'metadata': None,
            'date': {
                'negative_utc': None,
                'timestamp': 1443083765,
                'offset': 120,
            },
            'parents': [
                b'\xc3\xc5\x88q23`\x9f[\xbb\xb2\xd9\xe7\xf3\xfbJf\x0f?r'
            ],
            'synthetic': False,
        }

        self.assertEquals(revision, expected_revision)

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
