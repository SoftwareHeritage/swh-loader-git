# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import subprocess
import tempfile
import unittest

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
            ['git', 'fast-import'],
            stdin=xz.stdout,
            stdout=subprocess.PIPE,
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
