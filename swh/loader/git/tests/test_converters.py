# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest
import shutil
import subprocess
import tempfile
import unittest

import dulwich.repo

import swh.loader.git.converters as converters
from swh.model.hashutil import bytehex_to_hash, hash_to_bytes

TEST_DATA = os.path.join(os.path.dirname(__file__), 'data')


class SWHTargetType:
    """Dulwich lookalike TargetType class

    """
    def __init__(self, type_name):
        self.type_name = type_name


class SWHTag:
    """Dulwich lookalike tag class

    """
    def __init__(self, name, type_name, target, target_type, tagger, tag_time,
                 tag_timezone, message):
        self.name = name
        self.type_name = type_name
        self.object = SWHTargetType(target_type), target
        self.tagger = tagger
        self._message = message
        self.tag_time = tag_time
        self.tag_timezone = tag_timezone
        self._tag_timezone_neg_utc = False

    def sha(self):
        from hashlib import sha1
        return sha1()


@pytest.mark.fs
class TestConverters(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.repo_path = tempfile.mkdtemp()
        cls.repo = dulwich.repo.Repo.init_bare(cls.repo_path)

        fast_export = os.path.join(
            TEST_DATA, 'git-repos', 'example-submodule.fast-export.xz')

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

    def test_blob_to_content(self):
        content = converters.dulwich_blob_to_content(self.repo[self.blob_id])
        self.assertEqual(self.blob, content)

    def test_blob_to_content_absent(self):
        max_length = self.blob['length'] - 1
        content = converters.dulwich_blob_to_content(
            self.repo[self.blob_id], max_content_size=max_length)
        self.assertEqual(self.blob_hidden, content)

    def test_commit_to_revision(self):
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

        self.assertEqual(revision, expected_revision)

    def test_author_line_to_author(self):
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
            self.assertEqual(parsed_author,
                             converters.parse_author(author))

    def test_dulwich_tag_to_release_no_author_no_date(self):
        target = b'641fb6e08ddb2e4fd096dcf18e80b894bf'
        message = b'some release message'
        tag = SWHTag(name='blah',
                     type_name=b'tag',
                     target=target,
                     target_type=b'commit',
                     message=message,
                     tagger=None,
                     tag_time=None, tag_timezone=None)

        # when
        actual_release = converters.dulwich_tag_to_release(tag)

        # then
        expected_release = {
            'author': None,
            'date': None,
            'id': b'\xda9\xa3\xee^kK\r2U\xbf\xef\x95`\x18\x90\xaf\xd8\x07\t',
            'message': message,
            'metadata': None,
            'name': 'blah',
            'synthetic': False,
            'target': hash_to_bytes(target.decode()),
            'target_type': 'revision'
        }

        self.assertEqual(actual_release, expected_release)

    def test_dulwich_tag_to_release_author_and_date(self):
        tagger = b'hey dude <hello@mail.org>'
        target = b'641fb6e08ddb2e4fd096dcf18e80b894bf'
        message = b'some release message'

        import datetime
        date = datetime.datetime(
            2007, 12, 5, tzinfo=datetime.timezone.utc
        ).timestamp()

        tag = SWHTag(name='blah',
                     type_name=b'tag',
                     target=target,
                     target_type=b'commit',
                     message=message,
                     tagger=tagger,
                     tag_time=date,
                     tag_timezone=0)

        # when
        actual_release = converters.dulwich_tag_to_release(tag)

        # then
        expected_release = {
            'author': {
                'email': b'hello@mail.org',
                'fullname': b'hey dude <hello@mail.org>',
                'name': b'hey dude'
            },
            'date': {
                'negative_utc': False,
                'offset': 0,
                'timestamp': 1196812800.0
            },
            'id': b'\xda9\xa3\xee^kK\r2U\xbf\xef\x95`\x18\x90\xaf\xd8\x07\t',
            'message': message,
            'metadata': None,
            'name': 'blah',
            'synthetic': False,
            'target': hash_to_bytes(target.decode()),
            'target_type': 'revision'
        }

        self.assertEqual(actual_release, expected_release)

    def test_dulwich_tag_to_release_author_no_date(self):
        # to reproduce bug T815 (fixed)
        tagger = b'hey dude <hello@mail.org>'
        target = b'641fb6e08ddb2e4fd096dcf18e80b894bf'
        message = b'some release message'
        tag = SWHTag(name='blah',
                     type_name=b'tag',
                     target=target,
                     target_type=b'commit',
                     message=message,
                     tagger=tagger,
                     tag_time=None, tag_timezone=None)

        # when
        actual_release = converters.dulwich_tag_to_release(tag)

        # then
        expected_release = {
            'author': {
                'email': b'hello@mail.org',
                'fullname': b'hey dude <hello@mail.org>',
                'name': b'hey dude'
            },
            'date': None,
            'id': b'\xda9\xa3\xee^kK\r2U\xbf\xef\x95`\x18\x90\xaf\xd8\x07\t',
            'message': message,
            'metadata': None,
            'name': 'blah',
            'synthetic': False,
            'target': hash_to_bytes(target.decode()),
            'target_type': 'revision'
        }

        self.assertEqual(actual_release, expected_release)
