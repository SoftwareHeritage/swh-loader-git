# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os.path
import zipfile
import tempfile
import subprocess

from swh.loader.git.loader import GitLoader, GitLoaderFromArchive
from swh.loader.core.tests import BaseLoaderTest

from . import TEST_LOADER_CONFIG


class GitLoaderFromArchive(GitLoaderFromArchive):
    def project_name_from_archive(self, archive_path):
        # We don't want the project name to be 'resources'.
        return 'testrepo'


CONTENT1 = {
    '33ab5639bfd8e7b95eb1d8d0b87781d4ffea4d5d',  # README v1
    '349c4ff7d21f1ec0eda26f3d9284c293e3425417',  # README v2
    '799c11e348d39f1704022b8354502e2f81f3c037',  # file1.txt
    '4bdb40dfd6ec75cb730e678b5d7786e30170c5fb',  # file2.txt
    }

SNAPSHOT_ID = 'bdf3b06d6017e0d9ad6447a73da6ff1ae9efb8f0'

SNAPSHOT1 = {
    'id': SNAPSHOT_ID,
    'branches': {
        'HEAD': {
            'target': '2f01f5ca7e391a2f08905990277faf81e709a649',
            'target_type': 'revision',
        },
        'refs/heads/master': {
            'target': '2f01f5ca7e391a2f08905990277faf81e709a649',
            'target_type': 'revision',
        },
        'refs/heads/branch1': {
            'target': 'b0a77609903f767a2fd3d769904ef9ef68468b87',
            'target_type': 'revision',
        },
        'refs/heads/branch2': {
            'target': 'bd746cd1913721b269b395a56a97baf6755151c2',
            'target_type': 'revision',
        },
        'refs/tags/branch2-after-delete': {
            'target': 'bd746cd1913721b269b395a56a97baf6755151c2',
            'target_type': 'revision',
        },
        'refs/tags/branch2-before-delete': {
            'target': '1135e94ccf73b5f9bd6ef07b3fa2c5cc60bba69b',
            'target_type': 'revision',
        },
    },
}

# directory hashes obtained with:
# gco b6f40292c4e94a8f7e7b4aff50e6c7429ab98e2a
# swh-hashtree --ignore '.git' --path .
# gco 2f01f5ca7e391a2f08905990277faf81e709a649
# swh-hashtree --ignore '.git' --path .
# gco bcdc5ebfde1a3cd6c96e0c2ea4eed19c13208777
# swh-hashtree --ignore '.git' --path .
# gco 1135e94ccf73b5f9bd6ef07b3fa2c5cc60bba69b
# swh-hashtree --ignore '.git' --path .
# gco 79f65ac75f79dda6ff03d66e1242702ab67fb51c
# swh-hashtree --ignore '.git' --path .
# gco b0a77609903f767a2fd3d769904ef9ef68468b87
# swh-hashtree --ignore '.git' --path .
# gco bd746cd1913721b269b395a56a97baf6755151c2
# swh-hashtree --ignore '.git' --path .
REVISIONS1 = {
    'b6f40292c4e94a8f7e7b4aff50e6c7429ab98e2a':
        '40dbdf55dfd4065422462cc74a949254aefa972e',
    '2f01f5ca7e391a2f08905990277faf81e709a649':
        'e1d0d894835f91a0f887a4bc8b16f81feefdfbd5',
    'bcdc5ebfde1a3cd6c96e0c2ea4eed19c13208777':
        'b43724545b4759244bb54be053c690649161411c',
    '1135e94ccf73b5f9bd6ef07b3fa2c5cc60bba69b':
        'fbf70528223d263661b5ad4b80f26caf3860eb8e',
    '79f65ac75f79dda6ff03d66e1242702ab67fb51c':
        '5df34ec74d6f69072d9a0a6677d8efbed9b12e60',
    'b0a77609903f767a2fd3d769904ef9ef68468b87':
        '9ca0c7d6ffa3f9f0de59fd7912e08f11308a1338',
    'bd746cd1913721b269b395a56a97baf6755151c2':
        'e1d0d894835f91a0f887a4bc8b16f81feefdfbd5',
    }


class BaseGitLoaderTest(BaseLoaderTest):
    def setUp(self, archive_name, uncompress_archive, filename='testrepo'):
        super().setUp(archive_name=archive_name, filename=filename,
                      prefix_tmp_folder_name='swh.loader.git.',
                      start_path=os.path.dirname(__file__),
                      uncompress_archive=uncompress_archive)


class GitLoaderTest(GitLoader):
    def parse_config_file(self, *args, **kwargs):
        return TEST_LOADER_CONFIG


class BaseDirGitLoaderTest(BaseGitLoaderTest):
    """Mixin base loader test to prepare the git
       repository to uncompress, load and test the results.

       This sets up

    """
    def setUp(self):
        super().setUp('testrepo.tgz', True)
        self.loader = GitLoaderTest()
        self.storage = self.loader.storage

    def load(self):
        return self.loader.load(
            origin_url=self.repo_url,
            visit_date='2016-05-03 15:16:32+00',
            directory=self.destination_path)


class GitLoaderFromArchiveTest(GitLoaderFromArchive):
    def parse_config_file(self, *args, **kwargs):
        return TEST_LOADER_CONFIG


class BaseZipGitLoaderTest(BaseGitLoaderTest):
    """Mixin base loader test to prepare the git
       repository to uncompress, load and test the results.

       This sets up

    """
    def setUp(self):
        super().setUp('testrepo.tgz', True)
        self._setup_zip()
        self.loader = GitLoaderFromArchiveTest()
        self.storage = self.loader.storage

    def _setup_zip(self):
        self._zip_file = tempfile.NamedTemporaryFile('ab', suffix='.zip')
        dest_dir = os.path.normpath(self.destination_path) + '/'
        with zipfile.ZipFile(self._zip_file, 'a') as zip_writer:
            for root, dirs, files in os.walk(dest_dir):
                assert root.startswith(dest_dir)
                relative_root = os.path.join(
                        'testrepo',
                        root[len(dest_dir):])
                for file_ in files:
                    zip_writer.write(
                            filename=os.path.join(root, file_),
                            arcname=os.path.join(relative_root, file_))
        self.destination_path = self._zip_file.name
        self.tmp_root_path = None
        self.repo_url = 'file://' + self.destination_path

    def tearDown(self):
        self._zip_file.close()
        super().tearDown()

    def load(self):
        return self.loader.load(
            origin_url=self.repo_url,
            visit_date='2016-05-03 15:16:32+00',
            archive_path=self.destination_path)


class GitLoaderTests:
    """Common tests for all git loaders."""
    def test_load(self):
        """Loads a simple repository (made available by `setUp()`),
        and checks everything was added in the storage."""
        res = self.load()
        self.assertEqual(res['status'], 'eventful', res)

        self.assertContentsContain(CONTENT1)
        self.assertCountDirectories(7)
        self.assertCountReleases(0)  # FIXME: why not 2?
        self.assertCountRevisions(7)
        self.assertCountSnapshots(1)

        self.assertRevisionsContain(REVISIONS1)

        self.assertSnapshotEqual(SNAPSHOT1)

        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

    def test_load_unchanged(self):
        """Checks loading a repository a second time does not add
        any extra data."""
        res = self.load()
        self.assertEqual(res['status'], 'eventful')

        res = self.load()
        self.assertEqual(res['status'], 'uneventful')
        self.assertCountSnapshots(1)


class DirGitLoaderTest(BaseDirGitLoaderTest, GitLoaderTests):
    """Tests for the GitLoader. Includes the common ones, and
    add others that only work with a local dir."""

    def _git(self, *cmd):
        """Small wrapper around subprocess to call Git."""
        try:
            return subprocess.check_output(
                    ['git', '-C', self.destination_path] + list(cmd))
        except subprocess.CalledProcessError as e:
            print(e.output)
            print(e.stderr)
            raise

    def test_load_changed(self):
        """Loads a repository, makes some changes by adding files, commits,
        and merges, load it again, and check the storage contains everything
        it should."""
        # Initial load
        res = self.load()
        self.assertEqual(res['status'], 'eventful', res)

        self._git('config', '--local', 'user.email', 'you@example.com')
        self._git('config', '--local', 'user.name', 'Your Name')

        # Load with a new file + revision
        with open(os.path.join(self.destination_path, 'hello.py'), 'a') as fd:
            fd.write("print('Hello world')\n")

        self._git('add', 'hello.py')
        self._git('commit', '-m', 'Hello world')
        new_revision = self._git('rev-parse', 'master').decode().strip()

        revisions = REVISIONS1.copy()
        assert new_revision not in revisions
        revisions[new_revision] = '85dae072a5aa9923ffa7a7568f819ff21bf49858'

        res = self.load()
        self.assertEqual(res['status'], 'eventful')

        self.assertCountContents(4 + 1)
        self.assertCountDirectories(7 + 1)
        self.assertCountReleases(0)  # FIXME: why not 2?
        self.assertCountRevisions(7 + 1)
        self.assertCountSnapshots(1 + 1)

        self.assertRevisionsContain(revisions)

        # TODO: how to check the snapshot id?
        # self.assertSnapshotEqual(SNAPSHOT1)

        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')

        # Load with a new merge
        self._git('merge', 'branch1', '-m', 'merge')
        new_revision = self._git('rev-parse', 'master').decode().strip()

        assert new_revision not in revisions
        revisions[new_revision] = 'dab8a37df8db8666d4e277bef9a546f585b5bedd'

        res = self.load()
        self.assertEqual(res['status'], 'eventful')

        self.assertCountContents(4 + 1)
        self.assertCountDirectories(7 + 2)
        self.assertCountReleases(0)  # FIXME: why not 2?
        self.assertCountRevisions(7 + 2)
        self.assertCountSnapshots(1 + 1 + 1)

        self.assertRevisionsContain(revisions)

        # TODO: how to check the snapshot id?
        # self.assertSnapshotEqual(SNAPSHOT1)

        self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
        self.assertEqual(self.loader.visit_status(), 'full')


class ZipGitLoaderTest(BaseZipGitLoaderTest, GitLoaderTests):
    """Tests for GitLoaderFromArchive. Imports the common ones
    from GitLoaderTests."""
    pass
