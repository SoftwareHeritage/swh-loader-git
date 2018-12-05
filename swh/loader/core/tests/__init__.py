# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest
import shutil
import subprocess
import tempfile

from unittest import TestCase

from swh.model import hashutil
from swh.model.hashutil import hash_to_bytes


class BaseLoaderStorageTest:
    def _assertCountEqual(self, type, expected_length, msg=None):
        """Check typed 'type' state to have the same expected length.

        """
        self.storage.refresh_stat_counters()
        self.assertEqual(self.storage.stat_counters()[type],
                         expected_length, msg=msg)

    def assertCountContents(self, len_expected_contents, msg=None):
        self._assertCountEqual('content', len_expected_contents, msg=msg)

    def assertCountDirectories(self, len_expected_directories, msg=None):
        self._assertCountEqual('directory', len_expected_directories,
                               msg=msg)

    def assertCountReleases(self, len_expected_releases, msg=None):
        self._assertCountEqual('release', len_expected_releases, msg=msg)

    def assertCountRevisions(self, len_expected_revisions, msg=None):
        self._assertCountEqual('revision', len_expected_revisions, msg=msg)

    def assertCountSnapshots(self, len_expected_snapshot, msg=None):
        self._assertCountEqual('snapshot', len_expected_snapshot, msg=msg)

    def assertContentsContain(self, expected_contents):
        """Check the provided content are a subset of the stored ones.

        Args:
            expected_contents ([sha1]): List of content ids"""
        self._assertCountEqual('content', len(expected_contents))
        missing = list(self.storage.content_missing(
            {'sha1': hash_to_bytes(content_hash)}
            for content_hash in expected_contents))
        self.assertEqual(missing, [])

    def assertDirectoriesContain(self, expected_directories):
        """Check the provided directories are a subset of the stored ones.

        Args:
            expected_directories ([sha1]): List of directory ids."""
        self._assertCountEqual('directory', len(expected_directories))
        missing = list(self.storage.directory_missing(
            hash_to_bytes(dir_) for dir_ in expected_directories))
        self.assertEqual(missing, [])

    def assertReleasesContain(self, expected_releases):
        """Check the provided releases are a subset of the stored ones.

        Args:
            releases (list): list of swh releases' identifiers.

        """
        self._assertCountEqual('release', len(expected_releases))
        missing = list(self.storage.release_missing(
            hash_to_bytes(rel) for rel in expected_releases))
        self.assertEqual(missing, [])

    def assertRevisionsContain(self, expected_revisions):
        """Check the provided revisions are a subset of the stored ones.

        Expects self.loader to be instantiated and ready to be
        inspected (meaning the loading took place).

        Args:
            expected_revisions (dict): Dict with key revision id,
            value the targeted directory id.

        """
        self._assertCountEqual('revision', len(expected_revisions))

        revs = list(self.storage.revision_get(
            hashutil.hash_to_bytes(rev_id) for rev_id in expected_revisions))
        self.assertNotIn(None, revs)
        self.assertEqual(
            {rev['id']: rev['directory'] for rev in revs},
            {hash_to_bytes(rev_id): hash_to_bytes(rev_dir)
             for (rev_id, rev_dir) in expected_revisions.items()})

    def assertSnapshotEqual(self, expected_snapshot, expected_branches=[]):
        """Check for snapshot match.

        Provide the hashes as hexadecimal, the conversion is done
        within the method.

        Args:

            expected_snapshot (str/dict): Either the snapshot
                                          identifier or the full
                                          snapshot
            expected_branches (dict): expected branches or nothing is
                                      the full snapshot is provided

        """
        if isinstance(expected_snapshot, dict) and not expected_branches:
            expected_snapshot_id = expected_snapshot['id']
            expected_branches = expected_snapshot['branches']
        else:
            expected_snapshot_id = expected_snapshot

        self._assertCountEqual('snapshot', 1)

        snap = self.storage.snapshot_get(hash_to_bytes(expected_snapshot_id))
        self.assertIsNotNone(snap)

        def decode_target(target):
            if not target:
                return target
            target_type = target['target_type']

            if target_type == 'alias':
                decoded_target = target['target'].decode('utf-8')
            else:
                decoded_target = hashutil.hash_to_hex(target['target'])

            return {
                'target': decoded_target,
                'target_type': target_type
            }

        branches = {
            branch.decode('utf-8'): decode_target(target)
            for branch, target in snap['branches'].items()
        }
        self.assertEqual(expected_branches, branches)

    def assertOriginMetadataContains(self, origin_type, origin_url,
                                     expected_origin_metadata):
        """Check the storage contains this metadata for the given origin.

        Args:

            origin_type (str): type of origin ('deposit', 'git', 'svn', ...)
            origin_url (str): URL of the origin
            expected_origin_metadata (dict):
                              Extrinsic metadata of the origin
                              <https://forge.softwareheritage.org/T1344>
        """
        origin = self.storage.origin_get(
                dict(type=origin_type, url=origin_url))
        results = self.storage.origin_metadata_get_by(origin['id'])
        self.assertEqual(len(results), 1, results)
        result = results[0]
        self.assertEqual(result['metadata'], expected_origin_metadata)


@pytest.mark.fs
class BaseLoaderTest(TestCase, BaseLoaderStorageTest):
    """Mixin base loader test class.

    This allows to uncompress archives (mercurial, svn, git,
    ... repositories) into a temporary folder so that the loader under
    test can work with this.

    When setUp() is done, the following variables are defined:
    - self.repo_url: can be used as an origin_url for example
    - self.destination_path: can be used as a path to ingest the
                             <techno> repository.

    Args:
        archive_name (str): Name of the archive holding the repository
                            (folder, repository, dump, etc...)
        start_path (str): (mandatory) Path from where starting to look
                                      for resources
        filename (Optional[str]): Name of the filename/folder once the
            archive is uncompressed. When the filename is not
            provided, the archive name is used as a derivative. This
            is used both for the self.repo_url and
            self.destination_path computation (this one only when
            provided)
        resources_path (str): Folder name to look for archive
        prefix_tmp_folder_name (str): Prefix name to name the temporary folder
        uncompress_archive (bool): Uncompress the archive passed as
                                  parameters (default to True). It so
                                  happens we could avoid doing
                                  anything to the tarball.

    """
    def setUp(self, archive_name, *, start_path, filename=None,
              resources_path='resources', prefix_tmp_folder_name='',
              uncompress_archive=True):
        super().setUp()
        repo_path = os.path.join(start_path, resources_path, archive_name)
        if not uncompress_archive:
            # In that case, simply sets the archive's path
            self.destination_path = repo_path
            self.tmp_root_path = None
            self.repo_url = 'file://' + repo_path
            return
        tmp_root_path = tempfile.mkdtemp(
            prefix=prefix_tmp_folder_name, suffix='-tests')
        # uncompress folder/repositories/dump for the loader to ingest
        subprocess.check_output(['tar', 'xf', repo_path, '-C', tmp_root_path])
        # build the origin url (or some derivative form)
        _fname = filename if filename else os.path.basename(archive_name)
        self.repo_url = 'file://' + tmp_root_path + '/' + _fname
        # where is the data to ingest?
        if filename:
            # archive holds one folder with name <filename>
            self.destination_path = os.path.join(tmp_root_path, filename)
        else:
            self.destination_path = tmp_root_path
        self.tmp_root_path = tmp_root_path

    def tearDown(self):
        """Clean up temporary working directory

        """
        if self.tmp_root_path and os.path.exists(self.tmp_root_path):
            shutil.rmtree(self.tmp_root_path)
