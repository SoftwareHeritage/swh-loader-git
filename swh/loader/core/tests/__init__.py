# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import subprocess
import tempfile

from unittest import TestCase
from nose.plugins.attrib import attr

from swh.model import hashutil


@attr('fs')
class BaseLoaderTest(TestCase):
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
        repo_path = os.path.join(start_path, resources_path, archive_name)
        if not uncompress_archive:
            # In that case, simply sets the archive's path
            self.destination_path = repo_path
            self.tmp_root_path = None
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

    def state(self, _type):
        return self.loader.state(_type)

    def _assertCountOk(self, type, expected_length, msg=None):
        """Check typed 'type' state to have the same expected length.

        """
        self.assertEqual(len(self.state(type)), expected_length, msg=msg)

    def assertCountContents(self, len_expected_contents, msg=None):
        self._assertCountOk('content', len_expected_contents, msg=msg)

    def assertCountDirectories(self, len_expected_directories, msg=None):
        self._assertCountOk('directory', len_expected_directories, msg=msg)

    def assertCountReleases(self, len_expected_releases, msg=None):
        self._assertCountOk('release', len_expected_releases, msg=msg)

    def assertCountRevisions(self, len_expected_revisions, msg=None):
        self._assertCountOk('revision', len_expected_revisions, msg=msg)

    def assertCountSnapshots(self, len_expected_snapshot, msg=None):
        self._assertCountOk('snapshot', len_expected_snapshot, msg=msg)

    def assertContentsOk(self, expected_contents):
        self._assertCountOk('content', len(expected_contents))
        for content in self.state('content'):
            content_id = hashutil.hash_to_hex(content['sha1'])
            self.assertIn(content_id, expected_contents)

    def assertDirectoriesOk(self, expected_directories):
        self._assertCountOk('directory', len(expected_directories))
        for _dir in self.state('directory'):
            _dir_id = hashutil.hash_to_hex(_dir['id'])
            self.assertIn(_dir_id, expected_directories)

    def assertReleasesOk(self, expected_releases):
        """Check the loader's releases match the expected releases.

        Args:
            releases ([dict]): List of dictionaries representing swh releases.

        """
        self._assertCountOk('release', len(expected_releases))
        for i, rel in enumerate(self.state('release')):
            rel_id = hashutil.hash_to_hex(rel['id'])
            self.assertEqual(expected_releases[i], rel_id)

    def assertRevisionsOk(self, expected_revisions):
        """Check the loader's revisions match the expected revisions.

        Expects self.loader to be instantiated and ready to be
        inspected (meaning the loading took place).

        Args:
            expected_revisions (dict): Dict with key revision id,
            value the targeted directory id.

        """
        self._assertCountOk('revision', len(expected_revisions))
        for rev in self.state('revision'):
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEqual(expected_revisions[rev_id], directory_id)

    def assertSnapshotOk(self, expected_snapshot, expected_branches=[]):
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

        snapshots = self.state('snapshot')
        self.assertEqual(len(snapshots), 1)

        snap = snapshots[0]
        snap_id = hashutil.hash_to_hex(snap['id'])
        self.assertEqual(snap_id, expected_snapshot_id)

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


class LoaderNoStorage:
    """Mixin class to inhibit the persistence and keep in memory the data
    sent for storage (for testing purposes).

    This overrides the core loader's behavior to store in a dict the
    swh objects.

    cf. :class:`HgLoaderNoStorage`, :class:`SvnLoaderNoStorage`, etc...

    """
    def __init__(self, *args, **kwargs):
        super().__init__()
        self._state = {
            'content': [],
            'directory': [],
            'revision': [],
            'release': [],
            'snapshot': [],
        }

    def state(self, type):
        return self._state[type]

    def _add(self, type, l):
        """Add without duplicates and keeping the insertion order.

        Args:
            type (str): Type of objects concerned by the action
            l ([object]): List of 'type' object

        """
        col = self.state(type)
        for o in l:
            if o in col:
                continue
            col.append(o)

    def maybe_load_contents(self, all_contents):
        self._add('content', all_contents)

    def maybe_load_directories(self, all_directories):
        self._add('directory', all_directories)

    def maybe_load_revisions(self, all_revisions):
        self._add('revision', all_revisions)

    def maybe_load_releases(self, all_releases):
        self._add('release', all_releases)

    def maybe_load_snapshot(self, snapshot):
        self._add('snapshot', [snapshot])

    def send_batch_contents(self, all_contents):
        self._add('content', all_contents)

    def send_batch_directories(self, all_directories):
        self._add('directory', all_directories)

    def send_batch_revisions(self, all_revisions):
        self._add('revision', all_revisions)

    def send_batch_releases(self, all_releases):
        self._add('release', all_releases)

    def send_snapshot(self, snapshot):
        self._add('snapshot', [snapshot])

    def _store_origin_visit(self):
        pass

    def open_fetch_history(self):
        pass

    def close_fetch_history_success(self, fetch_history_id):
        pass

    def close_fetch_history_failure(self, fetch_history_id):
        pass

    def update_origin_visit(self, origin_id, visit, status):
        pass

    def close_failure(self):
        pass

    def close_success(self):
        pass

    def pre_cleanup(self):
        pass
