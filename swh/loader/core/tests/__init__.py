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

    """
    def setUp(self, archive_name, *, start_path, filename=None,
              resources_path='resources', prefix_tmp_folder_name=''):
        tmp_root_path = tempfile.mkdtemp(
            prefix=prefix_tmp_folder_name, suffix='-tests')
        repo_path = os.path.join(start_path, resources_path, archive_name)
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
        shutil.rmtree(self.tmp_root_path)

    def assertContentsOk(self, expected_contents):
        contents = self.loader.all_contents
        self.assertEquals(len(contents), len(expected_contents))

        for content in contents:
            content_id = hashutil.hash_to_hex(content['sha1'])
            self.assertIn(content_id, expected_contents)

    def assertDirectoriesOk(self, expected_directories):
        directories = self.loader.all_directories
        self.assertEquals(len(directories), len(expected_directories))

        for _dir in directories:
            _dir_id = hashutil.hash_to_hex(_dir['id'])
            self.assertIn(_dir_id, expected_directories)

    def assertReleasesOk(self, expected_releases):
        """Check the loader's releases match the expected releases.

        Args:
            releases ([dict]): List of dictionaries representing swh releases.

        """
        releases = self.loader.all_releases
        self.assertEqual(len(releases), len(expected_releases))
        for i, rel in enumerate(self.loader.all_releases):
            rel_id = hashutil.hash_to_hex(rel['id'])
            self.assertEquals(expected_releases[i], rel_id)

    def assertRevisionsOk(self, expected_revisions):  # noqa: N802
        """Check the loader's revisions match the expected revisions.

        Expects self.loader to be instantiated and ready to be
        inspected (meaning the loading took place).

        Args:
            expected_revisions (dict): Dict with key revision id,
            value the targeted directory id.

        """
        revisions = self.loader.all_revisions
        self.assertEqual(len(revisions), len(expected_revisions))
        for rev in revisions:
            rev_id = hashutil.hash_to_hex(rev['id'])
            directory_id = hashutil.hash_to_hex(rev['directory'])

            self.assertEquals(expected_revisions[rev_id], directory_id)

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

        snapshots = self.loader.all_snapshots
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

    cf. HgLoaderNoStorage, SvnLoaderNoStorage, etc...

    """
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.all_contents = []
        self.all_directories = []
        self.all_revisions = []
        self.all_releases = []
        self.all_snapshots = []
        self.__objects = {
            'content': self.all_contents,
            'directory': self.all_directories,
            'revision': self.all_revisions,
            'release': self.all_releases,
            'snapshot': self.all_snapshots,
        }

    def _add(self, type, l):
        """Add without duplicates and keeping the insertion order.

        Args:
            type (str): Type of objects concerned by the action
            l ([object]): List of 'type' object

        """
        col = self.__objects[type]
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
