# coding: utf-8

# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import unittest
import pygit2
import tempfile
import shutil

from nose.plugins.attrib import attr
from nose.tools import istest

from swh.loader.git.storage import db, models
from swh.loader.git import loader
from swh.loader.git.conf import reader

import test_initdb
from test_utils import list_files_from
from test_git_utils import create_commit_with_content, create_tag

@attr('slow')
class TestLocalLoader(unittest.TestCase):
    def setUp(self):
        """Initialize a git repository for the remaining test to manipulate.
        """
        tmp_git_folder_path = tempfile.mkdtemp(prefix='test-sgloader.',
                                               dir='/tmp')
        self.tmp_git_repo = pygit2.init_repository(tmp_git_folder_path)

        self.conf_back = reader.read('./resources/test/back.ini',
                                     {'port': ('int', 9999)})

        self.db_url = self.conf_back['db_url']

        self.conf = {
            'action': 'load',
            'repo_path': self.tmp_git_repo.workdir,
            'backend-type': 'local',
            'backend': './resources/test/back.ini'
        }

    def init_db_setup(self):
        """Initialize a git repository for the remaining test to manipulate.
        """
        test_initdb.prepare_db(self.db_url)

    def tearDown(self):
        """Destroy the test git repository.
        """
        shutil.rmtree(self.tmp_git_repo.workdir)
        shutil.rmtree(self.conf_back['content_storage_dir'], ignore_errors=True)

    @istest
    def should_fail_on_bad_action(self):
        # when
        try:
            loader.load({'action': 'unknown'})
        except:
            pass

    @istest
    def should_fail_on_inexistant_folder(self):
        # when
        try:
            loader.load({'action': 'load',
                         'repo_path': 'something-that-definitely-does-not-exist'})
        except:
            pass

    @istest
    def should_fail_on_inexistant_backend_type(self):
        # when
        try:
            loader.load({'action': 'load',
                         'repo_path': '.',
                         'backend-type': 'unknown'})  # only local or remote supported
        except:
            pass

    @istest
    def local_loader(self):
        """Trigger loader and make sure everything is ok.
        """
        self.init_db_setup()

        # given
        commit0 = create_commit_with_content(self.tmp_git_repo, 'blob 0',
                                             'commit msg 0')
        commit1 = create_commit_with_content(self.tmp_git_repo, 'blob 1',
                                             'commit msg 1',
                                             [commit0.hex])
        commit2 = create_commit_with_content(self.tmp_git_repo, 'blob 2',
                                             'commit msg 2',
                                             [commit1.hex])
        commit3 = create_commit_with_content(self.tmp_git_repo, None,
                                             'commit msg 3',
                                             [commit2.hex])
        commit4 = create_commit_with_content(self.tmp_git_repo, 'blob 4',
                                             'commit msg 4',
                                             [commit3.hex])

        # when
        loader.load(self.conf)

        # then
        nb_files = len(list_files_from(self.conf_back['content_storage_dir']))
        self.assertEquals(nb_files, 4, "4 blobs.")

        with db.connect(self.db_url) as db_conn:
            self.assertEquals(
                models.count_revisions(db_conn),
                5,
                "Should be 5 commits")
            self.assertEquals(
                models.count_directories(db_conn),
                5,
                "Should be 5 trees")
            self.assertEquals(
                models.count_contents(db_conn),
                4,
                "Should be 4 blobs as we created one commit without data!")
            self.assertEquals(
                models.count_release(db_conn),
                0,
                "No tag created so 0 release.")
            self.assertEquals(
                models.count_occurrence(db_conn),
                1,
                "Should be 1 reference (master) so 1 occurrence.")

        # given
        commit5 = create_commit_with_content(self.tmp_git_repo, 'new blob 5',
                                             'commit msg 5',
                                             [commit4.hex])
        commit6 = create_commit_with_content(self.tmp_git_repo,
                                             'new blob and last 6',
                                             'commit msg 6',
                                             [commit5.hex])
        commit7 = create_commit_with_content(self.tmp_git_repo, 'new blob 7',
                                             'commit msg 7',
                                             [commit6.hex])

        # when
        loader.load(self.conf)

        # then
        nb_files = len(list_files_from(self.conf_back['content_storage_dir']))
        self.assertEquals(nb_files, 4+3, "3 new blobs.")

        with db.connect(self.db_url) as db_conn:
            self.assertEquals(
                models.count_revisions(db_conn),
                8,
                "Should be 5+3 == 8 commits now")
            self.assertEquals(
                models.count_directories(db_conn),
                8,
                "Should be 5+3 == 8 trees")
            self.assertEquals(
                models.count_contents(db_conn),
                7,
                "Should be 4+3 == 7 blobs")
            self.assertEquals(
                models.count_release(db_conn),
                0,
                "No tag created so 0 release.")
            self.assertEquals(
                models.count_occurrence(db_conn),
                2,
                "Should be 1 reference which changed twice so 2 occurrences (master changed).")

        # given
        create_commit_with_content(self.tmp_git_repo, None,
                                   'commit 8 with parent 2',
                                   [commit7.hex])

        # when
        loader.load(self.conf)

        # then
        nb_files = len(list_files_from(self.conf_back['content_storage_dir']))
        self.assertEquals(nb_files, 7, "no new blob.")

        with db.connect(self.db_url) as db_conn:
            self.assertEquals(
                models.count_revisions(db_conn),
                9,
                "Should be 8+1 == 9 commits now")
            self.assertEquals(
                models.count_directories(db_conn),
                8,
                "Should be 8 trees (new commit without blob so no new tree)")
            self.assertEquals(
                models.count_contents(db_conn),
                7,
                "Should be 7 blobs (new commit without new blob)")
            self.assertEquals(
                models.count_release(db_conn),
                0,
                "No tag created so 0 release.")
            self.assertEquals(
                models.count_occurrence(db_conn),
                3,
                "Should be 1 reference which changed thrice so 3 occurrences (master changed again).")
            self.assertEquals(
                models.count_person(db_conn),
                2,
                "1 author + 1 committer")


        # add tag
        create_tag(self.tmp_git_repo, '0.0.1', commit5, 'bad ass release 0.0.1, towards infinity...')
        create_tag(self.tmp_git_repo, '0.0.2', commit7, 'release 0.0.2... and beyond')

        loader.load(self.conf)

        # then
        nb_files = len(list_files_from(self.conf_back['content_storage_dir']))
        self.assertEquals(nb_files, 7, "no new blob.")

        with db.connect(self.db_url) as db_conn:
            self.assertEquals(
                models.count_revisions(db_conn),
                9,
                "Should be 8+1 == 9 commits now")
            self.assertEquals(
                models.count_directories(db_conn),
                8,
                "Should be 8 trees (new commit without blob so no new tree)")
            self.assertEquals(
                models.count_contents(db_conn),
                7,
                "Should be 7 blobs (new commit without new blob)")
            self.assertEquals(
                models.count_release(db_conn),
                2,
                "Should be 2 annotated tags so 2 releases")
            self.assertEquals(
                models.count_occurrence(db_conn),
                3,
                "master did not change this time so still 3 occurrences")
            self.assertEquals(
                models.count_person(db_conn),
                3,
                "1 author + 1 committer + 1 tagger")
