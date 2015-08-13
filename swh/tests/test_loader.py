# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import pygit2
import tempfile
import shutil

from nose.plugins.attrib import attr
from nose.tools import istest

from swh.storage import db, models
from swh.gitloader import loader
from swh.conf import reader

import test_initdb
from test_git_utils import create_commit_with_content

@attr('slow')
class FuncUseCase(unittest.TestCase):
    def setUp(self):
        """Initialize a git repository for the remaining test to manipulate.
        """
        tmp_git_folder_path = tempfile.mkdtemp(prefix='test-sgloader.',
                                               dir='/tmp')
        self.tmp_git_repo = pygit2.init_repository(tmp_git_folder_path)

        self.conf = reader.read('./resources/test/back.ini',
                                {'port': ('int', 9999)})

        self.db_url = self.conf['db_url']
        self.conf.update({
            'action': 'load',
            'repo_path': self.tmp_git_repo.workdir,
            'backend_url': 'http://localhost:%s' % self.conf['port']
        })


        test_initdb.prepare_db(self.db_url)

    def tearDown(self):
        """Destroy the test git repository.
        """
        shutil.rmtree(self.tmp_git_repo.workdir)

    @istest
    def use_case_0(self):
        """Trigger loader and make sure everything is ok.
        """
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

        # given
        create_commit_with_content(self.tmp_git_repo, None,
                                   'commit 8 with parent 2',
                                   [commit7.hex])

        # when
        loader.load(self.conf)

        # then
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
