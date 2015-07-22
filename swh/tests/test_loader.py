# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
import os
import pygit2
import tempfile
import shutil

from nose.plugins.attrib import attr

from swh.storage import db, models
from swh.gitloader import loader

import test_initdb


@attr('slow')
class FuncUseCase(unittest.TestCase):
    tmpGitRepo = None
    db_url = None
    db_conn = None

    def create_blob(self, blob_content):
        """Create a blob with blob_content and returns its oid.
        """
        return self.tmpGitRepo.create_blob(blob_content)

    def create_tree(self, blob_content=None):
        """Create a tree.
        If blob_content is specified, create a blob then
        create a tree which points to this blob.
        Returns the tree's oid.
        """
        treeBuilder = self.tmpGitRepo.TreeBuilder()
        if blob_content is not None:
            new_blob = self.create_blob(blob_content)
            treeBuilder.insert('blob', new_blob,
                               pygit2.GIT_FILEMODE_BLOB_EXECUTABLE)

        return treeBuilder.write()

    def create_author_and_committer(self):
        author = pygit2.Signature('Alice Cooper',
                                  'alice@cooper.tld')
        committer = pygit2.Signature('Vincent Furnier',
                                     'vincent@committers.tld')
        return (author, committer)

    def create_commit_with_content(self, blob_content,
                                   commit_msg,
                                   commit_parents=None):
        """Create a commit inside the git repository and return its oid.
        """
        author, committer = self.create_author_and_committer()
        tree = self.create_tree(blob_content)
        return self.tmpGitRepo.create_commit(
            'refs/heads/master',  # the name of the reference to update
            author, committer, commit_msg,
            tree,  # binary string representing the tree object ID
            [] if commit_parents is None else commit_parents  # commit parents
        )

    def setUp(self):
        """Initialize a git repository for the remaining test to manipulate.
        """
        tmpGitFolder = tempfile.mkdtemp(prefix='test-sgloader.',
                                        dir='/tmp')
        # create temporary git repository
        self.tmpGitRepo = pygit2.init_repository(tmpGitFolder)
        # print("Git repository: %s " % tmpGitFolder)

        # trigger the script
        repo_path = self.tmpGitRepo.workdir
        content_storage_dir = os.path.join(repo_path, "content-storage")

        os.makedirs(content_storage_dir, exist_ok=True)

        self.db_url = "dbname=swhgitloader-test"
        self.conf = {
            'db_url': self.db_url,
            'repository': repo_path,
            'content_storage_dir': content_storage_dir,
            'blob_compression': None,
            'folder_depth': 2,
            }

        test_initdb.prepare_db(self.db_url)

    def tearDown(self):
        """Destroy the test git repository.
        """
        # Remove temporary git repository
        shutil.rmtree(self.tmpGitRepo.workdir)

    def use_case_0(self):
        """Trigger loader and make sure everything is ok.
        """

        # given
        commit0 = self.create_commit_with_content('blob 0', 'commit msg 0')
        commit1 = self.create_commit_with_content('blob 1', 'commit msg 1',
                                                  [commit0.hex])
        commit2 = self.create_commit_with_content('blob 2', 'commit msg 2',
                                                  [commit1.hex])
        commit3 = self.create_commit_with_content(None, 'commit msg 3',
                                                  [commit2.hex])
        commit4 = self.create_commit_with_content('blob 4', 'commit msg 4',
                                                  [commit3.hex])

        # when
        self.conf['action'] = "load"
        loader.load(self.conf)

        # then
        with db.connect(self.db_url) as db_conn:
            self.assertEquals(
                models.count_objects(db_conn, models.Type.commit),
                5,
                "Should be 5 commits")
            self.assertEquals(
                models.count_objects(db_conn, models.Type.tree),
                5,
                "Should be 5 trees")
            self.assertEquals(
                models.count_files(db_conn),
                4,
                "Should be 4 blobs as we created one commit without data!")

        # given
        commit5 = self.create_commit_with_content('new blob 5', 'commit msg 5',
                                                  [commit4.hex])
        commit6 = self.create_commit_with_content('new blob and last 6',
                                                  'commit msg 6',
                                                  [commit5.hex])
        commit7 = self.create_commit_with_content('new blob 7', 'commit msg 7',
                                                  [commit6.hex])

        # when
        loader.load(self.conf)

        # then
        with db.connect(self.db_url) as db_conn:
            self.assertEquals(
                models.count_objects(db_conn, models.Type.commit),
                8,
                "Should be 5+3 == 8 commits now")
            self.assertEquals(
                models.count_objects(db_conn, models.Type.tree),
                8,
                "Should be 5+3 == 8 trees")
            self.assertEquals(
                models.count_files(db_conn),
                7,
                "Should be 4+3 == 7 blobs")

        # given
        self.create_commit_with_content(None, 'commit 8 with parent 2',
                                        [commit7.hex])

        # when
        loader.load(self.conf)

        # then
        with db.connect(self.db_url) as db_conn:
            self.assertEquals(
                models.count_objects(db_conn, models.Type.commit),
                9,
                "Should be 8+1 == 9 commits now")
            self.assertEquals(
                models.count_objects(db_conn, models.Type.tree),
                8,
                "Should be 8 trees (new commit without blob so no new tree)")
            self.assertEquals(
                models.count_files(db_conn),
                7,
                "Should be 7 blobs (new commit without new blob)")
