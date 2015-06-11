import unittest

import os
import pygit2
import tempfile
import shutil

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.gitloader import loader, models
from swh import db


@attr('slow')
class TestingLearning(unittest.TestCase):
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

    def create_content_and_commit(self, blob_content,
                                  commit_msg,
                                  commit_parent=None):
        """Create a commit inside the git repository and return its oid.
        """
        author, committer = self.create_author_and_committer()

        tree = self.create_tree(blob_content)

        return self.tmpGitRepo.create_commit(
            'refs/heads/master',  # the name of the reference to update
            author, committer, commit_msg,
            tree,  # binary string representing the tree object ID
            [] if commit_parent is None else [commit_parent]  # commit parents
        )

    def setUp(self):
        """Initialize a git repository for the remaining test to manipulate.
        """
        tmpGitFolder = tempfile.mkdtemp(prefix='test-sgloader.',
                                        dir='/tmp')

        # create temporary git repository
        self.tmpGitRepo = pygit2.init_repository(tmpGitFolder)
        print("Git repository: {}".format(tmpGitFolder))

        commit0 = self.create_content_and_commit('blob 0', 'commit msg 0')
        commit1 = self.create_content_and_commit('blob 1', 'commit msg 1',
                                                 commit0.hex)
        commit2 = self.create_content_and_commit('blob 2', 'commit msg 2',
                                                 commit1.hex)
        commit3 = self.create_content_and_commit(None, 'commit msg 3',
                                                 commit2.hex)
        self.create_content_and_commit('blob 4', 'commit msg 4', commit3.hex)

        # open connection to db
        self.db_url = "dbname=swhgitloader-test user=tony"

    def tearDown(self):
        """Destroy the test git repository.
        """

        # Remove temporary git repository
        shutil.rmtree(self.tmpGitRepo.workdir)

    @istest
    def tryout(self):
        """Trigger sgloader and make sure everything is ok.
        """
        # trigger the script
        repo_path = self.tmpGitRepo.workdir
        file_content_storage_dir = os.path.join(repo_path, "dataset")

        os.makedirs(file_content_storage_dir, exist_ok=True)

        conf = {
            'db_url': self.db_url,
            'repository': repo_path,
            'file_content_storage_dir': file_content_storage_dir,
            'object_content_storage_dir': None
            }

        conf['action'] = "cleandb"
        loader.run(conf)

        conf['action'] = "initdb"
        loader.run(conf)

        conf['action'] = "load"
        loader.run(conf)

        with db.connect(self.db_url) as db_conn:
            assert models.count_objects(db_conn, models.Type.commit) == 5
            assert models.count_objects(db_conn, models.Type.tree) == 5
            assert models.count_files(db_conn) == 4
