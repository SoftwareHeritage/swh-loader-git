from swh.loader.git.updater import BulkUpdater
from swh.loader.git.tests.test_loader import DirGitLoaderTest


class BulkUpdaterTest(DirGitLoaderTest):
    """Same tests as for the GitLoader, but running on BulkUpdater."""
    def setUp(self):
        super().setUp()
        self.loader = BulkUpdater()
        self.loader.storage = self.storage

    def load(self):
        return self.loader.load(
            origin_url=self.repo_url)
