from swh.loader.git.updater import BulkUpdater
from swh.loader.git.tests.test_loader import (
        DirGitLoaderTest, LoaderNoStorageMixin)


class BulkUpdaterNoStorage(LoaderNoStorageMixin, BulkUpdater):
    """Subclass of BulkUpdater that uses a mock storage."""
    pass


class BulkUpdaterTest(DirGitLoaderTest):
    """Same tests as for the GitLoader, but running on BulkUpdater."""
    def setUp(self):
        super().setUp()
        self.loader = BulkUpdaterNoStorage()

    def load(self):
        return self.loader.load(
            origin_url=self.repo_url)
