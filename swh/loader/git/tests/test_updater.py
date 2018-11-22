from swh.loader.git.updater import BulkUpdater
from swh.loader.git.tests.test_loader import DirGitLoaderTest


class TestBulkUpdater(BulkUpdater):
    def parse_config_file(self, *args, **kwargs):
        return {
            **super().parse_config_file(*args, **kwargs),
            'storage': {'cls': 'memory', 'args': {}}
        }


class BulkUpdaterTest(DirGitLoaderTest):
    """Same tests as for the GitLoader, but running on BulkUpdater."""
    def setUp(self):
        super().setUp()
        self.loader = TestBulkUpdater()
        self.storage = self.loader.storage

    def load(self):
        return self.loader.load(
            origin_url=self.repo_url)
