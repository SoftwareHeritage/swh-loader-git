from swh.loader.git.loader import BulkUpdater
from swh.loader.git.tests.test_from_disk import DirGitLoaderTest


class BulkUpdaterTest(BulkUpdater):
    def parse_config_file(self, *args, **kwargs):
        return {
            **super().parse_config_file(*args, **kwargs),
            'storage': {'cls': 'memory', 'args': {}}
        }


class TestBulkUpdater(DirGitLoaderTest):
    """Same tests as for the GitLoaderFromDisk, but running on BulkUpdater."""
    def setUp(self):
        super().setUp()
        self.loader = BulkUpdaterTest()
        self.storage = self.loader.storage

    def load(self):
        return self.loader.load(
            origin_url=self.repo_url)
