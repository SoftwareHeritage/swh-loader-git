# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh.storage import get_storage
from swh.core.config import SWHConfig
from swh.scheduler.task import Task
from swh.model.git import GitType


class LoaderCoreTask(SWHConfig, Task):
    """Main task to inherit from and implement run function.

    """
    CONFIG_BASE_FILENAME = None

    ADDITIONAL_CONFIG = {
        'storage_class': ('str', 'remote_storage'),
        'storage_args': ('list[str]', ['http://localhost:5000/']),
    }

    def __init__(self):
        self.config = SWHConfig.parse_config_file(
            base_filename=self.CONFIG_BASE_FILENAME,
            additional_configs=[self.ADDITIONAL_CONFIG])
        self.storage = get_storage(self.config['storage_class'],
                                   self.config['storage_args'])

        l = logging.getLogger('requests.packages.urllib3.connectionpool')
        l.setLevel(logging.WARN)

    def open_fetch_history(self, origin_id):
        return self.storage.fetch_history_start(origin_id)

    def close_fetch_history(self, fetch_history_id, res):
        result = None
        if res and 'objects' in res:
            result = {
                'contents': len(res['objects'].get(GitType.BLOB, [])),
                'directories': len(res['objects'].get(GitType.TREE, [])),
                'revisions': len(res['objects'].get(GitType.COMM, [])),
                'releases': len(res['objects'].get(GitType.RELE, [])),
                'occurrences': len(res['objects'].get(GitType.REFS, [])),
            }

        data = {
            'status': res['status'],
            'result': result,
            'stderr': res.get('stderr')
        }
        return self.storage.fetch_history_end(fetch_history_id, data)

    def run(self, *args, **kwargs):
        raise NotImplemented('Need to be overriden by subclass.')
