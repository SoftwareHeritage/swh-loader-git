# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from swh.scheduler.task import Task
from swh.model.git import GitType


class LoaderCoreTask(Task):
    """Main task to inherit from and implement run function.

    """
    def __init__(self):
        l = logging.getLogger('requests.packages.urllib3.connectionpool')
        l.setLevel(logging.WARN)

    def open_fetch_history(self, storage, origin_id):
        return storage.fetch_history_start(origin_id)

    def close_fetch_history(self, storage, fetch_history_id, res):
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
        return storage.fetch_history_end(fetch_history_id, data)

    def run(self, *args, **kwargs):
        raise NotImplemented('Need to be overriden by subclass.')
