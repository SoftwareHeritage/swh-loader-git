# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import datetime

from collections import defaultdict

from swh.core import hashutil, utils

from .updater import BulkUpdater
from .loader import GitLoader


class GitSha1Reader(GitLoader):
    """Disk git sha1 reader. Only read and dump sha1s in stdout.

    """
    def fetch_data(self):
        """Fetch the data from the data source"""
        for oid in self.iter_objects():
            type_name = self.repo[oid].type_name
            if type_name != b'blob':
                continue
            yield hashutil.hex_to_hash(oid.decode('utf-8'))

    def load(self, *args, **kwargs):
        self.prepare(*args, **kwargs)
        yield from self.fetch_data()


class GitSha1RemoteReader(BulkUpdater):
    """Disk git sha1 reader to dump only repo's content sha1 list.

    """
    CONFIG_BASE_FILENAME = 'loader/git-remote-reader'

    ADDITIONAL_CONFIG = {
        'pack_size_bytes': ('int', 4 * 1024 * 1024 * 1024),
        'pack_storage_base': ('str', ''),  # don't want to store packs so empty
        'next_task': (
            'dict', {
                'queue': 'swh.storage.archiver.tasks.SWHArchiverToBackendTask',
                'batch_size': 100,
                'destination': 'azure'
            }
        )
    }

    def __init__(self):
        super().__init__()
        self.next_task = self.config['next_task']
        self.batch_size = self.next_task['batch_size']
        self.task_destination = self.next_task.get('queue')
        self.destination = self.next_task['destination']

    def list_pack(self, pack_data, pack_size):
        """Override list_pack to only keep blobs data.

        """
        id_to_type = {}
        type_to_ids = defaultdict(set)

        inflater = self.get_inflater()

        for obj in inflater:
            type, id = obj.type_name, obj.id
            if type != b'blob':  # don't keep other types
                continue
            oid = hashutil.hex_to_hash(id.decode('utf-8'))
            id_to_type[oid] = type
            type_to_ids[type].add(oid)

        return id_to_type, type_to_ids

    def load(self, *args, **kwargs):
        self.prepare(*args, **kwargs)
        origin = self.get_origin()
        self.origin_id = self.send_origin(origin)

        self.fetch_data()
        data = self.id_to_type.keys()
        if not self.task_destination:  # to stdout
            return data

        from swh.scheduler.celery_backend.config import app
        try:
            # optional dependency
            from swh.storage.archiver import tasks  # noqa
        except ImportError:
            pass
        from celery import group

        task_destination = app.tasks[self.task_destination]
        groups = []
        for ids in utils.grouper(data, self.batch_size):
            sig_ids = task_destination.s(destination=self.destination,
                                         batch=list(ids))
            groups.append(sig_ids)
        group(groups).delay()


@click.command()
@click.option('--origin-url', help='Origin\'s url')
@click.option('--source', help='origin\'s source url (disk or remote)')
def main(origin_url, source):
    import logging

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(process)d %(message)s'
    )

    if source.startswith('/'):
        loader = GitSha1Reader()
        fetch_date = datetime.datetime.now(tz=datetime.timezone.utc)
        ids = loader.load(origin_url, source, fetch_date)
    else:
        loader = GitSha1RemoteReader()
        ids = loader.load(origin_url, source)

    for oid in ids:
        print(oid)


if __name__ == '__main__':
    main()
