# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import datetime
import logging

from collections import defaultdict

from swh.core import hashutil, utils

from .updater import BulkUpdater, SWHRepoRepresentation
from .loader import GitLoader
from . import converters


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


class SWHRepoFullRepresentation(SWHRepoRepresentation):
    """Overridden representation of a swh repository to permit to read
    completely the remote repository.

    """
    def __init__(self, storage, origin_id, occurrences=None):
        self.storage = storage
        self._parents_cache = {}
        self._type_cache = {}
        self.heads = set()

    def determine_wants(self, refs):
        """Filter the remote references to figure out which ones Software
           Heritage needs. In this particular context, we want to know
           everything.

        """
        if not refs:
            return []

        for target in refs.values():
            self.heads.add(target)

        return self.filter_unwanted_refs(refs).values()

    def find_remote_ref_types_in_swh(self, remote_refs):
        """Find the known swh remote.
        In that particular context, we know nothing.

        """
        return {}


class DummyGraphWalker(object):
    """Dummy graph walker which claims that the client doesn’t have any
       objects.

    """
    def ack(self, sha): pass

    def next(self): pass

    def __next__(self): pass


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
        super().__init__(SWHRepoFullRepresentation)
        self.next_task = self.config['next_task']
        self.batch_size = self.next_task['batch_size']
        self.task_destination = self.next_task.get('queue')
        self.destination = self.next_task['destination']

    def graph_walker(self):
        return DummyGraphWalker()

    def prepare(self, origin_url, base_url=None):
        """Only retrieve information about the origin, set everything else to
           empty.

        """
        ori = converters.origin_url_to_origin(origin_url)
        self.origin = self.storage.origin_get(ori)
        self.origin_id = self.origin['id']
        self.base_occurrences = []
        self.base_origin_id = self.origin['id']

    def list_pack(self, pack_data, pack_size):
        """Override list_pack to only keep contents' sha1.

        Returns:
            id_to_type (dict): keys are sha1, values are their associated type
            type_to_ids (dict): keys are types, values are list of associated
            ids (sha1 for blobs)

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
        """Override the loading part which simply reads the repository's
           contents' sha1.

        Returns:
            If the configuration holds a destination queue, send those
            sha1s as batch of sha1s to it for consumption.  Otherwise,
            returns the list of discovered sha1s.

        """
        try:
            self.prepare(*args, **kwargs)
        except:
            self.log.error('Unknown repository, skipping...')
            return []

        self.fetch_data()
        data = self.type_to_ids[b'blob']

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
        return data


@click.command()
@click.option('--origin-url', help='Origin\'s url')
@click.option('--source', default=None,
              help='origin\'s source url (disk or remote)')
def main(origin_url, source):
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(process)d %(message)s'
    )

    local_reader = (source and source.startswith('/')) or origin_url.startswith('/')  # noqa

    if local_reader:
        loader = GitSha1Reader()
        fetch_date = datetime.datetime.now(tz=datetime.timezone.utc)
        ids = loader.load(origin_url, source, fetch_date)
    else:
        loader = GitSha1RemoteReader()
        ids = loader.load(origin_url, source)

    if ids:
        count = 0
        for oid in ids:
            print(oid)
            count += 1
        print("sha1s: %s" % count)


if __name__ == '__main__':
    main()
