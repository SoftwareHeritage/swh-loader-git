# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
import logging
import pprint

import click

from swh.core import utils
from swh.model.hashutil import MultiHash, hash_to_hex

from .updater import BulkUpdater, SWHRepoRepresentation
from . import converters


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


class BaseGitRemoteReader(BulkUpdater):
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
        self.task_destination = self.next_task['queue']
        self.destination = self.next_task['destination']

    def graph_walker(self):
        return DummyGraphWalker()

    def prepare_origin_visit(self, origin_url, base_url=None):
        self.origin = converters.origin_url_to_origin(origin_url)
        self.origin_id = 0

    def prepare(self, origin_url, base_url=None):
        """Only retrieve information about the origin, set everything else to
           empty.

        """
        self.base_occurrences = []
        self.base_origin_id = 0

    def keep_object(self, obj):
        """Do we want to keep this object or not?"""
        raise NotImplementedError('Please implement keep_object')

    def get_id_and_data(self, obj):
        """Get the id, type and data of the given object"""
        raise NotImplementedError('Please implement get_id_and_data')

    def list_pack(self, pack_data, pack_size):
        """Override list_pack to only keep contents' sha1.

        Returns:
            id_to_type (dict): keys are sha1, values are their associated type
            type_to_ids (dict): keys are types, values are list of associated
            data (sha1 for blobs)

        """
        self.data = {}
        id_to_type = {}
        type_to_ids = defaultdict(set)

        inflater = self.get_inflater()

        for obj in inflater:
            if not self.keep_object(obj):
                continue

            object_id, type, data = self.get_id_and_data(obj)

            id_to_type[object_id] = type
            type_to_ids[type].add(object_id)
            self.data[object_id] = data

        return id_to_type, type_to_ids

    def load(self, *args, **kwargs):
        """Override the loading part which simply reads the repository's
           contents' sha1.

        Returns:
            Returns the list of discovered sha1s for that origin.

        """
        self.prepare(*args, **kwargs)
        self.fetch_data()


class GitSha1RemoteReader(BaseGitRemoteReader):
    """Read sha1 git from a remote repository and dump only repository's
       content sha1 as list.

    """
    def keep_object(self, obj):
        """Only keep blobs"""
        return obj.type_name == b'blob'

    def get_id_and_data(self, obj):
        """We want to store only object identifiers"""
        # compute the sha1 (obj.id is the sha1_git)
        data = obj.as_raw_string()
        hashes = MultiHash.from_data(data, {'sha1'}).digest()
        oid = hashes['sha1']
        return (oid, b'blob', oid)


class GitSha1RemoteReaderAndSendToQueue(GitSha1RemoteReader):
    """Read sha1 git from a remote repository and dump only repository's
       content sha1 as list and send batch of those sha1s to a celery
       queue for consumption.

    """
    def load(self, *args, **kwargs):
        """Retrieve the list of sha1s for a particular origin and send those
           sha1s as group of sha1s to a specific queue.

        """
        super().load(*args, **kwargs)

        data = self.type_to_ids[b'blob']

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


class GitCommitRemoteReader(BaseGitRemoteReader):
    def keep_object(self, obj):
        return obj.type_name == b'commit'

    def get_id_and_data(self, obj):
        return obj.id, b'commit', converters.dulwich_commit_to_revision(obj)

    def load(self, *args, **kwargs):
        super().load(*args, **kwargs)
        return self.data


@click.group()
@click.option('--origin-url', help='Origin url')
@click.pass_context
def main(ctx, origin_url):
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(process)d %(message)s'
    )
    ctx.obj['origin_url'] = origin_url


@main.command()
@click.option('--send/--nosend', default=False, help='Origin\'s url')
@click.pass_context
def blobs(ctx, send):
    origin_url = ctx.obj['origin_url']

    if send:
        loader = GitSha1RemoteReaderAndSendToQueue()
        ids = loader.load(origin_url)
        print('%s sha1s were sent to queue' % len(ids))
        return

    loader = GitSha1RemoteReader()
    ids = loader.load(origin_url)

    if ids:
        for oid in ids:
            print(hash_to_hex(oid))


@main.command()
@click.option('--ids-only', is_flag=True, help='print ids only')
@click.pass_context
def commits(ctx, ids_only):
    origin_url = ctx.obj['origin_url']

    reader = GitCommitRemoteReader()
    commits = reader.load(origin_url)
    for commit_id, commit in commits.items():
        if ids_only:
            print(commit_id.decode())
        else:
            pprint.pprint(commit)


if __name__ == '__main__':
    main(obj={})
