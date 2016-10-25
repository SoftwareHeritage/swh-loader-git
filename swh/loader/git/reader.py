# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import datetime

from collections import defaultdict

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
            yield oid

    def load(self, *args, **kwargs):
        self.prepare(*args, **kwargs)
        try:
            for oid in self.fetch_data():
                yield oid.decode('utf-8')
        except:
            pass


class GitSha1RemoteReader(BulkUpdater):
    """Disk git sha1 reader to dump only repo's content sha1 list.

    """
    def list_pack(self, pack_data, pack_size):
        """Override list_pack to only keep blobs data.

        """
        id_to_type = {}
        type_to_ids = defaultdict(set)

        inflater = self.get_inflater()

        for obj in inflater:
            type, id = obj.type_name, obj.id
            if type != b'blob':
                continue
            id_to_type[id] = type
            type_to_ids[type].add(id)

        return id_to_type, type_to_ids

    def load(self, *args, **kwargs):
        self.prepare(*args, **kwargs)
        origin = self.get_origin()
        self.origin_id = self.send_origin(origin)

        try:
            self.fetch_data()
            for oid in self.id_to_type.keys():
                yield oid.decode('utf-8')
        except:
            pass


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
        r = loader.load(origin_url, source, fetch_date)
    else:
        loader = GitSha1RemoteReader()
        r = loader.load(origin_url, source)

    for id in r:
        print(id)


if __name__ == '__main__':
    main()
