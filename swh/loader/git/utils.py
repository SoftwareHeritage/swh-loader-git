# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import glob
import os
import subprocess

from collections import defaultdict

from pygit2 import Oid


def format_date(signature):
    """Convert the date from a signature to a datetime"""
    return datetime.datetime.fromtimestamp(signature.time,
                                           datetime.timezone.utc)


def list_objects_from_packfile_index(packfile_index):
    """List the objects indexed by this packfile, in packfile offset
    order.
    """
    input_file = open(packfile_index, 'rb')

    with subprocess.Popen(
        ['/usr/bin/git', 'show-index'],
        stdin=input_file,
        stdout=subprocess.PIPE,
    ) as process:

        data = []

        for line in process.stdout.readlines():
            # git show-index returns the line as:
            # <packfile offset> <object_id> (<object CRC>)
            line_components = line.split()
            offset = int(line_components[0])
            object_id = line_components[1]

            data.append((offset, object_id))

        yield from (Oid(hex=object_id.decode('ascii'))
                    for _, object_id in sorted(data))

    input_file.close()


def simple_list_objects(repo):
    """List the objects in a given repository. Watch out for duplicates!"""
    objects_dir = os.path.join(repo.path, 'objects')
    # Git hashes are 40-character long
    objects_glob = os.path.join(objects_dir, '[0-9a-f]' * 2, '[0-9a-f]' * 38)

    packfile_dir = os.path.join(objects_dir, 'pack')

    if os.path.isdir(packfile_dir):
        for packfile_index in os.listdir(packfile_dir):
            if not packfile_index.endswith('.idx'):
                # Not an index file
                continue
            packfile_index_path = os.path.join(packfile_dir, packfile_index)
            yield from list_objects_from_packfile_index(packfile_index_path)

    for object_file in glob.glob(objects_glob):
        # Rebuild the object id as the last two components of the path
        yield Oid(hex=''.join(object_file.split(os.path.sep)[-2:]))


def list_objects(repo):
    """List the objects in a given repository, removing duplicates"""
    seen = set()
    for oid in simple_list_objects(repo):
        if oid not in seen:
            yield oid
            seen.add(oid)


def get_objects_per_object_type(repo):
    """Get all the (pygit2-parsed) objects from repo per object type"""
    objects_per_object_type = defaultdict(list)

    for object_id in list_objects(repo):
        object = repo[object_id]
        objects_per_object_type[object.type].append(object_id)

    return objects_per_object_type
