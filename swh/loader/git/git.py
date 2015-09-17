# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import glob
import logging
import os
import subprocess

import pygit2

from collections import defaultdict
from pygit2 import Oid
from pygit2 import GIT_OBJ_BLOB, GIT_OBJ_TREE, GIT_OBJ_COMMIT, GIT_OBJ_TAG

from swh.core import hashutil
from swh.storage.storage import Storage


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


HASH_ALGORITHMS = ['sha1', 'sha256']


def send_in_packets(repo, source_list, formatter, sender, packet_size):
    """Send objects from `source_list`, passed through `formatter` (being
    passed the `repo`), by the `sender`, in packets
    of `packet_size` objects

    """
    formatted_objects = []
    for obj in source_list:
        formatted_object = formatter(repo, obj)
        if formatted_object:
            formatted_objects.append(formatted_object)
        if len(formatted_objects) >= packet_size:
            sender(formatted_objects)
            formatted_objects = []

    sender(formatted_objects)


def send_contents(content_list):
    """Actually send properly formatted contents to the database"""
    logging.info("Sending %d contents" % len(content_list))
    s = Storage('dbname=softwareheritage-dev', '/tmp/swh-loader-git/test')

    s.content_add(content_list)
    logging.info("Done sending %d contents" % len(content_list))


def send_directories(directory_list):
    """Actually send properly formatted directories to the database"""
    logging.info("Sending %d directories" % len(directory_list))
    s = Storage('dbname=softwareheritage-dev', '/tmp/swh-loader-git/test')

    s.directory_add(directory_list)
    logging.info("Done sending %d directories" % len(directory_list))


def send_revisions(revision_list):
    """Actually send properly formatted revisions to the database"""
    logging.info("Sending %d revisions" % len(revision_list))
    s = Storage('dbname=softwareheritage-dev', '/tmp/swh-loader-git/test')

    s.revision_add(revision_list)
    logging.info("Done sending %d revisions" % len(revision_list))


def send_releases(release_list):
    """Actually send properly formatted releases to the database"""
    logging.info("Sending %d releases" % len(release_list))
    s = Storage('dbname=softwareheritage-dev', '/tmp/swh-loader-git/test')

    s.release_add(release_list)
    logging.info("Done sending %d releases" % len(release_list))


def send_occurrences(occurrence_list):
    """Actually send properly formatted occurrences to the database"""
    logging.info("Sending %d occurrences" % len(occurrence_list))
    s = Storage('dbname=softwareheritage-dev', '/tmp/swh-loader-git/test')

    s.occurrence_add(occurrence_list)
    logging.info("Done sending %d occurrences" % len(occurrence_list))


def blob_to_content(repo, id):
    """Format a blob as a content"""
    blob = repo[id]
    data = blob.data
    hashes = hashutil.hashdata(data, HASH_ALGORITHMS)
    return {
        'sha1_git': id.raw,
        'sha1': hashes['sha1'],
        'sha256': hashes['sha256'],
        'data': data,
        'length': blob.size,
    }


def tree_to_directory(repo, id):
    """Format a tree as a directory"""
    ret = {
        'id': id.raw,
    }
    entries = []
    ret['entries'] = entries

    entry_type_map = {
        'tree': 'dir',
        'blob': 'file',
        'commit': 'rev',
    }

    for entry in repo[id]:
        entries.append({
            'type': entry_type_map[entry.type],
            'perms': entry.filemode,
            'name': entry.name,
            'target': entry.id.raw,
            'atime': None,
            'mtime': None,
            'ctime': None,
        })

    return ret


def commit_to_revision(repo, id):
    """Format a commit as a revision"""
    commit = repo[id]

    author = commit.author
    committer = commit.committer
    return {
        'id': id.raw,
        'date': format_date(author),
        'date_offset': author.offset,
        'committer_date': format_date(committer),
        'committer_date_offset': committer.offset,
        'type': 'git',
        'directory': commit.tree_id.raw,
        'message': commit.raw_message,
        'author_name': author.name,
        'author_email': author.email,
        'committer_name': committer.name,
        'committer_email': committer.email,
        'parents': [p.raw for p in commit.parent_ids],
    }


def annotated_tag_to_release(repo, id):
    """Format an annotated tag as a release"""
    tag = repo[id]

    tag_pointer = repo[tag.target]
    if tag_pointer.type != GIT_OBJ_COMMIT:
        logging.warn("Ignoring tag %s pointing at %s %s" % (
            tag.id.hex, tag_pointer.__class__.__name__, tag_pointer.id.hex))
        return

    author = tag.tagger

    if not author:
        logging.warn("Tag %s has no author, using default values" % id.hex)
        author_name = ''
        author_email = ''
        date = None
        date_offset = 0
    else:
        author_name = author.name
        author_email = author.email
        date = format_date(author)
        date_offset = author.offset

    return {
        'id': id.raw,
        'date': date,
        'date_offset': date_offset,
        'revision': tag.target.raw,
        'comment': tag.message.encode('utf-8'),
        'author_name': author_name,
        'author_email': author_email,
    }


def ref_to_occurrence(repo, ref):
    """Format a reference as an occurrence"""
    return ref


def bulk_send_blobs(repo, blob_dict):
    """Format blobs as swh contents and send them to the database in bulks
    of maximum `threshold` objects

    """
    # TODO: move to config file
    content_packet_size = 100000

    send_in_packets(repo, blob_dict, blob_to_content, send_contents, content_packet_size)


def bulk_send_trees(repo, tree_dict):
    """Format trees as swh directories and send them to the database

    """
    # TODO: move to config file
    directory_packet_size = 25000

    send_in_packets(repo, tree_dict, tree_to_directory, send_directories, directory_packet_size)


def bulk_send_commits(repo, commit_dict):
    """Format commits as swh revisions and send them to the database

    """
    # TODO: move to config file
    revision_packet_size = 100000

    send_in_packets(repo, commit_dict, commit_to_revision, send_revisions, revision_packet_size)


def bulk_send_annotated_tags(repo, tag_dict):
    """Format annotated tags (pygit2.Tag objects) as swh releases and send
    them to the database

    """
    # TODO: move to config file
    release_packet_size = 100000

    send_in_packets(repo, tag_dict, annotated_tag_to_release, send_releases, release_packet_size)


def bulk_send_refs(repo, refs):
    """Format git references as swh occurrences and send them to the database
    """
    # TODO: move to config file
    occurrence_packet_size = 100000

    send_in_packets(repo, refs, ref_to_occurrence,
                    send_occurrences, occurrence_packet_size)


def parse_via_object_list(repo_path):
    logging.info("Started loading %s" % repo_path)
    repo = pygit2.Repository(repo_path)
    objects_per_object_type = get_objects_per_object_type(repo)

    refs = []
    ref_names = repo.listall_references()
    for ref_name in ref_names:
        ref = repo.lookup_reference(ref_name)
        target = ref.target

        if not isinstance(target, Oid):
            logging.info("Skipping symbolic ref %s pointing at %s" % (
                ref_name, ref.target))
            continue

        target_obj = repo[target]

        if not target_obj.type == GIT_OBJ_COMMIT:
            logging.info("Skipping ref %s pointing to %s %s" % (
                ref_name, target_obj.__class__.__name__, target.hex))

        refs.append({
            'name': ref_name,
            'revision': target.hex,
        })

    logging.info("Done listing the objects in %s: will load %d contents, "
                 "%d directories, %d revisions, %d releases, "
                 "%d occurrences" % (
                     repo_path,
                     len(objects_per_object_type[GIT_OBJ_BLOB]),
                     len(objects_per_object_type[GIT_OBJ_TREE]),
                     len(objects_per_object_type[GIT_OBJ_COMMIT]),
                     len(objects_per_object_type[GIT_OBJ_TAG]),
                     len(refs)
                 ))

    bulk_send_blobs(repo, objects_per_object_type[GIT_OBJ_BLOB])
    bulk_send_trees(repo, objects_per_object_type[GIT_OBJ_TREE])
    bulk_send_commits(repo, objects_per_object_type[GIT_OBJ_COMMIT])
    bulk_send_annotated_tags(repo, objects_per_object_type[GIT_OBJ_TAG])
    bulk_send_refs(repo, refs)
