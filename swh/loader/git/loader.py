# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

import pygit2
from pygit2 import Oid, GIT_OBJ_BLOB, GIT_OBJ_TREE, GIT_OBJ_COMMIT, GIT_OBJ_TAG

from swh.core import hashutil

from .utils import format_date, get_objects_per_object_type

HASH_ALGORITHMS = ['sha1', 'sha256']


def send_in_packets(source_list, formatter, sender, packet_size):
    """Send objects from `source_list`, passed through `formatter`, by the
    `sender`, in packets of `packet_size` objects

    """
    formatted_objects = []
    for obj in source_list:
        formatted_object = formatter(obj)
        if formatted_object:
            formatted_objects.append(formatted_object)
        if len(formatted_objects) >= packet_size:
            sender(formatted_objects)
            formatted_objects = []

    sender(formatted_objects)


class BulkLoader:
    """A bulk loader for a git repository"""
    def __init__(self, config):
        self.config = config

        if self.config['storage_class'] == 'remote_storage':
            from swh.storage.remote_storage import RemoteStorage as Storage
        else:
            from swh.storage import Storage

        self.storage = Storage(*self.config['storage_args'])

        self.repo = pygit2.Repository(config['repo_path'])

        self.log = logging.getLogger('swh.loader.git.BulkLoader')

    def send_contents(self, content_list):
        """Actually send properly formatted contents to the database"""
        self.log.info("Sending %d contents" % len(content_list))
        self.storage.content_add(content_list)
        self.log.info("Done sending %d contents" % len(content_list))

    def send_directories(self, directory_list):
        """Actually send properly formatted directories to the database"""
        self.log.info("Sending %d directories" % len(directory_list))
        self.storage.directory_add(directory_list)
        self.log.info("Done sending %d directories" % len(directory_list))

    def send_revisions(self, revision_list):
        """Actually send properly formatted revisions to the database"""
        self.log.info("Sending %d revisions" % len(revision_list))
        self.storage.revision_add(revision_list)
        self.log.info("Done sending %d revisions" % len(revision_list))

    def send_releases(self, release_list):
        """Actually send properly formatted releases to the database"""
        self.log.info("Sending %d releases" % len(release_list))
        self.storage.release_add(release_list)
        self.log.info("Done sending %d releases" % len(release_list))

    def send_occurrences(self, occurrence_list):
        """Actually send properly formatted occurrences to the database"""
        self.log.info("Sending %d occurrences" % len(occurrence_list))
        self.storage.occurrence_add(occurrence_list)
        self.log.info("Done sending %d occurrences" % len(occurrence_list))

    def blob_to_content(self, id):
        """Format a blob as a content"""
        blob = self.repo[id]
        data = blob.data
        hashes = hashutil.hashdata(data, HASH_ALGORITHMS)
        return {
            'sha1_git': id.raw,
            'sha1': hashes['sha1'],
            'sha256': hashes['sha256'],
            'data': data,
            'length': blob.size,
        }

    def tree_to_directory(self, id):
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

        for entry in self.repo[id]:
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

    def commit_to_revision(self, id):
        """Format a commit as a revision"""
        commit = self.repo[id]

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

    def annotated_tag_to_release(self, id):
        """Format an annotated tag as a release"""
        tag = self.repo[id]

        tag_pointer = self.repo[tag.target]
        if tag_pointer.type != GIT_OBJ_COMMIT:
            self.log.warn("Ignoring tag %s pointing at %s %s" % (
                tag.id.hex, tag_pointer.__class__.__name__,
                tag_pointer.id.hex))
            return

        author = tag.tagger

        if not author:
            self.log.warn("Tag %s has no author, using default values"
                          % id.hex)
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

    def ref_to_occurrence(self, ref):
        """Format a reference as an occurrence"""
        ref = ref.copy()

        ref.update(origin=self.origin, authority=self.config['authority'],
                   validity=self.config['validity'])

        return ref

    def get_origin(self):
        origin = {
            'type': 'git',
            'url': 'file://%s' % self.config['repo_path'],
        }

        origin['id'] = self.storage.origin_get(origin)

        return origin

    def create_origin(self):
        origin = self.get_origin()
        id = origin['id']

        if not id:
            id = self.storage.origin_add_one(origin)

        self.origin = id

    def bulk_send_blobs(self, blob_dict):
        """Format blobs as swh contents and send them to the database"""
        packet_size = self.config['content_packet_size']

        send_in_packets(blob_dict, self.blob_to_content,
                        self.send_contents, packet_size)

    def bulk_send_trees(self, tree_dict):
        """Format trees as swh directories and send them to the database"""
        packet_size = self.config['directory_packet_size']

        send_in_packets(tree_dict, self.tree_to_directory,
                        self.send_directories, packet_size)

    def bulk_send_commits(self, commit_dict):
        """Format commits as swh revisions and send them to the database"""
        packet_size = self.config['revision_packet_size']

        send_in_packets(commit_dict, self.commit_to_revision,
                        self.send_revisions, packet_size)

    def bulk_send_annotated_tags(self, tag_dict):
        """Format annotated tags (pygit2.Tag objects) as swh releases and send
        them to the database
        """
        packet_size = self.config['release_packet_size']

        send_in_packets(tag_dict, self.annotated_tag_to_release,
                        self.send_releases, packet_size)

    def bulk_send_refs(self, refs):
        """Format git references as swh occurrences and send them to the
        database
        """
        packet_size = self.config['occurrence_packet_size']

        send_in_packets(refs, self.ref_to_occurrence,
                        self.send_occurrences, packet_size)

    def list_repo(self):
        self.log.info("Started listing %s" % self.config['repo_path'])
        self.objects = get_objects_per_object_type(self.repo)

        refs = []
        ref_names = self.repo.listall_references()
        for ref_name in ref_names:
            ref = self.repo.lookup_reference(ref_name)
            target = ref.target

            if not isinstance(target, Oid):
                self.log.debug("Peeling symbolic ref %s pointing at %s" % (
                    ref_name, ref.target))
                target_obj = ref.peel()
            else:
                target_obj = self.repo[target]

            if target_obj.type == GIT_OBJ_TAG:
                self.log.debug("Peeling ref %s pointing at tag %s" % (
                    ref_name, target_obj.name))
                target_obj = ref.peel()

            if not target_obj.type == GIT_OBJ_COMMIT:
                self.log.info("Skipping ref %s pointing to %s %s" % (
                    ref_name, target_obj.__class__.__name__,
                    target_obj.id.hex))

            refs.append({
                'branch': ref_name,
                'revision': target_obj.id.raw,
            })

        self.objects['refs'] = refs

        self.log.info("Done listing the objects in %s: %d contents, "
                      "%d directories, %d revisions, %d releases, "
                      "%d occurrences" % (
                         self.config['repo_path'],
                         len(self.objects[GIT_OBJ_BLOB]),
                         len(self.objects[GIT_OBJ_TREE]),
                         len(self.objects[GIT_OBJ_COMMIT]),
                         len(self.objects[GIT_OBJ_TAG]),
                         len(self.objects['refs'])
                      ))

    def load_repo(self):
        if self.config['create_origin']:
            self.create_origin()
        else:
            self.log.info('Not creating origin, pulling id from config')
            self.origin = self.config['origin']

        if not self.objects['refs']:
            self.log.info('Skipping empty repository')
            return

        if self.config['send_contents']:
            self.bulk_send_blobs(self.objects[GIT_OBJ_BLOB])
        else:
            self.log.info('Not sending contents')

        if self.config['send_directories']:
            self.bulk_send_trees(self.objects[GIT_OBJ_TREE])
        else:
            self.log.info('Not sending directories')

        if self.config['send_revisions']:
            self.bulk_send_commits(self.objects[GIT_OBJ_COMMIT])
        else:
            self.log.info('Not sending revisions')

        if self.config['send_releases']:
            self.bulk_send_annotated_tags(self.objects[GIT_OBJ_TAG])
        else:
            self.log.info('Not sending releases')

        if self.config['send_occurrences']:
            self.bulk_send_refs(self.objects['refs'])
        else:
            self.log.info('Not sending occurrences')

    def process(self):
        self.list_repo()
        self.load_repo()
