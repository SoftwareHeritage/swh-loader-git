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


def send_in_packets(repo, source_list, formatter, sender, packet_size):
    """Send objects from `source_list`, passed through `formatter`, by the
    `sender`, in packets of `packet_size` objects

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


class BulkLoader:
    """A bulk loader for a git repository"""
    def __init__(self, config):
        self.config = config

        if self.config['storage_class'] == 'remote_storage':
            from swh.storage.remote_storage import RemoteStorage as Storage
        else:
            from swh.storage import Storage

        self.storage = Storage(*self.config['storage_args'])

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

    def blob_to_content(self, repo, id):
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

    def tree_to_directory(self, repo, id):
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

    def commit_to_revision(self, repo, id):
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

    def annotated_tag_to_release(self, repo, id):
        """Format an annotated tag as a release"""
        tag = repo[id]

        tag_pointer = repo[tag.target]
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

    def ref_to_occurrence(self, repo, ref):
        """Format a reference as an occurrence"""
        return ref

    def get_origin(self, repo):
        return {
            'type': 'git',
            'url': 'file://%s' % repo.path,
        }

    def get_or_create_origin(self, repo):
        origin = self.get_origin(repo)

        origin['id'] = self.storage.origin_add_one(origin)

        return origin

    def repo_origin(self, repo, origin_id):
        if self.config['create_origin']:
            self.log.info('Creating origin')
            return self.get_or_create_origin(repo)
        else:
            self.log.info('Not creating origin, pulling id from config')
            origin = self.get_origin(repo)
            origin['id'] = origin_id
            return origin

    def bulk_send_blobs(self, repo, blobs):
        """Format blobs as swh contents and send them to the database"""
        packet_size = self.config['content_packet_size']

        send_in_packets(repo, blobs, self.blob_to_content,
                        self.send_contents, packet_size)

    def bulk_send_trees(self, repo, trees):
        """Format trees as swh directories and send them to the database"""
        packet_size = self.config['directory_packet_size']

        send_in_packets(repo, trees, self.tree_to_directory,
                        self.send_directories, packet_size)

    def bulk_send_commits(self, repo, commits):
        """Format commits as swh revisions and send them to the database"""
        packet_size = self.config['revision_packet_size']

        send_in_packets(repo, commits, self.commit_to_revision,
                        self.send_revisions, packet_size)

    def bulk_send_annotated_tags(self, repo, tags):
        """Format annotated tags (pygit2.Tag objects) as swh releases and send
        them to the database
        """
        packet_size = self.config['release_packet_size']

        send_in_packets(repo, tags, self.annotated_tag_to_release,
                        self.send_releases, packet_size)

    def bulk_send_refs(self, repo, refs):
        """Format git references as swh occurrences and send them to the
        database
        """
        packet_size = self.config['occurrence_packet_size']

        send_in_packets(repo, refs, self.ref_to_occurrence,
                        self.send_occurrences, packet_size)

    def list_repo_refs(self, repo, origin_id, authority_id, validity):
        """List all the refs from the given repository.

        Args:
            - repo (pygit2.Repository): the repository to list
            - origin_id (int): the id of the origin from which the repo is
                taken
            - validity (datetime.datetime): the validity date for the
                repository's refs
            - authority_id (int): the id of the authority on `validity`.

        Returns:
            A list of dicts with keys:
                - branch (str): name of the ref
                - revision (sha1_git): revision pointed at by the ref
                - origin (int)
                - validity (datetime.DateTime)
                - authority (int)
            Compatible with occurrence_add.
        """

        refs = []
        ref_names = repo.listall_references()
        for ref_name in ref_names:
            ref = repo.lookup_reference(ref_name)
            target = ref.target

            if not isinstance(target, Oid):
                self.log.debug("Peeling symbolic ref %s pointing at %s" % (
                    ref_name, ref.target))
                target_obj = ref.peel()
            else:
                target_obj = repo[target]

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
                'origin': origin_id,
                'validity': validity,
                'authority': authority_id,
            })

        return refs

    def list_repo_objs(self, repo):
        """List all the objects from repo.

        Args:
            - repo (pygit2.Repository): the repository to list

        Returns:
            a dict containing lists of `Oid`s with keys for each object type:
            - GIT_OBJ_BLOB
            - GIT_OBJ_TREE
            - GIT_OBJ_COMMIT
            - GIT_OBJ_TAG
        """
        self.log.info("Started listing %s" % repo.path)
        objects = get_objects_per_object_type(repo)
        self.log.info("Done listing the objects in %s: %d contents, "
                      "%d directories, %d revisions, %d releases" % (
                         repo.path,
                         len(objects[GIT_OBJ_BLOB]),
                         len(objects[GIT_OBJ_TREE]),
                         len(objects[GIT_OBJ_COMMIT]),
                         len(objects[GIT_OBJ_TAG]),
                      ))

        return objects

    def open_repo(self, repo_path):
        return pygit2.Repository(repo_path)

    def load_repo(self, repo, objects, refs):

        if self.config['send_contents']:
            self.bulk_send_blobs(repo, objects[GIT_OBJ_BLOB])
        else:
            self.log.info('Not sending contents')

        if self.config['send_directories']:
            self.bulk_send_trees(repo, objects[GIT_OBJ_TREE])
        else:
            self.log.info('Not sending directories')

        if self.config['send_revisions']:
            self.bulk_send_commits(repo, objects[GIT_OBJ_COMMIT])
        else:
            self.log.info('Not sending revisions')

        if self.config['send_releases']:
            self.bulk_send_annotated_tags(repo, objects[GIT_OBJ_TAG])
        else:
            self.log.info('Not sending releases')

        if self.config['send_occurrences']:
            self.bulk_send_refs(repo, refs)
        else:
            self.log.info('Not sending occurrences')

    def process(self, repo_path, origin_id, authority_id, validity):
        # Open repository
        repo = self.open_repo(repo_path)

        # Add origin to storage if needed, use the one from config if not
        origin = self.repo_origin(repo, origin_id)

        # Parse all the refs from our repo
        refs = self.list_repo_refs(repo, origin['id'], authority_id,
                                   validity)

        if not refs:
            self.log.info('Skipping empty repository')
            return

        # We want to load the repository, walk all the objects
        objects = self.list_repo_objs(repo)

        # Finally, load the repository
        self.load_repo(repo, objects, refs)
