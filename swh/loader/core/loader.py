# Copyright (C) 2015-2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import psycopg2
import requests
import traceback
import uuid

from retrying import retry

from swh.core import config

from . import converters
from swh.model.git import GitType
from swh.storage import get_storage

from .queue import QueuePerSizeAndNbUniqueElements
from .queue import QueuePerNbUniqueElements
from .queue import QueuePerNbElements


def retry_loading(error):
    """Retry policy when the database raises an integrity error"""
    exception_classes = [
        # raised when two parallel insertions insert the same data.
        psycopg2.IntegrityError,
        # raised when uWSGI restarts and hungs up on the worker.
        requests.exceptions.ConnectionError,
    ]

    if not any(isinstance(error, exc) for exc in exception_classes):
        return False

    logger = logging.getLogger('swh.loader')

    error_name = error.__module__ + '.' + error.__class__.__name__
    logger.warning('Retry loading a batch', exc_info=False, extra={
        'swh_type': 'storage_retry',
        'swh_exception_type': error_name,
        'swh_exception': traceback.format_exception(
            error.__class__,
            error,
            error.__traceback__,
        ),
    })

    return True


class SWHLoader(config.SWHConfig):
    """This base class is a pattern for loaders.

    The external calling convention is as such:
      - instantiate the class once (loads storage and the configuration)
      - for each origin, call process with the origin-specific arguments (for
        instance, an origin URL).

        Method to implement in subclass:
        - process(*args, **kwargs)
    """
    CONFIG_BASE_FILENAME = None

    DEFAULT_CONFIG = {
        'storage_class': ('str', 'remote_storage'),
        'storage_args': ('list[str]', ['http://localhost:5000/']),

        'send_contents': ('bool', True),
        'send_directories': ('bool', True),
        'send_revisions': ('bool', True),
        'send_releases': ('bool', True),
        'send_occurrences': ('bool', True),

        # Number of contents
        'content_packet_size': ('int', 10000),
        # If this size threshold is reached, the content is condidered missing
        # in swh-storage
        'content_packet_size_bytes': ('int', 1024 * 1024 * 1024),
        # packet of 100Mib contents
        'content_packet_block_size_bytes': ('int', 100 * 1024 * 1024),
        'directory_packet_size': ('int', 25000),
        'revision_packet_size': ('int', 100000),
        'release_packet_size': ('int', 100000),
        'occurrence_packet_size': ('int', 100000),
    }

    ADDITIONAL_CONFIG = {}

    def __init__(self, origin_id, logging_class, config=None):
        if config:
            self.config = config
        else:
            self.config = self.parse_config_file(
                additional_configs=[self.ADDITIONAL_CONFIG])

        self.origin_id = origin_id

        self.storage = get_storage(self.config['storage_class'],
                                   self.config['storage_args'])

        self.log = logging.getLogger(logging_class)

        self.contents = QueuePerSizeAndNbUniqueElements(
            key='sha1',
            max_nb_elements=self.config['content_packet_size'],
            max_size=self.config['content_packet_block_size_bytes'])

        self.contents_seen = set()

        self.directories = QueuePerNbUniqueElements(
            key='id',
            max_nb_elements=self.config['directory_packet_size'])

        self.directories_seen = set()

        self.revisions = QueuePerNbUniqueElements(
            key='id',
            max_nb_elements=self.config['revision_packet_size'])

        self.revisions_seen = set()

        self.releases = QueuePerNbUniqueElements(
            key='id',
            max_nb_elements=self.config['release_packet_size'])

        self.releases_seen = set()

        self.occurrences = QueuePerNbElements(
            self.config['occurrence_packet_size'])

        l = logging.getLogger('requests.packages.urllib3.connectionpool')
        l.setLevel(logging.WARN)

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_contents(self, content_list):
        """Actually send properly formatted contents to the database.

        """
        num_contents = len(content_list)
        if num_contents > 0:
            log_id = str(uuid.uuid4())
            self.log.debug("Sending %d contents" % num_contents,
                           extra={
                               'swh_type': 'storage_send_start',
                               'swh_content_type': 'content',
                               'swh_num': num_contents,
                               'swh_id': log_id,
                           })
            self.storage.content_add(content_list)
            self.log.debug("Done sending %d contents" % num_contents,
                           extra={
                               'swh_type': 'storage_send_end',
                               'swh_content_type': 'content',
                               'swh_num': num_contents,
                               'swh_id': log_id,
                           })

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_directories(self, directory_list):
        """Actually send properly formatted directories to the database.

        """
        num_directories = len(directory_list)
        if num_directories > 0:
            log_id = str(uuid.uuid4())
            self.log.debug("Sending %d directories" % num_directories,
                           extra={
                               'swh_type': 'storage_send_start',
                               'swh_content_type': 'directory',
                               'swh_num': num_directories,
                               'swh_id': log_id,
                           })
            self.storage.directory_add(directory_list)
            self.log.debug("Done sending %d directories" % num_directories,
                           extra={
                               'swh_type': 'storage_send_end',
                               'swh_content_type': 'directory',
                               'swh_num': num_directories,
                               'swh_id': log_id,
                           })

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_revisions(self, revision_list):
        """Actually send properly formatted revisions to the database.

        """
        num_revisions = len(revision_list)
        if num_revisions > 0:
            log_id = str(uuid.uuid4())
            self.log.debug("Sending %d revisions" % num_revisions,
                           extra={
                               'swh_type': 'storage_send_start',
                               'swh_content_type': 'revision',
                               'swh_num': num_revisions,
                               'swh_id': log_id,
                           })
            self.storage.revision_add(revision_list)
            self.log.debug("Done sending %d revisions" % num_revisions,
                           extra={
                               'swh_type': 'storage_send_end',
                               'swh_content_type': 'revision',
                               'swh_num': num_revisions,
                               'swh_id': log_id,
                           })

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_releases(self, release_list):
        """Actually send properly formatted releases to the database.

        """
        num_releases = len(release_list)
        if num_releases > 0:
            log_id = str(uuid.uuid4())
            self.log.debug("Sending %d releases" % num_releases,
                           extra={
                               'swh_type': 'storage_send_start',
                               'swh_content_type': 'release',
                               'swh_num': num_releases,
                               'swh_id': log_id,
                           })
            self.storage.release_add(release_list)
            self.log.debug("Done sending %d releases" % num_releases,
                           extra={
                               'swh_type': 'storage_send_end',
                               'swh_content_type': 'release',
                               'swh_num': num_releases,
                               'swh_id': log_id,
                           })

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_occurrences(self, occurrence_list):
        """Actually send properly formatted occurrences to the database.

        """
        num_occurrences = len(occurrence_list)
        if num_occurrences > 0:
            log_id = str(uuid.uuid4())
            self.log.debug("Sending %d occurrences" % num_occurrences,
                           extra={
                               'swh_type': 'storage_send_start',
                               'swh_content_type': 'occurrence',
                               'swh_num': num_occurrences,
                               'swh_id': log_id,
                           })
            self.storage.occurrence_add(occurrence_list)
            self.log.debug("Done sending %d occurrences" % num_occurrences,
                           extra={
                               'swh_type': 'storage_send_end',
                               'swh_content_type': 'occurrence',
                               'swh_num': num_occurrences,
                               'swh_id': log_id,
                           })

    def filter_missing_blobs(self, blobs):
        """Filter missing blobs from swh.

        """
        max_content_size = self.config['content_packet_size_bytes']
        blobs_per_sha1 = {}
        shallow_blobs = []
        for key, blob in ((b['sha1'], b) for b in blobs
                          if b['sha1'] not in self.contents_seen):
            blobs_per_sha1[key] = blob
            shallow_blobs.append(converters.shallow_blob(blob))
            self.contents_seen.add(key)

        for sha1 in self.storage.content_missing(shallow_blobs,
                                                 key_hash='sha1'):
            yield converters.blob_to_content(blobs_per_sha1[sha1],
                                             max_content_size=max_content_size,
                                             origin_id=self.origin_id)

    def bulk_send_blobs(self, blobs):
        """Format blobs as swh contents and send them to the database.

        """
        threshold_reached = self.contents.add(
            self.filter_missing_blobs(blobs))
        if threshold_reached:
            self.send_contents(self.contents.pop())

    def filter_missing_trees(self, trees):
        """Filter missing tree from swh.

        """
        trees_per_sha1 = {}
        shallow_trees = []
        for key, tree in ((t['sha1_git'], t) for t in trees
                          if t['sha1_git'] not in self.directories_seen):
            trees_per_sha1[key] = tree
            shallow_trees.append(converters.shallow_tree(tree))
            self.directories_seen.add(key)

        for sha in self.storage.directory_missing(shallow_trees):
            yield converters.tree_to_directory(trees_per_sha1[sha])

    def bulk_send_trees(self, trees):
        """Format trees as swh directories and send them to the database.

        """
        threshold_reached = self.directories.add(
            self.filter_missing_trees(trees))
        if threshold_reached:
            self.send_contents(self.contents.pop())
            self.send_directories(self.directories.pop())

    def filter_missing_commits(self, commits):
        """Filter missing commit from swh.

        """
        commits_per_sha1 = {}
        shallow_commits = []
        for key, commit in ((c['id'], c) for c in commits
                            if c['id'] not in self.revisions_seen):
            commits_per_sha1[key] = commit
            shallow_commits.append(converters.shallow_commit(commit))
            self.revisions_seen.add(key)

        for sha in self.storage.revision_missing(shallow_commits):
            yield commits_per_sha1[sha]

    def bulk_send_commits(self, commits):
        """Format commits as swh revisions and send them to the database.

        """
        threshold_reached = self.revisions.add(
            self.filter_missing_commits(commits))
        if threshold_reached:
            self.send_contents(self.contents.pop())
            self.send_directories(self.directories.pop())
            self.send_revisions(self.revisions.pop())

    def filter_missing_tags(self, tags):
        """Filter missing tags from swh.

        """
        tags_per_sha1 = {}
        shallow_tags = []
        for key, tag in ((t['id'], t) for t in tags
                         if t['id'] not in self.releases_seen):
            tags_per_sha1[key] = tag
            shallow_tags.append(converters.shallow_tag(tag))
            self.releases_seen.add(key)

        for sha in self.storage.release_missing(shallow_tags):
            yield tags_per_sha1[sha]

    def bulk_send_annotated_tags(self, tags):
        """Format annotated tags (pygit2.Tag objects) as swh releases and send
        them to the database.

        """
        threshold_reached = self.releases.add(
            self.filter_missing_tags(tags))
        if threshold_reached:
            self.send_contents(self.contents.pop())
            self.send_directories(self.directories.pop())
            self.send_revisions(self.revisions.pop())
            self.send_releases(self.releases.pop())

    def bulk_send_refs(self, refs):
        """Format git references as swh occurrences and send them to the
        database.

        """
        threshold_reached = self.occurrences.add(
            (converters.ref_to_occurrence(r) for r in refs))
        if threshold_reached:
            self.send_contents(self.contents.pop())
            self.send_directories(self.directories.pop())
            self.send_revisions(self.revisions.pop())
            self.send_releases(self.releases.pop())
            self.send_occurrences(self.occurrences.pop())

    def maybe_load_contents(self, contents):
        """Load contents in swh-storage if need be.

        """
        if self.config['send_contents']:
            self.bulk_send_blobs(contents)

    def maybe_load_directories(self, trees, objects_per_path):
        """Load directories in swh-storage if need be.

        """
        if self.config['send_directories']:
            self.bulk_send_trees(trees, objects_per_path)

    def maybe_load_revisions(self, revisions):
        """Load revisions in swh-storage if need be.

        """
        if self.config['send_revisions']:
            self.bulk_send_commits(revisions)

    def maybe_load_releases(self, releases):
        """Load releases in swh-storage if need be.

        """
        if self.config['send_releases']:
            self.bulk_send_annotated_tags(releases)

    def maybe_load_occurrences(self, occurrences):
        """Load occurrences in swh-storage if need be.

        """
        if self.config['send_occurrences']:
            self.bulk_send_refs(occurrences)

    def load(self, objects_per_type):
        """Load all data to swh-storage.

        Args:
            objects_per_type: Dictionary of:
            - GitType.BLOB: blob/content,
            - GitType.TREE: tree/directory
            - GitType.COMM: commit/revision
            - GitType.RELE: annotated tag/release
            - GitType.REFS: reference/occurrence
            objects_per_path: Dictionary of path, children information.

        """
        self.maybe_load_contents(objects_per_type[GitType.BLOB])
        self.maybe_load_directories(objects_per_type[GitType.TREE])
        self.maybe_load_revisions(objects_per_type[GitType.COMM])
        self.maybe_load_releases(objects_per_type[GitType.RELE])
        self.maybe_load_occurrences(objects_per_type[GitType.REFS])

    def flush(self):
        """Flush any potential dangling data not sent to swh-storage.

        """
        contents = self.contents.pop()
        directories = self.directories.pop()
        revisions = self.revisions.pop()
        occurrences = self.occurrences.pop()
        releases = self.releases.pop()
        # and send those to storage if asked
        if self.config['send_contents']:
            self.send_contents(contents)
        if self.config['send_directories']:
            self.send_directories(directories)
        if self.config['send_revisions']:
            self.send_revisions(revisions)
        if self.config['send_occurrences']:
            self.send_occurrences(occurrences)
        if self.config['send_releases']:
            self.send_releases(releases)

    def process(self, *args, **kwargs):
        """Method to implement in subclass."""
        raise NotImplementedError
