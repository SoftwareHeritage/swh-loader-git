# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import logging
import psycopg2
import requests
import traceback
import uuid

from abc import ABCMeta, abstractmethod
from retrying import retry

from . import converters

from swh.core import config
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


class SWHLoader(config.SWHConfig, metaclass=ABCMeta):
    """Mixin base class for loader.

    To use this class, you must:

    - inherit from this class

    - and implement the @abstractmethod methods

    :func:`cleanup`: Last step executed by the loader.

    :func:`prepare`: First step executed by the loader to prepare some state
                     needed by the `func`:load method.

    :func:`get_origin`: Retrieve the origin that is currently being
                        loaded.

    :func:`fetch_data`: Fetch the data is actually the method to
                        implement to compute data to inject in swh
                        (through the store_data method)

    :func:`store_data`: Store data fetched.

    You can take a look at some example classes:

        :class:BaseSvnLoader
        :class:TarLoader
        :class:DirLoader

    """
    CONFIG_BASE_FILENAME = None

    DEFAULT_CONFIG = {
        'storage': ('dict', {
            'cls': 'remote',
            'args': {
                'url': 'http://localhost:5002/',
            }
        }),

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

    def __init__(self, logging_class, config=None):
        if config:
            self.config = config
        else:
            self.config = self.parse_config_file(
                additional_configs=[self.ADDITIONAL_CONFIG])

        self.storage = get_storage(**self.config['storage'])

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

        self.counters = {
            'contents': 0,
            'directories': 0,
            'revisions': 0,
            'releases': 0,
            'occurrences': 0,
        }

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_origin(self, origin):
        log_id = str(uuid.uuid4())
        self.log.debug('Creating %s origin for %s' % (origin['type'],
                                                      origin['url']),
                       extra={
                           'swh_type': 'storage_send_start',
                           'swh_content_type': 'origin',
                           'swh_num': 1,
                           'swh_id': log_id
                       })
        origin_id = self.storage.origin_add_one(origin)
        self.log.debug('Done creating %s origin for %s' % (origin['type'],
                                                           origin['url']),
                       extra={
                           'swh_type': 'storage_send_end',
                           'swh_content_type': 'origin',
                           'swh_num': 1,
                           'swh_id': log_id
                       })

        return origin_id

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_origin_visit(self, origin_id, visit_date):
        log_id = str(uuid.uuid4())
        self.log.debug(
            'Creating origin_visit for origin %s at time %s' % (
                origin_id, visit_date),
            extra={
                'swh_type': 'storage_send_start',
                'swh_content_type': 'origin_visit',
                'swh_num': 1,
                'swh_id': log_id
            })
        origin_visit = self.storage.origin_visit_add(origin_id, visit_date)
        self.log.debug(
            'Done Creating origin_visit for origin %s at time %s' % (
                origin_id, visit_date),
            extra={
                'swh_type': 'storage_send_end',
                'swh_content_type': 'origin_visit',
                'swh_num': 1,
                'swh_id': log_id
            })

        return origin_visit

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def update_origin_visit(self, origin_id, visit, status):
        log_id = str(uuid.uuid4())
        self.log.debug(
            'Updating origin_visit for origin %s with status %s' % (
                origin_id, status),
            extra={
                'swh_type': 'storage_send_start',
                'swh_content_type': 'origin_visit',
                'swh_num': 1,
                'swh_id': log_id
            })
        self.storage.origin_visit_update(origin_id, visit, status)
        self.log.debug(
            'Done updating origin_visit for origin %s with status %s' % (
                origin_id, status),
            extra={
                'swh_type': 'storage_send_end',
                'swh_content_type': 'origin_visit',
                'swh_num': 1,
                'swh_id': log_id
            })

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
            self.counters['contents'] += num_contents
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
            self.counters['directories'] += num_directories
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
            self.counters['revisions'] += num_revisions
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
            self.counters['releases'] += num_releases
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
            self.counters['occurrences'] += num_occurrences
            self.log.debug("Done sending %d occurrences" % num_occurrences,
                           extra={
                               'swh_type': 'storage_send_end',
                               'swh_content_type': 'occurrence',
                               'swh_num': num_occurrences,
                               'swh_id': log_id,
                           })

    def filter_missing_contents(self, contents):
        """Return only the contents missing from swh"""
        max_content_size = self.config['content_packet_size_bytes']
        contents_per_key = {}
        content_key = 'blake2s256'

        for content in contents:
            if content[content_key] in self.contents_seen:
                continue
            key = content[content_key]
            contents_per_key[key] = content
            self.contents_seen.add(key)

        for key in self.storage.content_missing(
                list(contents_per_key.values()),
                key_hash=content_key
        ):
            yield converters.content_for_storage(
                contents_per_key[key],
                max_content_size=max_content_size,
                origin_id=self.origin_id,
            )

    def bulk_send_contents(self, contents):
        """Format contents as swh contents and send them to the database.

        """
        threshold_reached = self.contents.add(
            self.filter_missing_contents(contents))
        if threshold_reached:
            self.send_contents(self.contents.pop())

    def filter_missing_directories(self, directories):
        """Return only directories missing from swh"""

        directories_per_id = {}
        search_dirs = []

        for directory in directories:
            dir_id = directory['id']
            if dir_id in self.directories_seen:
                continue

            search_dirs.append(dir_id)
            directories_per_id[dir_id] = directory
            self.directories_seen.add(dir_id)

        for dir_id in self.storage.directory_missing(search_dirs):
            yield directories_per_id[dir_id]

    def bulk_send_directories(self, directories):
        """Send missing directories to the database"""
        threshold_reached = self.directories.add(
            self.filter_missing_directories(directories))
        if threshold_reached:
            self.send_contents(self.contents.pop())
            self.send_directories(self.directories.pop())

    def filter_missing_revisions(self, revisions):
        """Return only revisions missing from swh"""
        revisions_per_id = {}
        search_revs = []

        for revision in revisions:
            rev_id = revision['id']
            if rev_id in self.revisions_seen:
                continue

            search_revs.append(rev_id)
            revisions_per_id[rev_id] = revision
            self.revisions_seen.add(rev_id)

        for rev_id in self.storage.revision_missing(search_revs):
            yield revisions_per_id[rev_id]

    def bulk_send_revisions(self, revisions):
        """Send missing revisions to the database"""
        threshold_reached = self.revisions.add(
            self.filter_missing_revisions(revisions))
        if threshold_reached:
            self.send_contents(self.contents.pop())
            self.send_directories(self.directories.pop())
            self.send_revisions(self.revisions.pop())

    def filter_missing_releases(self, releases):
        """Return only releases missing from swh"""
        releases_per_id = {}
        search_rels = []

        for release in releases:
            rel_id = release['id']
            if rel_id in self.releases_seen:
                continue

            search_rels.append(rel_id)
            releases_per_id[rel_id] = release
            self.releases_seen.add(rel_id)

        for rel_id in self.storage.release_missing(search_rels):
            yield releases_per_id[rel_id]

    def bulk_send_releases(self, releases):
        """Send missing releases to the database"""
        threshold_reached = self.releases.add(
            self.filter_missing_releases(releases))
        if threshold_reached:
            self.send_contents(self.contents.pop())
            self.send_directories(self.directories.pop())
            self.send_revisions(self.revisions.pop())
            self.send_releases(self.releases.pop())

    def bulk_send_occurrences(self, occurrences):
        """Send the occurrences to the SWH archive"""
        threshold_reached = self.occurrences.add(occurrences)
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
            self.bulk_send_contents(contents)

    def maybe_load_directories(self, directories):
        """Load directories in swh-storage if need be.

        """
        if self.config['send_directories']:
            self.bulk_send_directories(directories)

    def maybe_load_revisions(self, revisions):
        """Load revisions in swh-storage if need be.

        """
        if self.config['send_revisions']:
            self.bulk_send_revisions(revisions)

    def maybe_load_releases(self, releases):
        """Load releases in swh-storage if need be.

        """
        if self.config['send_releases']:
            self.bulk_send_releases(releases)

    def maybe_load_occurrences(self, occurrences):
        """Load occurrences in swh-storage if need be.

        """
        if self.config['send_occurrences']:
            self.bulk_send_occurrences(occurrences)

    def open_fetch_history(self):
        return self.storage.fetch_history_start(self.origin_id)

    def close_fetch_history_success(self, fetch_history_id):
        data = {
            'status': True,
            'result': self.counters,
        }
        return self.storage.fetch_history_end(fetch_history_id, data)

    def close_fetch_history_failure(self, fetch_history_id):
        import traceback
        data = {
            'status': False,
            'stderr': traceback.format_exc(),
        }
        if self.counters['contents'] > 0 or \
           self.counters['directories'] or \
           self.counters['revisions'] > 0 or \
           self.counters['releases'] > 0 or \
           self.counters['occurrences'] > 0:
            data['result'] = self.counters

        return self.storage.fetch_history_end(fetch_history_id, data)

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

    @abstractmethod
    def cleanup(self):
        """Last step executed by the loader.

        """
        pass

    @abstractmethod
    def prepare(self, *args, **kwargs):
        """First step executed by the loader to prepare some state needed by
           the loader.

        """
        pass

    @abstractmethod
    def get_origin(self):
        """Get the origin that is currently being loaded.

        """
        pass

    @abstractmethod
    def fetch_data(self):
        """Fetch the data we want to store.

        """
        pass

    @abstractmethod
    def store_data(self):
        """Store the data we actually fetched.

        """
        pass

    def load(self, *args, **kwargs):
        """Loading logic for the loader to follow:

        1. def prepare(\*args, \**kwargs): Prepare any eventual state
        2. def get_origin(): Get the origin we work with and store
        3. def fetch_data(): Fetch the data to store
        4. def store_data(): Store the data
        5. def cleanup(): Clean up any eventual state put in place in prepare
           method.

        """
        self.prepare(*args, **kwargs)
        origin = self.get_origin()
        self.origin_id = self.send_origin(origin)

        fetch_history_id = self.open_fetch_history()
        if self.visit_date:  # overwriting the visit_date if provided
            visit_date = self.visit_date
        else:
            visit_date = datetime.datetime.now(tz=datetime.timezone.utc)

        origin_visit = self.send_origin_visit(
            self.origin_id,
            visit_date)
        self.visit = origin_visit['visit']

        try:
            self.fetch_data()
            self.store_data()

            self.close_fetch_history_success(fetch_history_id)
            self.update_origin_visit(
                self.origin_id, self.visit, status='full')
        except Exception:
            self.log.exception('Loading failure, updating to `partial` status')
            self.close_fetch_history_failure(fetch_history_id)
            self.update_origin_visit(
                self.origin_id, self.visit, status='partial')
        finally:
            self.flush()
            self.cleanup()
