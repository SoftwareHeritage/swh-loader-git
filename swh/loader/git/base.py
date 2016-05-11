# Copyright (C) 2016 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import traceback
import uuid

import psycopg2
import requests
from retrying import retry

from swh.core import config
from swh.storage import get_storage


def send_in_packets(objects, sender, packet_size, packet_size_bytes=None):
    """Send `objects`, using the `sender`, in packets of `packet_size` objects (and
    of max `packet_size_bytes`).
    """
    formatted_objects = []
    count = 0
    if not packet_size_bytes:
        packet_size_bytes = 0
    for obj in objects:
        if not obj:
            continue
        formatted_objects.append(obj)
        if packet_size_bytes:
            count += obj['length']
        if len(formatted_objects) >= packet_size or count > packet_size_bytes:
            sender(formatted_objects)
            formatted_objects = []
            count = 0

    if formatted_objects:
        sender(formatted_objects)


def retry_loading(error):
    """Retry policy when we catch a recoverable error"""
    exception_classes = [
        # raised when two parallel insertions insert the same data.
        psycopg2.IntegrityError,
        # raised when uWSGI restarts and hungs up on the worker.
        requests.exceptions.ConnectionError,
    ]

    if not any(isinstance(error, exc) for exc in exception_classes):
        return False

    logger = logging.getLogger('swh.loader.git.BulkLoader')

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


class BaseLoader(config.SWHConfig):
    """This base class is a pattern for loaders.

    The external calling convention is as such:
      - instantiate the class once (loads storage and the configuration)
      - for each origin, call load with the origin-specific arguments (for
        instance, an origin URL).

    load calls several methods that must be implemented in subclasses:

     - prepare(*args, **kwargs) prepares the loader for the new origin
     - get_origin gets the origin object associated to the current loader
     - fetch_data downloads the necessary data from the origin
     - get_{contents,directories,revisions,releases,occurrences} retrieve each
       kind of object from the origin
     - has_* checks whether there are some objects to load for that object type
     - get_fetch_history_result retrieves the data to insert in the
       fetch_history table once the load was successful
     - eventful returns whether the load was eventful or not
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

        'content_packet_size': ('int', 10000),
        'content_packet_size_bytes': ('int', 1024 * 1024 * 1024),
        'directory_packet_size': ('int', 25000),
        'revision_packet_size': ('int', 100000),
        'release_packet_size': ('int', 100000),
        'occurrence_packet_size': ('int', 100000),
    }

    ADDITIONAL_CONFIG = {}

    def __init__(self):
        self.config = self.parse_config_file(
            additional_configs=[self.ADDITIONAL_CONFIG])

        self.storage = get_storage(self.config['storage_class'],
                                   self.config['storage_args'])
        self.log = logging.getLogger('swh.loader.git.BulkLoader')

    def prepare(self, *args, **kwargs):
        """Prepare the data source to be loaded"""
        raise NotImplementedError

    def get_origin(self):
        """Get the origin that is currently being loaded"""
        raise NotImplementedError

    def fetch_data(self):
        """Fetch the data from the data source"""
        raise NotImplementedError

    def has_contents(self):
        """Checks whether we need to load contents"""
        return True

    def get_contents(self):
        """Get the contents that need to be loaded"""
        raise NotImplementedError

    def has_directories(self):
        """Checks whether we need to load directories"""
        return True

    def get_directories(self):
        """Get the directories that need to be loaded"""
        raise NotImplementedError

    def has_revisions(self):
        """Checks whether we need to load revisions"""
        return True

    def get_revisions(self):
        """Get the revisions that need to be loaded"""
        raise NotImplementedError

    def has_releases(self):
        """Checks whether we need to load releases"""
        return True

    def get_releases(self):
        """Get the releases that need to be loaded"""
        raise NotImplementedError

    def has_occurrences(self):
        """Checks whether we need to load occurrences"""
        return True

    def get_occurrences(self):
        """Get the occurrences that need to be loaded"""
        raise NotImplementedError

    def get_fetch_history_result(self):
        """Return the data to store in fetch_history for the current loader"""
        raise NotImplementedError

    def eventful(self):
        """Whether the load was eventful"""
        raise NotImplementedError

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_contents(self, content_list):
        """Actually send properly formatted contents to the database"""
        num_contents = len(content_list)
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
        """Actually send properly formatted directories to the database"""
        num_directories = len(directory_list)
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
        """Actually send properly formatted revisions to the database"""
        num_revisions = len(revision_list)
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
        """Actually send properly formatted releases to the database"""
        num_releases = len(release_list)
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
        """Actually send properly formatted occurrences to the database"""
        num_occurrences = len(occurrence_list)
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

    def send_all_contents(self, contents):
        """Send all the contents to the database"""
        packet_size = self.config['content_packet_size']
        packet_size_bytes = self.config['content_packet_size_bytes']

        send_in_packets(contents, self.send_contents, packet_size,
                        packet_size_bytes=packet_size_bytes)

    def send_all_directories(self, directories):
        """Send all the directories to the database"""
        packet_size = self.config['directory_packet_size']

        send_in_packets(directories, self.send_directories, packet_size)

    def send_all_revisions(self, revisions):
        """Send all the revisions to the database"""
        packet_size = self.config['revision_packet_size']

        send_in_packets(revisions, self.send_revisions, packet_size)

    def send_all_releases(self, releases):
        """Send all the releases to the database
        """
        packet_size = self.config['release_packet_size']

        send_in_packets(releases, self.send_releases, packet_size)

    def send_all_occurrences(self, occurrences):
        """Send all the occurrences to the database
        """
        packet_size = self.config['occurrence_packet_size']

        send_in_packets(occurrences, self.send_occurrences, packet_size)

    def open_fetch_history(self):
        return self.storage.fetch_history_start(self.origin_id)

    def close_fetch_history_success(self, fetch_history_id, result):
        data = {
            'status': True,
            'result': result,
        }
        return self.storage.fetch_history_end(fetch_history_id, data)

    def close_fetch_history_failure(self, fetch_history_id):
        import traceback
        data = {
            'status': False,
            'stderr': traceback.format_exc(),
        }
        return self.storage.fetch_history_end(fetch_history_id, data)

    def load(self, *args, **kwargs):

        self.prepare(*args, **kwargs)
        origin = self.get_origin()
        self.origin_id = self.send_origin(origin)

        fetch_history_id = self.open_fetch_history()
        try:
            self.fetch_data()

            if self.config['send_contents'] and self.has_contents():
                self.send_all_contents(self.get_contents())

            if self.config['send_directories'] and self.has_directories():
                self.send_all_directories(self.get_directories())

            if self.config['send_revisions'] and self.has_revisions():
                self.send_all_revisions(self.get_revisions())

            if self.config['send_releases'] and self.has_releases():
                self.send_all_releases(self.get_releases())

            if self.config['send_occurrences'] and self.has_occurrences():
                self.send_all_occurrences(self.get_occurrences())

            self.close_fetch_history_success(fetch_history_id,
                                             self.get_fetch_history_result())
        except:
            self.close_fetch_history_failure(fetch_history_id)
            raise

        return self.eventful()
