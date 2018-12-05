# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import logging
import os
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


def send_in_packets(objects, sender, packet_size, packet_size_bytes=None):
    """Send `objects`, using the `sender`, in packets of `packet_size` objects
    (and of max `packet_size_bytes`).
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


class BufferedLoader(config.SWHConfig, metaclass=ABCMeta):
    """Mixin base class for loader.

    To use this class, you must:

    - inherit from this class
    - and implement the @abstractmethod methods:

      - :func:`prepare`: First step executed by the loader to prepare some
        state needed by the `func`:load method.

      - :func:`get_origin`: Retrieve the origin that is currently being loaded.

      - :func:`fetch_data`: Fetch the data is actually the method to implement
        to compute data to inject in swh (through the store_data method)

      - :func:`store_data`: Store data fetched.

      - :func:`visit_status`: Explicit status of the visit ('partial' or
        'full')

      - :func:`load_status`: Explicit status of the loading, for use by the
        scheduler (eventful/uneventful/temporary failure/permanent failure).

      - :func:`cleanup`: Last step executed by the loader.

    The entry point for the resulting loader is :func:`load`.

    You can take a look at some example classes:

    - :class:`BaseSvnLoader`
    - :class:`TarLoader`
    - :class:`DirLoader`
    - :class:`DebianLoader`

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
        'send_snapshot': ('bool', True),

        'save_data': ('bool', False),
        'save_data_path': ('str', ''),

        # Number of contents
        'content_packet_size': ('int', 10000),
        # packet of 100Mib contents
        'content_packet_size_bytes': ('int', 100 * 1024 * 1024),
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
            max_size=self.config['content_packet_size_bytes'])

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

        self.snapshot = None

        _log = logging.getLogger('requests.packages.urllib3.connectionpool')
        _log.setLevel(logging.WARN)

        self.counters = {
            'contents': 0,
            'directories': 0,
            'revisions': 0,
            'releases': 0,
        }

        # Make sure the config is sane
        save_data = self.config.get('save_data')
        if save_data:
            path = self.config['save_data_path']
            os.stat(path)
            if not os.access(path, os.R_OK | os.W_OK):
                raise PermissionError("Permission denied: %r" % path)

    def save_data(self):
        """Save the data associated to the current load"""
        raise NotImplementedError

    def get_save_data_path(self):
        """The path to which we save the data"""
        if not hasattr(self, '__save_data_path'):
            origin_id = self.origin_id
            year = str(self.visit_date.year)

            path = os.path.join(
                self.config['save_data_path'],
                "%04d" % (origin_id % 10000),
                "%08d" % origin_id,
                year,
            )

            os.makedirs(path, exist_ok=True)
            self.__save_data_path = path

        return self.__save_data_path

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
    def send_tool(self, tool):
        log_id = str(uuid.uuid4())
        self.log.debug(
            'Creating tool with name %s version %s configuration %s' % (
                 tool['name'], tool['version'], tool['configuration']),
            extra={
                'swh_type': 'storage_send_start',
                'swh_content_type': 'tool',
                'swh_num': 1,
                'swh_id': log_id
            })

        tools = list(self.storage.tool_add([tool]))
        tool_id = tools[0]['id']

        self.log.debug(
            'Done creating tool with name %s version %s and configuration %s' % (  # noqa
                 tool['name'], tool['version'], tool['configuration']),
            extra={
                'swh_type': 'storage_send_end',
                'swh_content_type': 'tool',
                'swh_num': 1,
                'swh_id': log_id
            })
        return tool_id

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_provider(self, provider):
        log_id = str(uuid.uuid4())
        self.log.debug(
            'Creating metadata_provider with name %s type %s url %s' % (
                provider['provider_name'], provider['provider_type'],
                provider['provider_url']),
            extra={
                'swh_type': 'storage_send_start',
                'swh_content_type': 'metadata_provider',
                'swh_num': 1,
                'swh_id': log_id
            })
        # FIXME: align metadata_provider_add with indexer_configuration_add
        _provider = self.storage.metadata_provider_get_by(provider)
        if _provider and 'id' in _provider:
            provider_id = _provider['id']
        else:
            provider_id = self.storage.metadata_provider_add(
                provider['provider_name'],
                provider['provider_type'],
                provider['provider_url'],
                provider['metadata'])

        self.log.debug(
            'Done creating metadata_provider with name %s type %s url %s' % (
                provider['provider_name'], provider['provider_type'],
                provider['provider_url']),
            extra={
                'swh_type': 'storage_send_end',
                'swh_content_type': 'metadata_provider',
                'swh_num': 1,
                'swh_id': log_id
            })
        return provider_id

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def send_origin_metadata(self, origin_id, visit_date, provider_id,
                             tool_id, metadata):
        log_id = str(uuid.uuid4())
        self.log.debug(
            'Creating origin_metadata for origin %s at time %s with provider_id %s and tool_id %s' % (  # noqa
                origin_id, visit_date, provider_id, tool_id),
            extra={
                'swh_type': 'storage_send_start',
                'swh_content_type': 'origin_metadata',
                'swh_num': 1,
                'swh_id': log_id
            })

        self.storage.origin_metadata_add(origin_id, visit_date, provider_id,
                                         tool_id, metadata)
        self.log.debug(
            'Done Creating origin_metadata for origin %s at time %s with provider %s and tool %s' % (  # noqa
                origin_id, visit_date, provider_id, tool_id),
            extra={
                'swh_type': 'storage_send_end',
                'swh_content_type': 'origin_metadata',
                'swh_num': 1,
                'swh_id': log_id
            })

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
    def send_snapshot(self, snapshot):
        self.storage.snapshot_add(self.origin_id, self.visit, snapshot)

    @retry(retry_on_exception=retry_loading, stop_max_attempt_number=3)
    def filter_missing_contents(self, contents):
        """Return only the contents missing from swh"""
        max_content_size = self.config['content_size_limit']
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
            self.send_batch_contents(self.contents.pop())

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
            self.send_batch_contents(self.contents.pop())
            self.send_batch_directories(self.directories.pop())

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
            self.send_batch_contents(self.contents.pop())
            self.send_batch_directories(self.directories.pop())
            self.send_batch_revisions(self.revisions.pop())

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
            self.send_batch_contents(self.contents.pop())
            self.send_batch_directories(self.directories.pop())
            self.send_batch_revisions(self.revisions.pop())
            self.send_batch_releases(self.releases.pop())

    def bulk_send_snapshot(self, snapshot):
        """Send missing releases to the database"""
        self.send_batch_contents(self.contents.pop())
        self.send_batch_directories(self.directories.pop())
        self.send_batch_revisions(self.revisions.pop())
        self.send_batch_releases(self.releases.pop())
        self.send_snapshot(snapshot)

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

    def maybe_load_snapshot(self, snapshot):
        """Load the snapshot in swh-storage if need be."""
        if self.config['send_snapshot']:
            self.bulk_send_snapshot(snapshot)

    def send_batch_contents(self, contents):
        """Send contents batches to the storage"""
        packet_size = self.config['content_packet_size']
        packet_size_bytes = self.config['content_packet_size_bytes']
        send_in_packets(contents, self.send_contents, packet_size,
                        packet_size_bytes=packet_size_bytes)

    def send_batch_directories(self, directories):
        """Send directories batches to the storage"""
        packet_size = self.config['directory_packet_size']
        send_in_packets(directories, self.send_directories, packet_size)

    def send_batch_revisions(self, revisions):
        """Send revisions batches to the storage"""
        packet_size = self.config['revision_packet_size']
        send_in_packets(revisions, self.send_revisions, packet_size)

    def send_batch_releases(self, releases):
        """Send releases batches to the storage
        """
        packet_size = self.config['release_packet_size']
        send_in_packets(releases, self.send_releases, packet_size)

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
           self.counters['directories'] > 0 or \
           self.counters['revisions'] > 0 or \
           self.counters['releases'] > 0:
            data['result'] = self.counters

        return self.storage.fetch_history_end(fetch_history_id, data)

    def flush(self):
        """Flush any potential dangling data not sent to swh-storage.

        Bypass the maybe_load_* methods which awaits threshold reached
        signal. We actually want to store those as we are done
        loading.

        """
        contents = self.contents.pop()
        directories = self.directories.pop()
        revisions = self.revisions.pop()
        releases = self.releases.pop()

        # and send those to storage if asked
        if self.config['send_contents']:
            self.send_batch_contents(contents)
        if self.config['send_contents']:
            self.send_batch_directories(directories)
        if self.config['send_revisions']:
            self.send_batch_revisions(revisions)
        if self.config['send_releases']:
            self.send_batch_releases(releases)
        if self.config['send_snapshot'] and self.snapshot:
            self.send_snapshot(self.snapshot)

    def prepare_metadata(self):
        """First step for origin_metadata insertion, resolving the
        provider_id and the tool_id by fetching data from the storage
        or creating tool and provider on the fly if the data isn't available

        """
        origin_metadata = self.origin_metadata

        tool = origin_metadata['tool']
        try:
            tool_id = self.send_tool(tool)
            self.origin_metadata['tool']['tool_id'] = tool_id
        except Exception:
            self.log.exception('Problem when storing new tool')
            raise

        provider = origin_metadata['provider']
        try:
            provider_id = self.send_provider(provider)
            self.origin_metadata['provider']['provider_id'] = provider_id
        except Exception:
            self.log.exception('Problem when storing new provider')
            raise

    @abstractmethod
    def cleanup(self):
        """Last step executed by the loader.

        """
        pass

    @abstractmethod
    def prepare_origin_visit(self, *args, **kwargs):
        """First step executed by the loader to prepare origin and visit
           references. Set/update self.origin, self.origin_id and
           optionally self.origin_url, self.visit_date.

        """
        pass

    def _store_origin_visit(self):
        """Store origin and visit references. Sets the self.origin_visit and
           self.visit references.

        """
        origin_id = self.origin.get('id')
        if origin_id:
            self.origin_id = origin_id
        else:
            self.origin_id = self.send_origin(self.origin)
        self.origin['id'] = self.origin_id

        if not self.visit_date:  # now as default visit_date if not provided
            self.visit_date = datetime.datetime.now(tz=datetime.timezone.utc)
        self.origin_visit = self.send_origin_visit(
            self.origin_id, self.visit_date)
        self.visit = self.origin_visit['visit']

    @abstractmethod
    def prepare(self, *args, **kwargs):
        """Second step executed by the loader to prepare some state needed by
           the loader.

        """
        pass

    def get_origin(self):
        """Get the origin that is currently being loaded.
        self.origin should be set in :func:`prepare_origin`

        Returns:
          dict: an origin ready to be sent to storage by
          :func:`origin_add_one`.
        """
        return self.origin

    @abstractmethod
    def fetch_data(self):
        """Fetch the data from the source the loader is currently loading
           (ex: git/hg/svn/... repository).

        Returns:
            a value that is interpreted as a boolean. If True, fetch_data needs
            to be called again to complete loading.

        """
        pass

    @abstractmethod
    def store_data(self):
        """Store fetched data in the database.

        Should call the :func:`maybe_load_xyz` methods, which handle the
        bundles sent to storage, rather than send directly.
        """
        pass

    def store_metadata(self):
        """Store fetched metadata in the database.

        For more information, see implementation in :class:`DepositLoader`.
        """
        pass

    def load_status(self):
        """Detailed loading status.

        Defaults to logging an eventful load.

        Returns: a dictionary that is eventually passed back as the task's
          result to the scheduler, allowing tuning of the task recurrence
          mechanism.
        """
        return {
            'status': 'eventful',
        }

    def post_load(self, success=True):
        """Permit the loader to do some additional actions according to status
        after the loading is done. The flag success indicates the
        loading's status.

        Defaults to doing nothing.

        This is up to the implementer of this method to make sure this
        does not break.

        Args:
            success (bool): the success status of the loading

        """
        pass

    def visit_status(self):
        """Detailed visit status.

        Defaults to logging a full visit.
        """
        return 'full'

    def pre_cleanup(self):
        """As a first step, will try and check for dangling data to cleanup.
        This should do its best to avoid raising issues.

        """
        pass

    def load(self, *args, **kwargs):
        r"""Loading logic for the loader to follow:

        - 1. Call :meth:`prepare_origin_visit` to prepare the
             origin and visit we will associate loading data to
        - 2. Store the actual ``origin_visit`` to storage
        - 3. Call :meth:`prepare` to prepare any eventual state
        - 4. Call :meth:`get_origin` to get the origin we work with and store

        - while True:

          - 5. Call :meth:`fetch_data` to fetch the data to store
          - 6. Call :meth:`store_data` to store the data

        - 7. Call :meth:`cleanup` to clean up any eventual state put in place
             in :meth:`prepare` method.

        """
        try:
            self.pre_cleanup()
        except Exception:
            msg = 'Cleaning up dangling data failed! Continue loading.'
            self.log.warning(msg)

        self.prepare_origin_visit(*args, **kwargs)
        self._store_origin_visit()
        fetch_history_id = self.open_fetch_history()

        try:
            self.prepare(*args, **kwargs)

            while True:
                more_data_to_fetch = self.fetch_data()
                self.store_data()
                if not more_data_to_fetch:
                    break

            self.store_metadata()
            self.close_fetch_history_success(fetch_history_id)
            self.update_origin_visit(
                self.origin_id, self.visit, status=self.visit_status())
            self.post_load()
        except Exception:
            self.log.exception('Loading failure, updating to `partial` status',
                               extra={
                                   'swh_task_args': args,
                                   'swh_task_kwargs': kwargs,
                               })
            self.close_fetch_history_failure(fetch_history_id)
            self.update_origin_visit(
                self.origin_id, self.visit, status='partial')
            self.post_load(success=False)
            return {'status': 'failed'}
        finally:
            self.flush()
            self.cleanup()

        return self.load_status()


class UnbufferedLoader(BufferedLoader):
    """This base class is a pattern for unbuffered loaders.

    UnbufferedLoader loaders are able to load all the data in one go. For
    example, the loader defined in swh-loader-git
    :class:`BulkUpdater`.

    For other loaders (stateful one, (e.g :class:`SWHSvnLoader`),
    inherit directly from :class:`BufferedLoader`.

    """
    ADDITIONAL_CONFIG = {}

    def __init__(self, logging_class=None, config=None):
        super().__init__(logging_class=logging_class, config=config)
        self.visit_date = None  # possibly overridden in self.prepare method

    def cleanup(self):
        """Clean up an eventual state installed for computations."""
        pass

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

    def get_snapshot(self):
        """Get the snapshot that needs to be loaded"""
        raise NotImplementedError

    def get_fetch_history_result(self):
        """Return the data to store in fetch_history for the current loader"""
        raise NotImplementedError

    def eventful(self):
        """Whether the load was eventful"""
        raise NotImplementedError

    def save_data(self):
        """Save the data associated to the current load"""
        raise NotImplementedError

    def flush(self):
        """Unbuffered loader does not flush since it has no state to flush.

        """
        pass

    def store_data(self):
        if self.config['save_data']:
            self.save_data()

        if self.config['send_contents'] and self.has_contents():
            self.send_batch_contents(self.get_contents())
        if self.config['send_directories'] and self.has_directories():
            self.send_batch_directories(self.get_directories())
        if self.config['send_revisions'] and self.has_revisions():
            self.send_batch_revisions(self.get_revisions())
        if self.config['send_releases'] and self.has_releases():
            self.send_batch_releases(self.get_releases())
        if self.config['send_snapshot']:
            self.send_snapshot(self.get_snapshot())
