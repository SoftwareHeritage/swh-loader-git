# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from io import BytesIO
import logging
import sys
import traceback
import uuid

from collections import defaultdict
import dulwich.client
from dulwich.object_store import ObjectStoreGraphWalker
from dulwich.pack import PackData, PackInflater
import psycopg2
import requests
from retrying import retry
from urllib.parse import urlparse

from swh.core import config, hashutil
from swh.storage import get_storage

from . import converters


class SWHRepoRepresentation:
    """Repository representation for a Software Heritage origin."""
    def __init__(self, storage, origin_id, occurrences=None):
        self.storage = storage

        self._parents_cache = {}
        self._type_cache = {}

        if origin_id:
            self.heads = set(self._cache_heads(origin_id, occurrences))
        else:
            self.heads = set()

    def _fill_parents_cache(self, commit):
        """When querying for a commit's parents, we fill the cache to a depth of 100
        commits."""
        root_rev = hashutil.bytehex_to_hash(commit)
        for rev, parents in self.storage.revision_shortlog([root_rev], 100):
            rev_id = hashutil.hash_to_bytehex(rev)
            if rev_id not in self._parents_cache:
                self._parents_cache[rev_id] = [
                    hashutil.hash_to_bytehex(parent) for parent in parents
                ]

    def _cache_heads(self, origin_id, occurrences):
        """Return all the known head commits for `origin_id`"""
        if not occurrences:
            occurrences = self.storage.occurrence_get(origin_id)

        return self._decode_from_storage(
            occurrence['target'] for occurrence in occurrences
        )

    def get_parents(self, commit):
        """get the parent commits for `commit`"""
        if commit not in self._parents_cache:
            self._fill_parents_cache(commit)
        return self._parents_cache.get(commit, [])

    def get_heads(self):
        return self.heads

    @staticmethod
    def _encode_for_storage(objects):
        return [hashutil.bytehex_to_hash(object) for object in objects]

    @staticmethod
    def _decode_from_storage(objects):
        return set(hashutil.hash_to_bytehex(object) for object in objects)

    def graph_walker(self):
        return ObjectStoreGraphWalker(self.get_heads(), self.get_parents)

    @staticmethod
    def filter_unwanted_refs(refs):
        """Filter the unwanted references from refs"""
        ret = {}
        for ref, val in refs.items():
            if ref.endswith(b'^{}'):
                # Peeled refs make the git protocol explode
                continue
            elif ref.startswith(b'refs/pull/') and ref.endswith(b'/merge'):
                # We filter-out auto-merged GitHub pull requests
                continue
            else:
                ret[ref] = val

        return ret

    def determine_wants(self, refs):
        """Filter the remote references to figure out which ones
        Software Heritage needs.
        """
        if not refs:
            return []

        # Find what objects Software Heritage has
        refs = self.find_remote_ref_types_in_swh(refs)

        # Cache the objects found in swh as existing heads
        for target in refs.values():
            if target['target_type'] is not None:
                self.heads.add(target['target'])

        ret = set()
        for target in self.filter_unwanted_refs(refs).values():
            if target['target_type'] is None:
                # The target doesn't exist in Software Heritage, let's retrieve
                # it.
                ret.add(target['target'])

        return list(ret)

    def get_stored_objects(self, objects):
        return self.storage.object_find_by_sha1_git(
            self._encode_for_storage(objects))

    def find_remote_ref_types_in_swh(self, remote_refs):
        """Parse the remote refs information and list the objects that exist in
        Software Heritage.
        """

        all_objs = set(remote_refs.values()) - set(self._type_cache)
        type_by_id = {}

        for id, objs in self.get_stored_objects(all_objs).items():
            id = hashutil.hash_to_bytehex(id)
            if objs:
                type_by_id[id] = objs[0]['type']

        self._type_cache.update(type_by_id)

        ret = {}
        for ref, id in remote_refs.items():
            ret[ref] = {
                'target': id,
                'target_type': self._type_cache.get(id),
            }
        return ret


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

    def get_occurrences(self, refs):
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
        self.log.debug('Creating origin for %s' % origin_url,
                       extra={
                           'swh_type': 'storage_send_start',
                           'swh_content_type': 'origin',
                           'swh_num': 1,
                           'swh_id': log_id
                       })
        origin_id = self.storage.origin_add_one(origin)
        self.log.debug('Done creating origin for %s' % origin_url,
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
                        packet_size_bytes=packet_size_bytes, log=self.log)

    def send_all_directories(self, directories):
        """Send all the directories to the database"""
        packet_size = self.config['directory_packet_size']

        send_in_packets(directories, self.send_directories, packet_size,
                        log=self.log)

    def send_all_revisions(self, revisions):
        """Send all the revisions to the database"""
        packet_size = self.config['revision_packet_size']

        send_in_packets(revisions, self.send_revisions, packet_size,
                        log=self.log)

    def send_all_releases(self, releases):
        """Send all the releases to the database
        """
        packet_size = self.config['release_packet_size']

        send_in_packets(releases, self.send_releases, packet_size,
                        log=self.log)

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


class BulkUpdater(BaseLoader):
    """A bulk loader for a git repository"""
    CONFIG_BASE_FILENAME = 'loader/git-updater.ini'

    ADDITIONAL_CONFIG = {
        'pack_size_bytes': ('int', 4 * 1024 * 1024 * 1024),
    }

    def fetch_pack_from_origin(self, origin_url, base_origin_id,
                               base_occurrences, do_activity):
        """Fetch a pack from the origin"""
        pack_buffer = BytesIO()

        base_repo = SWHRepoRepresentation(self.storage, base_origin_id,
                                          base_occurrences)

        parsed_uri = urlparse(origin_url)

        path = parsed_uri.path
        if not path.endswith('.git'):
            path += '.git'

        client = dulwich.client.TCPGitClient(parsed_uri.netloc,
                                             thin_packs=False)

        size_limit = self.config['pack_size_bytes']

        def do_pack(data, pack_buffer=pack_buffer, limit=size_limit):
            cur_size = pack_buffer.tell()
            would_write = len(data)
            if cur_size + would_write > limit:
                raise IOError('Pack file too big for repository %s, '
                              'limit is %d bytes, current size is %d, '
                              'would write %d' %
                              (origin_url, limit, cur_size, would_write))

            pack_buffer.write(data)

        remote_refs = client.fetch_pack(path.encode('ascii'),
                                        base_repo.determine_wants,
                                        base_repo.graph_walker(),
                                        do_pack,
                                        progress=do_activity)

        if remote_refs:
            local_refs = base_repo.find_remote_ref_types_in_swh(remote_refs)
        else:
            local_refs = remote_refs = {}

        pack_buffer.flush()
        pack_size = pack_buffer.tell()
        pack_buffer.seek(0)
        return {
            'remote_refs': base_repo.filter_unwanted_refs(remote_refs),
            'local_refs': local_refs,
            'pack_buffer': pack_buffer,
            'pack_size': pack_size,
        }

    def list_pack(self, pack_data, pack_size):
        id_to_type = {}
        type_to_ids = defaultdict(set)

        inflater = self.get_inflater()

        for obj in inflater:
            type, id = obj.type_name, obj.id
            id_to_type[id] = type
            type_to_ids[type].add(id)

        return id_to_type, type_to_ids

    def prepare(self, origin_url, base_url=None):
        origin = converters.origin_url_to_origin(origin_url)
        base_origin = converters.origin_url_to_origin(base_url)

        base_occurrences = []
        base_origin_id = origin_id = None

        db_origin = self.storage.origin_get(origin)
        if db_origin:
            base_origin_id = origin_id = db_origin['id']

        if origin_id:
            base_occurrences = self.storage.occurrence_get(origin_id)

        if base_url and not base_occurrences:
            base_origin = self.storage.origin_get(base_origin)
            if base_origin:
                base_origin_id = base_origin['id']
                base_occurrences = self.storage.occurrence_get(base_origin_id)

        self.base_occurrences = list(sorted(base_occurrences,
                                            key=lambda occ: occ['branch']))
        self.base_origin_id = base_origin_id
        self.origin = origin

    def get_origin(self):
        return self.origin

    def fetch_data(self):
        def do_progress(msg):
            sys.stderr.buffer.write(msg)
            sys.stderr.flush()

        self.fetch_date = datetime.datetime.now(tz=datetime.timezone.utc)

        fetch_info = self.fetch_pack_from_origin(
            self.origin['url'], self.base_origin_id, self.base_occurrences,
            do_progress)

        self.pack_buffer = fetch_info['pack_buffer']
        self.pack_size = fetch_info['pack_size']

        self.remote_refs = fetch_info['remote_refs']
        self.local_refs = fetch_info['local_refs']
        if not self.remote_refs:
            raise ValueError('Handle no remote refs')

        origin_url = self.origin['url']

        self.log.info('Listed %d refs for repo %s' % (
            len(self.remote_refs), origin_url), extra={
                'swh_type': 'git_repo_list_refs',
                'swh_repo': origin_url,
                'swh_num_refs': len(self.remote_refs),
            })

        # We want to load the repository, walk all the objects
        id_to_type, type_to_ids = self.list_pack(self.pack_buffer,
                                                 self.pack_size)

        self.id_to_type = id_to_type
        self.type_to_ids = type_to_ids

    def get_inflater(self):
        """Reset the pack buffer and get an object inflater from it"""
        self.pack_buffer.seek(0)
        return PackInflater.for_pack_data(
            PackData.from_file(self.pack_buffer, self.pack_size))

    def has_contents(self):
        return bool(self.type_to_ids[b'blob'])

    def get_contents(self):
        """Format the blobs from the git repository as swh contents"""
        max_content_size = self.config['content_size_limit']
        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b'blob':
                continue

            yield converters.dulwich_blob_to_content(
                raw_obj, log=self.log, max_content_size=max_content_size,
                origin_id=self.origin_id)

    def has_directories(self):
        return bool(self.type_to_ids[b'tree'])

    def get_directories(self):
        """Format the trees as swh directories"""
        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b'tree':
                continue

            yield converters.dulwich_tree_to_directory(raw_obj, log=self.log)

    def has_revisions(self):
        return bool(self.type_to_ids[b'commit'])

    def get_revisions(self):
        """Format commits as swh revisions"""
        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b'commit':
                continue

            yield converters.dulwich_commit_to_revision(raw_obj, log=self.log)

    def has_releases(self):
        return bool(self.type_to_ids[b'tag'])

    def get_releases(self):
        """Retrieve all the release objects from the git repository"""
        for raw_obj in self.get_inflater():
            if raw_obj.type_name != b'tag':
                continue

            yield converters.dulwich_tag_to_release(raw_obj, log=self.log)

    def has_occurrences(self):
        return bool(self.remote_refs)

    def get_occurrences(self):
        ret = []
        for ref in self.remote_refs:
            ret_ref = self.local_refs[ref].copy()
            ret_ref.update({
                'branch': ref,
                'origin': self.origin_id,
                'date': self.fetch_date,
            })
            if not ret_ref['target_type']:
                target_type = self.id_to_type[ret_ref['target']]
                ret_ref['target_type'] = converters.DULWICH_TYPES[target_type]

            ret_ref['target'] = hashutil.bytehex_to_hash(ret_ref['target'])

            ret.append(ret_ref)

        return ret

    def get_fetch_history_result(self):
        return {
            'contents': len(self.type_to_ids[b'blob']),
            'directories': len(self.type_to_ids[b'tree']),
            'revisions': len(self.type_to_ids[b'commit']),
            'releases': len(self.type_to_ids[b'tag']),
            'occurrences': len(self.remote_refs),
        }

    def eventful(self):
        """The load was eventful if the current occurrences are different to
           the ones we retrieved at the beginning of the run"""
        current_occurrences = list(sorted(
            self.storage.occurrence_get(self.origin_id),
            key=lambda occ: occ['branch'],
        ))

        return self.base_occurrences != current_occurrences


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(process)d %(message)s'
    )
    bulkupdater = BulkUpdater()

    origin_url = sys.argv[1]
    base_url = origin_url
    if len(sys.argv) > 2:
        base_url = sys.argv[2]

    print(bulkupdater.load(origin_url, base_url))
