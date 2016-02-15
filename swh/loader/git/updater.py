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
    def __init__(self, storage, origin_id):
        self.storage = storage

        self._parents_cache = {}
        self._type_cache = {}

        if origin_id:
            self.heads = self._cache_heads(origin_id)
        else:
            self.heads = []

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

    def _cache_heads(self, origin_id):
        """Return all the known head commits for `origin_id`"""
        return [
            hashutil.hash_to_bytehex(revision['id'])
            for revision in self.storage.revision_get_by(
                    origin_id, branch_name=None, timestamp=None, limit=None)
        ]

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

    def get_stored_commits(self, commits):
        return commits - self._decode_from_storage(
            self.storage.revision_missing(
                self._encode_for_storage(commits)
            )
        )

    def get_stored_tags(self, tags):
        return tags - self._decode_from_storage(
            self.storage.release_missing(
                self._encode_for_storage(tags)
            )
        )

    def get_stored_trees(self, trees):
        return trees - self._decode_from_storage(
            self.storage.directory_missing(
                self._encode_for_storage(trees)
            )
        )

    def get_stored_blobs(self, blobs):
        ret = set()
        for blob in blobs:
            if self.storage.content_find({
                    'sha1_git': hashutil.bytehex_to_hash(blob),
            }):
                ret.add(blob)

        return ret

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
        if not refs:
            return []
        refs = self.parse_local_refs(refs)
        ret = set()
        for ref, target in self.filter_unwanted_refs(refs).items():
            if target['target_type'] is None:
                # The target doesn't exist in Software Heritage
                ret.add(target['target'])

        return list(ret)

    def parse_local_refs(self, remote_refs):
        """Parse the remote refs information and list the objects that exist in
        Software Heritage"""

        all_objs = set(remote_refs.values()) - set(self._type_cache)
        type_by_id = {}

        tags = self.get_stored_tags(all_objs)
        all_objs -= tags
        for tag in tags:
            type_by_id[tag] = 'release'

        commits = self.get_stored_commits(all_objs)
        all_objs -= commits
        for commit in commits:
            type_by_id[commit] = 'revision'

        trees = self.get_stored_trees(all_objs)
        all_objs -= trees
        for tree in trees:
            type_by_id[tree] = 'directory'

        blobs = self.get_stored_blobs(all_objs)
        all_objs -= blobs
        for blob in blobs:
            type_by_id[blob] = 'content'

        self._type_cache.update(type_by_id)

        ret = {}
        for ref, id in remote_refs.items():
            ret[ref] = {
                'target': id,
                'target_type': self._type_cache.get(id),
            }
        return ret


def send_in_packets(source_list, formatter, sender, packet_size,
                    packet_size_bytes=None, *args, **kwargs):
    """Send objects from `source_list`, passed through `formatter` (with
    extra args *args, **kwargs), using the `sender`, in packets of
    `packet_size` objects (and of max `packet_size_bytes`).

    """
    formatted_objects = []
    count = 0
    if not packet_size_bytes:
        packet_size_bytes = 0
    for obj in source_list:
        formatted_object = formatter(obj, *args, **kwargs)
        if formatted_object:
            formatted_objects.append(formatted_object)
        else:
            continue
        if packet_size_bytes:
            count += formatted_object['length']
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


class BulkUpdater(config.SWHConfig):
    """A bulk loader for a git repository"""
    CONFIG_BASE_FILENAME = 'loader/git-updater.ini'

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

    def __init__(self, config):
        self.config = config
        self.storage = get_storage(config['storage_class'],
                                   config['storage_args'])
        self.log = logging.getLogger('swh.loader.git.BulkLoader')

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

    def fetch_pack_from_origin(self, origin_url, base_origin_id, do_activity):
        """Fetch a pack from the origin"""
        pack_buffer = BytesIO()

        base_repo = SWHRepoRepresentation(self.storage, base_origin_id)

        parsed_uri = urlparse(origin_url)

        path = parsed_uri.path
        if not path.endswith('.git'):
            path += '.git'

        client = dulwich.client.TCPGitClient(parsed_uri.netloc,
                                             thin_packs=False)

        def do_pack(data, pack_buffer=pack_buffer):
            pack_buffer.write(data)

        remote_refs = client.fetch_pack(path.encode('ascii'),
                                        base_repo.determine_wants,
                                        base_repo.graph_walker(),
                                        do_pack,
                                        progress=do_activity)

        if remote_refs:
            local_refs = base_repo.parse_local_refs(remote_refs)
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

    def get_origin(self, origin_url):
        origin = converters.origin_url_to_origin(origin_url)

        return self.storage.origin_get(origin)

    def get_or_create_origin(self, origin_url):
        origin = converters.origin_url_to_origin(origin_url)

        origin['id'] = self.storage.origin_add_one(origin)

        return origin

    def create_origin(self, origin_url):
        log_id = str(uuid.uuid4())
        self.log.debug('Creating origin for %s' % origin_url,
                       extra={
                           'swh_type': 'storage_send_start',
                           'swh_content_type': 'origin',
                           'swh_num': 1,
                           'swh_id': log_id
                       })
        origin = self.get_or_create_origin(origin_url)
        self.log.debug('Done creating origin for %s' % origin_url,
                       extra={
                           'swh_type': 'storage_send_end',
                           'swh_content_type': 'origin',
                           'swh_num': 1,
                           'swh_id': log_id
                       })

        return origin

    def bulk_send_blobs(self, inflater, origin_id):
        """Format blobs as swh contents and send them to the database"""
        packet_size = self.config['content_packet_size']
        packet_size_bytes = self.config['content_packet_size_bytes']
        max_content_size = self.config['content_size_limit']

        send_in_packets(inflater, converters.dulwich_blob_to_content,
                        self.send_contents, packet_size,
                        packet_size_bytes=packet_size_bytes,
                        log=self.log, max_content_size=max_content_size,
                        origin_id=origin_id)

    def bulk_send_trees(self, inflater):
        """Format trees as swh directories and send them to the database"""
        packet_size = self.config['directory_packet_size']

        send_in_packets(inflater, converters.dulwich_tree_to_directory,
                        self.send_directories, packet_size,
                        log=self.log)

    def bulk_send_commits(self, inflater):
        """Format commits as swh revisions and send them to the database"""
        packet_size = self.config['revision_packet_size']

        send_in_packets(inflater, converters.dulwich_commit_to_revision,
                        self.send_revisions, packet_size,
                        log=self.log)

    def bulk_send_tags(self, inflater):
        """Format annotated tags (dulwich.objects.Tag objects) as swh releases and send
        them to the database
        """
        packet_size = self.config['release_packet_size']

        send_in_packets(inflater, converters.dulwich_tag_to_release,
                        self.send_releases, packet_size,
                        log=self.log)

    def bulk_send_refs(self, refs):
        """Format git references as swh occurrences and send them to the
        database
        """
        packet_size = self.config['occurrence_packet_size']

        send_in_packets(refs, lambda x: x, self.send_occurrences, packet_size)

    def open_fetch_history(self, origin_id):
        return self.storage.fetch_history_start(origin_id)

    def close_fetch_history_success(self, fetch_history_id, objects, refs):
        data = {
            'status': True,
            'result': {
                'contents': len(objects[b'blob']),
                'directories': len(objects[b'tree']),
                'revisions': len(objects[b'commit']),
                'releases': len(objects[b'tag']),
                'occurrences': len(refs),
            },
        }
        return self.storage.fetch_history_end(fetch_history_id, data)

    def close_fetch_history_failure(self, fetch_history_id):
        import traceback
        data = {
            'status': False,
            'stderr': traceback.format_exc(),
        }
        return self.storage.fetch_history_end(fetch_history_id, data)

    def get_inflater(self, pack_buffer, pack_size):
        """Reset the pack buffer and get an object inflater from it"""
        pack_buffer.seek(0)
        return PackInflater.for_pack_data(
            PackData.from_file(pack_buffer, pack_size))

    def list_pack(self, pack_data, pack_size):
        id_to_type = {}
        type_to_ids = defaultdict(set)

        inflater = self.get_inflater(pack_data, pack_size)

        for obj in inflater:
            type, id = obj.type_name, obj.id
            id_to_type[id] = type
            type_to_ids[type].add(id)

        return id_to_type, type_to_ids

    def list_refs(self, remote_refs, local_refs, id_to_type, origin_id, date):
        ret = []
        for ref in remote_refs:
            ret_ref = local_refs[ref].copy()
            ret_ref.update({
                'branch': ref,
                'origin': origin_id,
                'date': date,
            })
            if not ret_ref['target_type']:
                target_type = id_to_type[ret_ref['target']]
                ret_ref['target_type'] = converters.DULWICH_TYPES[target_type]

            ret_ref['target'] = hashutil.bytehex_to_hash(ret_ref['target'])

            ret.append(ret_ref)

        return ret

    def load_pack(self, pack_buffer, pack_size, refs, origin_id):
        if self.config['send_contents']:
            self.bulk_send_blobs(self.get_inflater(pack_buffer, pack_size),
                                 origin_id)
        else:
            self.log.info('Not sending contents')

        if self.config['send_directories']:
            self.bulk_send_trees(self.get_inflater(pack_buffer, pack_size))
        else:
            self.log.info('Not sending directories')

        if self.config['send_revisions']:
            self.bulk_send_commits(self.get_inflater(pack_buffer, pack_size))
        else:
            self.log.info('Not sending revisions')

        if self.config['send_releases']:
            self.bulk_send_tags(self.get_inflater(pack_buffer, pack_size))
        else:
            self.log.info('Not sending releases')

        if self.config['send_occurrences']:
            self.bulk_send_refs(refs)
        else:
            self.log.info('Not sending occurrences')

    def process(self, origin_url, base_url):
        date = datetime.datetime.now(tz=datetime.timezone.utc)

        # Add origin to storage if needed, use the one from config if not
        origin = self.create_origin(origin_url)
        base_origin = origin
        if base_url:
            base_origin = self.get_origin(base_url)

        # Create fetch_history
        fetch_history = self.open_fetch_history(origin['id'])
        closed = False

        def do_progress(msg):
            sys.stderr.buffer.write(msg)
            sys.stderr.flush()

        try:
            fetch_info = self.fetch_pack_from_origin(
                origin_url, base_origin['id'], do_progress)

            pack_buffer = fetch_info['pack_buffer']
            pack_size = fetch_info['pack_size']

            remote_refs = fetch_info['remote_refs']
            local_refs = fetch_info['local_refs']
            if not remote_refs:
                self.log.info('Skipping empty repository %s' % origin_url,
                              extra={
                                  'swh_type': 'git_repo_list_refs',
                                  'swh_repo': origin_url,
                                  'swh_num_refs': 0,
                              })
                # End fetch_history
                self.close_fetch_history_success(fetch_history,
                                                 defaultdict(set), [])
                closed = True
                return
            else:
                self.log.info('Listed %d refs for repo %s' % (
                    len(remote_refs), origin_url), extra={
                        'swh_type': 'git_repo_list_refs',
                        'swh_repo': origin_url,
                        'swh_num_refs': len(remote_refs),
                    })

            # We want to load the repository, walk all the objects
            id_to_type, type_to_ids = self.list_pack(pack_buffer, pack_size)

            # Parse the remote references and add info from the local ones
            refs = self.list_refs(remote_refs, local_refs,
                                  id_to_type, origin['id'], date)

            # Finally, load the repository
            self.load_pack(pack_buffer, pack_size, refs, origin['id'])

            # End fetch_history
            self.close_fetch_history_success(fetch_history, type_to_ids, refs)
            closed = True

        finally:
            if not closed:
                self.close_fetch_history_failure(fetch_history)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(process)d %(message)s'
    )
    config = BulkUpdater.parse_config_file(
            base_filename='loader/git-updater.ini'
        )

    bulkupdater = BulkUpdater(config)

    origin_url = sys.argv[1]
    base_url = origin_url
    if len(sys.argv) > 2:
        base_url = sys.argv[2]

    bulkupdater.process(origin_url, base_url)
