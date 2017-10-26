# Copyright (C) 2015-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import dulwich.repo
import os
import shutil

from collections import defaultdict

from swh.model import hashutil
from . import base, converters, utils


class GitLoader(base.BaseLoader):
    """Load a git repository from a directory.
    """

    CONFIG_BASE_FILENAME = 'loader/git-loader'

    def prepare(self, origin_url, directory, visit_date):
        self.origin_url = origin_url
        self.origin = self.get_origin()
        self.repo = dulwich.repo.Repo(directory)
        self.visit_date = visit_date

    def get_origin(self):
        """Get the origin that is currently being loaded"""
        return converters.origin_url_to_origin(self.origin_url)

    def iter_objects(self):
        # from dulwich import object_store
        object_store = self.repo.object_store

        for pack in object_store.packs:
            objs = list(pack.index.iterentries())
            objs.sort(key=lambda x: x[1])
            for sha, offset, crc32 in objs:
                yield hashutil.hash_to_bytehex(sha)

        yield from object_store._iter_loose_objects()
        yield from object_store._iter_alternate_objects()

    def fetch_data(self):
        """Fetch the data from the data source"""
        type_to_ids = defaultdict(list)
        for oid in self.iter_objects():
            try:
                obj = self.repo[oid]
            except:
                self.log.warn('object %s not found, skipping' % (
                    oid.decode('utf-8'), ))
                continue
            type_name = obj.type_name
            type_to_ids[type_name].append(oid)

        self.type_to_ids = type_to_ids

    def has_contents(self):
        """Checks whether we need to load contents"""
        return bool(self.type_to_ids[b'blob'])

    def get_content_ids(self):
        """Get the content identifiers from the git repository"""
        for oid in self.type_to_ids[b'blob']:
            yield converters.dulwich_blob_to_content_id(self.repo[oid])

    def get_contents(self):
        """Get the contents that need to be loaded"""
        max_content_size = self.config['content_size_limit']

        missing_contents = set(self.storage.content_missing(
            self.get_content_ids(), 'sha1_git'))

        for oid in missing_contents:
            yield converters.dulwich_blob_to_content(
                self.repo[hashutil.hash_to_bytehex(oid)], log=self.log,
                max_content_size=max_content_size,
                origin_id=self.origin_id)

    def has_directories(self):
        """Checks whether we need to load directories"""
        return bool(self.type_to_ids[b'tree'])

    def get_directory_ids(self):
        """Get the directory identifiers from the git repository"""
        return (hashutil.hash_to_bytes(id.decode())
                for id in self.type_to_ids[b'tree'])

    def get_directories(self):
        """Get the directories that need to be loaded"""
        missing_dirs = set(self.storage.directory_missing(
            sorted(self.get_directory_ids())))

        for oid in missing_dirs:
            yield converters.dulwich_tree_to_directory(
                self.repo[hashutil.hash_to_bytehex(oid)], log=self.log)

    def has_revisions(self):
        """Checks whether we need to load revisions"""
        return bool(self.type_to_ids[b'commit'])

    def get_revision_ids(self):
        """Get the revision identifiers from the git repository"""
        return (hashutil.hash_to_bytes(id.decode())
                for id in self.type_to_ids[b'commit'])

    def get_revisions(self):
        """Get the revisions that need to be loaded"""
        missing_revs = set(self.storage.revision_missing(
            sorted(self.get_revision_ids())))

        for oid in missing_revs:
            yield converters.dulwich_commit_to_revision(
                self.repo[hashutil.hash_to_bytehex(oid)], log=self.log)

    def has_releases(self):
        """Checks whether we need to load releases"""
        return bool(self.type_to_ids[b'tag'])

    def get_release_ids(self):
        """Get the release identifiers from the git repository"""
        return (hashutil.hash_to_bytes(id.decode())
                for id in self.type_to_ids[b'tag'])

    def get_releases(self):
        """Get the releases that need to be loaded"""
        missing_rels = set(self.storage.release_missing(
            sorted(self.get_release_ids())))

        for oid in missing_rels:
            yield converters.dulwich_tag_to_release(
                self.repo[hashutil.hash_to_bytehex(oid)], log=self.log)

    def has_occurrences(self):
        """Checks whether we need to load occurrences"""
        return True

    def get_occurrences(self):
        """Get the occurrences that need to be loaded"""
        repo = self.repo
        origin_id = self.origin_id
        visit = self.visit

        for ref, target in repo.refs.as_dict().items():
            target_type_name = repo[target].type_name
            target_type = converters.DULWICH_TYPES[target_type_name]
            yield {
                'branch': ref,
                'origin': origin_id,
                'target': hashutil.bytehex_to_hash(target),
                'target_type': target_type,
                'visit': visit,
            }

    def get_fetch_history_result(self):
        """Return the data to store in fetch_history for the current loader"""
        return {
            'contents': len(self.type_to_ids[b'blob']),
            'directories': len(self.type_to_ids[b'tree']),
            'revisions': len(self.type_to_ids[b'commit']),
            'releases': len(self.type_to_ids[b'tag']),
            'occurrences': len(self.repo.refs.allkeys()),
        }

    def save_data(self):
        """We already have the data locally, no need to save it"""
        pass

    def eventful(self):
        """Whether the load was eventful"""
        return True


class GitLoaderFromArchive(GitLoader):
    """Load a git repository from an archive.

    """
    def project_name_from_archive(self, archive_path):
        """Compute the project name from the archive's path.

        """
        return os.path.basename(os.path.dirname(archive_path))

    def prepare(self, origin_url, archive_path, visit_date):
        """1. Uncompress the archive in temporary location.
           2. Prepare as the GitLoader does
           3. Load as GitLoader does

        """
        project_name = self.project_name_from_archive(archive_path)
        self.temp_dir, self.repo_path = utils.init_git_repo_from_archive(
            project_name, archive_path)

        self.log.info('Project %s - Uncompressing archive %s at %s' % (
            origin_url, os.path.basename(archive_path), self.repo_path))
        super().prepare(origin_url, self.repo_path, visit_date)

    def cleanup(self):
        """Cleanup the temporary location (if it exists).

        """
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        self.log.info('Project %s - Done injecting %s' % (
            self.origin_url, self.repo_path))


if __name__ == '__main__':
    import logging
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(process)d %(message)s'
    )
    loader = GitLoader()

    origin_url = sys.argv[1]
    directory = sys.argv[2]
    visit_date = datetime.datetime.now(tz=datetime.timezone.utc)

    print(loader.load(origin_url, directory, visit_date))
