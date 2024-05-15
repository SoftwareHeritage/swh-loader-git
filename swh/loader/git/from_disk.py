# Copyright (C) 2015-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
from collections import defaultdict
from datetime import datetime
import logging
import os
import shutil
from typing import Dict, Optional

from deprecated import deprecated
from dulwich.errors import ObjectFormatException
import dulwich.objects
from dulwich.objects import EmptyFileException
import dulwich.repo

from swh.loader.git.utils import raise_not_found_repository
from swh.model import hashutil
from swh.model.model import Snapshot, SnapshotBranch, SnapshotTargetType
from swh.storage.algos.origin import origin_get_latest_visit_status
from swh.storage.interface import StorageInterface

from . import converters, utils
from .base import BaseGitLoader

logger = logging.getLogger(__name__)


def _check_tag(tag):
    """Copy-paste of dulwich.objects.Tag, minus the tagger and time checks,
    which are too strict and error on old tags."""
    # Copyright (C) 2007 James Westby <jw+debian@jameswestby.net>
    # Copyright (C) 2008-2013 Jelmer Vernooij <jelmer@jelmer.uk>
    #
    # Dulwich is dual-licensed under the Apache License, Version 2.0 and the GNU
    # General Public License as public by the Free Software Foundation; version 2.0
    # or (at your option) any later version. You can redistribute it and/or
    # modify it under the terms of either of these two licenses.
    #
    # Unless required by applicable law or agreed to in writing, software
    # distributed under the License is distributed on an "AS IS" BASIS,
    # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    # See the License for the specific language governing permissions and
    # limitations under the License.
    #
    # You should have received a copy of the licenses; if not, see
    # <http://www.gnu.org/licenses/> for a copy of the GNU General Public License
    # and <http://www.apache.org/licenses/LICENSE-2.0> for a copy of the Apache
    # License, Version 2.0.
    dulwich.objects.ShaFile.check(tag)
    tag._check_has_member("_object_sha", "missing object sha")
    tag._check_has_member("_object_class", "missing object type")
    tag._check_has_member("_name", "missing tag name")

    if not tag._name:
        raise ObjectFormatException("empty tag name")

    dulwich.objects.check_hexsha(tag._object_sha, "invalid object sha")

    if tag._tag_time is not None:
        dulwich.objects.check_time(tag._tag_time)

    from dulwich.objects import (
        _OBJECT_HEADER,
        _TAG_HEADER,
        _TAGGER_HEADER,
        _TYPE_HEADER,
    )

    last = None
    for field, _ in dulwich.objects._parse_message(tag._chunked_text):
        if field == _OBJECT_HEADER and last is not None:
            raise ObjectFormatException("unexpected object")
        elif field == _TYPE_HEADER and last != _OBJECT_HEADER:
            raise ObjectFormatException("unexpected type")
        elif field == _TAG_HEADER and last != _TYPE_HEADER:
            raise ObjectFormatException("unexpected tag name")
        elif field == _TAGGER_HEADER and last != _TAG_HEADER:
            raise ObjectFormatException("unexpected tagger")
        last = field


@deprecated(version="1.1", reason="Use `swh.loader.git.loader.GitLoader` instead")
class GitLoaderFromDisk(BaseGitLoader):
    """Load a git repository from a directory."""

    visit_type = "git"

    def __init__(
        self,
        storage: StorageInterface,
        url: str,
        visit_date: Optional[datetime] = None,
        directory: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(storage=storage, origin_url=url, **kwargs)
        if visit_date is not None:
            self.visit_date = visit_date
        self.directory = directory

    def prepare(self):
        with raise_not_found_repository():
            self.repo = dulwich.repo.Repo(self.directory)

    def iter_objects(self):
        object_store = self.repo.object_store

        for pack in object_store.packs:
            objs = list(pack.index.iterentries())
            objs.sort(key=lambda x: x[1])
            for sha, offset, crc32 in objs:
                yield hashutil.hash_to_bytehex(sha)

        yield from object_store._iter_loose_objects()
        yield from object_store._iter_alternate_objects()

    def _check(self, obj):
        """Check the object's repository representation.

        If any errors in check exists, an ObjectFormatException is
        raised.

        Args:
            obj (object): Dulwich object read from the repository.

        """
        if isinstance(obj, dulwich.objects.Tag):
            _check_tag(obj)
        else:
            obj.check()

        try:
            # For additional checks on dulwich objects with date
            # for now, only checks on *time
            if isinstance(obj, dulwich.objects.Commit):
                commit_time = obj._commit_time
                utils.check_date_time(commit_time)
                author_time = obj._author_time
                utils.check_date_time(author_time)
            elif isinstance(obj, dulwich.objects.Tag):
                tag_time = obj._tag_time
                if tag_time:
                    utils.check_date_time(tag_time)
        except Exception as e:
            raise ObjectFormatException(e)

    def get_object(self, oid):
        """Given an object id, return the object if it is found and not
           malformed in some way.

        Args:
            oid (bytes): the object's identifier

        Returns:
            The object if found without malformation

        """
        try:
            # some errors are raised when reading the object
            obj = self.repo[oid]
            # some we need to check ourselves
            self._check(obj)
        except KeyError:
            _id = oid.decode("utf-8")
            logger.warn(
                "object %s not found, skipping" % _id,
                extra={
                    "swh_type": "swh_loader_git_missing_object",
                    "swh_object_id": _id,
                    "origin_url": self.origin.url,
                },
            )
            return None
        except ObjectFormatException as e:
            id_ = oid.decode("utf-8")
            logger.warn(
                "object %s malformed (%s), skipping",
                id_,
                e.args[0],
                extra={
                    "swh_type": "swh_loader_git_missing_object",
                    "swh_object_id": id_,
                    "origin_url": self.origin.url,
                },
            )
            return None
        except EmptyFileException:
            id_ = oid.decode("utf-8")
            logger.warn(
                "object %s corrupted (empty file), skipping",
                id_,
                extra={
                    "swh_type": "swh_loader_git_missing_object",
                    "swh_object_id": id_,
                    "origin_url": self.origin.url,
                },
            )
        else:
            return obj

    def fetch_data(self):
        """Fetch the data from the data source"""
        visit_status = origin_get_latest_visit_status(
            self.storage, self.origin.url, require_snapshot=True
        )
        self.previous_snapshot_id = (
            None if visit_status is None else visit_status.snapshot
        )

        type_to_ids = defaultdict(list)
        for oid in self.iter_objects():
            obj = self.get_object(oid)
            if obj is None:
                continue
            type_name = obj.type_name
            type_to_ids[type_name].append(oid)

        self.type_to_ids = type_to_ids

    def has_contents(self):
        """Checks whether we need to load contents"""
        return bool(self.type_to_ids[b"blob"])

    def get_content_ids(self):
        """Get the content identifiers from the git repository"""
        for oid in self.type_to_ids[b"blob"]:
            yield converters.dulwich_blob_to_content_id(self.repo[oid])

    def get_contents(self):
        """Get the contents that need to be loaded"""
        missing_contents = set(
            self.storage.content_missing(self.get_content_ids(), "sha1_git")
        )

        for oid in missing_contents:
            yield converters.dulwich_blob_to_content(
                self.repo[hashutil.hash_to_bytehex(oid)]
            )

    def has_directories(self):
        """Checks whether we need to load directories"""
        return bool(self.type_to_ids[b"tree"])

    def get_directory_ids(self):
        """Get the directory identifiers from the git repository"""
        return (hashutil.hash_to_bytes(id.decode()) for id in self.type_to_ids[b"tree"])

    def get_directories(self):
        """Get the directories that need to be loaded"""
        missing_dirs = set(
            self.storage.directory_missing(sorted(self.get_directory_ids()))
        )

        for oid in missing_dirs:
            yield converters.dulwich_tree_to_directory(
                self.repo[hashutil.hash_to_bytehex(oid)],
            )

    def has_revisions(self):
        """Checks whether we need to load revisions"""
        return bool(self.type_to_ids[b"commit"])

    def get_revision_ids(self):
        """Get the revision identifiers from the git repository"""
        return (
            hashutil.hash_to_bytes(id.decode()) for id in self.type_to_ids[b"commit"]
        )

    def get_revisions(self):
        """Get the revisions that need to be loaded"""
        missing_revs = set(
            self.storage.revision_missing(sorted(self.get_revision_ids()))
        )

        for oid in missing_revs:
            yield converters.dulwich_commit_to_revision(
                self.repo[hashutil.hash_to_bytehex(oid)],
            )

    def has_releases(self):
        """Checks whether we need to load releases"""
        return bool(self.type_to_ids[b"tag"])

    def get_release_ids(self):
        """Get the release identifiers from the git repository"""
        return (hashutil.hash_to_bytes(id.decode()) for id in self.type_to_ids[b"tag"])

    def get_releases(self):
        """Get the releases that need to be loaded"""
        missing_rels = set(self.storage.release_missing(sorted(self.get_release_ids())))

        for oid in missing_rels:
            yield converters.dulwich_tag_to_release(
                self.repo[hashutil.hash_to_bytehex(oid)],
            )

    def get_snapshot(self):
        """Turn the list of branches into a snapshot to load"""
        branches: Dict[bytes, Optional[SnapshotBranch]] = {}

        for ref, target in self.repo.refs.as_dict().items():
            if utils.ignore_branch_name(ref):
                continue
            obj = self.get_object(target)
            if obj:
                target_type = converters.DULWICH_TARGET_TYPES[obj.type_name]
                branches[ref] = SnapshotBranch(
                    target=hashutil.bytehex_to_hash(target),
                    target_type=target_type,
                )
            else:
                branches[ref] = None

        dangling_branches = {}
        for ref, target in self.repo.refs.get_symrefs().items():
            if utils.ignore_branch_name(ref):
                continue
            branches[ref] = SnapshotBranch(
                target=target, target_type=SnapshotTargetType.ALIAS
            )
            if target not in branches:
                # This handles the case where the pointer is "dangling".
                # There's a chance that a further symbolic reference will
                # override this default value, which is totally fine.
                dangling_branches[target] = ref
                branches[target] = None

        utils.warn_dangling_branches(
            branches, dangling_branches, logger, self.origin.url
        )

        self.snapshot = Snapshot(branches=branches)
        return self.snapshot

    def save_data(self):
        """We already have the data locally, no need to save it"""
        pass

    def load_status(self):
        """The load was eventful if the current occurrences are different to
        the ones we retrieved at the beginning of the run"""
        eventful = False

        if self.previous_snapshot_id:
            eventful = self.snapshot.id != self.previous_snapshot_id
        else:
            eventful = bool(self.snapshot.branches)

        return {"status": ("eventful" if eventful else "uneventful")}


class GitLoaderFromArchive(GitLoaderFromDisk):
    """Load a git repository from an archive.

    This loader ingests a git repository compressed into an archive.
    The supported archive formats are ``.zip`` and ``.tar.gz``.

    From an input tarball named ``my-git-repo.zip``, the following layout is
    expected in it::

        my-git-repo/
        ├── .git
        │   ├── branches
        │   ├── COMMIT_EDITMSG
        │   ├── config
        │   ├── description
        │   ├── HEAD
        ...

    Nevertheless, the loader is able to ingest tarballs with the following
    layouts too::

        .
        ├── .git
        │   ├── branches
        │   ├── COMMIT_EDITMSG
        │   ├── config
        │   ├── description
        │   ├── HEAD
        ...

    or::

        other-repo-name/
        ├── .git
        │   ├── branches
        │   ├── COMMIT_EDITMSG
        │   ├── config
        │   ├── description
        │   ├── HEAD
        ...

    """

    def __init__(self, *args, archive_path, **kwargs):
        super().__init__(*args, **kwargs)
        self.temp_dir = self.repo_path = None
        self.archive_path = archive_path

    def project_name_from_archive(self, archive_path):
        """Compute the project name from the archive's path."""
        archive_name = os.path.basename(archive_path)
        for ext in (".zip", ".tar.gz", ".tgz"):
            if archive_name.lower().endswith(ext):
                archive_name = archive_name[: -len(ext)]
                break
        return archive_name

    def prepare(self):
        """1. Uncompress the archive in temporary location.
        2. Prepare as the GitLoaderFromDisk does
        3. Load as GitLoaderFromDisk does

        """
        project_name = self.project_name_from_archive(self.archive_path)
        self.temp_dir, self.repo_path = utils.init_git_repo_from_archive(
            project_name, self.archive_path
        )

        logger.info(
            "Project %s - Uncompressing archive %s at %s",
            self.origin.url,
            os.path.basename(self.archive_path),
            self.repo_path,
        )
        self.directory = self.repo_path
        super().prepare()

    def cleanup(self):
        """Cleanup the temporary location (if it exists)."""
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        logger.info(
            "Project %s - Done injecting %s" % (self.origin.url, self.repo_path)
        )
