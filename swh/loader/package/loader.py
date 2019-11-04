# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import logging
import tempfile
import os

from typing import (
    Any, Dict, Generator, List, Mapping, Optional, Sequence, Tuple
)

from swh.core.tarball import uncompress
from swh.core.config import SWHConfig
from swh.model.from_disk import Directory
from swh.model.identifiers import (
    revision_identifier, snapshot_identifier, identifier_to_bytes
)
from swh.storage import get_storage
from swh.storage.algos.snapshot import snapshot_get_all_branches
from swh.loader.core.converters import content_for_storage
from swh.loader.package.utils import download


logger = logging.getLogger(__name__)


# Not implemented yet:
# - clean up disk routines from previous killed workers (when OOMkilled)
# -> separation of concern would like this to be abstracted from the code
# -> experience tells us it's complicated to do as such (T903, T964, T982,
#    etc...)
#
# - model: swh.model.merkle.from_disk should output swh.model.model.* objects
#          to avoid this layer's conversion routine call
# -> Take this up within swh.model's current implementation


class PackageLoader:
    # Origin visit type (str) set by the loader
    visit_type = ''

    def __init__(self, url):
        """Loader's constructor. This raises exception if the minimal required
           configuration is missing (cf. fn:`check` method).

        Args:
            url (str): Origin url to load data from

        """
        # This expects to use the environment variable SWH_CONFIG_FILENAME
        self.config = SWHConfig.parse_config_file()
        self._check_configuration()
        self.storage = get_storage(**self.config['storage'])
        self.url = url
        self.visit_date = datetime.datetime.now(tz=datetime.timezone.utc)

    def _check_configuration(self):
        """Checks the minimal configuration required is set for the loader.

        If some required configuration is missing, exception detailing the
        issue is raised.

        """
        if 'storage' not in self.config:
            raise ValueError(
                'Misconfiguration, at least the storage key should be set')

    def get_versions(self) -> Sequence[str]:
        """Return the list of all published package versions.

        Returns:
            Sequence of published versions

        """
        return []

    def get_package_info(self, version: str) -> Generator[
            Tuple[str, Mapping[str, Any]], None, None]:
        """Given a release version of a package, retrieve the associated
           package information for such version.

        Args:
            version: Package version

        Returns:
            (branch name, package metadata)

        """
        yield from {}

    def build_revision(
            self, a_metadata: Dict, uncompressed_path: str) -> Dict:
        """Build the revision dict from the archive metadata (extrinsic
        artifact metadata) and the intrinsic metadata.

        Args:
            a_metadata: Artifact metadata
            uncompressed_path: Artifact uncompressed path on disk

        Returns:
            SWH data dict

        """
        return {}

    def get_default_version(self) -> str:
        """Retrieve the latest release version if any.

        Returns:
            Latest version

        """
        return ''

    def last_snapshot(self) -> Optional[Dict]:
        """Retrieve the last snapshot

        """
        snapshot = None
        visit = self.storage.origin_visit_get_latest(
            self.url, require_snapshot=True)
        if visit:
            snapshot = snapshot_get_all_branches(
                self.storage, visit['snapshot'])
        return snapshot

    def known_artifacts(self, snapshot: Optional[Dict]) -> Dict:
        """Retrieve the known releases/artifact for the origin.

        Args
            snapshot: snapshot for the visit

        Returns:
            Dict of keys revision id (bytes), values a metadata Dict.

        """
        if not snapshot or 'branches' not in snapshot:
            return {}

        # retrieve only revisions (e.g the alias we do not want here)
        revs = [rev['target']
                for rev in snapshot['branches'].values()
                if rev and rev['target_type'] == 'revision']
        known_revisions = self.storage.revision_get(revs)

        ret = {}
        for revision in known_revisions:
            if not revision:  # revision_get can return None
                continue
            ret[revision['id']] = revision['metadata']

        return ret

    def resolve_revision_from(
            self, known_artifacts: Dict, artifact_metadata: Dict) \
            -> Optional[bytes]:
        """Resolve the revision from a snapshot and an artifact metadata dict.

        If the artifact has already been downloaded, this will return the
        existing revision targeting that uncompressed artifact directory.
        Otherwise, this returns None.

        Args:
            snapshot: Snapshot
            artifact_metadata: Information dict

        Returns:
            None or revision identifier

        """
        return None

    def download_package(self, p_info: Mapping[str, Any],
                         tmpdir: str) -> List[Tuple[str, Mapping]]:
        """Download artifacts for a specific package. All downloads happen in
        in the tmpdir folder.

        Default implementation expects the artifacts package info to be
        about one artifact per package.

        Note that most implementation have 1 artifact per package. But some
        implementation have multiple artifacts per package (debian), some have
        none, the package is the artifact (gnu).

        Args:
            artifacts_package_info: Information on the package artifacts to
                download (url, filename, etc...)
            tmpdir: Location to retrieve such artifacts

        Returns:
            List of (path, computed hashes)

        """
        a_uri = p_info['url']
        filename = p_info.get('filename')
        return [download(a_uri, dest=tmpdir, filename=filename)]

    def uncompress(self, dl_artifacts: List[Tuple[str, Mapping[str, Any]]],
                   dest: str) -> str:
        """Uncompress the artifact(s) in the destination folder dest.

        Optionally, this could need to use the p_info dict for some more
        information (debian).

        """
        uncompressed_path = os.path.join(dest, 'src')
        for a_path, _ in dl_artifacts:
            uncompress(a_path, dest=uncompressed_path)
        return uncompressed_path

    def load(self) -> Dict:
        """Load for a specific origin the associated contents.

        for each package version of the origin

        1. Fetch the files for one package version By default, this can be
           implemented as a simple HTTP request. Loaders with more specific
           requirements can override this, e.g.: the PyPI loader checks the
           integrity of the downloaded files; the Debian loader has to download
           and check several files for one package version.

        2. Extract the downloaded files By default, this would be a universal
           archive/tarball extraction.

           Loaders for specific formats can override this method (for instance,
           the Debian loader uses dpkg-source -x).

        3. Convert the extracted directory to a set of Software Heritage
           objects Using swh.model.from_disk.

        4. Extract the metadata from the unpacked directories This would only
           be applicable for "smart" loaders like npm (parsing the
           package.json), PyPI (parsing the PKG-INFO file) or Debian (parsing
           debian/changelog and debian/control).

           On "minimal-metadata" sources such as the GNU archive, the lister
           should provide the minimal set of metadata needed to populate the
           revision/release objects (authors, dates) as an argument to the
           task.

        5. Generate the revision/release objects for the given version. From
           the data generated at steps 3 and 4.

        end for each

        6. Generate and load the snapshot for the visit

        Using the revisions/releases collected at step 5., and the branch
        information from step 0., generate a snapshot and load it into the
        Software Heritage archive

        """
        status_load = 'uneventful'  # either: eventful, uneventful, failed
        status_visit = 'full'       # either: partial, full
        tmp_revisions = {}  # type: Dict[str, List]
        snapshot = None

        try:
            # Prepare origin and origin_visit
            origin = {'url': self.url}
            self.storage.origin_add_one(origin)
            visit_id = self.storage.origin_visit_add(
                origin=self.url,
                date=self.visit_date,
                type=self.visit_type)['visit']
            last_snapshot = self.last_snapshot()
            logger.debug('last snapshot: %s', last_snapshot)
            known_artifacts = self.known_artifacts(last_snapshot)
            logger.debug('known artifacts: %s', known_artifacts)

            # Retrieve the default release version (the "latest" one)
            default_version = self.get_default_version()
            logger.debug('default version: %s', default_version)

            for version in self.get_versions():  # for each
                logger.debug('version: %s', version)
                tmp_revisions[version] = []
                # `p_` stands for `package_`
                for branch_name, p_info in self.get_package_info(version):
                    logger.debug('package_info: %s', p_info)
                    revision_id = self.resolve_revision_from(
                        known_artifacts, p_info['raw'])
                    if revision_id is None:
                        with tempfile.TemporaryDirectory() as tmpdir:
                            try:
                                dl_artifacts = self.download_package(
                                    p_info, tmpdir)
                            except Exception:
                                logger.exception('Unable to retrieve %s',
                                                 p_info)
                                status_visit = 'partial'
                                continue

                            uncompressed_path = self.uncompress(
                                dl_artifacts, dest=tmpdir)
                            logger.debug('uncompressed_path: %s',
                                         uncompressed_path)

                            directory = Directory.from_disk(
                                path=uncompressed_path.encode('utf-8'),
                                data=True)  # noqa
                            # FIXME: Try not to load the full raw content in
                            # memory
                            objects = directory.collect()

                            contents = objects['content'].values()
                            logger.debug('Number of contents: %s',
                                         len(contents))

                            self.storage.content_add(
                                [content_for_storage(x) for x in contents])

                            status_load = 'eventful'
                            directories = list(objects['directory'].values())

                            logger.debug('Number of directories: %s',
                                         len(directories))

                            self.storage.directory_add(directories)

                            # FIXME: This should be release. cf. D409
                            revision = self.build_revision(
                                p_info['raw'], uncompressed_path)
                            revision.update({
                                'synthetic': True,
                                'directory': directory.hash,
                            })

                        revision['metadata'].update({
                            'original_artifact': [
                                hashes for _, hashes in dl_artifacts
                            ],
                        })

                        revision['id'] = revision_id = identifier_to_bytes(
                            revision_identifier(revision))

                        logger.debug('Revision: %s', revision)

                        self.storage.revision_add([revision])

                    tmp_revisions[version].append((branch_name, revision_id))

            logger.debug('tmp_revisions: %s', tmp_revisions)
            # Build and load the snapshot
            branches = {}  # type: Dict[bytes, Mapping[str, Any]]
            for version, branch_name_revisions in tmp_revisions.items():
                if version == default_version and \
                   len(branch_name_revisions) == 1:
                    # only 1 branch (no ambiguity), we can create an alias
                    # branch 'HEAD'
                    branch_name, _ = branch_name_revisions[0]
                    # except for some corner case (deposit)
                    if branch_name != 'HEAD':
                        branches[b'HEAD'] = {
                            'target_type': 'alias',
                            'target': branch_name.encode('utf-8'),
                        }

                for branch_name, target in branch_name_revisions:
                    branches[branch_name.encode('utf-8')] = {
                        'target_type': 'revision',
                        'target': target,
                    }

            snapshot = {
                'branches': branches
            }
            logger.debug('snapshot: %s', snapshot)

            snapshot['id'] = identifier_to_bytes(
                snapshot_identifier(snapshot))

            logger.debug('snapshot: %s', snapshot)
            self.storage.snapshot_add([snapshot])
            if hasattr(self.storage, 'flush'):
                self.storage.flush()
        except Exception:
            logger.exception('Fail to load %s' % self.url)
            status_visit = 'partial'
            status_load = 'failed'
        finally:
            self.storage.origin_visit_update(
                origin=self.url, visit_id=visit_id, status=status_visit,
                snapshot=snapshot and snapshot['id'])
        result = {
            'status': status_load,
        }  # type: Dict[str, Any]
        if snapshot:
            result['snapshot_id'] = snapshot['id']
        return result
