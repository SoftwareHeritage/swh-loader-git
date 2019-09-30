# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import logging
import tempfile
import os

from typing import Generator, Dict, Tuple, Sequence, List, Optional

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

    def get_artifacts(self, version: str) -> Generator[
            Tuple[str, str, Dict], None, None]:
        """Given a release version of a package, retrieve the associated
           artifact information for such version.

        Args:
            version: Package version

        Returns:
            (artifact filename, artifact uri, raw artifact metadata)

        """
        yield from {}

    def fetch_artifact_archive(
            self, artifact_uri: str, dest: str) -> Tuple[str, Dict]:
        """Fetch artifact archive to a temporary folder and returns its
           path.

        Args:
            artifact_uri: Artifact uri to fetch
            dest: Directory to write the downloaded archive to

        Returns:
            the locally retrieved artifact path

        """
        return download(artifact_uri, dest=dest)

    def build_revision(
            self, a_metadata: Dict, a_uncompressed_path: str) -> Dict:
        """Build the revision dict

        Returns:
            SWH data dict

        """
        return {}

    def get_default_release(self) -> str:
        """Retrieve the latest release version

        Returns:
            Latest version

        """
        return ''

    def last_snapshot(self) -> Optional[Dict]:
        """Retrieve the last snapshot

        """
        visit = self.storage.origin_visit_get_latest(
            self.url, require_snapshot=True)
        if visit:
            return snapshot_get_all_branches(
                self.storage, visit['snapshot']['id'])

    def known_artifacts(self, snapshot: Dict) -> [Dict]:
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
            original_artifact = revision['metadata'].get('original_artifact')
            if original_artifact:
                ret[revision['id']] = original_artifact

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
        tmp_revisions: Dict[str, List] = {}
        snapshot = None

        try:
            # Prepare origin and origin_visit
            origin = {'url': self.url}
            self.storage.origin_add([origin])
            visit_date = datetime.datetime.now(tz=datetime.timezone.utc)
            visit_id = self.storage.origin_visit_add(
                origin=self.url,
                date=visit_date,
                type=self.visit_type)['visit']
            last_snapshot = self.last_snapshot()
            logger.debug('last snapshot: %s', last_snapshot)
            known_artifacts = self.known_artifacts(last_snapshot)
            logger.debug('known artifacts: %s', known_artifacts)

            # Retrieve the default release (the "latest" one)
            default_release = self.get_default_release()
            logger.debug('default release: %s', default_release)

            for version in self.get_versions():  # for each
                logger.debug('version: %s', version)
                tmp_revisions[version] = []
                # `a_` stands for `artifact_`
                for a_filename, a_uri, a_metadata in self.get_artifacts(
                        version):
                    revision_id = self.resolve_revision_from(
                        known_artifacts, a_metadata)
                    if revision_id is None:
                        with tempfile.TemporaryDirectory() as tmpdir:
                            try:
                                # a_c_: archive_computed_
                                a_path, a_c_metadata = self.fetch_artifact_archive(  # noqa
                                    a_uri, dest=tmpdir)
                            except Exception as e:
                                logger.warning(
                                    'Unable to retrieve %s. Reason: %s',
                                    a_uri, e)
                                status_visit = 'partial'
                                continue

                            logger.debug('archive_path: %s', a_path)
                            logger.debug('archive_computed_metadata: %s',
                                         a_c_metadata)

                            uncompressed_path = os.path.join(tmpdir, 'src')
                            uncompress(a_path, dest=uncompressed_path)

                            logger.debug('uncompressed_path: %s',
                                         uncompressed_path)

                            directory = Directory.from_disk(
                                path=uncompressed_path.encode('utf-8'), data=True)  # noqa
                            # FIXME: Try not to load the full raw content in
                            # memory
                            objects = directory.collect()

                            contents = objects['content'].values()
                            logger.debug('Number of contents: %s',
                                         len(contents))

                            self.storage.content_add(
                                map(content_for_storage, contents))

                            status_load = 'eventful'
                            directories = objects['directory'].values()

                            logger.debug('Number of directories: %s',
                                         len(directories))

                            self.storage.directory_add(directories)

                            # FIXME: This should be release. cf. D409
                            revision = self.build_revision(
                                a_metadata, uncompressed_path)
                            revision.update({
                                'type': 'tar',
                                'synthetic': True,
                                'directory': directory.hash,
                            })

                        # FIXME: Standardize those metadata keys and use the
                        # correct ones
                        revision['metadata'].update({
                            'original_artifact': a_metadata,
                            'hashes_artifact': a_c_metadata
                        })

                        revision['id'] = revision_id = identifier_to_bytes(
                            revision_identifier(revision))

                        logger.debug('Revision: %s', revision)

                        self.storage.revision_add([revision])

                    tmp_revisions[version].append({
                        'filename': a_filename,
                        'target': revision_id,
                    })

            # Build and load the snapshot
            branches = {}
            for version, v_branches in tmp_revisions.items():
                if len(v_branches) == 1:
                    branch_name = ('releases/%s' % version).encode('utf-8')
                    if version == default_release:
                        branches[b'HEAD'] = {
                            'target_type': 'alias',
                            'target': branch_name,
                        }

                    branches[branch_name] = {
                        'target_type': 'revision',
                        'target': v_branches[0]['target'],
                    }
                else:
                    for x in v_branches:
                        branch_name = ('releases/%s/%s' % (
                            version, v_branches['filename'])).encode('utf-8')
                        branches[branch_name] = {
                            'target_type': 'revision',
                            'target': x['target'],
                        }

            snapshot = {
                'branches': branches
            }
            snapshot['id'] = identifier_to_bytes(
                snapshot_identifier(snapshot))

            logger.debug('snapshot: %s', snapshot)
            self.storage.snapshot_add([snapshot])
            if hasattr(self.storage, 'flush'):
                self.storage.flush()
        except Exception as e:
            logger.warning('Fail to load %s. Reason: %s' % (self.url, e))
            status_visit = 'partial'
        finally:
            self.storage.origin_visit_update(
                origin=self.url, visit_id=visit_id, status=status_visit,
                snapshot=snapshot)
            return {'status': status_load}
