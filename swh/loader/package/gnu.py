# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from typing import Any, Dict, Generator, Mapping, Optional, Sequence, Tuple

from swh.loader.package.loader import PackageLoader
from swh.loader.package.utils import release_name

from swh.model.identifiers import normalize_timestamp


logger = logging.getLogger(__name__)
SWH_PERSON = {
    'name': b'Software Heritage',
    'fullname': b'Software Heritage',
    'email': b'robot@softwareheritage.org'
}
REVISION_MESSAGE = b'swh-loader-package: synthetic revision message'


class GNULoader(PackageLoader):
    visit_type = 'tar'

    def __init__(self, url: str, artifacts: Sequence):
        """Loader constructor.

        For now, this is the lister's task output.

        Args:
            url: Origin url
            artifacts: List of dict with keys:

               **time**: last modification time
               **url**: the artifact url to retrieve
               **filename**: artifact's filename
               **version**: artifact's version
               **length**: artifact's size

        """
        super().__init__(url=url)
        self.artifacts = list(sorted(artifacts, key=lambda v: v['time']))

    def get_versions(self) -> Sequence[str]:
        versions = []
        for archive in self.artifacts:
            v = archive.get('version')
            if v:
                versions.append(v)
        return versions

    def get_default_version(self) -> str:
        # It's the most recent, so for this loader, it's the last one
        return self.artifacts[-1]['version']

    def get_package_info(self, version: str) -> Generator[
            Tuple[str, Mapping[str, Any]], None, None]:
        for a_metadata in self.artifacts:
            url = a_metadata['archive']
            package_version = get_version(url)
            if version == package_version:
                p_info = {
                    'url': url,
                    'filename': path.split(url)[-1],
                    'raw': a_metadata,
                }
                # FIXME: this code assumes we have only 1 artifact per
                # versioned package
                yield release_name(version), p_info

    def resolve_revision_from(
            self, known_artifacts: Dict, artifact_metadata: Dict) \
            -> Optional[bytes]:
        def pk(d):
            return [d.get(k) for k in ['time', 'url', 'length', 'version']]

        artifact_pk = pk(artifact_metadata)
        for rev_id, known_artifact in known_artifacts.items():
            logging.debug('known_artifact: %s', known_artifact)
            known_pk = pk(known_artifact['extrinsic']['raw'])
            if artifact_pk == known_pk:
                return rev_id

    def build_revision(
            self, a_metadata: Mapping[str, Any],
            uncompressed_path: str) -> Dict:
        normalized_date = normalize_timestamp(int(a_metadata['time']))
        return {
            'type': 'tar',
            'message': REVISION_MESSAGE,
            'date': normalized_date,
            'author': SWH_PERSON,
            'committer': SWH_PERSON,
            'committer_date': normalized_date,
            'parents': [],
            'metadata': {
                'intrinsic': {},
                'extrinsic': {
                    'provider': self.url,
                    'when': self.visit_date.isoformat(),
                    'raw': a_metadata,
                },
            },
        }
