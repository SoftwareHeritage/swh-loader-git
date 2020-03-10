# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import requests

from typing import Dict, Optional, Any, Mapping

from swh.model import hashutil

from swh.model.model import (
    Sha1Git, Revision, RevisionType
)

from swh.loader.package.utils import EMPTY_AUTHOR

from swh.loader.package.loader import PackageLoader


def retrieve_sources(url: str) -> Dict[str, Any]:
    response = requests.get(url,
                            allow_redirects=True)
    if response.status_code != 200:
        raise ValueError("Got %d HTTP code on %s",
                         response.status_code, url)

    return json.loads(response.content.decode('utf-8'))


class FunctionalLoader(PackageLoader):
    """Load sources from a sources.json file. This loader is used to load
    sources used by functional package manager (eg. Nix and Guix).

    """
    visit_type = 'functional'

    def __init__(self, url):
        super().__init__(url=url)
        s = retrieve_sources(url)
        self.sources = s['sources']
        self.provider_url = url
        # The revision used to create the sources.json file. For Nix,
        # this revision belongs to the github.com/nixos/nixpkgs
        # repository
        self.revision = s['revision']

    # Note: this could be renamed get_artifacts in the PackageLoader
    # base class.
    def get_versions(self):
        # TODO: try all mirrors and not only the first one. A source
        # can be fetched from several urls, called mirrors. We
        # currently only use the first one, but if the first one
        # fails, we should try the second one and so on.
        return [s['url'][0] for s in self.sources]

    # Note: this could be renamed get_artifact_info in the PackageLoader
    # base class.
    def get_package_info(self, source):
        # TODO: we need to provide the sha256 of the source also
        yield source, {'url': source, 'raw': {'url': source}}

    def resolve_revision_from(
            self, known_artifacts: Dict, artifact_metadata: Dict) \
            -> Optional[bytes]:
        for rev_id, known_artifact in known_artifacts.items():
            known_url = known_artifact['extrinsic']['raw']['url']
            if artifact_metadata['url'] == known_url:
                return rev_id
        return None

    def extra_branches(self) -> Dict[bytes, Mapping[str, Any]]:
        """We add a branch to the snapshot called 'evaluation' pointing to the
        revision used to generate the sources.json file. This revision
        is specified in the sources.json file itself. For the nixpkgs
        origin, this revision is coming from the
        github.com/nixos/nixpkgs repository.

        Note this repository is not loaded explicitly. So, this
        pointer can target a nonexistent revision for a time. However,
        the github and gnu loaders are supposed to load this revision
        and should create the revision pointed by this branch.

        This branch can be used to identify the snapshot associated to
        a Nix/Guix evaluation.

        """
        return {
            b'evaluation': {
                'target_type': 'revision',
                'target': hashutil.hash_to_bytes(self.revision)
            }
        }

    def build_revision(self, a_metadata: Dict, uncompressed_path: str,
                       directory: Sha1Git) -> Optional[Revision]:
        return Revision(
            type=RevisionType.TAR,
            message=b'',
            author=EMPTY_AUTHOR,
            date=None,
            committer=EMPTY_AUTHOR,
            committer_date=None,
            parents=[],
            directory=directory,
            synthetic=True,
            metadata={
                'extrinsic': {
                    'provider': self.provider_url,
                    'when': self.visit_date.isoformat(),
                    'raw': a_metadata,
                },
            }
        )
