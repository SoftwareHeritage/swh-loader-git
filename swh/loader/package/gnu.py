# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os import path

from typing import Generator, Dict, Tuple, Sequence

from swh.loader.package.loader import PackageLoader
from swh.loader.package.utils import download

from swh.model.identifiers import normalize_timestamp


def get_version(url):
    """Extract branch name from tarball url

    Args:
        url (str): Tarball URL

    Returns:
        byte: Branch name

    Example:
        For url = https://ftp.gnu.org/gnu/8sync/8sync-0.2.0.tar.gz

        >>> find_branch_name(url)
        b'release/8sync-0.2.0'

    """
    branch_name = ''
    filename = path.basename(url)
    filename_parts = filename.split(".")
    if len(filename_parts) > 1 and filename_parts[-2] == 'tar':
        for part in filename_parts[:-2]:
            branch_name += '.' + part
    elif len(filename_parts) > 1 and filename_parts[-1] == 'zip':
        for part in filename_parts[:-1]:
            branch_name += '.' + part

    return '%s' % branch_name[1:]


class GNULoader(PackageLoader):
    visit_type = 'gnu'
    SWH_PERSON = {
        'name': b'Software Heritage',
        'fullname': b'Software Heritage',
        'email': b'robot@softwareheritage.org'
    }
    REVISION_MESSAGE = b'swh-loader-package: synthetic revision message'

    def __init__(self, package: str, package_url: str, tarballs: Sequence):
        """Loader constructor.

        For now, this is the lister's task output.

        Args:
            package: Package's name (unused)
            package_url: Origin url

            tarballs: List of dict with keys `date` (date) and `archive` (str)
            the url to retrieve one versioned archive

        """
        super().__init__(url=package_url)
        # Sort tarballs by upload date
        sorted(tarballs, key=lambda v: int(v['date']))
        self.tarballs = tarballs

    def get_versions(self) -> Sequence[str]:
        for archive in self.tarballs:
            yield get_version(archive['archive'])

    def get_default_release(self) -> str:
        # It's the most recent, so for this loader, it's the last one
        return get_version(self.tarballs[-1]['archive'])

    def get_artifacts(self, version: str) -> Generator[
            Tuple[str, str, Dict], None, None]:
        for a_metadata in self.tarballs:
            url = a_metadata['archive']
            filename = path.split(url)[-1]
            yield filename, url, a_metadata

    def fetch_artifact_archive(
            self, artifact_uri: str, dest: str) -> Tuple[str, Dict]:
        return download(artifact_uri, dest=dest)

    def build_revision(
            self, a_metadata: Dict, a_uncompressed_path: str) -> Dict:

        normalized_date = normalize_timestamp(int(a_metadata['date']))
        return {
            'message': self.REVISION_MESSAGE,
            'date': normalized_date,
            'author': self.SWH_PERSON,
            'committer': self.SWH_PERSON,
            'committer_date': normalized_date,
            'parents': [],
            'metadata': {
                'package': {
                    'date': a_metadata['date'],
                    'archive': a_metadata['archive'],
                },
            },
        }
