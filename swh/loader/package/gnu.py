# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re

from os import path

from typing import Generator, Dict, Tuple, Sequence

from swh.loader.package.loader import PackageLoader
from swh.loader.package.utils import download

from swh.model.identifiers import normalize_timestamp


# to recognize existing naming pattern
extensions = [
    'zip',
    'tar',
    'gz', 'tgz',
    'bz2', 'bzip2',
    'lzma', 'lz',
    'xz',
    'Z',
]

version_keywords = [
    'cygwin_me',
    'w32', 'win32', 'nt', 'cygwin', 'mingw',
    'latest', 'alpha', 'beta',
    'release', 'stable',
    'hppa',
    'solaris', 'sunos', 'sun4u', 'sparc', 'sun',
    'aix', 'ibm', 'rs6000',
    'i386', 'i686',
    'linux', 'redhat', 'linuxlibc',
    'mips',
    'powerpc', 'macos', 'apple', 'darwin', 'macosx', 'powermacintosh',
    'unknown',
    'netbsd', 'freebsd',
    'sgi', 'irix',
]

# Match a filename into components.
#
# We use Debian's release number heuristic: A release number starts
# with a digit, and is followed by alphanumeric characters or any of
# ., +, :, ~ and -
#
# We hardcode a list of possible extensions, as this release number
# scheme would match them too... We match on any combination of those.
#
# Greedy matching is done right to left (we only match the extension
# greedily with +, software_name and release_number are matched lazily
# with +? and *?).

pattern = r'''
^
(?:
    # We have a software name and a release number, separated with a
    # -, _ or dot.
    (?P<software_name1>.+?[-_.])
    (?P<release_number>(%(vkeywords)s|[0-9][0-9a-zA-Z_.+:~-]*?)+)
|
    # We couldn't match a release number, put everything in the
    # software name.
    (?P<software_name2>.+?)
)
(?P<extension>(?:\.(?:%(extensions)s))+)
$
''' % {
    'extensions': '|'.join(extensions),
    'vkeywords': '|'.join('%s[-]?' % k for k in version_keywords),
}


def get_version(url: str) -> str:
    """Extract branch name from tarball url

    Args:
        url (str): Tarball URL

    Returns:
        byte: Branch name

    Example:
        For url = https://ftp.gnu.org/gnu/8sync/8sync-0.2.0.tar.gz

        >>> get_version(url)
        '0.2.0'

    """
    filename = path.split(url)[-1]
    m = re.match(pattern, filename,
                 flags=re.VERBOSE | re.IGNORECASE)
    if m:
        d = m.groupdict()
        if d['software_name1'] and d['release_number']:
            return d['release_number']
        if d['software_name2']:
            return d['software_name2']


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
            v = get_version(archive['archive'])
            if v:
                yield v

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
