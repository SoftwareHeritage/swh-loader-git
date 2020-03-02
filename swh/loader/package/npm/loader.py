# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
import os

from codecs import BOM_UTF8
from typing import Any, Dict, Generator, Mapping, Sequence, Tuple, Optional

import attr
import chardet

from urllib.parse import quote
from swh.model.model import (
    Person, RevisionType, Revision, TimestampWithTimezone, Sha1Git,
)

from swh.loader.package.loader import PackageLoader
from swh.loader.package.utils import (
    api_info, release_name
)


logger = logging.getLogger(__name__)


class NpmLoader(PackageLoader):
    """Load npm origin's artifact releases into swh archive.

    """
    visit_type = 'npm'

    def __init__(self, url: str):
        """Constructor

        Args
            str: origin url (e.g. https://www.npmjs.com/package/<package-name>)
        """
        super().__init__(url=url)
        package_name = url.split('https://www.npmjs.com/package/')[1]
        safe_name = quote(package_name, safe='')
        self.provider_url = f'https://replicate.npmjs.com/{safe_name}/'
        self._info: Dict[str, Any] = {}
        self._versions = None

    @property
    def info(self) -> Dict[str, Any]:
        """Return the project metadata information (fetched from npm registry)

        """
        if not self._info:
            self._info = api_info(self.provider_url)
        return self._info

    def get_versions(self) -> Sequence[str]:
        return sorted(list(self.info['versions'].keys()))

    def get_default_version(self) -> str:
        return self.info['dist-tags'].get('latest', '')

    def get_package_info(self, version: str) -> Generator[
            Tuple[str, Mapping[str, Any]], None, None]:
        meta = self.info['versions'][version]
        url = meta['dist']['tarball']
        p_info = {
            'url': url,
            'filename': os.path.basename(url),
            'raw': meta,
        }
        yield release_name(version), p_info

    def resolve_revision_from(
            self, known_artifacts: Dict, artifact_metadata: Dict) \
            -> Optional[bytes]:
        return artifact_to_revision_id(known_artifacts, artifact_metadata)

    def build_revision(
            self, a_metadata: Dict, uncompressed_path: str,
            directory: Sha1Git) -> Optional[Revision]:
        i_metadata = extract_intrinsic_metadata(uncompressed_path)
        if not i_metadata:
            return None
        # from intrinsic metadata
        author = extract_npm_package_author(i_metadata)
        message = i_metadata['version'].encode('ascii')

        # from extrinsic metadata

        # No date available in intrinsic metadata: retrieve it from the API
        # metadata, using the version number that the API claims this package
        # has.
        extrinsic_version = a_metadata['version']

        if 'time' in self.info:
            date = self.info['time'][extrinsic_version]
        elif 'mtime' in a_metadata:
            date = a_metadata['mtime']
        else:
            artifact_name = os.path.basename(a_metadata['dist']['tarball'])
            raise ValueError(
                'Origin %s: Cannot determine upload time for artifact %s.' %
                (self.url, artifact_name)
            )

        date = TimestampWithTimezone.from_iso8601(date)

        # FIXME: this is to remain bug-compatible with earlier versions:
        date = attr.evolve(date, timestamp=attr.evolve(
            date.timestamp, microseconds=0))

        r = Revision(
            type=RevisionType.TAR,
            message=message,
            author=author,
            date=date,
            committer=author,
            committer_date=date,
            parents=[],
            directory=directory,
            synthetic=True,
            metadata={
                'intrinsic': {
                    'tool': 'package.json',
                    'raw': i_metadata,
                },
                'extrinsic': {
                    'provider': self.provider_url,
                    'when': self.visit_date.isoformat(),
                    'raw': a_metadata,
                },
            },
        )
        return r


def artifact_to_revision_id(
        known_artifacts: Dict, artifact_metadata: Dict) -> Optional[bytes]:
    """Given metadata artifact, solves the associated revision id.

    The following code allows to deal with 2 metadata formats:

    - old format sample::

        {
            'package_source': {
                'sha1': '05181c12cd8c22035dd31155656826b85745da37',
            }
        }

    - new format sample::

        {
            'original_artifact': [{
                'checksums': {
                    'sha256': '6975816f2c5ad4046acc676ba112f2fff945b01522d63948531f11f11e0892ec', # noqa
                    ...
                },
            }],
            ...
        }

    """
    shasum = artifact_metadata['dist']['shasum']
    for rev_id, known_artifact in known_artifacts.items():
        known_original_artifact = known_artifact.get('original_artifact')
        if not known_original_artifact:
            # previous loader-npm version kept original artifact elsewhere
            known_original_artifact = known_artifact.get('package_source')
            if not known_original_artifact:
                continue
            original_hash = known_original_artifact['sha1']
        else:
            assert isinstance(known_original_artifact, list)
            original_hash = known_original_artifact[0]['checksums']['sha1']
        if shasum == original_hash:
            return rev_id
    return None


def extract_npm_package_author(package_json) -> Person:
    """
    Extract package author from a ``package.json`` file content and
    return it in swh format.

    Args:
        package_json (dict): Dict holding the content of parsed
            ``package.json`` file

    Returns:
        Person

    """

    def _author_str(author_data):
        if type(author_data) is dict:
            author_str = ''
            if 'name' in author_data:
                author_str += author_data['name']
            if 'email' in author_data:
                author_str += ' <%s>' % author_data['email']
            return author_str
        elif type(author_data) is list:
            return _author_str(author_data[0]) if len(author_data) > 0 else ''
        else:
            return author_data

    for author_key in ('author', 'authors'):
        if author_key in package_json:
            author_str = _author_str(package_json[author_key])
            return Person.from_fullname(author_str.encode())

    return Person(fullname=b'', name=None, email=None)


def _lstrip_bom(s, bom=BOM_UTF8):
    if s.startswith(bom):
        return s[len(bom):]
    else:
        return s


def load_json(json_bytes):
    """
    Try to load JSON from bytes and return a dictionary.

    First try to decode from utf-8. If the decoding failed,
    try to detect the encoding and decode again with replace
    error handling.

    If JSON is malformed, an empty dictionary will be returned.

    Args:
        json_bytes (bytes): binary content of a JSON file

    Returns:
        dict: JSON data loaded in a dictionary
    """
    json_data = {}
    try:
        json_str = _lstrip_bom(json_bytes).decode('utf-8')
    except UnicodeDecodeError:
        encoding = chardet.detect(json_bytes)['encoding']
        if encoding:
            json_str = json_bytes.decode(encoding, 'replace')
    try:
        json_data = json.loads(json_str)
    except json.decoder.JSONDecodeError:
        pass
    return json_data


def extract_intrinsic_metadata(dir_path: str) -> Dict:
    """Given an uncompressed path holding the pkginfo file, returns a
       pkginfo parsed structure as a dict.

       The release artifact contains at their root one folder. For example:
       $ tar tvf zprint-0.0.6.tar.gz
       drwxr-xr-x root/root         0 2018-08-22 11:01 zprint-0.0.6/
       ...

    Args:

        dir_path (str): Path to the uncompressed directory
                        representing a release artifact from npm.

    Returns:
        the pkginfo parsed structure as a dict if any or None if
        none was present.

    """
    # Retrieve the root folder of the archive
    if not os.path.exists(dir_path):
        return {}
    lst = os.listdir(dir_path)
    if len(lst) == 0:
        return {}
    project_dirname = lst[0]
    package_json_path = os.path.join(dir_path, project_dirname, 'package.json')
    if not os.path.exists(package_json_path):
        return {}
    with open(package_json_path, 'rb') as package_json_file:
        package_json_bytes = package_json_file.read()
        return load_json(package_json_bytes)
