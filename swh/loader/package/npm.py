# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
import os
import re

from codecs import BOM_UTF8
from typing import Generator, Dict, Tuple, Sequence, List

import chardet
import iso8601
import requests
import tempfile

from swh.model.identifiers import normalize_timestamp
from swh.loader.package.loader import PackageLoader
from swh.loader.package.utils import download


logger = logging.getLogger(__name__)


class NpmClient:
    """
    Helper class internally used by the npm loader to fetch
    metadata for a specific package hosted on the npm registry.

    Args:
        temp_dir (str): Path to the temporary disk location used
            to uncompress the package tarballs

    """
    def __init__(self, log=None):
        self.root_temp_dir = tempfile.mkdtemp()
        self.session = requests.session()
        self.params = {
            'headers': {
                'User-Agent': 'Software Heritage npm loader'
            }
        }
        self.log = log or logging

    def fetch_package_metadata(self, package_metadata_url) -> None:
        """
        Fetch metadata for a given package and make it the focused one.
        This must be called prior any other operations performed
        by the other methods below.

        Args:
            package_metadata_url: the package metadata url provided
                by the npm loader
        """
        self.package_metadata_url = package_metadata_url
        self.package_metadata = self.session.get(
            self.package_metadata_url).json()
        self.package = self.package_metadata['name']
        self.temp_dir = os.path.join(self.root_temp_dir, self.package)
        return self.package_metadata

    def package_versions(self, known_versions=None) -> List[Dict]:
        """
        Return the available versions for the focused package.

        Args:
            known_versions (dict): may be provided by the loader, it enables
                to filter out versions already ingested in the archive.

        Returns:
            dict: A dict whose keys are Tuple[version, tarball_sha1] and
            values dicts with the following entries:

                    * **name**: the package name
                    * **version**: the package version
                    * **filename**: the package source tarball filename
                    * **sha1**: the package source tarball sha1 checksum
                    * **date**: the package release date
                    * **url**: the package source tarball download url
        """
        versions = {}
        if 'versions' in self.package_metadata:
            for version, data in self.package_metadata['versions'].items():
                sha1 = data['dist']['shasum']
                key = (version, sha1)
                if known_versions and key in known_versions:
                    continue
                tarball_url = data['dist']['tarball']
                filename = os.path.basename(tarball_url)
                date = self.package_metadata['time'][version]
                versions[key] = {
                    'name': self.package,
                    'version': version,
                    'filename': filename,
                    'sha1': sha1,
                    'date': date,
                    'url': tarball_url
                }
        return versions


_EMPTY_AUTHOR = {'fullname': b'', 'name': None, 'email': None}

# https://github.com/jonschlinkert/author-regex
_author_regexp = r'([^<(]+?)?[ \t]*(?:<([^>(]+?)>)?[ \t]*(?:\(([^)]+?)\)|$)'


def parse_npm_package_author(author_str):
    """
    Parse npm package author string.

    It works with a flexible range of formats, as detailed below::

        name
        name <email> (url)
        name <email>(url)
        name<email> (url)
        name<email>(url)
        name (url) <email>
        name (url)<email>
        name(url) <email>
        name(url)<email>
        name (url)
        name(url)
        name <email>
        name<email>
        <email> (url)
        <email>(url)
        (url) <email>
        (url)<email>
        <email>
        (url)

    Args:
        author_str (str): input author string

    Returns:
        dict: A dict that may contain the following keys:
            * name
            * email
            * url

    """
    author = {}
    matches = re.findall(_author_regexp,
                         author_str.replace('<>', '').replace('()', ''),
                         re.M)
    for match in matches:
        if match[0].strip():
            author['name'] = match[0].strip()
        if match[1].strip():
            author['email'] = match[1].strip()
        if match[2].strip():
            author['url'] = match[2].strip()
    return author


def extract_npm_package_author(package_json):
    """
    Extract package author from a ``package.json`` file content and
    return it in swh format.

    Args:
        package_json (dict): Dict holding the content of parsed
            ``package.json`` file

    Returns:
        dict: A dict with the following keys:
            * fullname
            * name
            * email

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

    author_data = {}
    for author_key in ('author', 'authors'):
        if author_key in package_json:
            author_str = _author_str(package_json[author_key])
            author_data = parse_npm_package_author(author_str)

    name = author_data.get('name')
    email = author_data.get('email')

    fullname = None

    if name and email:
        fullname = '%s <%s>' % (name, email)
    elif name:
        fullname = name

    if not fullname:
        return _EMPTY_AUTHOR

    if fullname:
        fullname = fullname.encode('utf-8')

    if name:
        name = name.encode('utf-8')

    if email:
        email = email.encode('utf-8')

    return {'fullname': fullname, 'name': name, 'email': email}


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


class NpmLoader(PackageLoader):
    visit_type = 'npm'

    def __init__(self, package_name, package_url, package_metadata_url):
        super().__init__(url=package_url)
        self.package_metadata_url = package_metadata_url

        self._info = None
        self._versions = None
        self.client = NpmClient()

        # if package_url is None:
        #     package_url = 'https://www.npmjs.com/package/%s' % package_name
        # if package_metadata_url is None:
        #     package_metadata_url = 'https://replicate.npmjs.com/%s/' %\
        #                             quote(package_name, safe='')

    @property
    def info(self) -> Dict:
        """Return the project metadata information (fetched from npm registry)

        """
        if not self._info:
            # This initializes the metadata retrieval on npm api
            self._info = self.client.fetch_package_metadata(
                self.package_metadata_url)
        return self._info

    def get_versions(self) -> Sequence[str]:
        return sorted(self.info['versions'].keys())

    def get_default_release(self) -> str:
        return self.info['dist-tags'].get('latest', '')

    def get_artifacts(self, version: str) -> Generator[
            Tuple[str, str, Dict], None, None]:
        meta = self.info['versions'][version]
        url = meta['dist']['tarball']
        filename = os.path.basename(url)
        yield filename, url, meta

    def fetch_artifact_archive(
            self, artifact_uri: str, dest: str) -> Tuple[str, Dict]:
        return download(artifact_uri, dest=dest)

    def build_revision(
            self, a_metadata: Dict, a_uncompressed_path: str) -> Dict:
        # Parse metadata (project, artifact metadata)
        i_metadata = extract_intrinsic_metadata(a_uncompressed_path)

        # from intrinsic metadata
        author = extract_npm_package_author(i_metadata)
        # extrinsic metadata
        version = i_metadata['version']
        date = self.info['time'][version]
        date = iso8601.parse_date(date)
        date = normalize_timestamp(int(date.timestamp()))
        message = version.encode('ascii')

        return {
            'author': author,
            'date': date,
            'committer': author,
            'committer_date': date,
            'message': message,
            'metadata': {
                'intrinsic_metadata': i_metadata,
            },
            'parents': [],
        }
