# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

from typing import Generator, Dict, Tuple, Sequence
from urllib.parse import urljoin, urlparse
from pkginfo import UnpackedSDist

import iso8601
import requests

from swh.model.identifiers import normalize_timestamp
from swh.model.hashutil import MultiHash, HASH_BLOCK_SIZE
from swh.loader.package.loader import PackageLoader

try:
    from swh.loader.core._version import __version__
except ImportError:
    __version__ = 'devel'


DEFAULT_PARAMS = {
    'headers': {
        'User-Agent': 'Software Heritage Loader (%s)' % (
            __version__
        )
    }
}


class PyPIClient:
    """PyPI client in charge of discussing with the pypi server.

    Args:
        url (str): PyPI instance's url (e.g: https://pypi.org/project/request)
        api:
        - https://pypi.org/pypi/requests/json
        - https://pypi.org/pypi/requests/1.0.0/json (release description)

    """
    def __init__(self, url):
        self.version = __version__
        _url = urlparse(url)
        project_name = _url.path.split('/')[-1]
        self.url = '%s://%s/pypi/%s' % (_url.scheme, _url.netloc, project_name)
        self.session = requests.session()
        self.params = DEFAULT_PARAMS

    def _get(self, url):
        """Get query to the url.

        Args:
            url (str): Url

        Raises:
            ValueError in case of failing to query

        Returns:
            Response as dict if ok

        """
        response = self.session.get(url, **self.params)
        if response.status_code != 200:
            raise ValueError("Fail to query '%s'. Reason: %s" % (
                url, response.status_code))

        return response.json()

    def info_project(self):
        """Given a url, retrieve the raw json response

        Returns:
            Main project information as dict.

        """
        return self._get(urljoin(self.url, 'json'))

    def info_release(self, release):
        """Given a project and a release name, retrieve the raw information
           for said project's release.

        Args:
            release (dict): Release information

        Returns:
            Release information as dict

        """
        return self._get(urljoin(self.url, release, 'json'))


def download(url: str, dest: str) -> Tuple[str, Dict]:
    """Download a remote tarball from url, uncompresses and computes swh hashes
       on it.

    Args:
        url: Artifact uri to fetch, uncompress and hash
        dest: Directory to write the archive to

    Raises:
        ValueError in case of any error when fetching/computing

    Returns:
        Tuple of local (filepath, hashes of filepath)

    """
    response = requests.get(url, **DEFAULT_PARAMS, stream=True)
    if response.status_code != 200:
        raise ValueError("Fail to query '%s'. Reason: %s" % (
            url, response.status_code))
    length = int(response.headers['content-length'])

    filepath = os.path.join(dest, os.path.basename(url))

    h = MultiHash(length=length)
    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=HASH_BLOCK_SIZE):
            h.update(chunk)
            f.write(chunk)

    actual_length = os.path.getsize(filepath)
    if length != actual_length:
        raise ValueError('Error when checking size: %s != %s' % (
            length, actual_length))

    # hashes = h.hexdigest()
    # actual_digest = hashes['sha256']
    # if actual_digest != artifact['sha256']:
    #     raise ValueError(
    #         '%s %s: Checksum mismatched: %s != %s' % (
    #             project, version, artifact['sha256'], actual_digest))

    return filepath, {
        'length': length,
        **h.hexdigest()
    }


def sdist_parse(dir_path):
    """Given an uncompressed path holding the pkginfo file, returns a
       pkginfo parsed structure as a dict.

       The release artifact contains at their root one folder. For example:
       $ tar tvf zprint-0.0.6.tar.gz
       drwxr-xr-x root/root         0 2018-08-22 11:01 zprint-0.0.6/
       ...

    Args:

        dir_path (str): Path to the uncompressed directory
                        representing a release artifact from pypi.

    Returns:
        the pkginfo parsed structure as a dict if any or None if
        none was present.

    """
    # Retrieve the root folder of the archive
    project_dirname = os.listdir(dir_path)[0]
    pkginfo_path = os.path.join(dir_path, project_dirname, 'PKG-INFO')
    if not os.path.exists(pkginfo_path):
        return None
    pkginfo = UnpackedSDist(pkginfo_path)
    return pkginfo.__dict__


def author(data: Dict) -> Dict:
    """Given a dict of project/release artifact information (coming from
       PyPI), returns an author subset.

    Args:
        data (dict): Representing either artifact information or
                     release information.

    Returns:
        swh-model dict representing a person.

    """
    name = data.get('author')
    email = data.get('author_email')

    if email:
        fullname = '%s <%s>' % (name, email)
    else:
        fullname = name

    if not fullname:
        return {'fullname': b'', 'name': None, 'email': None}

    fullname = fullname.encode('utf-8')

    if name is not None:
        name = name.encode('utf-8')

    if email is not None:
        email = email.encode('utf-8')

    return {'fullname': fullname, 'name': name, 'email': email}


class PyPILoader(PackageLoader):
    """Load pypi origin's artifact releases into swh archive.

    """
    visit_type = 'pypi'

    def __init__(self, url):
        super().__init__(url=url, visit_type='pypi')
        self.client = PyPIClient(url)
        self._info = None

    @property
    def info(self) -> Dict:
        """Return the project metadata information (fetched from pypi registry)

        """
        if not self._info:
            self._info = self.client.info_project()  # dict
        return self._info

    def get_versions(self) -> Sequence[str]:
        return self.info['releases'].keys()

    def get_artifacts(self, version: str) -> Generator[
            Tuple[str, str, Dict], None, None]:
        for meta in self.info['releases'][version]:
            yield meta['filename'], meta['url'], meta

    def fetch_artifact_archive(
            self, artifact_uri: str, dest: str) -> Tuple[str, Dict]:
        return download(artifact_uri, dest=dest)

    def build_revision(self, artifact_uncompressed_path: str) -> Dict:
        # Parse metadata (project, artifact metadata)
        metadata = sdist_parse(artifact_uncompressed_path)

        # Build revision
        name = metadata['version'].encode('utf-8')
        message = metadata['message'].encode('utf-8')
        message = b'%s: %s' % (name, message) if message else name

        _author = author(metadata)
        _date = normalize_timestamp(
            int(iso8601.parse_date(metadata['date']).timestamp()))
        return {
            'name': name,
            'message': message,
            'author': _author,
            'date': _date,
            'committer': _author,
            'committer_date': _date,
            'parents': [],
            'metadata': {
                'intrinsic_metadata': metadata,
            }
        }
