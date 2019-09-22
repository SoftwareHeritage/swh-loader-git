# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

from typing import Generator, Dict, Tuple, Sequence
from urllib.parse import urlparse
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


def pypi_api_url(url: str) -> str:
    """Compute api url from a project url

    Args:
        url (str): PyPI instance's url (e.g: https://pypi.org/project/requests)
        This deals with correctly transforming the project's api url (e.g
        https://pypi.org/pypi/requests/json)

    Returns:
        api url

    """
    p_url = urlparse(url)
    project_name = p_url.path.split('/')[-1]
    url = '%s://%s/pypi/%s/json' % (p_url.scheme, p_url.netloc, project_name)
    return url


def pypi_info(url: str) -> Dict:
    """PyPI api client to retrieve information on project. This deals with
       fetching json metadata about pypi projects.

    Args:
        url (str): PyPI instance's url (e.g: https://pypi.org/project/requests)
        This deals with correctly transforming the project's api url (e.g
        https://pypi.org/pypi/requests/json)

    Raises:
        ValueError in case of query failures (for some reasons: 404, ...)

    Returns:
        PyPI's information dict

    """
    api_url = pypi_api_url(url)
    response = requests.get(api_url, **DEFAULT_PARAMS)
    if response.status_code != 200:
        raise ValueError("Fail to query '%s'. Reason: %s" % (
            api_url, response.status_code))
    return response.json()


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


def sdist_parse(dir_path: str) -> Dict:
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
    if not os.path.exists(dir_path):
        return None
    lst = os.listdir(dir_path)
    if len(lst) == 0:
        return None
    project_dirname = lst[0]
    pkginfo_path = os.path.join(dir_path, project_dirname, 'PKG-INFO')
    if not os.path.exists(pkginfo_path):
        return None
    pkginfo = UnpackedSDist(pkginfo_path)
    raw = pkginfo.__dict__
    raw.pop('filename')  # this gets added with the ondisk location
    return raw


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
        super().__init__(url=url)
        self._info = None

    @property
    def info(self) -> Dict:
        """Return the project metadata information (fetched from pypi registry)

        """
        if not self._info:
            self._info = pypi_info(self.url)
        return self._info

    def get_versions(self) -> Sequence[str]:
        return self.info['releases'].keys()

    def get_default_release(self) -> str:
        return self.info['info']['version']

    def get_artifacts(self, version: str) -> Generator[
            Tuple[str, str, Dict], None, None]:
        for meta in self.info['releases'][version]:
            yield meta['filename'], meta['url'], meta

    def fetch_artifact_archive(
            self, artifact_uri: str, dest: str) -> Tuple[str, Dict]:
        return download(artifact_uri, dest=dest)

    def build_revision(
            self, a_metadata: Dict, a_uncompressed_path: str) -> Dict:
        # Parse metadata (project, artifact metadata)
        metadata = sdist_parse(a_uncompressed_path)

        # from intrinsic metadata
        name = metadata['version']
        _author = author(metadata)

        # from extrinsic metadata
        message = a_metadata.get('comment_text', '')
        message = '%s: %s' % (name, message) if message else name
        date = normalize_timestamp(
            int(iso8601.parse_date(a_metadata['upload_time']).timestamp()))

        return {
            'message': message.encode('utf-8'),
            'author': _author,
            'date': date,
            'committer': _author,
            'committer_date': date,
            'parents': [],
            'metadata': {
                'intrinsic_metadata': metadata,
            }
        }
