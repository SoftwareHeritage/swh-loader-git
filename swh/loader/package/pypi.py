# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from urllib.parse import urljoin, urlparse
from pkginfo import UnpackedSDist

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


class ArchiveFetcher:
    """Http/Local client in charge of downloading archives from a
       remote/local server.

    Args:
        temp_directory (str): Path to the temporary disk location used
                              for downloading the release artifacts

    """
    def __init__(self, temp_directory=None):
        self.temp_directory = temp_directory
        self.session = requests.session()
        self.params = DEFAULT_PARAMS

    def download(self, url):
        """Download the remote tarball url locally.

        Args:
            url (str): Url (file or http*)

        Raises:
            ValueError in case of failing to query

        Returns:
            Tuple of local (filepath, hashes of filepath)

        """
        url_parsed = urlparse(url)
        if url_parsed.scheme == 'file':
            path = url_parsed.path
            response = LocalResponse(path)
            length = os.path.getsize(path)
        else:
            response = self.session.get(url, **self.params, stream=True)
            if response.status_code != 200:
                raise ValueError("Fail to query '%s'. Reason: %s" % (
                    url, response.status_code))
            length = int(response.headers['content-length'])

        filepath = os.path.join(self.temp_directory, os.path.basename(url))

        h = MultiHash(length=length)
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=HASH_BLOCK_SIZE):
                h.update(chunk)
                f.write(chunk)

        actual_length = os.path.getsize(filepath)
        if length != actual_length:
            raise ValueError('Error when checking size: %s != %s' % (
                length, actual_length))

        return {
            'path': filepath,
            'length': length,
            **h.hexdigest()
        }


def sdist_parse(path):
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
    def _to_dict(pkginfo):
        """Given a pkginfo parsed structure, convert it to a dict.

        Args:
            pkginfo (UnpackedSDist): The sdist parsed structure

        Returns:
            parsed structure as a dict

        """
        m = {}
        for k in pkginfo:
            m[k] = getattr(pkginfo, k)
        return m

    # Retrieve the root folder of the archive
    project_dirname = os.listdir(dir_path)[0]
    pkginfo_path = os.path.join(dir_path, project_dirname, 'PKG-INFO')
    if not os.path.exists(pkginfo_path):
        return None
    pkginfo = UnpackedSDist(pkginfo_path)
    return _to_dict(pkginfo)


class PyPILoader(PackageLoader):
    """Load pypi origin's artifact releases into swh archive.

    """
    visit_type = 'pypi'

    def __init__(self, url):
        super().__init__(url=url, visit_type='pypi')
        self.client = PyPIClient(url)
        self.archive_fetcher = ArchiveFetcher(
            temp_directory=os.mkdtemp())
        self.info = self.client.info_project() # dict
        self.artifact_metadata = {}

    def get_versions(self):
        """Return the list of all published package versions.

        """
        return self.info['releases'].keys()

    def retrieve_artifacts(self, version):
        """Given a release version of a package, retrieve the associated
           artifacts for such version.

        Args:
            version (str): Package version

        Returns:
            a list of metadata dict about the artifacts for that version.
            For each dict, the 'name' field is the uri to retrieve the
            artifacts

        """
        artifact_metadata = self.info['releases'][version]
        for meta in artifact_metadata:
            url = meta.pop('url')
            meta['uri'] = url
        return artifact_metadata

    def fetch_and_uncompress_artifact_archive(self, artifact_archive_path):
        """Uncompress artifact archive to a temporary folder and returns its
           path.

        Args:
            artifact_archive_path (str): Path to artifact archive to uncompress

        Returns:
            the uncompressed artifact path (str)

        """
        return self.sdist_parse(artifact_archive_path)

    def get_project_metadata(self, artifact):
        """Given an artifact dict, extract the relevant project metadata.
           Those will be set within the revision's metadata built

        Args:
            artifact (dict): A dict of metadata about a release artifact.

        Returns:
            dict of relevant project metadata (e.g, in pypi loader:
            {'project_info': {...}})

        """
        version = artifact['version']
        if version not in self.artifact_metadata:
            self.artifact_metadata[version] = self.client.info_release(version)
        return self.artifact_metadata[version]['info']

    def get_revision_metadata(self, artifact):
        """Given an artifact dict, extract the relevant revision metadata.
           Those will be set within the 'name' (bytes) and 'message' (bytes)
           built revision fields.

        Args:
            artifact (dict): A dict of metadata about a release artifact.

        Returns:
            dict of relevant revision metadata (name, message keys with values
            as bytes)

        """
        version = artifact['version']
        # fetch information
        if version not in self.artifact_metadata:
            self.artifact_metadata[version] = self.client.info_release(version)

        releases = self.artifact_metadata[version]['releases']
        for _artifact in releases[version]:
            if _artifact['url'] == artifact['uri']:
                break

        if not _artifact:  # should we?
            raise ValueError('Revision metadata not found for artifact %s' % artifact['uri'])

        return {
            'name': version,
            'message': _artifact.get('comment_text', '').encode('utf-8')
        }
