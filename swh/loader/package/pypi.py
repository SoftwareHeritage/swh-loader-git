# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.loader.package.loader import PackageLoader
from urllib.parse import urljoin, urlparse


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
        self.params = {
            'headers': {
                'User-Agent': 'Software Heritage PyPI Loader (%s)' % (
                    __version__
                )
            }
        }

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


class SDist():
    """In charge of fetching release artifacts, uncompressing it in temporary
       location, and parsing PKGINFO files.

    """

    def __init__(self, url):
        """

        Args:
            url (str): artifact "release" URI (either local or remote)

        """
        pass


class PyPILoader(PackageLoader):
      """Load pypi origin's artifact releases into swh archive.

    """
    visit_type = 'pypi'

    def __init__(self, url):
        super().__init__(url=url, visit_type='pypi')
        self.client = PyPIClient(url)
        self.sdist = SDist()

    def get_versions(self):
        """Return the list of all published package versions.

        """
        return []

    def retrieve_artifacts(self, version):
        """Given a release version of a package, retrieve the associated
           artifact for such version.

        Args:
            version (str): Package version

        Returns:
            xxx

        """
        pass

    def fetch_and_uncompress_artifact_archive(self, artifact_archive_path):
        """Uncompress artifact archive to a temporary folder and returns its
           path.

        Args:
            artifact_archive_path (str): Path to artifact archive to uncompress

        Returns:
            the uncompressed artifact path (str)

        """
        pass

    def get_project_metadata(self, artifact):
        """Given an artifact dict, extract the relevant project metadata.
           Those will be set within the revision's metadata built

        Args:
            artifact (dict): A dict of metadata about a release artifact.

        Returns:
            dict of relevant project metadata (e.g, in pypi loader:
            {'project_info': {...}})

        """
        return {}

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
        pass
