# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import requests

try:
    from _version import __version__
except ImportError:
    __version__ = 'devel'


from tempfile import mkdtemp

from swh.core import tarball
from swh.loader.core.utils import clean_dangling_folders
from swh.loader.core.loader import BufferedLoader
from swh.model.identifiers import normalize_timestamp
from swh.model.hashutil import MultiHash, HASH_BLOCK_SIZE
from swh.model.from_disk import Directory

from swh.model.identifiers import (
    identifier_to_bytes, revision_identifier, snapshot_identifier
)

DEBUG_MODE = '** DEBUG MODE **'


class GNULoader(BufferedLoader):

    SWH_PERSON = {
        'name': b'Software Heritage',
        'fullname': b'Software Heritage',
        'email': b'robot@softwareheritage.org'
    }
    REVISION_MESSAGE = b'swh-loader-package: synthetic revision message'

    visit_type = 'gnu'

    def __init__(self):
        self.TEMPORARY_DIR_PREFIX_PATTERN = 'swh.loader.gnu.'
        super().__init__(logging_class='swh.loader.package.GNULoader')

        self.dir_path = None
        temp_directory = self.config['temp_directory']
        os.makedirs(temp_directory, exist_ok=True)

        self.temp_directory = mkdtemp(
            suffix='-%s' % os.getpid(),
            prefix=self.TEMPORARY_DIR_PREFIX_PATTERN,
            dir=temp_directory)

        self.debug = self.config.get('debug', False)
        self.session = requests.session()
        self.params = {
            'headers': {
                'User-Agent': 'Software Heritage Loader (%s)' % (
                    __version__
                )
            }
        }

    def pre_cleanup(self):
        """To prevent disk explosion if some other workers exploded
        in mid-air (OOM killed), we try and clean up dangling files.

        """
        if self.debug:
            self.log.warning('%s Will not pre-clean up temp dir %s' % (
                DEBUG_MODE, self.temp_directory
            ))
            return
        clean_dangling_folders(self.temp_directory,
                               pattern_check=self.TEMPORARY_DIR_PREFIX_PATTERN,
                               log=self.log)

    def prepare_origin_visit(self, name, origin_url, **kwargs):
        """Prepare package visit.

        Args:
            name (str): Package Name
            origin_url (str): Package origin url
            **kwargs: Arbitrary keyword arguments passed by the lister.

        """
        # reset statuses
        self._load_status = 'uneventful'
        self._visit_status = 'full'
        self.done = False

        self.origin = {
            'url': origin_url,
            'type': self.visit_type,
        }

        self.visit_date = None  # loader core will populate it

    def prepare(self, name, origin_url, **kwargs):
        """Prepare effective loading of source tarballs for a package manager
           package.

        Args:
            name (str): Package Name
            origin_url (str): Package origin url
            **kwargs: Arbitrary keyword arguments passed by the lister.

        """
        self.package_contents = []
        self.package_directories = []
        self.package_revisions = []
        self.all_version_data = []
        self.latest_timestamp = 0
        # Conceled the data into one dictionary to eleminate the need of
        # passing all the parameters when required in some method
        self.package_details = {
            'name': name,
            'origin_url': origin_url,
            'tarballs': kwargs['tarballs'],
        }

        self.package_temp_dir = os.path.join(self.temp_directory,
                                             self.package_details['name'])

        self.new_versions = \
            self.prepare_package_versions(self.package_details['tarballs'])

    def prepare_package_versions(self, tarballs):
        """
        Instantiate a generator that will process a specific package release
        version at each iteration step. The following operations will be
        performed:

            1. Create a temporary directory to download and extract the
               release tarball.
            2. Download the tarball.
            3. Uncompress the tarball.
            4. Parse the file associated to the package version to extract
               metadata (optional).
            5. Delete unnecessary files (optional).

        Args:
            tarballs (list): a list of dicts containing information about the
                respective tarball that is provided by lister.
            known_versions (dict): may be provided by the loader, it enables
                to filter out versions already ingested in the archive.

        Yields:
            Tuple[dict, str]: tuples containing the following
            members:

                * a dict holding package tarball information and metadata
                * a string holding the path of the uncompressed package to
                  load into the archive

        """
        for package_version_data in tarballs:

            tarball_url = package_version_data['archive']
            tarball_request = self._request(tarball_url,
                                            throw_error=False)
            if tarball_request.status_code == 404:
                self.log.warning('Tarball url %s returns a 404 error.',
                                 tarball_url)
                self._visit_status = 'partial'
                # FIX ME: Do we need to mark it `partial` here
                continue

            yield self._prepare_package_version(package_version_data,
                                                tarball_request)

    def _request(self, url, throw_error=True):
        """Request the remote tarball url.

        Args:
            url (str): Url (file or http*).

        Raises:
            ValueError in case of failing to query.

        Returns:
            Tuple of local (filepath, hashes of filepath).

        """
        response = self.session.get(url, **self.params, stream=True)
        if response.status_code != 200 and throw_error:
            raise ValueError("Fail to query '%s'. Reason: %s" % (
                url, response.status_code))

        return response

    def _prepare_package_version(self, package_version_data, tarball_request):
        """Process the package release version.

        The following operations are performed:

            1. Download the tarball
            2. Uncompress the tarball
            3. Delete unnecessary files (optional)
            4. Parse the file associated to the package version to extract
               metadata (optional)

        Args:
            package_version_data (dict): containing information
                about the focused package version.
            known_versions (dict): may be provided by the loader, it enables
                to filter out versions already ingested in the archive.

        Return:
            Tuple[dict, str]: tuples containing the following
            members:

                * a dict holding package tarball information and metadata
                * a string holding the path of the uncompressed package to
                  load into the archive

        """
        url = package_version_data['archive']
        tarball_path, hashes = self.download_generate_hash(tarball_request,
                                                           url)
        uncompressed_path = os.path.join(self.package_temp_dir, 'uncompressed',
                                         os.path.basename(url))     # SEE ME
        self.uncompress_tarball(tarball_path, uncompressed_path)

        # remove tarball
        os.remove(tarball_path)

        if self.tarball_invalid:
            return None, None

        return package_version_data, uncompressed_path

    def download_generate_hash(self, response, url):
        """Store file in temp directory and computes hash of its filepath.

        Args:
            response (Response): Server response of the url
            url (str): Url of the tarball

        Returns:
            Tuple of local (filepath, hashes of filepath)

        """
        length = int(response.headers['content-length'])
        os.makedirs(self.package_temp_dir, exist_ok=True)
        # SEE ME
        filepath = os.path.join(self.package_temp_dir, os.path.basename(url))

        # Convert the server response to a file.
        h = MultiHash(length=length)
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=HASH_BLOCK_SIZE):
                h.update(chunk)
                f.write(chunk)

        # Check for the validity of the tarball downloaded.
        actual_length = os.path.getsize(filepath)
        if length != actual_length:
            raise ValueError('Error when checking size: %s != %s' % (
                length, actual_length))

        hashes = {
            'length': length,
            **h.hexdigest()
        }
        return filepath, hashes

    def uncompress_tarball(self, filepath, path):
        """Uncompress a tarball.

        Args:
            filepath (str): Path of tarball to uncompress
            path (str): The destination folder where to uncompress the tarball
        Returns:
            The nature of the tarball, zip or tar.

        """
        try:
            self.tarball_invalid = False
            tarball.uncompress(filepath, path)
        except Exception:
            self.tarball_invalid = True
            self._visit_status = 'partial'

    def fetch_data(self):
        """Called once per release artifact version (can be many for one
           release).

        This will for each call:
        - retrieve a release artifact (associated to a release version)
        - Computes the swh objects

        Returns:
            True as long as data to fetch exist

        """
        data = None
        if self.done:
            return False

        try:
            data = next(self.new_versions)
            self._load_status = 'eventful'
        except StopIteration:
            self.done = True
            return False

        package_version_data, dir_path = data

        #  package release tarball was corrupted
        if self.tarball_invalid:
            return not self.done

        dir_path = dir_path.encode('utf-8')
        directory = Directory.from_disk(path=dir_path, data=True)
        objects = directory.collect()

        if 'content' not in objects:
            objects['content'] = {}
        if 'directory' not in objects:
            objects['directory'] = {}

        self.package_contents = objects['content'].values()
        self.package_directories = objects['directory'].values()

        revision = self.build_revision(directory,
                                       package_version_data)

        revision['id'] = identifier_to_bytes(
            revision_identifier(revision))
        self.package_revisions.append(revision)
        self.log.debug(revision)
        package_version_data['id'] = revision['id']
        self.all_version_data.append(package_version_data)

        # To find the latest version
        if self.latest_timestamp < int(package_version_data['date']):
            self.latest_timestamp = int(package_version_data['date'])

        self.log.debug('Removing unpacked package files at %s', dir_path)
        shutil.rmtree(dir_path)

        return not self.done

    def build_revision(self, directory, package_version_data):
        normalize_date = normalize_timestamp(int(package_version_data['date']))
        return {
            'metadata': {
                'package': {
                    'date': package_version_data['date'],
                    'archive': package_version_data['archive'],
                    },
                },
            'date': normalize_date,
            'committer_date': normalize_date,
            'author': self.SWH_PERSON,
            'committer': self.SWH_PERSON,
            'type': 'tar',
            'message': self.REVISION_MESSAGE,
            'directory': directory.hash,
            'synthetic': True,
            'parents': [],
            }

    def store_data(self):
        """Store fetched data in the database.

        """
        self.maybe_load_contents(self.package_contents)
        self.maybe_load_directories(self.package_directories)
        self.maybe_load_revisions(self.package_revisions)

        if self.done:
            self.generate_and_load_snapshot()
            self.flush()

    def generate_and_load_snapshot(self):
        """Generate and load snapshot for the package visit.

        """
        branches = {}
        for version_data in self.all_version_data:
            branch_name = self.find_branch_name(version_data['archive'])

            target = self.target_from_version(version_data['id'])
            branches[branch_name] = target
            branches = self.find_head(branches, branch_name,
                                      version_data['date'])

            if not target:
                self._visit_status = 'partial'

        snapshot = {
            'branches': branches,
        }

        snapshot['id'] = identifier_to_bytes(snapshot_identifier(snapshot))
        self.maybe_load_snapshot(snapshot)

    def find_branch_name(self, url):
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
        filename = os.path.basename(url)
        filename_parts = filename.split(".")
        if len(filename_parts) > 1 and filename_parts[-2] == 'tar':
            for part in filename_parts[:-2]:
                branch_name += '.' + part
        elif len(filename_parts) > 1 and filename_parts[-1] == 'zip':
            for part in filename_parts[:-1]:
                branch_name += '.' + part

        return (('release/%s') % branch_name[1:]).encode('ascii')

    def find_head(self, branches, branch_name, timestamp):
        """Make branch head.

        Checks if the current version is the latest version. Make it as head
        if it is the latest version.

        Args:
            branches (dict): Branches for the focused package.
            branch_name (str): Branch name

        Returns:
            dict: Branches for the focused package

        """
        if self.latest_timestamp == int(timestamp):
            branches[b'HEAD'] = {
                'target_type': 'alias',
                'target': branch_name,
            }
        return branches

    def target_from_version(self, revision_id):
        return {
            'target': revision_id,
            'target_type': 'revision',
        } if revision_id else None

    def load_status(self):
        return {
            'status': self._load_status,
        }

    def visit_status(self):
        return self._visit_status

    def cleanup(self):
        """Clean up temporary disk use after downloading and extracting
        package tarballs.

        """
        if self.debug:
            self.log.warning('%s Will not clean up temp dir %s' % (
                DEBUG_MODE, self.temp_directory
            ))
            return
        if os.path.exists(self.temp_directory):
            self.log.debug('Clean up %s' % self.temp_directory)
            shutil.rmtree(self.temp_directory)
