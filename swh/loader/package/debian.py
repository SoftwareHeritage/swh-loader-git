# Copyright (C) 2017-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import email.utils
import logging
import re
import subprocess

from dateutil.parser import parse as parse_date
from debian.changelog import Changelog
from debian.deb822 import Dsc
from os import path
from typing import Any, Dict, Generator, Mapping, Optional, Sequence, Tuple

from swh.loader.package.loader import PackageLoader
from swh.loader.package.utils import download


logger = logging.getLogger(__name__)
UPLOADERS_SPLIT = re.compile(r'(?<=\>)\s*,\s*')


class DebianLoader(PackageLoader):
    """Load debian origins into swh archive.

    """
    visit_type = 'debian'

    def __init__(self, url: str, date: str, packages: Mapping[str, Any]):
        super().__init__(url=url)
        self.packages = packages

    def get_versions(self) -> Sequence[str]:
        """Returns the keys of the packages input (e.g.
           stretch/contrib/0.7.2-3, etc...)

        """
        return self.packages.keys()

    def get_default_release(self) -> str:
        """Take the first version as default release

        """
        return list(self.packages.keys())[0]

    def get_artifacts(self, version: str) -> Generator[
            Tuple[Mapping[str, Any], Dict], None, None]:
        a_metadata = self.packages[version]
        artifacts_package_info = a_metadata.copy()
        artifacts_package_info['filename'] = version
        yield artifacts_package_info, a_metadata

    def resolve_revision_from(
            self, known_artifacts: Dict, artifact_metadata: Dict) \
            -> Optional[bytes]:
        pass  # for now

    def download_package(self, a_p_info: str, tmpdir: str) -> Tuple[str, Dict]:
        """Contrary to other package loaders (1 package, 1 artifact),
        `a_metadata` represents the package's datafiles set to fetch:
        - <package-version>.orig.tar.gz
        - <package-version>.dsc
        - <package-version>.diff.gz

        This is delegated to the `download_package` function.

        """
        logger.debug('debian: artifactS_package_info: %s', a_p_info)
        return tmpdir, download_package(a_p_info, tmpdir)

    def uncompress(self, a_path: str, tmpdir: str, a_metadata: Dict) -> str:
        return extract_package(a_metadata, tmpdir)

    def read_intrinsic_metadata(self, a_metadata: Dict,
                                a_uncompressed_path: str) -> Dict:
        _, dsc_name = dsc_information(a_metadata)
        dsc_path = path.join(path.dirname(a_uncompressed_path), dsc_name)
        return get_package_metadata(
            a_metadata, dsc_path, a_uncompressed_path)

    def build_revision(
            self, a_metadata: Dict, i_metadata: Dict) -> Dict:
        dsc_url, _ = dsc_information(a_metadata)
        logger.debug('i_metadata: %s', i_metadata)
        logger.debug('a_metadata: %s', a_metadata)

        msg = 'Synthetic revision for Debian source package %s version %s' % (
            a_metadata['name'], a_metadata['version'])

        date = i_metadata['changelog']['date']
        author = prepare_person(i_metadata['changelog']['person'])

        # inspired from swh.loader.debian.converters.package_metadata_to_revision  # noqa
        return {
            'type': 'dsc',
            'message': msg.encode('utf-8'),
            'author': author,
            'date': date,
            'committer': author,
            'committer_date': date,
            'parents': [],
            'metadata': {
                'intrinsic': {
                    'tool': 'dsc',
                    'raw': i_metadata,
                },
                'extrinsic': {
                    'provider': dsc_url,
                    'when': self.visit_date.isoformat(),
                    'raw': a_metadata,
                },
            }
        }


def uid_to_person(uid: str) -> Mapping[str, str]:
    """Convert an uid to a person suitable for insertion.

    Args:
        uid: an uid of the form "Name <email@ddress>"

    Returns:
        a dictionary with the following keys:

        - name: the name associated to the uid
        - email: the mail associated to the uid
        - fullname: the actual uid input

    """
    logger.debug('uid: %s', uid)
    ret = {
        'name': '',
        'email': '',
        'fullname': uid,
    }

    name, mail = email.utils.parseaddr(uid)
    if name and email:
        ret['name'] = name
        ret['email'] = mail
    else:
        ret['name'] = uid
    return ret


def prepare_person(person: Mapping[str, str]) -> Mapping[str, bytes]:
    """Prepare person for swh serialization...

    Args:
        A person dict

    Returns:
        A person dict ready for storage

    """
    ret = {}
    for key, value in person.items():
        ret[key] = value.encode('utf-8')
    return ret


def download_package(
        package: Mapping[str, Any], tmpdir: Any) -> Mapping[str, Any]:
    """Fetch a source package in a temporary directory and check the checksums
    for all files.

    Args:
        package: Dict defining the set of files representing a debian package
        tmpdir: Where to download and extract the files to ingest

    Returns:
        Dict of swh hashes per filename key

    """
    all_hashes = {}
    for filename, fileinfo in package['files'].items():
        uri = fileinfo['uri']
        logger.debug('fileinfo: %s', fileinfo)
        extrinsic_hashes = {'sha256': fileinfo['sha256']}
        logger.debug('extrinsic_hashes(%s): %s', filename, extrinsic_hashes)
        filepath, hashes = download(uri, dest=tmpdir, filename=filename,
                                    hashes=extrinsic_hashes)
        all_hashes[filename] = hashes

    logger.debug('all_hashes: %s', all_hashes)
    return all_hashes


def dsc_information(package: Mapping[str, Any]) -> Tuple[str, str]:
    """Retrieve dsc information from a package.

    Args:
        package: Package metadata information

    Returns:
        Tuple of dsc file's uri, dsc's full disk path

    """
    dsc_name = None
    dsc_url = None
    for filename, fileinfo in package['files'].items():
        if filename.endswith('.dsc'):
            if dsc_name:
                raise ValueError(
                    'Package %s_%s references several dsc files' %
                    (package['name'], package['version'])
                )
            dsc_url = fileinfo['uri']
            dsc_name = filename

    return dsc_url, dsc_name


def extract_package(package: Mapping[str, Any], tmpdir: str) -> str:
    """Extract a Debian source package to a given directory.

    Note that after extraction the target directory will be the root of the
    extracted package, rather than containing it.

    Args:
        package (dict): package information dictionary
        tmpdir (str): directory where the package files are stored

    Returns:
        Package extraction directory

    """
    _, dsc_name = dsc_information(package)
    dsc_path = path.join(tmpdir, dsc_name)
    destdir = path.join(tmpdir, 'extracted')
    logfile = path.join(tmpdir, 'extract.log')
    logger.debug('extract Debian source package %s in %s' %
                 (dsc_path, destdir), extra={
                     'swh_type': 'deb_extract',
                     'swh_dsc': dsc_path,
                     'swh_destdir': destdir,
                 })

    cmd = ['dpkg-source',
           '--no-copy', '--no-check',
           '--ignore-bad-version',
           '-x', dsc_path,
           destdir]

    try:
        with open(logfile, 'w') as stdout:
            subprocess.check_call(cmd, stdout=stdout, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        logdata = open(logfile, 'r').read()
        raise ValueError('dpkg-source exited with code %s: %s' %
                         (e.returncode, logdata)) from None

    return destdir


def get_package_metadata(package: Mapping[str, Any], dsc_path: str,
                         extracted_path: str) -> Mapping[str, Any]:
    """Get the package metadata from the source package at dsc_path,
    extracted in extracted_path.

    Args:
        package: the package dict (with a dsc_path key)
        dsc_path: path to the package's dsc file
        extracted_path: the path where the package got extracted

    Returns:
        dict: a dictionary with the following keys:

        - history: list of (package_name, package_version) tuples parsed from
          the package changelog

    """
    with open(dsc_path, 'rb') as dsc:
        parsed_dsc = Dsc(dsc)

    # Parse the changelog to retrieve the rest of the package information
    changelog_path = path.join(extracted_path, 'debian/changelog')
    with open(changelog_path, 'rb') as changelog:
        try:
            parsed_changelog = Changelog(changelog)
        except UnicodeDecodeError:
            logger.warning('Unknown encoding for changelog %s,'
                           ' falling back to iso' %
                           changelog_path.decode('utf-8'), extra={
                               'swh_type': 'deb_changelog_encoding',
                               'swh_name': package['name'],
                               'swh_version': str(package['version']),
                               'swh_changelog': changelog_path.decode('utf-8'),
                           })

            # need to reset as Changelog scrolls to the end of the file
            changelog.seek(0)
            parsed_changelog = Changelog(changelog, encoding='iso-8859-15')

    package_info = {
        'name': package['name'],
        'version': str(package['version']),
        'changelog': {
            'person': uid_to_person(parsed_changelog.author),
            'date': parse_date(parsed_changelog.date),
            'history': [(block.package, str(block.version))
                        for block in parsed_changelog][1:],
        }
    }

    maintainers = [
        uid_to_person(parsed_dsc['Maintainer']),
    ]
    maintainers.extend(
        uid_to_person(person)
        for person in UPLOADERS_SPLIT.split(parsed_dsc.get('Uploaders', ''))
    )
    package_info['maintainers'] = maintainers

    return package_info
