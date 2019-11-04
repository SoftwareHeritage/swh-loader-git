# Copyright (C) 2017-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import email.utils
import iso8601
import logging
import re
import subprocess

from dateutil.parser import parse as parse_date
from debian.changelog import Changelog
from debian.deb822 import Dsc
from os import path
from typing import (
    Any, Dict, Generator, List, Mapping, Optional, Sequence, Tuple
)

from swh.loader.package.loader import PackageLoader
from swh.loader.package.utils import download, release_name


logger = logging.getLogger(__name__)
UPLOADERS_SPLIT = re.compile(r'(?<=\>)\s*,\s*')


class DebianLoader(PackageLoader):
    """Load debian origins into swh archive.

    """
    visit_type = 'debian'

    def __init__(self, url: str, date: str, packages: Mapping[str, Any]):
        """Debian Loader implementation.

        Args:
            url: Origin url (e.g. deb://Debian/packages/cicero)
            date: Ignored
            packages: versioned packages and associated artifacts, example::

              {
                'stretch/contrib/0.7.2-3': {
                  'name': 'cicero',
                  'version': '0.7.2-3'
                  'files': {
                    'cicero_0.7.2-3.diff.gz': {
                       'md5sum': 'a93661b6a48db48d59ba7d26796fc9ce',
                       'name': 'cicero_0.7.2-3.diff.gz',
                       'sha256': 'f039c9642fe15c75bed5254315e2a29f...',
                       'size': 3964,
                       'uri': 'http://d.d.o/cicero_0.7.2-3.diff.gz',
                    },
                    'cicero_0.7.2-3.dsc': {
                      'md5sum': 'd5dac83eb9cfc9bb52a15eb618b4670a',
                      'name': 'cicero_0.7.2-3.dsc',
                      'sha256': '35b7f1048010c67adfd8d70e4961aefb...',
                      'size': 1864,
                      'uri': 'http://d.d.o/cicero_0.7.2-3.dsc',
                    },
                    'cicero_0.7.2.orig.tar.gz': {
                      'md5sum': '4353dede07c5728319ba7f5595a7230a',
                      'name': 'cicero_0.7.2.orig.tar.gz',
                      'sha256': '63f40f2436ea9f67b44e2d4bd669dbab...',
                      'size': 96527,
                      'uri': 'http://d.d.o/cicero_0.7.2.orig.tar.gz',
                    }
                  },
                },
                # ...
              }

        """
        super().__init__(url=url)
        self.packages = packages

    def get_versions(self) -> Sequence[str]:
        """Returns the keys of the packages input (e.g.
           stretch/contrib/0.7.2-3, etc...)

        """
        return list(self.packages.keys())

    def get_package_info(self, version: str) -> Generator[
            Tuple[str, Mapping[str, Any]], None, None]:
        meta = self.packages[version]
        p_info = meta.copy()
        p_info['raw'] = meta
        yield release_name(version), p_info

    def resolve_revision_from(
            self, known_package_artifacts: Dict, artifact_metadata: Dict) \
            -> Optional[bytes]:
        artifacts_to_fetch = artifact_metadata['files']
        logger.debug('k_p_artifacts: %s', known_package_artifacts)
        logger.debug('artifacts_to_fetch: %s', artifacts_to_fetch)
        for rev_id, known_artifacts in known_package_artifacts.items():
            logger.debug('Revision: %s', rev_id)
            logger.debug('Associated known_artifacts: %s', known_artifacts)
            known_artifacts = known_artifacts['extrinsic']['raw']['files']
            rev_found = True
            for a_name, k_artifact in known_artifacts.items():
                artifact_to_fetch = artifacts_to_fetch.get(a_name)
                logger.debug('artifact_to_fetch: %s', artifact_to_fetch)
                if artifact_to_fetch is None:
                    # as soon as we do not see an artifact, we consider we need
                    # to check the other revision
                    rev_found = False
                if k_artifact['sha256'] != artifact_to_fetch['sha256']:
                    # Hash is different, we consider we need to check the other
                    # revisions
                    rev_found = False
            if rev_found:
                logger.debug('Existing revision %s found for new artifacts.',
                             rev_id)
                return rev_id
        logger.debug('No existing revision found for the new artifacts.')
        return None

    def download_package(self, p_info: Mapping[str, Any],
                         tmpdir: str) -> List[Tuple[str, Mapping]]:
        """Contrary to other package loaders (1 package, 1 artifact),
        `a_metadata` represents the package's datafiles set to fetch:
        - <package-version>.orig.tar.gz
        - <package-version>.dsc
        - <package-version>.diff.gz

        This is delegated to the `download_package` function.

        """
        all_hashes = download_package(p_info, tmpdir)
        logger.debug('all_hashes: %s', all_hashes)
        res = []
        for hashes in all_hashes.values():
            res.append((tmpdir, hashes))
            logger.debug('res: %s', res)
        return res

    def uncompress(self, dl_artifacts: List[Tuple[str, Mapping[str, Any]]],
                   dest: str) -> str:
        logger.debug('dl_artifacts: %s', dl_artifacts)
        return extract_package(dl_artifacts, dest=dest)

    def build_revision(self, a_metadata: Mapping[str, Any],
                       uncompressed_path: str) -> Dict:
        dsc_url, dsc_name = dsc_information(a_metadata)
        if not dsc_name:
            raise ValueError(
                'dsc name for url %s should not be None' % dsc_url)
        dsc_path = path.join(path.dirname(uncompressed_path), dsc_name)
        i_metadata = get_package_metadata(
            a_metadata, dsc_path, uncompressed_path)

        logger.debug('i_metadata: %s', i_metadata)
        logger.debug('a_metadata: %s', a_metadata)

        msg = 'Synthetic revision for Debian source package %s version %s' % (
            a_metadata['name'], a_metadata['version'])

        date = iso8601.parse_date(i_metadata['changelog']['date'])
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


def dsc_information(package: Mapping[str, Any]) -> Tuple[
        Optional[str], Optional[str]]:
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


def extract_package(dl_artifacts: List[Tuple[str, Mapping]], dest: str) -> str:
    """Extract a Debian source package to a given directory.

    Note that after extraction the target directory will be the root of the
    extracted package, rather than containing it.

    Args:
        package: package information dictionary
        dest: directory where the package files are stored

    Returns:
        Package extraction directory

    """
    a_path = dl_artifacts[0][0]
    logger.debug('dl_artifacts: %s', dl_artifacts)
    for _, hashes in dl_artifacts:
        logger.debug('hashes: %s', hashes)
        filename = hashes['filename']
        if filename.endswith('.dsc'):
            dsc_name = filename
            break

    dsc_path = path.join(a_path, dsc_name)
    destdir = path.join(dest, 'extracted')
    logfile = path.join(dest, 'extract.log')
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
                           changelog_path, extra={
                               'swh_type': 'deb_changelog_encoding',
                               'swh_name': package['name'],
                               'swh_version': str(package['version']),
                               'swh_changelog': changelog_path,
                           })

            # need to reset as Changelog scrolls to the end of the file
            changelog.seek(0)
            parsed_changelog = Changelog(changelog, encoding='iso-8859-15')

    package_info = {
        'name': package['name'],
        'version': str(package['version']),
        'changelog': {
            'person': uid_to_person(parsed_changelog.author),
            'date': parse_date(parsed_changelog.date).isoformat(),
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
