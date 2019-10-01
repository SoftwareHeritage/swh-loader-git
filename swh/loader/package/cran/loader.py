# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import dateutil.parser
import datetime
import os
import logging
import re

from datetime import timezone
from os import path
from typing import Any, Generator, Dict, List, Mapping, Optional, Tuple

from debian.deb822 import Deb822

from swh.loader.package.loader import PackageLoader
from swh.loader.package.utils import release_name, parse_author, swh_author
from swh.model.identifiers import normalize_timestamp


logger = logging.getLogger(__name__)


DATE_PATTERN = re.compile(r'^(?P<year>\d{4})-(?P<month>\d{2})$')


class CRANLoader(PackageLoader):
    visit_type = 'cran'

    def __init__(self, url: str, version: str):
        """Loader constructor.

        Args:
            url: Origin url to retrieve cran artifact from
            version: version of the cran artifact

        """
        super().__init__(url=url)
        self.version = version
        self.provider_url = url

    def get_versions(self) -> List[str]:
        # only 1 artifact
        return [self.version]

    def get_default_version(self) -> str:
        return self.version

    def get_package_info(self, version: str) -> Generator[
            Tuple[str, Dict[str, Any]], None, None]:
        p_info = {
            'url': self.url,
            'filename': path.split(self.url)[-1],
            'raw': {}
        }
        yield release_name(version), p_info

    def build_revision(
            self, a_metadata: Mapping[str, Any],
            uncompressed_path: str) -> Dict[str, Any]:
        # a_metadata is empty
        metadata = extract_intrinsic_metadata(uncompressed_path)
        normalized_date = normalize_timestamp(parse_date(metadata.get('Date')))
        author = swh_author(parse_author(metadata.get('Maintainer', {})))
        version = metadata.get('Version', self.version)
        return {
            'message': version.encode('utf-8'),
            'type': 'tar',
            'date': normalized_date,
            'author': author,
            'committer': author,
            'committer_date': normalized_date,
            'parents': [],
            'metadata': {
                'intrinsic': {
                    'tool': 'DESCRIPTION',
                    'raw': metadata,
                },
                'extrinsic': {
                    'provider': self.provider_url,
                    'when': self.visit_date.isoformat(),
                    'raw': a_metadata,
                },
            },
        }


def parse_debian_control(filepath: str) -> Dict[str, Any]:
    """Parse debian control at filepath"""
    metadata: Dict = {}
    logger.debug('Debian control file %s', filepath)
    for paragraph in Deb822.iter_paragraphs(open(filepath)):
        logger.debug('paragraph: %s', paragraph)
        metadata.update(**paragraph)

    logger.debug('metadata parsed: %s', metadata)
    return metadata


def extract_intrinsic_metadata(dir_path: str) -> Dict[str, Any]:
    """Given an uncompressed path holding the DESCRIPTION file, returns a
       DESCRIPTION parsed structure as a dict.

    Cran origins describes their intrinsic metadata within a DESCRIPTION file
    at the root tree of a tarball. This DESCRIPTION uses a simple file format
    called DCF, the Debian control format.

    The release artifact contains at their root one folder. For example:
    $ tar tvf zprint-0.0.6.tar.gz
    drwxr-xr-x root/root         0 2018-08-22 11:01 zprint-0.0.6/
    ...

    Args:
        dir_path (str): Path to the uncompressed directory
                        representing a release artifact from pypi.

    Returns:
        the DESCRIPTION parsed structure as a dict (or empty dict if missing)

    """
    # Retrieve the root folder of the archive
    if not os.path.exists(dir_path):
        return {}
    lst = os.listdir(dir_path)
    if len(lst) != 1:
        return {}
    project_dirname = lst[0]
    description_path = os.path.join(dir_path, project_dirname, 'DESCRIPTION')
    if not os.path.exists(description_path):
        return {}
    return parse_debian_control(description_path)


def parse_date(date: Optional[str]) -> Optional[datetime.datetime]:
    """Parse a date into a datetime

    """
    assert not date or isinstance(date, str)
    dt: Optional[datetime.datetime] = None
    if not date:
        return dt
    try:
        specific_date = DATE_PATTERN.match(date)
        if specific_date:
            year = int(specific_date.group('year'))
            month = int(specific_date.group('month'))
            dt = datetime.datetime(year, month, 1)
        else:
            dt = dateutil.parser.parse(date)

        if not dt.tzinfo:
            # up for discussion the timezone needs to be set or
            # normalize_timestamp is not happy: ValueError: normalize_timestamp
            # received datetime without timezone: 2001-06-08 00:00:00
            dt = dt.replace(tzinfo=timezone.utc)
    except Exception as e:
        logger.warning('Fail to parse date %s. Reason: %s', (date, e))
    return dt
