# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from datetime import timezone
import logging
import os
from os import path
import re
from typing import Any, Dict, Iterator, List, Mapping, Optional, Tuple

import attr
import dateutil.parser
from debian.deb822 import Deb822

from swh.loader.package.loader import BasePackageInfo, PackageLoader
from swh.loader.package.utils import release_name
from swh.model.model import (
    Person,
    Revision,
    RevisionType,
    Sha1Git,
    TimestampWithTimezone,
)
from swh.storage.interface import StorageInterface

logger = logging.getLogger(__name__)


DATE_PATTERN = re.compile(r"^(?P<year>\d{4})-(?P<month>\d{2})$")


@attr.s
class CRANPackageInfo(BasePackageInfo):
    raw_info = attr.ib(type=Dict[str, Any])
    version = attr.ib(type=str)

    ID_KEYS = ["url", "version"]

    @classmethod
    def from_metadata(cls, a_metadata: Dict[str, Any]) -> "CRANPackageInfo":
        url = a_metadata["url"]
        return CRANPackageInfo(
            url=url,
            filename=path.basename(url),
            raw_info=a_metadata,
            version=a_metadata["version"],
        )


class CRANLoader(PackageLoader[CRANPackageInfo]):
    visit_type = "cran"

    def __init__(
        self,
        storage: StorageInterface,
        url: str,
        artifacts: List[Dict],
        max_content_size: Optional[int] = None,
    ):
        """Loader constructor.

        Args:
            url: Origin url to retrieve cran artifact(s) from
            artifacts: List of associated artifact for the origin url

        """
        super().__init__(storage=storage, url=url, max_content_size=max_content_size)
        # explicit what we consider the artifact identity
        self.artifacts = artifacts

    def get_versions(self) -> List[str]:
        versions = []
        for artifact in self.artifacts:
            versions.append(artifact["version"])
        return versions

    def get_default_version(self) -> str:
        return self.artifacts[-1]["version"]

    def get_package_info(self, version: str) -> Iterator[Tuple[str, CRANPackageInfo]]:
        for a_metadata in self.artifacts:
            p_info = CRANPackageInfo.from_metadata(a_metadata)
            if version == p_info.version:
                yield release_name(version), p_info

    def resolve_revision_from(
        self, known_artifacts: Mapping[bytes, Mapping], p_info: CRANPackageInfo,
    ) -> Optional[bytes]:
        """Given known_artifacts per revision, try to determine the revision for
           artifact_metadata

        """
        new_identity = p_info.artifact_identity()
        for rev_id, known_artifact_meta in known_artifacts.items():
            logging.debug("known_artifact_meta: %s", known_artifact_meta)
            known_artifact = known_artifact_meta["extrinsic"]["raw"]
            known_identity = CRANPackageInfo.from_metadata(
                known_artifact
            ).artifact_identity()
            if new_identity == known_identity:
                return rev_id
        return None

    def build_revision(
        self, p_info: CRANPackageInfo, uncompressed_path: str, directory: Sha1Git
    ) -> Optional[Revision]:
        # a_metadata is empty
        metadata = extract_intrinsic_metadata(uncompressed_path)
        date = parse_date(metadata.get("Date"))
        author = Person.from_fullname(metadata.get("Maintainer", "").encode())
        version = metadata.get("Version", p_info.version)
        return Revision(
            message=version.encode("utf-8"),
            type=RevisionType.TAR,
            date=date,
            author=author,
            committer=author,
            committer_date=date,
            parents=(),
            directory=directory,
            synthetic=True,
            metadata={
                "intrinsic": {"tool": "DESCRIPTION", "raw": metadata,},
                "extrinsic": {
                    "provider": self.url,
                    "when": self.visit_date.isoformat(),
                    "raw": p_info.raw_info,
                },
            },
        )


def parse_debian_control(filepath: str) -> Dict[str, Any]:
    """Parse debian control at filepath"""
    metadata: Dict = {}
    logger.debug("Debian control file %s", filepath)
    for paragraph in Deb822.iter_paragraphs(open(filepath, "rb")):
        logger.debug("paragraph: %s", paragraph)
        metadata.update(**paragraph)

    logger.debug("metadata parsed: %s", metadata)
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
    description_path = os.path.join(dir_path, project_dirname, "DESCRIPTION")
    if not os.path.exists(description_path):
        return {}
    return parse_debian_control(description_path)


def parse_date(date: Optional[str]) -> Optional[TimestampWithTimezone]:
    """Parse a date into a datetime

    """
    assert not date or isinstance(date, str)
    dt: Optional[datetime.datetime] = None
    if not date:
        return None
    try:
        specific_date = DATE_PATTERN.match(date)
        if specific_date:
            year = int(specific_date.group("year"))
            month = int(specific_date.group("month"))
            dt = datetime.datetime(year, month, 1)
        else:
            dt = dateutil.parser.parse(date)

        if not dt.tzinfo:
            # up for discussion the timezone needs to be set or
            # normalize_timestamp is not happy: ValueError: normalize_timestamp
            # received datetime without timezone: 2001-06-08 00:00:00
            dt = dt.replace(tzinfo=timezone.utc)
    except Exception as e:
        logger.warning("Fail to parse date %s. Reason: %s", date, e)
    if dt:
        return TimestampWithTimezone.from_datetime(dt)
    else:
        return None
