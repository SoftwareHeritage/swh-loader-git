# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import logging
from os import path
from typing import Any, Dict, Iterator, Optional, Sequence, Tuple, Union

import attr
import iso8601

from swh.loader.package.loader import PackageLoader, BasePackageInfo
from swh.loader.package.utils import release_name
from swh.model.model import (
    Sha1Git,
    Person,
    TimestampWithTimezone,
    Revision,
    RevisionType,
)


logger = logging.getLogger(__name__)
SWH_PERSON = Person(
    name=b"Software Heritage",
    fullname=b"Software Heritage",
    email=b"robot@softwareheritage.org",
)
REVISION_MESSAGE = b"swh-loader-package: synthetic revision message"


@attr.s
class ArchivePackageInfo(BasePackageInfo):
    raw_info = attr.ib(type=Dict[str, Any])
    length = attr.ib(type=int)
    """Size of the archive file"""
    time = attr.ib(type=Union[str, datetime.datetime])
    """Timestamp of the archive file on the server"""
    version = attr.ib(type=str)

    # default keys for gnu
    ID_KEYS = ["time", "url", "length", "version"]

    def artifact_identity(self, id_keys=None):
        if id_keys is None:
            id_keys = self.ID_KEYS
        # TODO: use parsed attributes instead of self.raw_info
        return [self.raw_info.get(k) for k in id_keys]

    @classmethod
    def from_metadata(cls, a_metadata: Dict[str, Any]) -> "ArchivePackageInfo":
        url = a_metadata["url"]
        filename = a_metadata.get("filename")
        return cls(
            url=url,
            filename=filename if filename else path.split(url)[-1],
            raw_info=a_metadata,
            length=a_metadata["length"],
            time=a_metadata["time"],
            version=a_metadata["version"],
        )


class ArchiveLoader(PackageLoader[ArchivePackageInfo]):
    """Load archive origin's artifact files into swh archive

    """

    visit_type = "tar"

    def __init__(
        self,
        url: str,
        artifacts: Sequence[Dict[str, Any]],
        identity_artifact_keys: Optional[Sequence[str]] = None,
    ):
        """Loader constructor.

        For now, this is the lister's task output.

        Args:
            url: Origin url
            artifacts: List of artifact information with keys:

               - **time**: last modification time as either isoformat date
                 string or timestamp

               - **url**: the artifact url to retrieve filename

               - **filename**: optionally, the file's name

               - **version**: artifact's version

               - **length**: artifact's length

            identity_artifact_keys: Optional List of keys forming the
                "identity" of an artifact

        """
        super().__init__(url=url)
        self.artifacts = artifacts  # assume order is enforced in the lister
        self.identity_artifact_keys = identity_artifact_keys

    def get_versions(self) -> Sequence[str]:
        versions = []
        for archive in self.artifacts:
            v = archive.get("version")
            if v:
                versions.append(v)
        return versions

    def get_default_version(self) -> str:
        # It's the most recent, so for this loader, it's the last one
        return self.artifacts[-1]["version"]

    def get_package_info(
        self, version: str
    ) -> Iterator[Tuple[str, ArchivePackageInfo]]:
        for a_metadata in self.artifacts:
            p_info = ArchivePackageInfo.from_metadata(a_metadata)
            if version == p_info.version:
                # FIXME: this code assumes we have only 1 artifact per
                # versioned package
                yield release_name(version), p_info

    def resolve_revision_from(
        self, known_artifacts: Dict, p_info: ArchivePackageInfo
    ) -> Optional[bytes]:
        identity = p_info.artifact_identity(id_keys=self.identity_artifact_keys)
        for rev_id, known_artifact in known_artifacts.items():
            logging.debug("known_artifact: %s", known_artifact)
            reference_artifact = known_artifact["extrinsic"]["raw"]
            reference_artifact_info = ArchivePackageInfo.from_metadata(
                reference_artifact
            )
            known_identity = reference_artifact_info.artifact_identity(
                id_keys=self.identity_artifact_keys
            )
            if identity == known_identity:
                return rev_id
        return None

    def build_revision(
        self, p_info: ArchivePackageInfo, uncompressed_path: str, directory: Sha1Git
    ) -> Optional[Revision]:
        time = p_info.time  # assume it's a timestamp
        if isinstance(time, str):  # otherwise, assume it's a parsable date
            parsed_time = iso8601.parse_date(time)
        else:
            parsed_time = time
        normalized_time = TimestampWithTimezone.from_datetime(parsed_time)
        return Revision(
            type=RevisionType.TAR,
            message=REVISION_MESSAGE,
            date=normalized_time,
            author=SWH_PERSON,
            committer=SWH_PERSON,
            committer_date=normalized_time,
            parents=(),
            directory=directory,
            synthetic=True,
            metadata={
                "intrinsic": {},
                "extrinsic": {
                    "provider": self.url,
                    "when": self.visit_date.isoformat(),
                    "raw": p_info.raw_info,
                },
            },
        )
