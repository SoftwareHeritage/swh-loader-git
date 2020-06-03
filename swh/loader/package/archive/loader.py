# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import iso8601
import logging

from os import path
from typing import Any, Dict, Generator, Mapping, Optional, Sequence, Tuple

from swh.loader.package.loader import PackageLoader
from swh.loader.package.utils import release_name, artifact_identity
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


class ArchiveLoader(PackageLoader):
    """Load archive origin's artifact files into swh archive

    """

    visit_type = "tar"

    def __init__(
        self,
        url: str,
        artifacts: Sequence[Mapping[str, Any]],
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

               - **artifact's filename version**: artifact's version length

               - **length**: artifact's length

            identity_artifact_keys: Optional List of keys forming the
                "identity" of an artifact

        """
        super().__init__(url=url)
        self.artifacts = artifacts  # assume order is enforced in the lister
        if not identity_artifact_keys:
            # default keys for gnu
            identity_artifact_keys = ["time", "url", "length", "version"]
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
    ) -> Generator[Tuple[str, Mapping[str, Any]], None, None]:
        for a_metadata in self.artifacts:
            url = a_metadata["url"]
            package_version = a_metadata["version"]
            if version == package_version:
                filename = a_metadata.get("filename")
                p_info = {
                    "url": url,
                    "filename": filename if filename else path.split(url)[-1],
                    "raw": a_metadata,
                }
                # FIXME: this code assumes we have only 1 artifact per
                # versioned package
                yield release_name(version), p_info

    def resolve_revision_from(
        self, known_artifacts: Dict, artifact_metadata: Dict
    ) -> Optional[bytes]:
        identity = artifact_identity(
            artifact_metadata, id_keys=self.identity_artifact_keys
        )
        for rev_id, known_artifact in known_artifacts.items():
            logging.debug("known_artifact: %s", known_artifact)
            reference_artifact = known_artifact["extrinsic"]["raw"]
            known_identity = artifact_identity(
                reference_artifact, id_keys=self.identity_artifact_keys
            )
            if identity == known_identity:
                return rev_id
        return None

    def build_revision(
        self, a_metadata: Mapping[str, Any], uncompressed_path: str, directory: Sha1Git
    ) -> Optional[Revision]:
        time = a_metadata["time"]  # assume it's a timestamp
        if isinstance(time, str):  # otherwise, assume it's a parsable date
            time = iso8601.parse_date(time)
        normalized_time = TimestampWithTimezone.from_datetime(time)
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
                    "raw": a_metadata,
                },
            },
        )
