# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from codecs import BOM_UTF8
import json
import logging
import os
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union
from urllib.parse import quote

import attr
import chardet

from swh.loader.package.loader import (
    BasePackageInfo,
    PackageLoader,
    PartialExtID,
    RawExtrinsicMetadataCore,
)
from swh.loader.package.utils import api_info, cached_method, release_name
from swh.model.hashutil import hash_to_bytes
from swh.model.model import (
    MetadataAuthority,
    MetadataAuthorityType,
    Person,
    Revision,
    RevisionType,
    Sha1Git,
    TimestampWithTimezone,
)
from swh.storage.interface import StorageInterface

logger = logging.getLogger(__name__)


EMPTY_PERSON = Person(fullname=b"", name=None, email=None)


EXTID_TYPE = "npm-archive-sha1"


@attr.s
class NpmPackageInfo(BasePackageInfo):
    raw_info = attr.ib(type=Dict[str, Any])

    date = attr.ib(type=Optional[str])
    shasum = attr.ib(type=str)
    """sha1 checksum"""
    version = attr.ib(type=str)

    @classmethod
    def from_metadata(
        cls, project_metadata: Dict[str, Any], version: str
    ) -> "NpmPackageInfo":
        package_metadata = project_metadata["versions"][version]
        url = package_metadata["dist"]["tarball"]

        # No date available in intrinsic metadata: retrieve it from the API
        # metadata, using the version number that the API claims this package
        # has.
        extrinsic_version = package_metadata["version"]

        if "time" in project_metadata:
            date = project_metadata["time"][extrinsic_version]
        elif "mtime" in package_metadata:
            date = package_metadata["mtime"]
        else:
            date = None

        return cls(
            url=url,
            filename=os.path.basename(url),
            date=date,
            shasum=package_metadata["dist"]["shasum"],
            version=extrinsic_version,
            raw_info=package_metadata,
            directory_extrinsic_metadata=[
                RawExtrinsicMetadataCore(
                    format="replicate-npm-package-json",
                    metadata=json.dumps(package_metadata).encode(),
                )
            ],
        )

    def extid(self) -> PartialExtID:
        return (EXTID_TYPE, hash_to_bytes(self.shasum))


class NpmLoader(PackageLoader[NpmPackageInfo]):
    """Load npm origin's artifact releases into swh archive.

    """

    visit_type = "npm"

    def __init__(
        self,
        storage: StorageInterface,
        url: str,
        max_content_size: Optional[int] = None,
    ):
        """Constructor

        Args
            str: origin url (e.g. https://www.npmjs.com/package/<package-name>)
        """
        super().__init__(storage=storage, url=url, max_content_size=max_content_size)
        package_name = url.split("https://www.npmjs.com/package/")[1]
        safe_name = quote(package_name, safe="")
        self.provider_url = f"https://replicate.npmjs.com/{safe_name}/"
        self._info: Dict[str, Any] = {}
        self._versions = None

    @cached_method
    def _raw_info(self) -> bytes:
        return api_info(self.provider_url)

    @cached_method
    def info(self) -> Dict:
        """Return the project metadata information (fetched from npm registry)

        """
        return json.loads(self._raw_info())

    def get_versions(self) -> Sequence[str]:
        return sorted(list(self.info()["versions"].keys()))

    def get_default_version(self) -> str:
        return self.info()["dist-tags"].get("latest", "")

    def get_metadata_authority(self):
        return MetadataAuthority(
            type=MetadataAuthorityType.FORGE, url="https://npmjs.com/", metadata={},
        )

    def get_package_info(self, version: str) -> Iterator[Tuple[str, NpmPackageInfo]]:
        p_info = NpmPackageInfo.from_metadata(
            project_metadata=self.info(), version=version
        )
        yield release_name(version), p_info

    def build_revision(
        self, p_info: NpmPackageInfo, uncompressed_path: str, directory: Sha1Git
    ) -> Optional[Revision]:
        i_metadata = extract_intrinsic_metadata(uncompressed_path)
        if not i_metadata:
            return None
        author = extract_npm_package_author(i_metadata)
        message = i_metadata["version"].encode("ascii")

        if p_info.date is None:
            url = p_info.url
            artifact_name = os.path.basename(url)
            raise ValueError(
                "Origin %s: Cannot determine upload time for artifact %s."
                % (p_info.url, artifact_name)
            )

        date = TimestampWithTimezone.from_iso8601(p_info.date)

        # FIXME: this is to remain bug-compatible with earlier versions:
        date = attr.evolve(date, timestamp=attr.evolve(date.timestamp, microseconds=0))

        r = Revision(
            type=RevisionType.TAR,
            message=message,
            author=author,
            date=date,
            committer=author,
            committer_date=date,
            parents=(),
            directory=directory,
            synthetic=True,
        )
        return r


def _author_str(author_data: Union[Dict, List, str]) -> str:
    """Parse author from package.json author fields

    """
    if isinstance(author_data, dict):
        author_str = ""
        name = author_data.get("name")
        if name is not None:
            if isinstance(name, str):
                author_str += name
            elif isinstance(name, list):
                author_str += _author_str(name[0]) if len(name) > 0 else ""
        email = author_data.get("email")
        if email is not None:
            author_str += f" <{email}>"
        result = author_str
    elif isinstance(author_data, list):
        result = _author_str(author_data[0]) if len(author_data) > 0 else ""
    else:
        result = author_data
    return result


def extract_npm_package_author(package_json: Dict[str, Any]) -> Person:
    """
    Extract package author from a ``package.json`` file content and
    return it in swh format.

    Args:
        package_json: Dict holding the content of parsed
            ``package.json`` file

    Returns:
        Person

    """
    for author_key in ("author", "authors"):
        if author_key in package_json:
            author_data = package_json[author_key]
            if author_data is None:
                return EMPTY_PERSON
            author_str = _author_str(author_data)
            return Person.from_fullname(author_str.encode())

    return EMPTY_PERSON


def _lstrip_bom(s, bom=BOM_UTF8):
    if s.startswith(bom):
        return s[len(bom) :]
    else:
        return s


def load_json(json_bytes):
    """
    Try to load JSON from bytes and return a dictionary.

    First try to decode from utf-8. If the decoding failed,
    try to detect the encoding and decode again with replace
    error handling.

    If JSON is malformed, an empty dictionary will be returned.

    Args:
        json_bytes (bytes): binary content of a JSON file

    Returns:
        dict: JSON data loaded in a dictionary
    """
    json_data = {}
    try:
        json_str = _lstrip_bom(json_bytes).decode("utf-8")
    except UnicodeDecodeError:
        encoding = chardet.detect(json_bytes)["encoding"]
        if encoding:
            json_str = json_bytes.decode(encoding, "replace")
    try:
        json_data = json.loads(json_str)
    except json.decoder.JSONDecodeError:
        pass
    return json_data


def extract_intrinsic_metadata(dir_path: str) -> Dict:
    """Given an uncompressed path holding the pkginfo file, returns a
       pkginfo parsed structure as a dict.

       The release artifact contains at their root one folder. For example:
       $ tar tvf zprint-0.0.6.tar.gz
       drwxr-xr-x root/root         0 2018-08-22 11:01 zprint-0.0.6/
       ...

    Args:

        dir_path (str): Path to the uncompressed directory
                        representing a release artifact from npm.

    Returns:
        the pkginfo parsed structure as a dict if any or None if
        none was present.

    """
    # Retrieve the root folder of the archive
    if not os.path.exists(dir_path):
        return {}
    lst = os.listdir(dir_path)
    if len(lst) == 0:
        return {}
    project_dirname = lst[0]
    package_json_path = os.path.join(dir_path, project_dirname, "package.json")
    if not os.path.exists(package_json_path):
        return {}
    with open(package_json_path, "rb") as package_json_file:
        package_json_bytes = package_json_file.read()
        return load_json(package_json_bytes)
