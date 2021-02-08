# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from typing import Iterator, List, Sequence, Tuple

import attr

from swh.loader.package import __version__
from swh.loader.package.loader import (
    BasePackageInfo,
    PackageLoader,
    RawExtrinsicMetadataCore,
)
from swh.model.hashutil import hash_to_bytes
from swh.model.identifiers import SWHID
from swh.model.model import (
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    MetadataTargetType,
    Person,
    RawExtrinsicMetadata,
    Revision,
    RevisionType,
    Sha1Git,
)

EMPTY_SNAPSHOT_ID = "1a8893e6a86f444e8be8e7bda6cb34fb1735a00e"
FULL_SNAPSHOT_ID = "4a9b608c9f01860a627237dd2409d1d50ec4b054"

AUTHORITY = MetadataAuthority(
    type=MetadataAuthorityType.FORGE, url="http://example.org/",
)
ORIGIN_URL = "http://example.org/archive.tgz"

REVISION_ID = hash_to_bytes("8ff44f081d43176474b267de5451f2c2e88089d0")
REVISION_SWHID = SWHID(object_type="revision", object_id=REVISION_ID)
DIRECTORY_ID = hash_to_bytes("aa" * 20)
DIRECTORY_SWHID = SWHID(object_type="directory", object_id=DIRECTORY_ID)


FETCHER = MetadataFetcher(
    name="swh.loader.package.tests.test_loader_metadata.MetadataTestLoader",
    version=__version__,
)

DISCOVERY_DATE = datetime.datetime.now(tz=datetime.timezone.utc)

DIRECTORY_METADATA = [
    RawExtrinsicMetadata(
        type=MetadataTargetType.DIRECTORY,
        target=DIRECTORY_SWHID,
        discovery_date=DISCOVERY_DATE,
        authority=AUTHORITY,
        fetcher=FETCHER,
        format="test-format1",
        metadata=b"foo bar",
        origin=ORIGIN_URL,
        revision=REVISION_SWHID,
    ),
    RawExtrinsicMetadata(
        type=MetadataTargetType.DIRECTORY,
        target=DIRECTORY_SWHID,
        discovery_date=DISCOVERY_DATE + datetime.timedelta(seconds=1),
        authority=AUTHORITY,
        fetcher=FETCHER,
        format="test-format2",
        metadata=b"bar baz",
        origin=ORIGIN_URL,
        revision=REVISION_SWHID,
    ),
]

ORIGIN_METADATA = [
    RawExtrinsicMetadata(
        type=MetadataTargetType.ORIGIN,
        target=ORIGIN_URL,
        discovery_date=datetime.datetime.now(tz=datetime.timezone.utc),
        authority=AUTHORITY,
        fetcher=FETCHER,
        format="test-format3",
        metadata=b"baz qux",
    ),
]


class MetadataTestLoader(PackageLoader[BasePackageInfo]):
    def get_versions(self) -> Sequence[str]:
        return ["v1.0.0"]

    def _load_directory(self, dl_artifacts, tmpdir):
        class directory:
            hash = DIRECTORY_ID

        return (None, directory)  # just enough for _load_revision to work

    def download_package(self, p_info: BasePackageInfo, tmpdir: str):
        return [("path", {"artifact_key": "value", "length": 0})]

    def build_revision(
        self, p_info: BasePackageInfo, uncompressed_path: str, directory: Sha1Git
    ):
        return Revision(
            id=REVISION_ID,
            message=b"",
            author=Person.from_fullname(b""),
            committer=Person.from_fullname(b""),
            date=None,
            committer_date=None,
            type=RevisionType.TAR,
            directory=DIRECTORY_ID,
            synthetic=False,
        )

    def get_metadata_authority(self):
        return attr.evolve(AUTHORITY, metadata={})

    def get_package_info(self, version: str) -> Iterator[Tuple[str, BasePackageInfo]]:
        m0 = DIRECTORY_METADATA[0]
        m1 = DIRECTORY_METADATA[1]
        p_info = BasePackageInfo(
            url=ORIGIN_URL,
            filename="archive.tgz",
            directory_extrinsic_metadata=[
                RawExtrinsicMetadataCore(m0.format, m0.metadata, m0.discovery_date),
                RawExtrinsicMetadataCore(m1.format, m1.metadata, m1.discovery_date),
            ],
        )

        yield (version, p_info)

    def get_extrinsic_origin_metadata(self) -> List[RawExtrinsicMetadataCore]:
        m = ORIGIN_METADATA[0]
        return [RawExtrinsicMetadataCore(m.format, m.metadata, m.discovery_date)]


def test_load_artifact_metadata(swh_storage, caplog):
    loader = MetadataTestLoader(swh_storage, ORIGIN_URL)

    load_status = loader.load()
    assert load_status == {
        "status": "eventful",
        "snapshot_id": FULL_SNAPSHOT_ID,
    }

    authority = MetadataAuthority(
        type=MetadataAuthorityType.REGISTRY, url="https://softwareheritage.org/",
    )

    result = swh_storage.raw_extrinsic_metadata_get(
        MetadataTargetType.DIRECTORY, DIRECTORY_SWHID, authority,
    )
    assert result.next_page_token is None
    assert len(result.results) == 1
    assert result.results[0] == RawExtrinsicMetadata(
        type=MetadataTargetType.DIRECTORY,
        target=DIRECTORY_SWHID,
        discovery_date=result.results[0].discovery_date,
        authority=authority,
        fetcher=FETCHER,
        format="original-artifacts-json",
        metadata=b'[{"artifact_key": "value", "length": 0}]',
        origin=ORIGIN_URL,
        revision=REVISION_SWHID,
    )


def test_load_metadata(swh_storage, caplog):
    loader = MetadataTestLoader(swh_storage, ORIGIN_URL)

    load_status = loader.load()
    assert load_status == {
        "status": "eventful",
        "snapshot_id": FULL_SNAPSHOT_ID,
    }

    result = swh_storage.raw_extrinsic_metadata_get(
        MetadataTargetType.DIRECTORY, DIRECTORY_SWHID, AUTHORITY,
    )
    assert result.next_page_token is None
    assert result.results == DIRECTORY_METADATA

    result = swh_storage.raw_extrinsic_metadata_get(
        MetadataTargetType.ORIGIN, ORIGIN_URL, AUTHORITY,
    )
    assert result.next_page_token is None
    assert result.results == ORIGIN_METADATA

    assert caplog.text == ""


def test_existing_authority(swh_storage, caplog):
    loader = MetadataTestLoader(swh_storage, ORIGIN_URL)

    load_status = loader.load()
    assert load_status == {
        "status": "eventful",
        "snapshot_id": FULL_SNAPSHOT_ID,
    }

    result = swh_storage.raw_extrinsic_metadata_get(
        MetadataTargetType.DIRECTORY, DIRECTORY_SWHID, AUTHORITY,
    )
    assert result.next_page_token is None
    assert result.results == DIRECTORY_METADATA

    assert caplog.text == ""


def test_existing_fetcher(swh_storage, caplog):
    loader = MetadataTestLoader(swh_storage, ORIGIN_URL)

    load_status = loader.load()
    assert load_status == {
        "status": "eventful",
        "snapshot_id": FULL_SNAPSHOT_ID,
    }

    result = swh_storage.raw_extrinsic_metadata_get(
        MetadataTargetType.DIRECTORY, DIRECTORY_SWHID, AUTHORITY,
    )
    assert result.next_page_token is None
    assert result.results == DIRECTORY_METADATA

    assert caplog.text == ""
