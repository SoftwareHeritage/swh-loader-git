# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from typing import Iterator, List, Optional, Sequence, Tuple

import attr

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
    RawExtrinsicMetadata,
    Sha1Git,
)
from swh.storage import get_storage

from swh.loader.package import __version__

EMPTY_SNAPSHOT_ID = "1a8893e6a86f444e8be8e7bda6cb34fb1735a00e"
FULL_SNAPSHOT_ID = "4a9b608c9f01860a627237dd2409d1d50ec4b054"

AUTHORITY = MetadataAuthority(
    type=MetadataAuthorityType.FORGE, url="http://example.org/",
)
ORIGIN_URL = "http://example.org/archive.tgz"

REVISION_ID = hash_to_bytes("8ff44f081d43176474b267de5451f2c2e88089d0")
REVISION_SWHID = SWHID(object_type="revision", object_id=REVISION_ID)


FETCHER = MetadataFetcher(
    name="swh.loader.package.tests.test_loader_metadata.MetadataTestLoader",
    version=__version__,
)

REVISION_METADATA = [
    RawExtrinsicMetadata(
        type=MetadataTargetType.REVISION,
        id=REVISION_SWHID,
        discovery_date=datetime.datetime.now(),
        authority=AUTHORITY,
        fetcher=FETCHER,
        format="test-format1",
        metadata=b"foo bar",
        origin=ORIGIN_URL,
    ),
    RawExtrinsicMetadata(
        type=MetadataTargetType.REVISION,
        id=REVISION_SWHID,
        discovery_date=datetime.datetime.now() + datetime.timedelta(seconds=1),
        authority=AUTHORITY,
        fetcher=FETCHER,
        format="test-format2",
        metadata=b"bar baz",
        origin=ORIGIN_URL,
    ),
]

ORIGIN_METADATA = [
    RawExtrinsicMetadata(
        type=MetadataTargetType.ORIGIN,
        id=ORIGIN_URL,
        discovery_date=datetime.datetime.now(),
        authority=AUTHORITY,
        fetcher=FETCHER,
        format="test-format3",
        metadata=b"baz qux",
    ),
]


class MetadataTestLoader(PackageLoader[BasePackageInfo]):
    def get_versions(self) -> Sequence[str]:
        return ["v1.0.0"]

    def _load_revision(self, p_info: BasePackageInfo, origin) -> Optional[Sha1Git]:
        return REVISION_ID

    def get_metadata_authority(self):
        return attr.evolve(AUTHORITY, metadata={})

    def get_package_info(self, version: str) -> Iterator[Tuple[str, BasePackageInfo]]:
        m0 = REVISION_METADATA[0]
        m1 = REVISION_METADATA[1]
        p_info = BasePackageInfo(
            url=ORIGIN_URL,
            filename="archive.tgz",
            revision_extrinsic_metadata=[
                RawExtrinsicMetadataCore(m0.format, m0.metadata, m0.discovery_date),
                RawExtrinsicMetadataCore(m1.format, m1.metadata, m1.discovery_date),
            ],
        )

        yield (version, p_info)

    def get_extrinsic_origin_metadata(self) -> List[RawExtrinsicMetadataCore]:
        m = ORIGIN_METADATA[0]
        return [RawExtrinsicMetadataCore(m.format, m.metadata, m.discovery_date)]


def test_load_metadata(swh_config, caplog):
    storage = get_storage("memory")

    loader = MetadataTestLoader(ORIGIN_URL)
    loader.storage = storage

    load_status = loader.load()
    assert load_status == {
        "status": "eventful",
        "snapshot_id": FULL_SNAPSHOT_ID,
    }

    result = storage.raw_extrinsic_metadata_get(
        MetadataTargetType.REVISION, REVISION_SWHID, AUTHORITY,
    )
    assert result.next_page_token is None
    assert result.results == REVISION_METADATA

    result = storage.raw_extrinsic_metadata_get(
        MetadataTargetType.ORIGIN, ORIGIN_URL, AUTHORITY,
    )
    assert result.next_page_token is None
    assert result.results == ORIGIN_METADATA

    assert caplog.text == ""


def test_existing_authority(swh_config, caplog):
    storage = get_storage("memory")

    loader = MetadataTestLoader(ORIGIN_URL)
    loader.storage = storage
    loader.config["create_authorities"] = False

    storage.metadata_authority_add([attr.evolve(AUTHORITY, metadata={})])

    load_status = loader.load()
    assert load_status == {
        "status": "eventful",
        "snapshot_id": FULL_SNAPSHOT_ID,
    }

    result = storage.raw_extrinsic_metadata_get(
        MetadataTargetType.REVISION, REVISION_SWHID, AUTHORITY,
    )
    assert result.next_page_token is None
    assert result.results == REVISION_METADATA

    assert caplog.text == ""


def test_existing_fetcher(swh_config, caplog):
    storage = get_storage("memory")

    loader = MetadataTestLoader(ORIGIN_URL)
    loader.storage = storage
    loader.config["create_fetchers"] = False

    storage.metadata_fetcher_add([attr.evolve(FETCHER, metadata={})])

    load_status = loader.load()
    assert load_status == {
        "status": "eventful",
        "snapshot_id": FULL_SNAPSHOT_ID,
    }

    result = storage.raw_extrinsic_metadata_get(
        MetadataTargetType.REVISION, REVISION_SWHID, AUTHORITY,
    )
    assert result.next_page_token is None
    assert result.results == REVISION_METADATA

    assert caplog.text == ""
