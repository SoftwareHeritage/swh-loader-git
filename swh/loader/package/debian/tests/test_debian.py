# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import pytest
import random

from os import path

from swh.loader.package.debian.loader import (
    DebianLoader,
    DebianPackageInfo,
    DebianPackageChangelog,
    IntrinsicPackageMetadata,
    download_package,
    dsc_information,
    uid_to_person,
    prepare_person,
    get_intrinsic_package_metadata,
    extract_package,
)
from swh.loader.tests import (
    assert_last_visit_matches,
    check_snapshot,
    get_stats,
)

from swh.loader.package.debian.loader import resolve_revision_from

from swh.model.hashutil import hash_to_bytes
from swh.model.model import Person, Snapshot, SnapshotBranch, TargetType


logger = logging.getLogger(__name__)


URL = "deb://Debian/packages/cicero"

PACKAGE_FILES = {
    "name": "cicero",
    "version": "0.7.2-3",
    "files": {
        "cicero_0.7.2-3.diff.gz": {
            "md5sum": "a93661b6a48db48d59ba7d26796fc9ce",
            "name": "cicero_0.7.2-3.diff.gz",
            "sha256": "f039c9642fe15c75bed5254315e2a29f9f2700da0e29d9b0729b3ffc46c8971c",  # noqa
            "size": 3964,
            "uri": "http://deb.debian.org/debian/pool/contrib/c/cicero/cicero_0.7.2-3.diff.gz",  # noqa
        },
        "cicero_0.7.2-3.dsc": {
            "md5sum": "d5dac83eb9cfc9bb52a15eb618b4670a",
            "name": "cicero_0.7.2-3.dsc",
            "sha256": "35b7f1048010c67adfd8d70e4961aefd8800eb9a83a4d1cc68088da0009d9a03",  # noqa
            "size": 1864,
            "uri": "http://deb.debian.org/debian/pool/contrib/c/cicero/cicero_0.7.2-3.dsc",  # noqa
        },  # noqa
        "cicero_0.7.2.orig.tar.gz": {
            "md5sum": "4353dede07c5728319ba7f5595a7230a",
            "name": "cicero_0.7.2.orig.tar.gz",
            "sha256": "63f40f2436ea9f67b44e2d4bd669dbabe90e2635a204526c20e0b3c8ee957786",  # noqa
            "size": 96527,
            "uri": "http://deb.debian.org/debian/pool/contrib/c/cicero/cicero_0.7.2.orig.tar.gz",  # noqa
        },
    },
}

PACKAGE_FILES2 = {
    "name": "cicero",
    "version": "0.7.2-4",
    "files": {
        "cicero_0.7.2-4.diff.gz": {
            "md5sum": "1e7e6fc4a59d57c98082a3af78145734",
            "name": "cicero_0.7.2-4.diff.gz",
            "sha256": "2e6fa296ee7005473ff58d0971f4fd325617b445671480e9f2cfb738d5dbcd01",  # noqa
            "size": 4038,
            "uri": "http://deb.debian.org/debian/pool/contrib/c/cicero/cicero_0.7.2-4.diff.gz",  # noqa
        },
        "cicero_0.7.2-4.dsc": {
            "md5sum": "1a6c8855a73b4282bb31d15518f18cde",
            "name": "cicero_0.7.2-4.dsc",
            "sha256": "913ee52f7093913420de5cbe95d63cfa817f1a1daf997961149501894e754f8b",  # noqa
            "size": 1881,
            "uri": "http://deb.debian.org/debian/pool/contrib/c/cicero/cicero_0.7.2-4.dsc",  # noqa
        },  # noqa
        "cicero_0.7.2.orig.tar.gz": {
            "md5sum": "4353dede07c5728319ba7f5595a7230a",
            "name": "cicero_0.7.2.orig.tar.gz",
            "sha256": "63f40f2436ea9f67b44e2d4bd669dbabe90e2635a204526c20e0b3c8ee957786",  # noqa
            "size": 96527,
            "uri": "http://deb.debian.org/debian/pool/contrib/c/cicero/cicero_0.7.2.orig.tar.gz",  # noqa
        },
    },
}


PACKAGE_PER_VERSION = {
    "stretch/contrib/0.7.2-3": PACKAGE_FILES,
}


PACKAGES_PER_VERSION = {
    "stretch/contrib/0.7.2-3": PACKAGE_FILES,
    "buster/contrib/0.7.2-4": PACKAGE_FILES2,
}


def test_debian_first_visit(swh_config, requests_mock_datadir):
    """With no prior visit, load a gnu project ends up with 1 snapshot

    """
    loader = DebianLoader(
        url=URL, date="2019-10-12T05:58:09.165557+00:00", packages=PACKAGE_PER_VERSION,
    )

    actual_load_status = loader.load()
    expected_snapshot_id = "3b6b66e6ee4e7d903a379a882684a2a50480c0b4"
    assert actual_load_status == {
        "status": "eventful",
        "snapshot_id": expected_snapshot_id,
    }

    assert_last_visit_matches(loader.storage, URL, status="full", type="deb")

    stats = get_stats(loader.storage)
    assert {
        "content": 42,
        "directory": 2,
        "origin": 1,
        "origin_visit": 1,
        "person": 1,
        "release": 0,
        "revision": 1,  # all artifacts under 1 revision
        "skipped_content": 0,
        "snapshot": 1,
    } == stats

    expected_snapshot = Snapshot(
        id=hash_to_bytes(expected_snapshot_id),
        branches={
            b"releases/stretch/contrib/0.7.2-3": SnapshotBranch(
                target_type=TargetType.REVISION,
                target=hash_to_bytes("2807f5b3f84368b4889a9ae827fe85854ffecf07"),
            )
        },
    )  # different than the previous loader as no release is done

    check_snapshot(expected_snapshot, loader.storage)


def test_debian_first_visit_then_another_visit(swh_config, requests_mock_datadir):
    """With no prior visit, load a debian project ends up with 1 snapshot

    """
    loader = DebianLoader(
        url=URL, date="2019-10-12T05:58:09.165557+00:00", packages=PACKAGE_PER_VERSION
    )

    actual_load_status = loader.load()

    expected_snapshot_id = "3b6b66e6ee4e7d903a379a882684a2a50480c0b4"
    assert actual_load_status == {
        "status": "eventful",
        "snapshot_id": expected_snapshot_id,
    }

    assert_last_visit_matches(loader.storage, URL, status="full", type="deb")

    stats = get_stats(loader.storage)
    assert {
        "content": 42,
        "directory": 2,
        "origin": 1,
        "origin_visit": 1,
        "person": 1,
        "release": 0,
        "revision": 1,  # all artifacts under 1 revision
        "skipped_content": 0,
        "snapshot": 1,
    } == stats

    expected_snapshot = Snapshot(
        id=hash_to_bytes(expected_snapshot_id),
        branches={
            b"releases/stretch/contrib/0.7.2-3": SnapshotBranch(
                target_type=TargetType.REVISION,
                target=hash_to_bytes("2807f5b3f84368b4889a9ae827fe85854ffecf07"),
            )
        },
    )  # different than the previous loader as no release is done

    check_snapshot(expected_snapshot, loader.storage)

    # No change in between load
    actual_load_status2 = loader.load()
    assert actual_load_status2["status"] == "uneventful"
    assert_last_visit_matches(loader.storage, URL, status="full", type="deb")

    stats2 = get_stats(loader.storage)
    assert {
        "content": 42 + 0,
        "directory": 2 + 0,
        "origin": 1,
        "origin_visit": 1 + 1,  # a new visit occurred
        "person": 1,
        "release": 0,
        "revision": 1,
        "skipped_content": 0,
        "snapshot": 1,  # same snapshot across 2 visits
    } == stats2

    urls = [
        m.url
        for m in requests_mock_datadir.request_history
        if m.url.startswith("http://deb.debian.org")
    ]
    # visited each package artifact twice across 2 visits
    assert len(urls) == len(set(urls))


def test_uid_to_person():
    uid = "Someone Name <someone@orga.org>"
    actual_person = uid_to_person(uid)

    assert actual_person == {
        "name": "Someone Name",
        "email": "someone@orga.org",
        "fullname": uid,
    }


def test_prepare_person():
    actual_author = prepare_person(
        {
            "name": "Someone Name",
            "email": "someone@orga.org",
            "fullname": "Someone Name <someone@orga.org>",
        }
    )

    assert actual_author == Person(
        name=b"Someone Name",
        email=b"someone@orga.org",
        fullname=b"Someone Name <someone@orga.org>",
    )


def test_download_package(datadir, tmpdir, requests_mock_datadir):
    tmpdir = str(tmpdir)  # py3.5 work around (LocalPath issue)
    p_info = DebianPackageInfo.from_metadata(PACKAGE_FILES, url=URL)
    all_hashes = download_package(p_info, tmpdir)
    assert all_hashes == {
        "cicero_0.7.2-3.diff.gz": {
            "checksums": {
                "sha1": "0815282053f21601b0ec4adf7a8fe47eace3c0bc",
                "sha256": "f039c9642fe15c75bed5254315e2a29f9f2700da0e29d9b0729b3ffc46c8971c",  # noqa
            },
            "filename": "cicero_0.7.2-3.diff.gz",
            "length": 3964,
        },
        "cicero_0.7.2-3.dsc": {
            "checksums": {
                "sha1": "abbec4e8efbbc80278236e1dd136831eac08accd",
                "sha256": "35b7f1048010c67adfd8d70e4961aefd8800eb9a83a4d1cc68088da0009d9a03",  # noqa
            },
            "filename": "cicero_0.7.2-3.dsc",
            "length": 1864,
        },
        "cicero_0.7.2.orig.tar.gz": {
            "checksums": {
                "sha1": "a286efd63fe2c9c9f7bb30255c3d6fcdcf390b43",
                "sha256": "63f40f2436ea9f67b44e2d4bd669dbabe90e2635a204526c20e0b3c8ee957786",  # noqa
            },
            "filename": "cicero_0.7.2.orig.tar.gz",
            "length": 96527,
        },
    }


def test_dsc_information_ok():
    fname = "cicero_0.7.2-3.dsc"
    p_info = DebianPackageInfo.from_metadata(PACKAGE_FILES, url=URL)
    dsc_url, dsc_name = dsc_information(p_info)

    assert dsc_url == PACKAGE_FILES["files"][fname]["uri"]
    assert dsc_name == PACKAGE_FILES["files"][fname]["name"]


def test_dsc_information_not_found():
    fname = "cicero_0.7.2-3.dsc"
    p_info = DebianPackageInfo.from_metadata(PACKAGE_FILES, url=URL)
    p_info.files.pop(fname)

    dsc_url, dsc_name = dsc_information(p_info)

    assert dsc_url is None
    assert dsc_name is None


def test_dsc_information_too_many_dsc_entries():
    # craft an extra dsc file
    fname = "cicero_0.7.2-3.dsc"
    p_info = DebianPackageInfo.from_metadata(PACKAGE_FILES, url=URL)
    data = p_info.files[fname]
    fname2 = fname.replace("cicero", "ciceroo")
    p_info.files[fname2] = data

    with pytest.raises(
        ValueError,
        match="Package %s_%s references several dsc"
        % (PACKAGE_FILES["name"], PACKAGE_FILES["version"]),
    ):
        dsc_information(p_info)


def test_get_intrinsic_package_metadata(requests_mock_datadir, datadir, tmp_path):
    tmp_path = str(tmp_path)  # py3.5 compat.
    p_info = DebianPackageInfo.from_metadata(PACKAGE_FILES, url=URL)

    logger.debug("p_info: %s", p_info)

    # download the packages
    all_hashes = download_package(p_info, tmp_path)

    # Retrieve information from package
    _, dsc_name = dsc_information(p_info)

    dl_artifacts = [(tmp_path, hashes) for hashes in all_hashes.values()]

    # Extract information from package
    extracted_path = extract_package(dl_artifacts, tmp_path)

    # Retrieve information on package
    dsc_path = path.join(path.dirname(extracted_path), dsc_name)
    actual_package_info = get_intrinsic_package_metadata(
        p_info, dsc_path, extracted_path
    )

    logger.debug("actual_package_info: %s", actual_package_info)

    assert actual_package_info == IntrinsicPackageMetadata(
        changelog=DebianPackageChangelog(
            date="2014-10-19T16:52:35+02:00",
            history=[
                ("cicero", "0.7.2-2"),
                ("cicero", "0.7.2-1"),
                ("cicero", "0.7-1"),
            ],
            person={
                "email": "sthibault@debian.org",
                "fullname": "Samuel Thibault <sthibault@debian.org>",
                "name": "Samuel Thibault",
            },
        ),
        maintainers=[
            {
                "email": "debian-accessibility@lists.debian.org",
                "fullname": "Debian Accessibility Team "
                "<debian-accessibility@lists.debian.org>",
                "name": "Debian Accessibility Team",
            },
            {
                "email": "sthibault@debian.org",
                "fullname": "Samuel Thibault <sthibault@debian.org>",
                "name": "Samuel Thibault",
            },
        ],
        name="cicero",
        version="0.7.2-3",
    )


def test_debian_multiple_packages(swh_config, requests_mock_datadir):
    loader = DebianLoader(
        url=URL, date="2019-10-12T05:58:09.165557+00:00", packages=PACKAGES_PER_VERSION
    )

    actual_load_status = loader.load()
    expected_snapshot_id = "defc19021187f3727293121fcf6c5c82cb923604"
    assert actual_load_status == {
        "status": "eventful",
        "snapshot_id": expected_snapshot_id,
    }

    assert_last_visit_matches(loader.storage, URL, status="full", type="deb")

    expected_snapshot = Snapshot(
        id=hash_to_bytes(expected_snapshot_id),
        branches={
            b"releases/stretch/contrib/0.7.2-3": SnapshotBranch(
                target_type=TargetType.REVISION,
                target=hash_to_bytes("2807f5b3f84368b4889a9ae827fe85854ffecf07"),
            ),
            b"releases/buster/contrib/0.7.2-4": SnapshotBranch(
                target_type=TargetType.REVISION,
                target=hash_to_bytes("8224139c274c984147ef4b09aa0e462c55a10bd3"),
            ),
        },
    )

    check_snapshot(expected_snapshot, loader.storage)


def test_resolve_revision_from_edge_cases():
    """Solving revision with empty data will result in unknown revision

    """
    empty_artifact = {
        "name": PACKAGE_FILES["name"],
        "version": PACKAGE_FILES["version"],
    }
    for package_artifacts in [empty_artifact, PACKAGE_FILES]:
        p_info = DebianPackageInfo.from_metadata(package_artifacts, url=URL)
        actual_revision = resolve_revision_from({}, p_info)
        assert actual_revision is None

    for known_artifacts in [{}, PACKAGE_FILES]:
        actual_revision = resolve_revision_from(
            known_artifacts, DebianPackageInfo.from_metadata(empty_artifact, url=URL)
        )
        assert actual_revision is None

    known_package_artifacts = {
        b"(\x07\xf5\xb3\xf8Ch\xb4\x88\x9a\x9a\xe8'\xfe\x85\x85O\xfe\xcf\x07": {
            "extrinsic": {
                # empty
            },
            # ... removed the unnecessary intermediary data
        }
    }
    assert not resolve_revision_from(
        known_package_artifacts, DebianPackageInfo.from_metadata(PACKAGE_FILES, url=URL)
    )


def test_resolve_revision_from_edge_cases_hit_and_miss():
    """Solving revision with inconsistent data will result in unknown revision

    """
    artifact_metadata = PACKAGE_FILES2
    p_info = DebianPackageInfo.from_metadata(artifact_metadata, url=URL)
    expected_revision_id = (
        b"(\x08\xf5\xb3\xf8Ch\xb4\x88\x9a\x9a\xe8'\xff\x85\x85O\xfe\xcf\x07"  # noqa
    )
    known_package_artifacts = {
        expected_revision_id: {
            "extrinsic": {"raw": PACKAGE_FILES,},
            # ... removed the unnecessary intermediary data
        }
    }

    actual_revision = resolve_revision_from(known_package_artifacts, p_info)

    assert actual_revision is None


def test_resolve_revision_from():
    """Solving revision with consistent data will solve the revision

    """
    artifact_metadata = PACKAGE_FILES
    p_info = DebianPackageInfo.from_metadata(artifact_metadata, url=URL)
    expected_revision_id = (
        b"(\x07\xf5\xb3\xf8Ch\xb4\x88\x9a\x9a\xe8'\xfe\x85\x85O\xfe\xcf\x07"  # noqa
    )

    files = artifact_metadata["files"]
    # shuffling dict's keys
    keys = list(files.keys())
    random.shuffle(keys)
    package_files = {
        "name": PACKAGE_FILES["name"],
        "version": PACKAGE_FILES["version"],
        "files": {k: files[k] for k in keys},
    }

    known_package_artifacts = {
        expected_revision_id: {
            "extrinsic": {"raw": package_files,},
            # ... removed the unnecessary intermediary data
        }
    }

    actual_revision = resolve_revision_from(known_package_artifacts, p_info)

    assert actual_revision == expected_revision_id
