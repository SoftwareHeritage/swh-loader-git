# Copyright (C) 2019-2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from shutil import rmtree

from swh.loader.package.opam.loader import OpamLoader, OpamPackageInfo
from swh.loader.tests import assert_last_visit_matches, check_snapshot, get_stats
from swh.model.hashutil import hash_to_bytes
from swh.model.model import Person, Snapshot, SnapshotBranch, TargetType


def test_opam_loader_one_version(tmpdir, requests_mock_datadir, datadir, swh_storage):

    opam_url = f"file://{datadir}/fake_opam_repo"

    opam_root = tmpdir
    # the directory should NOT exist, we just need an unique name, so we delete it
    rmtree(tmpdir)

    opam_instance = "loadertest"

    opam_package = "agrid"
    url = f"opam+{opam_url}/packages/{opam_package}"

    loader = OpamLoader(
        swh_storage, url, opam_root, opam_instance, opam_url, opam_package
    )

    actual_load_status = loader.load()

    expected_snapshot_id = hash_to_bytes("4e4bf977312460329d7f769b0be89937c9827efc")
    assert actual_load_status == {
        "status": "eventful",
        "snapshot_id": expected_snapshot_id.hex(),
    }

    target = b"S\x8c\x8aq\xdcy\xa4/0\xa0\xb2j\xeb\xc1\x16\xad\xce\x06\xeaV"

    expected_snapshot = Snapshot(
        id=expected_snapshot_id,
        branches={
            b"HEAD": SnapshotBranch(target=b"agrid.0.1", target_type=TargetType.ALIAS,),
            b"agrid.0.1": SnapshotBranch(
                target=target, target_type=TargetType.REVISION,
            ),
        },
    )
    check_snapshot(expected_snapshot, swh_storage)

    assert_last_visit_matches(
        swh_storage, url, status="full", type="opam", snapshot=expected_snapshot_id
    )

    stats = get_stats(swh_storage)

    assert {
        "content": 18,
        "directory": 8,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 1,
        "skipped_content": 0,
        "snapshot": 1,
    } == stats


def test_opam_loader_many_version(tmpdir, requests_mock_datadir, datadir, swh_storage):

    opam_url = f"file://{datadir}/fake_opam_repo"

    opam_root = tmpdir
    # the directory should NOT exist, we just need an unique name, so we delete it
    rmtree(tmpdir)

    opam_instance = "loadertest"

    opam_package = "directories"
    url = f"opam+{opam_url}/packages/{opam_package}"

    loader = OpamLoader(
        swh_storage, url, opam_root, opam_instance, opam_url, opam_package
    )

    actual_load_status = loader.load()

    expected_snapshot_id = hash_to_bytes("1b49be175dcf17c0f568bcd7aac3d4faadc41249")
    assert actual_load_status == {
        "status": "eventful",
        "snapshot_id": expected_snapshot_id.hex(),
    }

    expected_snapshot = Snapshot(
        id=expected_snapshot_id,
        branches={
            b"HEAD": SnapshotBranch(
                target=b"directories.0.3", target_type=TargetType.ALIAS,
            ),
            b"directories.0.1": SnapshotBranch(
                target=b"N\x92jA\xb2\x892\xeb\xcc\x9c\xa9\xb3\xea\xa7kz\xb08\xa6V",
                target_type=TargetType.REVISION,
            ),
            b"directories.0.2": SnapshotBranch(
                target=b"yj\xc9\x1a\x8f\xe0\xaa\xff[\x88\xffz"
                b"\x91C\xcc\x96\xb7\xd4\xf65",
                target_type=TargetType.REVISION,
            ),
            b"directories.0.3": SnapshotBranch(
                target=b"hA \xc4\xb5\x18A8\xb8C\x12\xa3\xa5T\xb7/v\x85X\xcb",
                target_type=TargetType.REVISION,
            ),
        },
    )

    check_snapshot(expected_snapshot, swh_storage)

    assert_last_visit_matches(
        swh_storage, url, status="full", type="opam", snapshot=expected_snapshot_id
    )


def test_opam_revision(tmpdir, requests_mock_datadir, swh_storage, datadir):

    opam_url = f"file://{datadir}/fake_opam_repo"

    opam_root = tmpdir
    # the directory should NOT exist, we just need an unique name, so we delete it
    rmtree(tmpdir)

    opam_instance = "loadertest"

    opam_package = "ocb"
    url = f"opam+{opam_url}/packages/{opam_package}"

    loader = OpamLoader(
        swh_storage, url, opam_root, opam_instance, opam_url, opam_package
    )

    actual_load_status = loader.load()

    expected_snapshot_id = hash_to_bytes("398df115b9feb2f463efd21941d69b7d59cd9025")
    assert actual_load_status == {
        "status": "eventful",
        "snapshot_id": expected_snapshot_id.hex(),
    }

    info_iter = loader.get_package_info("0.1")
    branch_name, package_info = next(info_iter)
    expected_branch_name = "ocb.0.1"
    expected_package_info = OpamPackageInfo(
        url="https://github.com/OCamlPro/ocb/archive/0.1.tar.gz",
        filename=None,
        directory_extrinsic_metadata=[],
        author=Person(
            fullname=b"OCamlPro <contact@ocamlpro.com>", name=None, email=None
        ),
        committer=Person(
            fullname=b"OCamlPro <contact@ocamlpro.com>", name=None, email=None
        ),
        version="0.1",
    )

    assert branch_name == expected_branch_name
    assert package_info == expected_package_info

    revision_id = b"o\xad\x7f=\x07\xbb\xaah\xdbI(\xb0'\x10z\xfc\xff\x06x\x1b"

    revision = swh_storage.revision_get([revision_id])[0]

    assert revision is not None

    assert revision.author == expected_package_info.author
    assert revision.committer == expected_package_info.committer
