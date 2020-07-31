# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import os
import pytest

from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.model.identifiers import SWHID
from swh.model.model import (
    MetadataAuthority,
    MetadataAuthorityType,
    MetadataFetcher,
    MetadataTargetType,
    Person,
    RawExtrinsicMetadata,
    Snapshot,
    SnapshotBranch,
    TargetType,
)

from swh.storage.interface import PagedResult

from swh.loader.package import __version__
from swh.loader.package.npm.loader import (
    _author_str,
    NpmLoader,
    extract_npm_package_author,
    artifact_to_revision_id,
)
from swh.loader.package.tests.common import check_metadata_paths
from swh.loader.tests import (
    assert_last_visit_matches,
    check_snapshot,
    get_stats,
)


@pytest.fixture
def org_api_info(datadir) -> bytes:
    with open(os.path.join(datadir, "https_replicate.npmjs.com", "org"), "rb",) as f:
        return f.read()


def test_npm_author_str():
    for author, expected_author in [
        ("author", "author"),
        (
            ["Al from quantum leap", "hal from 2001 space odyssey"],
            "Al from quantum leap",
        ),
        ([], ""),
        ({"name": "groot", "email": "groot@galaxy.org",}, "groot <groot@galaxy.org>"),
        ({"name": "somebody",}, "somebody"),
        ({"email": "no@one.org"}, " <no@one.org>"),  # note first elt is an extra blank
        ({"name": "no one", "email": None,}, "no one"),
        ({"email": None,}, ""),
        ({"name": None}, ""),
        ({"name": None, "email": None,}, ""),
        ({}, ""),
        (None, None),
        ({"name": []}, "",),
        (
            {"name": ["Susan McSween", "William H. Bonney", "Doc Scurlock",]},
            "Susan McSween",
        ),
        (None, None),
    ]:
        assert _author_str(author) == expected_author


def test_extract_npm_package_author(datadir):
    package_metadata_filepath = os.path.join(
        datadir, "https_replicate.npmjs.com", "org_visit1"
    )

    with open(package_metadata_filepath) as json_file:
        package_metadata = json.load(json_file)

    extract_npm_package_author(package_metadata["versions"]["0.0.2"]) == Person(
        fullname=b"mooz <stillpedant@gmail.com>",
        name=b"mooz",
        email=b"stillpedant@gmail.com",
    )

    assert extract_npm_package_author(package_metadata["versions"]["0.0.3"]) == Person(
        fullname=b"Masafumi Oyamada <stillpedant@gmail.com>",
        name=b"Masafumi Oyamada",
        email=b"stillpedant@gmail.com",
    )

    package_json = json.loads(
        """
    {
        "name": "highlightjs-line-numbers.js",
        "version": "2.7.0",
        "description": "Highlight.js line numbers plugin.",
        "main": "src/highlightjs-line-numbers.js",
        "dependencies": {},
        "devDependencies": {
            "gulp": "^4.0.0",
            "gulp-rename": "^1.4.0",
            "gulp-replace": "^0.6.1",
            "gulp-uglify": "^1.2.0"
        },
        "repository": {
            "type": "git",
            "url": "https://github.com/wcoder/highlightjs-line-numbers.js.git"
        },
        "author": "Yauheni Pakala <evgeniy.pakalo@gmail.com>",
        "license": "MIT",
        "bugs": {
            "url": "https://github.com/wcoder/highlightjs-line-numbers.js/issues"
        },
        "homepage": "http://wcoder.github.io/highlightjs-line-numbers.js/"
    }"""
    )

    assert extract_npm_package_author(package_json) == Person(
        fullname=b"Yauheni Pakala <evgeniy.pakalo@gmail.com>",
        name=b"Yauheni Pakala",
        email=b"evgeniy.pakalo@gmail.com",
    )

    package_json = json.loads(
        """
    {
        "name": "3-way-diff",
        "version": "0.0.1",
        "description": "3-way diffing of JavaScript objects",
        "main": "index.js",
        "authors": [
            {
                "name": "Shawn Walsh",
                "url": "https://github.com/shawnpwalsh"
            },
            {
                "name": "Markham F Rollins IV",
                "url": "https://github.com/mrollinsiv"
            }
        ],
        "keywords": [
            "3-way diff",
            "3 way diff",
            "three-way diff",
            "three way diff"
        ],
        "devDependencies": {
            "babel-core": "^6.20.0",
            "babel-preset-es2015": "^6.18.0",
            "mocha": "^3.0.2"
        },
        "dependencies": {
            "lodash": "^4.15.0"
        }
    }"""
    )

    assert extract_npm_package_author(package_json) == Person(
        fullname=b"Shawn Walsh", name=b"Shawn Walsh", email=None
    )

    package_json = json.loads(
        """
    {
        "name": "yfe-ynpm",
        "version": "1.0.0",
        "homepage": "http://gitlab.ywwl.com/yfe/yfe-ynpm",
        "repository": {
            "type": "git",
            "url": "git@gitlab.ywwl.com:yfe/yfe-ynpm.git"
        },
        "author": [
            "fengmk2 <fengmk2@gmail.com> (https://fengmk2.com)",
            "xufuzi <xufuzi@ywwl.com> (https://7993.org)"
        ],
        "license": "MIT"
    }"""
    )

    assert extract_npm_package_author(package_json) == Person(
        fullname=b"fengmk2 <fengmk2@gmail.com> (https://fengmk2.com)",
        name=b"fengmk2",
        email=b"fengmk2@gmail.com",
    )

    package_json = json.loads(
        """
    {
        "name": "umi-plugin-whale",
        "version": "0.0.8",
        "description": "Internal contract component",
        "authors": {
            "name": "xiaohuoni",
            "email": "448627663@qq.com"
        },
        "repository": "alitajs/whale",
        "devDependencies": {
            "np": "^3.0.4",
            "umi-tools": "*"
        },
        "license": "MIT"
    }"""
    )

    assert extract_npm_package_author(package_json) == Person(
        fullname=b"xiaohuoni <448627663@qq.com>",
        name=b"xiaohuoni",
        email=b"448627663@qq.com",
    )

    package_json_no_authors = json.loads(
        """{
        "authors": null,
        "license": "MIT"
    }"""
    )

    assert extract_npm_package_author(package_json_no_authors) == Person(
        fullname=b"", name=None, email=None
    )


def normalize_hashes(hashes):
    if isinstance(hashes, str):
        return hash_to_bytes(hashes)
    if isinstance(hashes, list):
        return [hash_to_bytes(x) for x in hashes]
    return {hash_to_bytes(k): hash_to_bytes(v) for k, v in hashes.items()}


_expected_new_contents_first_visit = normalize_hashes(
    [
        "4ce3058e16ab3d7e077f65aabf855c34895bf17c",
        "858c3ceee84c8311adc808f8cdb30d233ddc9d18",
        "0fa33b4f5a4e0496da6843a38ff1af8b61541996",
        "85a410f8ef8eb8920f2c384a9555566ad4a2e21b",
        "9163ac8025923d5a45aaac482262893955c9b37b",
        "692cf623b8dd2c5df2c2998fd95ae4ec99882fb4",
        "18c03aac6d3e910efb20039c15d70ab5e0297101",
        "41265c42446aac17ca769e67d1704f99e5a1394d",
        "783ff33f5882813dca9239452c4a7cadd4dba778",
        "b029cfb85107aee4590c2434a3329bfcf36f8fa1",
        "112d1900b4c2e3e9351050d1b542c9744f9793f3",
        "5439bbc4bd9a996f1a38244e6892b71850bc98fd",
        "d83097a2f994b503185adf4e719d154123150159",
        "d0939b4898e83090ee55fd9d8a60e312cfadfbaf",
        "b3523a26f7147e4af40d9d462adaae6d49eda13e",
        "cd065fb435d6fb204a8871bcd623d0d0e673088c",
        "2854a40855ad839a54f4b08f5cff0cf52fca4399",
        "b8a53bbaac34ebb8c6169d11a4b9f13b05c583fe",
        "0f73d56e1cf480bded8a1ecf20ec6fc53c574713",
        "0d9882b2dfafdce31f4e77fe307d41a44a74cefe",
        "585fc5caab9ead178a327d3660d35851db713df1",
        "e8cd41a48d79101977e3036a87aeb1aac730686f",
        "5414efaef33cceb9f3c9eb5c4cc1682cd62d14f7",
        "9c3cc2763bf9e9e37067d3607302c4776502df98",
        "3649a68410e354c83cd4a38b66bd314de4c8f5c9",
        "e96ed0c091de1ebdf587104eaf63400d1974a1fe",
        "078ca03d2f99e4e6eab16f7b75fbb7afb699c86c",
        "38de737da99514de6559ff163c988198bc91367a",
    ]
)

_expected_new_directories_first_visit = normalize_hashes(
    [
        "3370d20d6f96dc1c9e50f083e2134881db110f4f",
        "42753c0c2ab00c4501b552ac4671c68f3cf5aece",
        "d7895533ef5edbcffdea3f057d9fef3a1ef845ce",
        "80579be563e2ef3e385226fe7a3f079b377f142c",
        "3b0ddc6a9e58b4b53c222da4e27b280b6cda591c",
        "bcad03ce58ac136f26f000990fc9064e559fe1c0",
        "5fc7e82a1bc72e074665c6078c6d3fad2f13d7ca",
        "e3cd26beba9b1e02f6762ef54bd9ac80cc5f25fd",
        "584b5b4b6cf7f038095e820b99386a9c232de931",
        "184c8d6d0d242f2b1792ef9d3bf396a5434b7f7a",
        "bb5f4ee143c970367eb409f2e4c1104898048b9d",
        "1b95491047add1103db0dfdfa84a9735dcb11e88",
        "a00c6de13471a2d66e64aca140ddb21ef5521e62",
        "5ce6c1cd5cda2d546db513aaad8c72a44c7771e2",
        "c337091e349b6ac10d38a49cdf8c2401ef9bb0f2",
        "202fafcd7c0f8230e89d5496ad7f44ab12b807bf",
        "775cc516543be86c15c1dc172f49c0d4e6e78235",
        "ff3d1ead85a14f891e8b3fa3a89de39db1b8de2e",
    ]
)

_expected_new_revisions_first_visit = normalize_hashes(
    {
        "d8a1c7474d2956ac598a19f0f27d52f7015f117e": (
            "42753c0c2ab00c4501b552ac4671c68f3cf5aece"
        ),
        "5f9eb78af37ffd12949f235e86fac04898f9f72a": (
            "3370d20d6f96dc1c9e50f083e2134881db110f4f"
        ),
        "ba019b192bdb94bd0b5bd68b3a5f92b5acc2239a": (
            "d7895533ef5edbcffdea3f057d9fef3a1ef845ce"
        ),
    }
)


def package_url(package):
    return "https://www.npmjs.com/package/%s" % package


def package_metadata_url(package):
    return "https://replicate.npmjs.com/%s/" % package


def test_revision_metadata_structure(swh_config, requests_mock_datadir):
    package = "org"
    loader = NpmLoader(package_url(package))

    actual_load_status = loader.load()
    assert actual_load_status["status"] == "eventful"
    assert actual_load_status["snapshot_id"] is not None

    expected_revision_id = hash_to_bytes("d8a1c7474d2956ac598a19f0f27d52f7015f117e")
    revision = list(loader.storage.revision_get([expected_revision_id]))[0]

    assert revision is not None

    check_metadata_paths(
        revision["metadata"],
        paths=[
            ("intrinsic.tool", str),
            ("intrinsic.raw", dict),
            ("extrinsic.provider", str),
            ("extrinsic.when", str),
            ("extrinsic.raw", dict),
            ("original_artifact", list),
        ],
    )

    for original_artifact in revision["metadata"]["original_artifact"]:
        check_metadata_paths(
            original_artifact,
            paths=[("filename", str), ("length", int), ("checksums", dict),],
        )


def test_npm_loader_first_visit(swh_config, requests_mock_datadir, org_api_info):
    package = "org"
    url = package_url(package)
    loader = NpmLoader(url)

    actual_load_status = loader.load()
    expected_snapshot_id = hash_to_bytes("d0587e1195aed5a8800411a008f2f2d627f18e2d")
    assert actual_load_status == {
        "status": "eventful",
        "snapshot_id": expected_snapshot_id.hex(),
    }

    assert_last_visit_matches(
        loader.storage, url, status="full", type="npm", snapshot=expected_snapshot_id
    )

    stats = get_stats(loader.storage)

    assert {
        "content": len(_expected_new_contents_first_visit),
        "directory": len(_expected_new_directories_first_visit),
        "origin": 1,
        "origin_visit": 1,
        "person": 2,
        "release": 0,
        "revision": len(_expected_new_revisions_first_visit),
        "skipped_content": 0,
        "snapshot": 1,
    } == stats

    assert len(
        list(loader.storage.content_get(_expected_new_contents_first_visit))
    ) == len(_expected_new_contents_first_visit)

    assert (
        list(loader.storage.directory_missing(_expected_new_directories_first_visit))
        == []
    )

    assert (
        list(loader.storage.revision_missing(_expected_new_revisions_first_visit)) == []
    )

    expected_snapshot = Snapshot(
        id=expected_snapshot_id,
        branches={
            b"HEAD": SnapshotBranch(
                target=b"releases/0.0.4", target_type=TargetType.ALIAS
            ),
            b"releases/0.0.2": SnapshotBranch(
                target=hash_to_bytes("d8a1c7474d2956ac598a19f0f27d52f7015f117e"),
                target_type=TargetType.REVISION,
            ),
            b"releases/0.0.3": SnapshotBranch(
                target=hash_to_bytes("5f9eb78af37ffd12949f235e86fac04898f9f72a"),
                target_type=TargetType.REVISION,
            ),
            b"releases/0.0.4": SnapshotBranch(
                target=hash_to_bytes("ba019b192bdb94bd0b5bd68b3a5f92b5acc2239a"),
                target_type=TargetType.REVISION,
            ),
        },
    )
    check_snapshot(expected_snapshot, loader.storage)

    snapshot_swhid = SWHID(
        object_type="snapshot", object_id=hash_to_hex(expected_snapshot_id)
    )
    metadata_authority = MetadataAuthority(
        type=MetadataAuthorityType.FORGE, url="https://npmjs.com/",
    )
    expected_metadata = [
        RawExtrinsicMetadata(
            type=MetadataTargetType.SNAPSHOT,
            id=snapshot_swhid,
            authority=metadata_authority,
            fetcher=MetadataFetcher(
                name="swh.loader.package.npm.loader.NpmLoader", version=__version__,
            ),
            discovery_date=loader.visit_date,
            format="replicate-npm-package-json",
            metadata=org_api_info,
            origin="https://www.npmjs.com/package/org",
        )
    ]
    assert loader.storage.raw_extrinsic_metadata_get(
        type=MetadataTargetType.SNAPSHOT,
        id=snapshot_swhid,
        authority=metadata_authority,
    ) == PagedResult(next_page_token=None, results=expected_metadata,)


def test_npm_loader_incremental_visit(swh_config, requests_mock_datadir_visits):
    package = "org"
    url = package_url(package)
    loader = NpmLoader(url)

    expected_snapshot_id = hash_to_bytes("d0587e1195aed5a8800411a008f2f2d627f18e2d")
    actual_load_status = loader.load()
    assert actual_load_status == {
        "status": "eventful",
        "snapshot_id": expected_snapshot_id.hex(),
    }
    assert_last_visit_matches(
        loader.storage, url, status="full", type="npm", snapshot=expected_snapshot_id
    )

    stats = get_stats(loader.storage)

    assert {
        "content": len(_expected_new_contents_first_visit),
        "directory": len(_expected_new_directories_first_visit),
        "origin": 1,
        "origin_visit": 1,
        "person": 2,
        "release": 0,
        "revision": len(_expected_new_revisions_first_visit),
        "skipped_content": 0,
        "snapshot": 1,
    } == stats

    # reset loader internal state
    del loader._cached_info
    del loader._cached__raw_info

    actual_load_status2 = loader.load()
    assert actual_load_status2["status"] == "eventful"
    snap_id2 = actual_load_status2["snapshot_id"]
    assert snap_id2 is not None
    assert snap_id2 != actual_load_status["snapshot_id"]

    assert_last_visit_matches(loader.storage, url, status="full", type="npm")

    stats = get_stats(loader.storage)

    assert {  # 3 new releases artifacts
        "content": len(_expected_new_contents_first_visit) + 14,
        "directory": len(_expected_new_directories_first_visit) + 15,
        "origin": 1,
        "origin_visit": 2,
        "person": 2,
        "release": 0,
        "revision": len(_expected_new_revisions_first_visit) + 3,
        "skipped_content": 0,
        "snapshot": 2,
    } == stats

    urls = [
        m.url
        for m in requests_mock_datadir_visits.request_history
        if m.url.startswith("https://registry.npmjs.org")
    ]
    assert len(urls) == len(set(urls))  # we visited each artifact once across


@pytest.mark.usefixtures("requests_mock_datadir")
def test_npm_loader_version_divergence(swh_config):
    package = "@aller_shared"
    url = package_url(package)
    loader = NpmLoader(url)

    actual_load_status = loader.load()
    expected_snapshot_id = hash_to_bytes("b11ebac8c9d0c9e5063a2df693a18e3aba4b2f92")
    assert actual_load_status == {
        "status": "eventful",
        "snapshot_id": expected_snapshot_id.hex(),
    }
    assert_last_visit_matches(
        loader.storage, url, status="full", type="npm", snapshot=expected_snapshot_id
    )

    stats = get_stats(loader.storage)

    assert {  # 1 new releases artifacts
        "content": 534,
        "directory": 153,
        "origin": 1,
        "origin_visit": 1,
        "person": 1,
        "release": 0,
        "revision": 2,
        "skipped_content": 0,
        "snapshot": 1,
    } == stats

    expected_snapshot = Snapshot(
        id=expected_snapshot_id,
        branches={
            b"HEAD": SnapshotBranch(
                target_type=TargetType.ALIAS, target=b"releases/0.1.0"
            ),
            b"releases/0.1.0": SnapshotBranch(
                target_type=TargetType.REVISION,
                target=hash_to_bytes("845673bfe8cbd31b1eaf757745a964137e6f9116"),
            ),
            b"releases/0.1.1-alpha.14": SnapshotBranch(
                target_type=TargetType.REVISION,
                target=hash_to_bytes("05181c12cd8c22035dd31155656826b85745da37"),
            ),
        },
    )
    check_snapshot(expected_snapshot, loader.storage)


def test_npm_artifact_to_revision_id_none():
    """Current loader version should stop soon if nothing can be found

    """

    class artifact_metadata:
        shasum = "05181c12cd8c22035dd31155656826b85745da37"

    known_artifacts = {
        "b11ebac8c9d0c9e5063a2df693a18e3aba4b2f92": {},
    }

    assert artifact_to_revision_id(known_artifacts, artifact_metadata) is None


def test_npm_artifact_to_revision_id_old_loader_version():
    """Current loader version should solve old metadata scheme

    """

    class artifact_metadata:
        shasum = "05181c12cd8c22035dd31155656826b85745da37"

    known_artifacts = {
        hash_to_bytes("b11ebac8c9d0c9e5063a2df693a18e3aba4b2f92"): {
            "package_source": {"sha1": "something-wrong"}
        },
        hash_to_bytes("845673bfe8cbd31b1eaf757745a964137e6f9116"): {
            "package_source": {"sha1": "05181c12cd8c22035dd31155656826b85745da37",}
        },
    }

    assert artifact_to_revision_id(known_artifacts, artifact_metadata) == hash_to_bytes(
        "845673bfe8cbd31b1eaf757745a964137e6f9116"
    )


def test_npm_artifact_to_revision_id_current_loader_version():
    """Current loader version should be able to solve current metadata scheme

    """

    class artifact_metadata:
        shasum = "05181c12cd8c22035dd31155656826b85745da37"

    known_artifacts = {
        hash_to_bytes("b11ebac8c9d0c9e5063a2df693a18e3aba4b2f92"): {
            "original_artifact": [
                {"checksums": {"sha1": "05181c12cd8c22035dd31155656826b85745da37"},}
            ],
        },
        hash_to_bytes("845673bfe8cbd31b1eaf757745a964137e6f9116"): {
            "original_artifact": [{"checksums": {"sha1": "something-wrong"},}],
        },
    }

    assert artifact_to_revision_id(known_artifacts, artifact_metadata) == hash_to_bytes(
        "b11ebac8c9d0c9e5063a2df693a18e3aba4b2f92"
    )


def test_npm_artifact_with_no_intrinsic_metadata(swh_config, requests_mock_datadir):
    """Skip artifact with no intrinsic metadata during ingestion

    """
    package = "nativescript-telerik-analytics"
    url = package_url(package)
    loader = NpmLoader(url)

    actual_load_status = loader.load()
    # no branch as one artifact without any intrinsic metadata
    expected_snapshot = Snapshot(
        id=hash_to_bytes("1a8893e6a86f444e8be8e7bda6cb34fb1735a00e"), branches={},
    )
    assert actual_load_status == {
        "status": "eventful",
        "snapshot_id": expected_snapshot.id.hex(),
    }

    check_snapshot(expected_snapshot, loader.storage)

    assert_last_visit_matches(
        loader.storage, url, status="full", type="npm", snapshot=expected_snapshot.id
    )


def test_npm_artifact_with_no_upload_time(swh_config, requests_mock_datadir):
    """With no time upload, artifact is skipped

    """
    package = "jammit-no-time"
    url = package_url(package)
    loader = NpmLoader(url)

    actual_load_status = loader.load()
    # no branch as one artifact without any intrinsic metadata
    expected_snapshot = Snapshot(
        id=hash_to_bytes("1a8893e6a86f444e8be8e7bda6cb34fb1735a00e"), branches={},
    )
    assert actual_load_status == {
        "status": "uneventful",
        "snapshot_id": expected_snapshot.id.hex(),
    }

    check_snapshot(expected_snapshot, loader.storage)

    assert_last_visit_matches(
        loader.storage, url, status="partial", type="npm", snapshot=expected_snapshot.id
    )


def test_npm_artifact_use_mtime_if_no_time(swh_config, requests_mock_datadir):
    """With no time upload, artifact is skipped

    """
    package = "jammit-express"
    url = package_url(package)
    loader = NpmLoader(url)

    actual_load_status = loader.load()
    expected_snapshot_id = hash_to_bytes("d6e08e19159f77983242877c373c75222d5ae9dd")

    assert actual_load_status == {
        "status": "eventful",
        "snapshot_id": expected_snapshot_id.hex(),
    }

    # artifact is used
    expected_snapshot = Snapshot(
        id=expected_snapshot_id,
        branches={
            b"HEAD": SnapshotBranch(
                target_type=TargetType.ALIAS, target=b"releases/0.0.1"
            ),
            b"releases/0.0.1": SnapshotBranch(
                target_type=TargetType.REVISION,
                target=hash_to_bytes("9e4dd2b40d1b46b70917c0949aa2195c823a648e"),
            ),
        },
    )
    check_snapshot(expected_snapshot, loader.storage)

    assert_last_visit_matches(
        loader.storage, url, status="full", type="npm", snapshot=expected_snapshot.id
    )


def test_npm_no_artifact(swh_config, requests_mock_datadir):
    """If no artifacts at all is found for origin, the visit fails completely

    """
    package = "catify"
    url = package_url(package)
    loader = NpmLoader(url)
    actual_load_status = loader.load()
    assert actual_load_status == {
        "status": "failed",
    }

    assert_last_visit_matches(loader.storage, url, status="partial", type="npm")
