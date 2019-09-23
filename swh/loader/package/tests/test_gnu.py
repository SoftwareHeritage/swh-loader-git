# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import re

from swh.model.hashutil import hash_to_bytes

from swh.loader.package.gnu import GNULoader, get_version
from swh.loader.package.tests.common import get_response_cb, check_snapshot


def test_get_version():
    """From url to branch name should yield something relevant

    """
    for url, expected_branchname in [
            ('https://gnu.org/sthg/info-2.1.0.tar.gz', '2.1.0'),
            ('https://gnu.org/sthg/info-2.1.2.zip', '2.1.2'),
            ('https://sthg.org/gnu/sthg.tar.gz', 'sthg'),
            ('https://sthg.org/gnu/DLDF-1.1.4.tar.gz', '1.1.4'),
    ]:
        actual_branchname = get_version(url)

        assert actual_branchname == expected_branchname


_expected_new_contents_first_visit = [
    'e9258d81faf5881a2f96a77ba609396f82cb97ad',
    '1170cf105b04b7e2822a0e09d2acf71da7b9a130',
    'fbd27c3f41f2668624ffc80b7ba5db9b92ff27ac',
    '0057bec9b5422aff9256af240b177ac0e3ac2608',
    '2b8d0d0b43a1078fc708930c8ddc2956a86c566e',
    '27de3b3bc6545d2a797aeeb4657c0e215a0c2e55',
    '2e6db43f5cd764e677f416ff0d0c78c7a82ef19b',
    'ae9be03bd2a06ed8f4f118d3fe76330bb1d77f62',
    'edeb33282b2bffa0e608e9d2fd960fd08093c0ea',
    'd64e64d4c73679323f8d4cde2643331ba6c20af9',
    '7a756602914be889c0a2d3952c710144b3e64cb0',
    '84fb589b554fcb7f32b806951dcf19518d67b08f',
    '8624bcdae55baeef00cd11d5dfcfa60f68710a02',
    'e08441aeab02704cfbd435d6445f7c072f8f524e',
    'f67935bc3a83a67259cda4b2d43373bd56703844',
    '809788434b433eb2e3cfabd5d591c9a659d5e3d8',
    '7d7c6c8c5ebaeff879f61f37083a3854184f6c41',
    'b99fec102eb24bffd53ab61fc30d59e810f116a2',
    '7d149b28eaa228b3871c91f0d5a95a2fa7cb0c68',
    'f0c97052e567948adf03e641301e9983c478ccff',
    '7fb724242e2b62b85ca64190c31dcae5303e19b3',
    '4f9709e64a9134fe8aefb36fd827b84d8b617ab5',
    '7350628ccf194c2c3afba4ac588c33e3f3ac778d',
    '0bb892d9391aa706dc2c3b1906567df43cbe06a2',
    '49d4c0ce1a16601f1e265d446b6c5ea6b512f27c',
    '6b5cc594ac466351450f7f64a0b79fdaf4435ad3',
    '3046e5d1f70297e2a507b98224b6222c9688d610',
    '1572607d456d7f633bc6065a2b3048496d679a31',
]

_expected_new_directories_first_visit = [
    'daabc65ec75d487b1335ffc101c0ac11c803f8fc',
    '263be23b4a8101d3ad0d9831319a3e0f2b065f36',
    '7f6e63ba6eb3e2236f65892cd822041f1a01dd5c',
    '4db0a3ecbc976083e2dac01a62f93729698429a3',
    'dfef1c80e1098dd5deda664bb44a9ab1f738af13',
    'eca971d346ea54d95a6e19d5051f900237fafdaa',
    '3aebc29ed1fccc4a6f2f2010fb8e57882406b528',
]

_expected_new_revisions_first_visit = {
    '44183488c0774ce3c957fa19ba695cf18a4a42b3':
    '3aebc29ed1fccc4a6f2f2010fb8e57882406b528'
}

_expected_branches_first_visit = {
    'HEAD': {
        'target_type': 'alias',
        'target': 'releases/0.1.0',
    },
    'releases/0.1.0': {
        'target_type': 'revision',
        'target': '44183488c0774ce3c957fa19ba695cf18a4a42b3',
    },
}

# hash is different then before as we changed the snapshot
# gnu used to use `release/` (singular) instead of plural
_expected_new_snapshot_first_visit_id = 'c419397fd912039825ebdbea378bc6283f006bf5'  # noqa

# def test_release_artifact_not_found(requests_mock):
#     package = '8sync'
#     package_url = 'https://ftp.gnu.org/gnu/8sync/'
#     tarballs = [{
#         'date': '944729610',
#         'archive': 'https://ftp.gnu.org/gnu/8sync/8sync-0.1.0.tar.gz',
#     }]

#     loader = GNULoader(package, package_url, tarballs)
#     requests_mock.get(re.compile('https://'), status_code=404)

#     assert actual_load_status == {'status': 'uneventful'}
#     stats = loader.storage.stat_counters()

#     assert {
#         'content': 0,
#         'directory': 0,
#         'origin': 1,
#         'origin_visit': 1,
#         'person': 0,
#         'release': 0,
#         'revision': 0,
#         'skipped_content': 0,
#         'snapshot': 0,
#     } == stats


def test_release_artifact_no_prior_visit(requests_mock):
    """With no prior visit, load a pypi project ends up with 1 snapshot

    """
    assert 'SWH_CONFIG_FILENAME' in os.environ  # cf. tox.ini
    package = '8sync'
    package_url = 'https://ftp.gnu.org/gnu/8sync/'
    tarballs = [{
        'date': '944729610',
        'archive': 'https://ftp.gnu.org/gnu/8sync/8sync-0.1.0.tar.gz',
    }]

    loader = GNULoader(package, package_url, tarballs)
    requests_mock.get(re.compile('https://'), body=get_response_cb)

    actual_load_status = loader.load()

    assert actual_load_status == {'status': 'eventful'}

    stats = loader.storage.stat_counters()

    assert {
        'content': len(_expected_new_contents_first_visit),
        'directory': len(_expected_new_directories_first_visit),
        'origin': 1,
        'origin_visit': 1,
        'person': 1,
        'release': 0,
        'revision': len(_expected_new_revisions_first_visit),
        'skipped_content': 0,
        'snapshot': 1
    } == stats

    expected_contents = map(hash_to_bytes, _expected_new_contents_first_visit)
    assert list(loader.storage.content_missing_per_sha1(expected_contents))\
        == []

    expected_dirs = map(hash_to_bytes, _expected_new_directories_first_visit)
    assert list(loader.storage.directory_missing(expected_dirs)) == []

    expected_revs = map(hash_to_bytes, _expected_new_revisions_first_visit)
    assert list(loader.storage.revision_missing(expected_revs)) == []

    check_snapshot(
        _expected_new_snapshot_first_visit_id,
        _expected_branches_first_visit,
        storage=loader.storage)
