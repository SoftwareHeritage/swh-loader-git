# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.model.hashutil import hash_to_bytes
from swh.loader.package.tests.common import decode_target, check_snapshot
from swh.storage import get_storage

hash_hex = '43e45d56f88993aae6a0198013efa80716fd8920'


def test_decode_target_edge():
    assert not decode_target(None)


def test_decode_target():
    actual_alias_decode_target = decode_target({
        'target_type': 'alias',
        'target': b'something',
    })

    assert actual_alias_decode_target == {
        'target_type': 'alias',
        'target': 'something',
    }

    actual_decode_target = decode_target({
        'target_type': 'revision',
        'target': hash_to_bytes(hash_hex),
    })

    assert actual_decode_target == {
        'target_type': 'revision',
        'target': hash_hex,
    }


def test_check_snapshot():
    storage = get_storage(cls='memory', args={})

    snap_id = '2498dbf535f882bc7f9a18fb16c9ad27fda7bab7'
    snapshot = {
        'id': hash_to_bytes(snap_id),
        'branches': {
            b'master': {
                'target': hash_to_bytes(hash_hex),
                'target_type': 'revision',
            },
        },
    }

    s = storage.snapshot_add([snapshot])
    assert s == {
        'snapshot:add': 1,
    }

    expected_snapshot = {
        'id': snap_id,
        'branches': {
            'master': {
                'target': hash_hex,
                'target_type': 'revision',
            }
        }
    }
    check_snapshot(expected_snapshot, storage)


def test_check_snapshot_failure():
    storage = get_storage(cls='memory', args={})

    snapshot = {
        'id': hash_to_bytes('2498dbf535f882bc7f9a18fb16c9ad27fda7bab7'),
        'branches': {
            b'master': {
                'target': hash_to_bytes(hash_hex),
                'target_type': 'revision',
            },
        },
    }

    s = storage.snapshot_add([snapshot])
    assert s == {
        'snapshot:add': 1,
    }

    unexpected_snapshot = {
        'id': '2498dbf535f882bc7f9a18fb16c9ad27fda7bab7',
        'branches': {
            'master': {
                'target': hash_hex,
                'target_type': 'release',  # wrong value
            }
        }
    }

    with pytest.raises(AssertionError):
        check_snapshot(unexpected_snapshot, storage)
