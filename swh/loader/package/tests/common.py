# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from os import path

import logging

from swh.model.hashutil import hash_to_bytes, hash_to_hex


logger = logging.getLogger(__file__)

DATADIR = path.join(path.abspath(path.dirname(__file__)), 'resources')


def decode_target(target):
    if not target:
        return target
    target_type = target['target_type']

    if target_type == 'alias':
        decoded_target = target['target'].decode('utf-8')
    else:
        decoded_target = hash_to_hex(target['target'])

    return {
        'target': decoded_target,
        'target_type': target_type
    }


def check_snapshot(expected_snapshot, expected_branches, storage):
    """Check for snapshot match.

    Provide the hashes as hexadecimal, the conversion is done
    within the method.

    Args:
        expected_snapshot (Union[str, dict]): Either the snapshot
                                      identifier or the full
                                      snapshot
        expected_branches ([dict]): expected branches or nothing is
                                  the full snapshot is provided

    """
    if isinstance(expected_snapshot, dict) and not expected_branches:
        expected_snapshot_id = expected_snapshot['id']
        expected_branches = expected_snapshot['branches']
    else:
        expected_snapshot_id = expected_snapshot

    snap = storage.snapshot_get(hash_to_bytes(expected_snapshot_id))
    assert snap is not None

    branches = {
        branch.decode('utf-8'): decode_target(target)
        for branch, target in snap['branches'].items()
    }
    assert expected_branches == branches
