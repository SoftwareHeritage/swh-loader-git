# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import re


from swh.model.hashutil import hash_to_bytes
from swh.loader.package.deposit.loader import DepositLoader

from swh.loader.package.tests.common import (
    check_snapshot, check_metadata_paths, get_stats
)

from swh.core.pytest_plugin import requests_mock_datadir_factory


def test_deposit_init_ok(swh_config, swh_loader_config):
    url = 'some-url'
    deposit_id = 999
    loader = DepositLoader(url, deposit_id)  # Something that does not exist

    assert loader.url == url
    assert loader.client is not None
    assert loader.client.base_url == swh_loader_config['deposit']['url']


def test_deposit_loading_unknown_deposit(
        swh_config, requests_mock_datadir):
    """Loading an unknown deposit should fail

    no origin, no visit, no snapshot
    """
    # private api url form: 'https://deposit.s.o/1/private/hal/666/raw/'
    url = 'some-url'
    unknown_deposit_id = 667
    loader = DepositLoader(url, unknown_deposit_id)  # does not exist

    actual_load_status = loader.load()
    assert actual_load_status == {'status': 'failed'}

    stats = get_stats(loader.storage)

    assert {
        'content': 0,
        'directory': 0,
        'origin': 0,
        'origin_visit': 0,
        'person': 0,
        'release': 0,
        'revision': 0,
        'skipped_content': 0,
        'snapshot': 0,
    } == stats


requests_mock_datadir_missing_one = requests_mock_datadir_factory(ignore_urls=[
    'https://deposit.softwareheritage.org/1/private/666/raw/',
])


def test_deposit_loading_failure_to_retrieve_1_artifact(
        swh_config, requests_mock_datadir_missing_one):
    """Deposit with missing artifact ends up with an uneventful/partial visit

    """
    # private api url form: 'https://deposit.s.o/1/private/hal/666/raw/'
    url = 'some-url-2'
    deposit_id = 666
    loader = DepositLoader(url, deposit_id)

    actual_load_status = loader.load()
    assert actual_load_status['status'] == 'uneventful'
    assert actual_load_status['snapshot_id'] is not None

    stats = get_stats(loader.storage)
    assert {
        'content': 0,
        'directory': 0,
        'origin': 1,
        'origin_visit': 1,
        'person': 0,
        'release': 0,
        'revision': 0,
        'skipped_content': 0,
        'snapshot': 1,
    } == stats

    origin_visit = next(loader.storage.origin_visit_get(url))
    assert origin_visit['status'] == 'partial'
    assert origin_visit['type'] == 'deposit'


def test_revision_metadata_structure(swh_config, requests_mock_datadir):
    # do not care for deposit update query
    requests_mock_datadir.put(re.compile('https'))

    url = 'https://hal-test.archives-ouvertes.fr/some-external-id'
    deposit_id = 666
    loader = DepositLoader(url, deposit_id)

    actual_load_status = loader.load()
    assert actual_load_status['status'] == 'eventful'
    assert actual_load_status['snapshot_id'] is not None
    expected_revision_id = hash_to_bytes(
        '637318680351f5d78856d13264faebbd91efe9bb')
    revision = list(loader.storage.revision_get([expected_revision_id]))[0]

    assert revision is not None

    check_metadata_paths(revision['metadata'], paths=[
        ('extrinsic.provider', str),
        ('extrinsic.when', str),
        ('extrinsic.raw', dict),
        ('original_artifact', list),
    ])

    for original_artifact in revision['metadata']['original_artifact']:
        check_metadata_paths(original_artifact, paths=[
            ('filename', str),
            ('length', int),
            ('checksums', dict),
        ])


def test_deposit_loading_ok(swh_config, requests_mock_datadir):
    requests_mock_datadir.put(re.compile('https'))  # do not care for put

    url = 'https://hal-test.archives-ouvertes.fr/some-external-id'
    deposit_id = 666
    loader = DepositLoader(url, deposit_id)

    actual_load_status = loader.load()
    expected_snapshot_id = 'b2b327b33dc85818bd23c3ccda8b7e675a66ecbd'
    assert actual_load_status == {
        'status': 'eventful',
        'snapshot_id': expected_snapshot_id,
    }

    stats = get_stats(loader.storage)
    assert {
        'content': 303,
        'directory': 12,
        'origin': 1,
        'origin_visit': 1,
        'person': 1,
        'release': 0,
        'revision': 1,
        'skipped_content': 0,
        'snapshot': 1,
    } == stats

    origin_visit = next(loader.storage.origin_visit_get(url))
    assert origin_visit['status'] == 'full'
    assert origin_visit['type'] == 'deposit'

    expected_branches = {
        'HEAD': {
            'target': '637318680351f5d78856d13264faebbd91efe9bb',
            'target_type': 'revision',
        },
    }

    expected_snapshot = {
        'id': expected_snapshot_id,
        'branches': expected_branches,
    }
    check_snapshot(expected_snapshot, storage=loader.storage)

    # check metadata

    tool = {
        "name": "swh-deposit",
        "version": "0.0.1",
        "configuration": {
            "sword_version": "2",
        }
    }

    tool = loader.storage.tool_get(tool)
    assert tool is not None
    assert tool['id'] is not None

    provider = {
        "provider_name": "hal",
        "provider_type": "deposit_client",
        "provider_url": "https://hal-test.archives-ouvertes.fr/",
        "metadata": None,
    }

    provider = loader.storage.metadata_provider_get_by(provider)
    assert provider is not None
    assert provider['id'] is not None

    metadata = list(loader.storage.origin_metadata_get_by(
        url, provider_type='deposit_client'))
    assert metadata is not None
    assert isinstance(metadata, list)
    assert len(metadata) == 1
    metadata0 = metadata[0]

    assert metadata0['provider_id'] == provider['id']
    assert metadata0['provider_type'] == 'deposit_client'
    assert metadata0['tool_id'] == tool['id']
