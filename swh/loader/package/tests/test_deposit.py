# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.loader.package.deposit import DepositLoader


def test_deposit_init_ok(swh_config):
    url = 'some-url'
    deposit_id = 999
    loader = DepositLoader(url, deposit_id)  # Something that does not exist

    assert loader.url == url
    assert loader.archive_url == '/%s/raw/' % deposit_id
    assert loader.metadata_url == '/%s/meta/' % deposit_id
    assert loader.deposit_update_url == '/%s/update/' % deposit_id
    assert loader.client is not None


def test_deposit_loading_failure_to_retrieve_artifact(swh_config):
    """Error during fetching artifact ends us with partial visit

    """
    # private api url form: 'https://deposit.s.o/1/private/hal/666/raw/'
    url = 'some-url'
    unknown_deposit_id = 666
    loader = DepositLoader(url, unknown_deposit_id)  # does not exist

    assert loader.archive_url
    actual_load_status = loader.load()

    assert actual_load_status == {'status': 'uneventful'}

    stats = loader.storage.stat_counters()

    assert {
        'content': 0,
        'directory': 0,
        'origin': 1,
        'origin_visit': 1,
        'person': 0,
        'release': 0,
        'revision': 0,
        'skipped_content': 0,
        'snapshot': 0,
    } == stats

    origin_visit = next(loader.storage.origin_visit_get(url))
    assert origin_visit['status'] == 'partial'


def test_deposit_loading_ok(swh_config, local_get):
    url = 'https://hal-test.archives-ouvertes.fr/some-external-id'
    deposit_id = 666
    loader = DepositLoader(url, deposit_id)

    assert loader.archive_url
    actual_load_status = loader.load()

    assert actual_load_status == {'status': 'eventful'}

    stats = loader.storage.stat_counters()
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
