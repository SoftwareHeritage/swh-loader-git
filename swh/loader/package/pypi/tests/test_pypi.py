# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

from os import path

import pytest

from unittest.mock import patch

from swh.core.tarball import uncompress
from swh.core.pytest_plugin import requests_mock_datadir_factory
from swh.model.hashutil import hash_to_bytes
from swh.model.model import Person

from swh.loader.package.pypi.loader import (
    PyPILoader, pypi_api_url, author, extract_intrinsic_metadata,
    artifact_to_revision_id
)
from swh.loader.package.tests.common import (
    check_snapshot, check_metadata_paths, get_stats
)


def test_author_basic():
    data = {
        'author': "i-am-groot",
        'author_email': 'iam@groot.org',
    }
    actual_author = author(data)

    expected_author = Person(
        fullname=b'i-am-groot <iam@groot.org>',
        name=b'i-am-groot',
        email=b'iam@groot.org',
    )

    assert actual_author == expected_author


def test_author_empty_email():
    data = {
        'author': 'i-am-groot',
        'author_email': '',
    }
    actual_author = author(data)

    expected_author = Person(
        fullname=b'i-am-groot',
        name=b'i-am-groot',
        email=b'',
    )

    assert actual_author == expected_author


def test_author_empty_name():
    data = {
        'author': "",
        'author_email': 'iam@groot.org',
    }
    actual_author = author(data)

    expected_author = Person(
        fullname=b' <iam@groot.org>',
        name=b'',
        email=b'iam@groot.org',
    )

    assert actual_author == expected_author


def test_author_malformed():
    data = {
        'author': "['pierre', 'paul', 'jacques']",
        'author_email': None,
    }

    actual_author = author(data)

    expected_author = Person(
        fullname=b"['pierre', 'paul', 'jacques']",
        name=b"['pierre', 'paul', 'jacques']",
        email=None,
    )

    assert actual_author == expected_author


def test_author_malformed_2():
    data = {
        'author': '[marie, jeanne]',
        'author_email': '[marie@some, jeanne@thing]',
    }

    actual_author = author(data)

    expected_author = Person(
        fullname=b'[marie, jeanne] <[marie@some, jeanne@thing]>',
        name=b'[marie, jeanne]',
        email=b'[marie@some, jeanne@thing]',
    )

    assert actual_author == expected_author


def test_author_malformed_3():
    data = {
        'author': '[marie, jeanne, pierre]',
        'author_email': '[marie@somewhere.org, jeanne@somewhere.org]',
    }

    actual_author = author(data)

    expected_author = Person(
        fullname=(
            b'[marie, jeanne, pierre] '
            b'<[marie@somewhere.org, jeanne@somewhere.org]>'
        ),
        name=b'[marie, jeanne, pierre]',
        email=b'[marie@somewhere.org, jeanne@somewhere.org]',
    )

    actual_author == expected_author


# configuration error #

def test_badly_configured_loader_raise(monkeypatch):
    """Badly configured loader should raise"""
    monkeypatch.delenv('SWH_CONFIG_FILENAME', raising=False)
    with pytest.raises(ValueError) as e:
        PyPILoader(url='some-url')

    assert 'Misconfiguration' in e.value.args[0]


def test_pypi_api_url():
    """Compute pypi api url from the pypi project url should be ok"""
    url = pypi_api_url('https://pypi.org/project/requests')
    assert url == 'https://pypi.org/pypi/requests/json'


def test_pypi_api_url_with_slash():
    """Compute pypi api url from the pypi project url should be ok"""
    url = pypi_api_url('https://pypi.org/project/requests/')
    assert url == 'https://pypi.org/pypi/requests/json'


@pytest.mark.fs
def test_extract_intrinsic_metadata(tmp_path, datadir):
    """Parsing existing archive's PKG-INFO should yield results"""
    uncompressed_archive_path = str(tmp_path)
    archive_path = path.join(
        datadir, 'https_files.pythonhosted.org', '0805nexter-1.1.0.zip')
    uncompress(archive_path, dest=uncompressed_archive_path)

    actual_metadata = extract_intrinsic_metadata(uncompressed_archive_path)
    expected_metadata = {
        'metadata_version': '1.0',
        'name': '0805nexter',
        'version': '1.1.0',
        'summary': 'a simple printer of nested lest',
        'home_page': 'http://www.hp.com',
        'author': 'hgtkpython',
        'author_email': '2868989685@qq.com',
        'platforms': ['UNKNOWN'],
    }

    assert actual_metadata == expected_metadata


@pytest.mark.fs
def test_extract_intrinsic_metadata_failures(tmp_path):
    """Parsing inexistent path/archive/PKG-INFO yield None"""
    tmp_path = str(tmp_path)  # py3.5 work around (PosixPath issue)
    # inexistent first level path
    assert extract_intrinsic_metadata('/something-inexistent') == {}
    # inexistent second level path (as expected by pypi archives)
    assert extract_intrinsic_metadata(tmp_path) == {}
    # inexistent PKG-INFO within second level path
    existing_path_no_pkginfo = path.join(tmp_path, 'something')
    os.mkdir(existing_path_no_pkginfo)
    assert extract_intrinsic_metadata(tmp_path) == {}


# LOADER SCENARIO #

# "edge" cases (for the same origin) #


# no release artifact:
# {visit full, status: uneventful, no contents, etc...}
requests_mock_datadir_missing_all = requests_mock_datadir_factory(ignore_urls=[
    'https://files.pythonhosted.org/packages/ec/65/c0116953c9a3f47de89e71964d6c7b0c783b01f29fa3390584dbf3046b4d/0805nexter-1.1.0.zip',  # noqa
    'https://files.pythonhosted.org/packages/c4/a0/4562cda161dc4ecbbe9e2a11eb365400c0461845c5be70d73869786809c4/0805nexter-1.2.0.zip',  # noqa
])


def test_no_release_artifact(swh_config, requests_mock_datadir_missing_all):
    """Load a pypi project with all artifacts missing ends up with no snapshot

    """
    url = 'https://pypi.org/project/0805nexter'
    loader = PyPILoader(url)

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
    assert origin_visit['type'] == 'pypi'


# problem during loading:
# {visit: partial, status: uneventful, no snapshot}


def test_release_with_traceback(swh_config, requests_mock_datadir):
    url = 'https://pypi.org/project/0805nexter'
    with patch('swh.loader.package.pypi.loader.PyPILoader.last_snapshot',
               side_effect=ValueError('Fake problem to fail the visit')):
        loader = PyPILoader(url)

        actual_load_status = loader.load()
        assert actual_load_status['status'] == 'failed'
        assert actual_load_status[
            'snapshot_id'] == '1a8893e6a86f444e8be8e7bda6cb34fb1735a00e'

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
    assert origin_visit['type'] == 'pypi'


# problem during loading: failure early enough in between swh contents...
# some contents (contents, directories, etc...) have been written in storage
# {visit: partial, status: eventful, no snapshot}

# problem during loading: failure late enough we can have snapshots (some
# revisions are written in storage already)
# {visit: partial, status: eventful, snapshot}

# "normal" cases (for the same origin) #


requests_mock_datadir_missing_one = requests_mock_datadir_factory(ignore_urls=[
    'https://files.pythonhosted.org/packages/ec/65/c0116953c9a3f47de89e71964d6c7b0c783b01f29fa3390584dbf3046b4d/0805nexter-1.1.0.zip',  # noqa
])

# some missing release artifacts:
# {visit partial, status: eventful, 1 snapshot}


def test_revision_metadata_structure(swh_config, requests_mock_datadir):
    url = 'https://pypi.org/project/0805nexter'
    loader = PyPILoader(url)

    actual_load_status = loader.load()
    assert actual_load_status['status'] == 'eventful'
    assert actual_load_status['snapshot_id'] is not None

    expected_revision_id = hash_to_bytes(
        'e445da4da22b31bfebb6ffc4383dbf839a074d21')
    revision = list(loader.storage.revision_get([expected_revision_id]))[0]

    assert revision is not None

    check_metadata_paths(revision['metadata'], paths=[
        ('intrinsic.tool', str),
        ('intrinsic.raw', dict),
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


def test_visit_with_missing_artifact(
        swh_config, requests_mock_datadir_missing_one):
    """Load a pypi project with some missing artifacts ends up with 1 snapshot

    """
    url = 'https://pypi.org/project/0805nexter'
    loader = PyPILoader(url)

    actual_load_status = loader.load()
    expected_snapshot_id = 'dd0e4201a232b1c104433741dbf45895b8ac9355'
    assert actual_load_status == {
        'status': 'eventful',
        'snapshot_id': expected_snapshot_id
    }

    stats = get_stats(loader.storage)

    assert {
        'content': 3,
        'directory': 2,
        'origin': 1,
        'origin_visit': 1,
        'person': 1,
        'release': 0,
        'revision': 1,
        'skipped_content': 0,
        'snapshot': 1
    } == stats

    expected_contents = map(hash_to_bytes, [
        '405859113963cb7a797642b45f171d6360425d16',
        'e5686aa568fdb1d19d7f1329267082fe40482d31',
        '83ecf6ec1114fd260ca7a833a2d165e71258c338',
    ])

    assert list(loader.storage.content_missing_per_sha1(expected_contents))\
        == []

    expected_dirs = map(hash_to_bytes, [
        'b178b66bd22383d5f16f4f5c923d39ca798861b4',
        'c3a58f8b57433a4b56caaa5033ae2e0931405338',
    ])

    assert list(loader.storage.directory_missing(expected_dirs)) == []

    # {revision hash: directory hash}
    expected_revs = {
        hash_to_bytes('e445da4da22b31bfebb6ffc4383dbf839a074d21'): hash_to_bytes('b178b66bd22383d5f16f4f5c923d39ca798861b4'),  # noqa
    }
    assert list(loader.storage.revision_missing(expected_revs)) == []

    expected_branches = {
        'releases/1.2.0': {
            'target': 'e445da4da22b31bfebb6ffc4383dbf839a074d21',
            'target_type': 'revision',
        },
        'HEAD': {
            'target': 'releases/1.2.0',
            'target_type': 'alias',
        },
    }

    expected_snapshot = {
        'id': expected_snapshot_id,
        'branches': expected_branches,
    }
    check_snapshot(expected_snapshot, storage=loader.storage)

    origin_visit = next(loader.storage.origin_visit_get(url))
    assert origin_visit['status'] == 'partial'
    assert origin_visit['type'] == 'pypi'


def test_visit_with_1_release_artifact(swh_config, requests_mock_datadir):
    """With no prior visit, load a pypi project ends up with 1 snapshot

    """
    url = 'https://pypi.org/project/0805nexter'
    loader = PyPILoader(url)

    actual_load_status = loader.load()
    expected_snapshot_id = 'ba6e158ada75d0b3cfb209ffdf6daa4ed34a227a'
    assert actual_load_status == {
        'status': 'eventful',
        'snapshot_id': expected_snapshot_id
    }

    stats = get_stats(loader.storage)
    assert {
        'content': 6,
        'directory': 4,
        'origin': 1,
        'origin_visit': 1,
        'person': 1,
        'release': 0,
        'revision': 2,
        'skipped_content': 0,
        'snapshot': 1
    } == stats

    expected_contents = map(hash_to_bytes, [
        'a61e24cdfdab3bb7817f6be85d37a3e666b34566',
        '938c33483285fd8ad57f15497f538320df82aeb8',
        'a27576d60e08c94a05006d2e6d540c0fdb5f38c8',
        '405859113963cb7a797642b45f171d6360425d16',
        'e5686aa568fdb1d19d7f1329267082fe40482d31',
        '83ecf6ec1114fd260ca7a833a2d165e71258c338',
    ])

    assert list(loader.storage.content_missing_per_sha1(expected_contents))\
        == []

    expected_dirs = map(hash_to_bytes, [
        '05219ba38bc542d4345d5638af1ed56c7d43ca7d',
        'cf019eb456cf6f78d8c4674596f1c9a97ece8f44',
        'b178b66bd22383d5f16f4f5c923d39ca798861b4',
        'c3a58f8b57433a4b56caaa5033ae2e0931405338',
    ])

    assert list(loader.storage.directory_missing(expected_dirs)) == []

    # {revision hash: directory hash}
    expected_revs = {
        hash_to_bytes('4c99891f93b81450385777235a37b5e966dd1571'): hash_to_bytes('05219ba38bc542d4345d5638af1ed56c7d43ca7d'),  # noqa
        hash_to_bytes('e445da4da22b31bfebb6ffc4383dbf839a074d21'): hash_to_bytes('b178b66bd22383d5f16f4f5c923d39ca798861b4'),  # noqa
    }
    assert list(loader.storage.revision_missing(expected_revs)) == []

    expected_branches = {
        'releases/1.1.0': {
            'target': '4c99891f93b81450385777235a37b5e966dd1571',
            'target_type': 'revision',
        },
        'releases/1.2.0': {
            'target': 'e445da4da22b31bfebb6ffc4383dbf839a074d21',
            'target_type': 'revision',
        },
        'HEAD': {
            'target': 'releases/1.2.0',
            'target_type': 'alias',
        },
    }

    expected_snapshot = {
        'id': expected_snapshot_id,
        'branches': expected_branches,
    }
    check_snapshot(expected_snapshot, loader.storage)

    origin_visit = next(loader.storage.origin_visit_get(url))
    assert origin_visit['status'] == 'full'
    assert origin_visit['type'] == 'pypi'


def test_multiple_visits_with_no_change(swh_config, requests_mock_datadir):
    """Multiple visits with no changes results in 1 same snapshot

    """
    url = 'https://pypi.org/project/0805nexter'
    loader = PyPILoader(url)

    actual_load_status = loader.load()
    snapshot_id = 'ba6e158ada75d0b3cfb209ffdf6daa4ed34a227a'
    assert actual_load_status == {
        'status': 'eventful',
        'snapshot_id': snapshot_id,
    }

    stats = get_stats(loader.storage)

    assert {
        'content': 6,
        'directory': 4,
        'origin': 1,
        'origin_visit': 1,
        'person': 1,
        'release': 0,
        'revision': 2,
        'skipped_content': 0,
        'snapshot': 1
    } == stats

    expected_branches = {
        'releases/1.1.0': {
            'target': '4c99891f93b81450385777235a37b5e966dd1571',
            'target_type': 'revision',
        },
        'releases/1.2.0': {
            'target': 'e445da4da22b31bfebb6ffc4383dbf839a074d21',
            'target_type': 'revision',
        },
        'HEAD': {
            'target': 'releases/1.2.0',
            'target_type': 'alias',
        },
    }

    expected_snapshot = {
        'id': snapshot_id,
        'branches': expected_branches,
    }
    check_snapshot(expected_snapshot, loader.storage)

    origin_visit = next(loader.storage.origin_visit_get(url))
    assert origin_visit['status'] == 'full'
    assert origin_visit['type'] == 'pypi'

    actual_load_status2 = loader.load()
    assert actual_load_status2 == {
        'status': 'uneventful',
        'snapshot_id': actual_load_status2['snapshot_id']
    }

    stats2 = get_stats(loader.storage)
    expected_stats2 = stats.copy()
    expected_stats2['origin_visit'] = 1 + 1
    assert expected_stats2 == stats2

    # same snapshot
    actual_snapshot_id = origin_visit['snapshot']
    assert actual_snapshot_id == hash_to_bytes(snapshot_id)


def test_incremental_visit(swh_config, requests_mock_datadir_visits):
    """With prior visit, 2nd load will result with a different snapshot

    """
    url = 'https://pypi.org/project/0805nexter'
    loader = PyPILoader(url)

    visit1_actual_load_status = loader.load()
    visit1_stats = get_stats(loader.storage)
    expected_snapshot_id = 'ba6e158ada75d0b3cfb209ffdf6daa4ed34a227a'
    assert visit1_actual_load_status == {
        'status': 'eventful',
        'snapshot_id': expected_snapshot_id
    }

    origin_visit1 = next(loader.storage.origin_visit_get(url))
    assert origin_visit1['status'] == 'full'
    assert origin_visit1['type'] == 'pypi'

    assert {
        'content': 6,
        'directory': 4,
        'origin': 1,
        'origin_visit': 1,
        'person': 1,
        'release': 0,
        'revision': 2,
        'skipped_content': 0,
        'snapshot': 1
    } == visit1_stats

    # Reset internal state
    loader._info = None

    visit2_actual_load_status = loader.load()
    visit2_stats = get_stats(loader.storage)

    assert visit2_actual_load_status['status'] == 'eventful'
    expected_snapshot_id2 = '2e5149a7b0725d18231a37b342e9b7c4e121f283'
    assert visit2_actual_load_status == {
        'status': 'eventful',
        'snapshot_id': expected_snapshot_id2
    }

    visits = list(loader.storage.origin_visit_get(url))
    assert len(visits) == 2
    assert visits[1]['status'] == 'full'
    assert visits[1]['type'] == 'pypi'

    assert {
        'content': 6 + 1,     # 1 more content
        'directory': 4 + 2,   # 2 more directories
        'origin': 1,
        'origin_visit': 1 + 1,
        'person': 1,
        'release': 0,
        'revision': 2 + 1,    # 1 more revision
        'skipped_content': 0,
        'snapshot': 1 + 1,    # 1 more snapshot
    } == visit2_stats

    expected_contents = map(hash_to_bytes, [
        'a61e24cdfdab3bb7817f6be85d37a3e666b34566',
        '938c33483285fd8ad57f15497f538320df82aeb8',
        'a27576d60e08c94a05006d2e6d540c0fdb5f38c8',
        '405859113963cb7a797642b45f171d6360425d16',
        'e5686aa568fdb1d19d7f1329267082fe40482d31',
        '83ecf6ec1114fd260ca7a833a2d165e71258c338',
        '92689fa2b7fb4d4fc6fb195bf73a50c87c030639'
    ])

    assert list(loader.storage.content_missing_per_sha1(expected_contents))\
        == []

    expected_dirs = map(hash_to_bytes, [
        '05219ba38bc542d4345d5638af1ed56c7d43ca7d',
        'cf019eb456cf6f78d8c4674596f1c9a97ece8f44',
        'b178b66bd22383d5f16f4f5c923d39ca798861b4',
        'c3a58f8b57433a4b56caaa5033ae2e0931405338',
        'e226e7e4ad03b4fc1403d69a18ebdd6f2edd2b3a',
        '52604d46843b898f5a43208045d09fcf8731631b',

    ])

    assert list(loader.storage.directory_missing(expected_dirs)) == []

    # {revision hash: directory hash}
    expected_revs = {
        hash_to_bytes('4c99891f93b81450385777235a37b5e966dd1571'): hash_to_bytes('05219ba38bc542d4345d5638af1ed56c7d43ca7d'),  # noqa
        hash_to_bytes('e445da4da22b31bfebb6ffc4383dbf839a074d21'): hash_to_bytes('b178b66bd22383d5f16f4f5c923d39ca798861b4'),  # noqa
        hash_to_bytes('51247143b01445c9348afa9edfae31bf7c5d86b1'): hash_to_bytes('e226e7e4ad03b4fc1403d69a18ebdd6f2edd2b3a'),  # noqa
    }

    assert list(loader.storage.revision_missing(expected_revs)) == []

    expected_branches = {
        'releases/1.1.0': {
            'target': '4c99891f93b81450385777235a37b5e966dd1571',
            'target_type': 'revision',
        },
        'releases/1.2.0': {
            'target': 'e445da4da22b31bfebb6ffc4383dbf839a074d21',
            'target_type': 'revision',
        },
        'releases/1.3.0': {
            'target': '51247143b01445c9348afa9edfae31bf7c5d86b1',
            'target_type': 'revision',
        },
        'HEAD': {
            'target': 'releases/1.3.0',
            'target_type': 'alias',
        },
    }
    expected_snapshot = {
        'id': expected_snapshot_id2,
        'branches': expected_branches,
    }

    check_snapshot(expected_snapshot, loader.storage)

    origin_visit = list(loader.storage.origin_visit_get(url))[-1]
    assert origin_visit['status'] == 'full'
    assert origin_visit['type'] == 'pypi'

    urls = [
        m.url for m in requests_mock_datadir_visits.request_history
        if m.url.startswith('https://files.pythonhosted.org')
    ]
    # visited each artifact once across 2 visits
    assert len(urls) == len(set(urls))


# release artifact, no new artifact
# {visit full, status uneventful, same snapshot as before}

# release artifact, old artifact with different checksums
# {visit full, status full, new snapshot with shared history and some new
# different history}

# release with multiple sdist artifacts per pypi "version"
# snapshot branch output is different

def test_visit_1_release_with_2_artifacts(swh_config, requests_mock_datadir):
    """With no prior visit, load a pypi project ends up with 1 snapshot

    """
    url = 'https://pypi.org/project/nexter'
    loader = PyPILoader(url)

    actual_load_status = loader.load()
    expected_snapshot_id = 'a27e638a4dad6fbfa273c6ebec1c4bf320fb84c6'
    assert actual_load_status == {
        'status': 'eventful',
        'snapshot_id': expected_snapshot_id,
    }

    expected_branches = {
        'releases/1.1.0/nexter-1.1.0.zip': {
            'target': '4c99891f93b81450385777235a37b5e966dd1571',
            'target_type': 'revision',
        },
        'releases/1.1.0/nexter-1.1.0.tar.gz': {
            'target': '0bf88f5760cca7665d0af4d6575d9301134fe11a',
            'target_type': 'revision',
        },
    }

    expected_snapshot = {
        'id': expected_snapshot_id,
        'branches': expected_branches,
    }
    check_snapshot(expected_snapshot, loader.storage)

    origin_visit = next(loader.storage.origin_visit_get(url))
    assert origin_visit['status'] == 'full'
    assert origin_visit['type'] == 'pypi'


def test_pypi_artifact_to_revision_id_none():
    """Current loader version should stop soon if nothing can be found

    """
    artifact_metadata = {
        'digests': {
            'sha256': '6975816f2c5ad4046acc676ba112f2fff945b01522d63948531f11f11e0892ec',  # noqa
        },
    }

    assert artifact_to_revision_id({}, artifact_metadata) is None

    known_artifacts = {
        'b11ebac8c9d0c9e5063a2df693a18e3aba4b2f92': {
            'original_artifact': {
                'sha256': 'something-irrelevant',
            },
        },
    }

    assert artifact_to_revision_id(known_artifacts, artifact_metadata) is None


def test_pypi_artifact_to_revision_id_old_loader_version():
    """Current loader version should solve old metadata scheme

    """
    artifact_metadata = {
        'digests': {
            'sha256': '6975816f2c5ad4046acc676ba112f2fff945b01522d63948531f11f11e0892ec',  # noqa
        }
    }

    known_artifacts = {
        hash_to_bytes('b11ebac8c9d0c9e5063a2df693a18e3aba4b2f92'): {
            'original_artifact': {
                'sha256': "something-wrong",
             },
        },
        hash_to_bytes('845673bfe8cbd31b1eaf757745a964137e6f9116'): {
            'original_artifact': {
                'sha256': '6975816f2c5ad4046acc676ba112f2fff945b01522d63948531f11f11e0892ec',  # noqa
             },
        }
    }

    assert artifact_to_revision_id(known_artifacts, artifact_metadata) \
        == hash_to_bytes('845673bfe8cbd31b1eaf757745a964137e6f9116')


def test_pypi_artifact_to_revision_id_current_loader_version():
    """Current loader version should be able to solve current metadata scheme

    """
    artifact_metadata = {
        'digests': {
            'sha256': '6975816f2c5ad4046acc676ba112f2fff945b01522d63948531f11f11e0892ec', # noqa
        }
    }

    known_artifacts = {
        hash_to_bytes('b11ebac8c9d0c9e5063a2df693a18e3aba4b2f92'): {
            'original_artifact': [{
                'checksums': {
                    'sha256': '6975816f2c5ad4046acc676ba112f2fff945b01522d63948531f11f11e0892ec',  # noqa
                },
            }],
        },
        hash_to_bytes('845673bfe8cbd31b1eaf757745a964137e6f9116'): {
            'original_artifact': [{
                'checksums': {
                    'sha256': 'something-wrong'
                },
            }],
        },
    }

    assert artifact_to_revision_id(known_artifacts, artifact_metadata) \
        == hash_to_bytes('b11ebac8c9d0c9e5063a2df693a18e3aba4b2f92')


def test_pypi_artifact_to_revision_id_failures():
    with pytest.raises(KeyError, match='sha256'):
        artifact_metadata = {
            'digests': {},
        }
        assert artifact_to_revision_id({}, artifact_metadata)

    with pytest.raises(KeyError, match='digests'):
        artifact_metadata = {
            'something': 'wrong',
        }
        assert artifact_to_revision_id({}, artifact_metadata)


def test_pypi_artifact_with_no_intrinsic_metadata(
        swh_config, requests_mock_datadir):
    """Skip artifact with no intrinsic metadata during ingestion

    """
    url = 'https://pypi.org/project/upymenu'
    loader = PyPILoader(url)

    actual_load_status = loader.load()
    expected_snapshot_id = '1a8893e6a86f444e8be8e7bda6cb34fb1735a00e'
    assert actual_load_status == {
        'status': 'eventful',
        'snapshot_id': expected_snapshot_id,
    }

    # no branch as one artifact without any intrinsic metadata
    expected_snapshot = {
        'id': expected_snapshot_id,
        'branches': {}
    }
    check_snapshot(expected_snapshot, loader.storage)

    origin_visit = next(loader.storage.origin_visit_get(url))
    assert origin_visit['status'] == 'full'
    assert origin_visit['type'] == 'pypi'