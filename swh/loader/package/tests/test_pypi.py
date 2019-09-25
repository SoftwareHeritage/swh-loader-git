# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

from os import path

import pytest

from unittest.mock import patch

from swh.core.tarball import uncompress
from swh.model.hashutil import hash_to_bytes
from swh.loader.package.pypi import (
    PyPILoader, pypi_api_url, pypi_info, author, sdist_parse
)

from swh.loader.package.tests.common import DATADIR, check_snapshot

from swh.loader.package.tests.conftest import local_get_factory


def test_author_basic():
    data = {
        'author': "i-am-groot",
        'author_email': 'iam@groot.org',
    }
    actual_author = author(data)

    expected_author = {
        'fullname': b'i-am-groot <iam@groot.org>',
        'name': b'i-am-groot',
        'email': b'iam@groot.org',
    }

    assert actual_author == expected_author


def test_author_empty_email():
    data = {
        'author': 'i-am-groot',
        'author_email': '',
    }
    actual_author = author(data)

    expected_author = {
        'fullname': b'i-am-groot',
        'name': b'i-am-groot',
        'email': b'',
    }

    assert actual_author == expected_author


def test_author_empty_name():
    data = {
        'author': "",
        'author_email': 'iam@groot.org',
    }
    actual_author = author(data)

    expected_author = {
        'fullname': b' <iam@groot.org>',
        'name': b'',
        'email': b'iam@groot.org',
    }

    assert actual_author == expected_author


def test_author_malformed():
    data = {
        'author': "['pierre', 'paul', 'jacques']",
        'author_email': None,
    }

    actual_author = author(data)

    expected_author = {
        'fullname': b"['pierre', 'paul', 'jacques']",
        'name': b"['pierre', 'paul', 'jacques']",
        'email': None,
    }

    assert actual_author == expected_author


def test_author_malformed_2():
    data = {
        'author': '[marie, jeanne]',
        'author_email': '[marie@some, jeanne@thing]',
    }

    actual_author = author(data)

    expected_author = {
        'fullname': b'[marie, jeanne] <[marie@some, jeanne@thing]>',
        'name': b'[marie, jeanne]',
        'email': b'[marie@some, jeanne@thing]',
    }

    assert actual_author == expected_author


def test_author_malformed_3():
    data = {
        'author': '[marie, jeanne, pierre]',
        'author_email': '[marie@somewhere.org, jeanne@somewhere.org]',
    }

    actual_author = author(data)

    expected_author = {
        'fullname': b'[marie, jeanne, pierre] <[marie@somewhere.org, jeanne@somewhere.org]>',  # noqa
        'name': b'[marie, jeanne, pierre]',
        'email': b'[marie@somewhere.org, jeanne@somewhere.org]',
    }

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


def test_pypi_info_failure(requests_mock):
    """Failure to fetch info/release information should raise"""
    project_url = 'https://pypi.org/project/requests'
    info_url = 'https://pypi.org/pypi/requests/json'
    status_code = 400
    requests_mock.get(info_url, status_code=status_code)

    with pytest.raises(ValueError) as e0:
        pypi_info(project_url)

    assert e0.value.args[0] == "Fail to query '%s'. Reason: %s" % (
        info_url, status_code
    )


def test_pypi_info(requests_mock):
    """Fetching json info from pypi project should be ok"""
    url = 'https://pypi.org/project/requests'
    info_url = 'https://pypi.org/pypi/requests/json'
    requests_mock.get(info_url,
                      text='{"version": "0.0.1"}')
    actual_info = pypi_info(url)
    assert actual_info == {
        'version': '0.0.1',
    }


@pytest.mark.fs
def test_sdist_parse(tmp_path):
    """Parsing existing archive's PKG-INFO should yield results"""
    uncompressed_archive_path = str(tmp_path)
    archive_path = path.join(
        DATADIR, 'files.pythonhosted.org', '0805nexter-1.1.0.zip')
    uncompress(archive_path, dest=uncompressed_archive_path)

    actual_sdist = sdist_parse(uncompressed_archive_path)
    expected_sdist = {
        'metadata_version': '1.0',
        'name': '0805nexter',
        'version': '1.1.0',
        'summary': 'a simple printer of nested lest',
        'home_page': 'http://www.hp.com',
        'author': 'hgtkpython',
        'author_email': '2868989685@qq.com',
        'platforms': ['UNKNOWN'],
    }

    assert actual_sdist == expected_sdist


@pytest.mark.fs
def test_sdist_parse_failures(tmp_path):
    """Parsing inexistant path/archive/PKG-INFO yield None"""
    # inexistant first level path
    assert sdist_parse('/something-inexistant') == {}
    # inexistant second level path (as expected by pypi archives)
    assert sdist_parse(tmp_path) == {}
    # inexistant PKG-INFO within second level path
    existing_path_no_pkginfo = str(tmp_path / 'something')
    os.mkdir(existing_path_no_pkginfo)
    assert sdist_parse(tmp_path) == {}


# LOADER SCENARIO #

# "edge" cases (for the same origin) #


# no release artifact:
# {visit full, status: uneventful, no contents, etc...}
local_get_missing_all = local_get_factory(ignore_urls=[
    'https://files.pythonhosted.org/packages/ec/65/c0116953c9a3f47de89e71964d6c7b0c783b01f29fa3390584dbf3046b4d/0805nexter-1.1.0.zip',  # noqa
    'https://files.pythonhosted.org/packages/c4/a0/4562cda161dc4ecbbe9e2a11eb365400c0461845c5be70d73869786809c4/0805nexter-1.2.0.zip',  # noqa
])


def test_no_release_artifact(swh_config, local_get_missing_all):
    """Load a pypi project with all artifacts missing ends up with no snapshot

    """
    url = 'https://pypi.org/project/0805nexter'
    loader = PyPILoader(url)

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


# problem during loading:
# {visit: partial, status: uneventful, no snapshot}


def test_release_with_traceback(swh_config):
    url = 'https://pypi.org/project/0805nexter'
    with patch('swh.loader.package.pypi.PyPILoader.get_default_release',
               side_effect=ValueError('Problem')):
        loader = PyPILoader(url)

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


# problem during loading: failure early enough in between swh contents...
# some contents (contents, directories, etc...) have been written in storage
# {visit: partial, status: eventful, no snapshot}

# problem during loading: failure late enough we can have snapshots (some
# revisions are written in storage already)
# {visit: partial, status: eventful, snapshot}

# "normal" cases (for the same origin) #


local_get_missing_one = local_get_factory(ignore_urls=[
    'https://files.pythonhosted.org/packages/ec/65/c0116953c9a3f47de89e71964d6c7b0c783b01f29fa3390584dbf3046b4d/0805nexter-1.1.0.zip',  # noqa
])

# some missing release artifacts:
# {visit partial, status: eventful, 1 snapshot}


def test_release_with_missing_artifact(swh_config, local_get_missing_one):
    """Load a pypi project with some missing artifacts ends up with 1 snapshot

    """
    url = 'https://pypi.org/project/0805nexter'
    loader = PyPILoader(url)

    actual_load_status = loader.load()

    assert actual_load_status == {'status': 'eventful'}

    stats = loader.storage.stat_counters()
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

    check_snapshot(
        'dd0e4201a232b1c104433741dbf45895b8ac9355',
        expected_branches,
        storage=loader.storage)

    origin_visit = next(loader.storage.origin_visit_get(url))
    assert origin_visit['status'] == 'partial'


def test_release_artifact_no_prior_visit(swh_config, local_get):
    """With no prior visit, load a pypi project ends up with 1 snapshot

    """
    url = 'https://pypi.org/project/0805nexter'
    loader = PyPILoader(url)

    actual_load_status = loader.load()

    assert actual_load_status == {'status': 'eventful'}

    stats = loader.storage.stat_counters()
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

    check_snapshot(
        'ba6e158ada75d0b3cfb209ffdf6daa4ed34a227a',
        expected_branches,
        storage=loader.storage)

    origin_visit = next(loader.storage.origin_visit_get(url))
    assert origin_visit['status'] == 'full'


# release artifact, no new artifact
# {visit full, status uneventful, same snapshot as before}

# release artifact, new artifact
# {visit full, status full, new snapshot with shared history as prior snapshot}

# release artifact, old artifact with different checksums
# {visit full, status full, new snapshot with shared history and some new
# different history}
