# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import re

from os import path
from urllib.parse import urlparse

import pytest

from swh.core.tarball import uncompress
from swh.model.hashutil import hash_to_bytes, hash_to_hex
from swh.loader.package.pypi import (
    PyPILoader, PyPIClient, author, sdist_parse, download
)

DATADIR = path.join(path.abspath(path.dirname(__file__)), 'resources')


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
    monkeypatch.delenv('SWH_CONFIG_FILENAME')
    with pytest.raises(ValueError) as e:
        PyPILoader(url='some-url')

    assert 'Misconfiguration' in e.value.args[0]


def test_pypiclient_init():
    """Initialization should set the api's base project url"""
    project_url = 'https://pypi.org/project/requests'
    expected_base_url = 'https://pypi.org/pypi/requests'
    pypi_client = PyPIClient(url=project_url)

    assert pypi_client.url == expected_base_url


def test_pypiclient_failure(requests_mock):
    """Failure to fetch info/release information should raise"""
    project_url = 'https://pypi.org/project/requests'
    pypi_client = PyPIClient(url=project_url)

    expected_status_code = 400
    info_url = '%s/json' % pypi_client.url
    requests_mock.get(info_url, status_code=expected_status_code)

    with pytest.raises(ValueError) as e0:
        pypi_client.info_project()

    assert e0.value.args[0] == "Fail to query '%s'. Reason: %s" % (
        info_url, expected_status_code
    )


def test_pypiclient(requests_mock):
    """Fetching info/release info should be ok"""
    pypi_client = PyPIClient('https://pypi.org/project/requests')

    info_url = '%s/json' % pypi_client.url
    requests_mock.get(info_url, text='{"version": "0.0.1"}')
    actual_info = pypi_client.info_project()
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
    assert sdist_parse('/something-inexistant') is None
    # inexistant second level path (as expected by pypi archives)
    assert sdist_parse(tmp_path) is None
    # inexistant PKG-INFO within second level path
    existing_path_no_pkginfo = str(tmp_path / 'something')
    os.mkdir(existing_path_no_pkginfo)
    assert sdist_parse(tmp_path) is None


@pytest.mark.fs
def test_download_fail_to_download(tmp_path, requests_mock):
    url = 'https://pypi.org/pypi/arrow/json'
    status_code = 404
    requests_mock.get(url, status_code=status_code)

    with pytest.raises(ValueError) as e:
        download(url, tmp_path)

    assert e.value.args[0] == "Fail to query '%s'. Reason: %s" % (
        url, status_code)


@pytest.mark.fs
def test_download_fail_length_mismatch(tmp_path, requests_mock):
    """Mismatch length after download should raise

    """
    filename = 'requests-0.0.1.tar.gz'
    url = 'https://pypi.org/pypi/requests/%s' % filename
    data = 'this is something'
    wrong_size = len(data) - 3
    requests_mock.get(url, text=data, headers={
        'content-length': str(wrong_size)  # wrong size!
    })

    with pytest.raises(ValueError) as e:
        download(url, dest=str(tmp_path))

    assert e.value.args[0] == "Error when checking size: %s != %s" % (
        wrong_size, len(data)
    )


@pytest.mark.fs
def test_download_ok(tmp_path, requests_mock):
    """Download without issue should provide filename and hashes"""
    filename = 'requests-0.0.1.tar.gz'
    url = 'https://pypi.org/pypi/requests/%s' % filename
    data = 'this is something'
    requests_mock.get(url, text=data, headers={
        'content-length': str(len(data))
    })

    actual_filepath, actual_hashes = download(url, dest=str(tmp_path))

    actual_filename = os.path.basename(actual_filepath)
    assert actual_filename == filename
    assert actual_hashes['length'] == len(data)
    assert actual_hashes['sha1'] == 'fdd1ce606a904b08c816ba84f3125f2af44d92b2'
    assert (actual_hashes['sha256'] ==
            '1d9224378d77925d612c9f926eb9fb92850e6551def8328011b6a972323298d5')


@pytest.mark.fs
def test_download_fail_hashes_mismatch(tmp_path, requests_mock):
    """Mismatch hash after download should raise

    """
    pass


# LOADER SCENARIO #


def get_response_cb(request, context):
    """"""
    url = urlparse(request.url)
    dirname = url.hostname  # pypi.org | files.pythonhosted.org
    # url.path: pypi/<project>/json -> local file: pypi_<project>_json
    filename = url.path[1:].replace('/', '_')
    filepath = path.join(DATADIR, dirname, filename)
    fd = open(filepath, 'rb')
    context.headers['content-length'] = str(os.path.getsize(filepath))
    return fd

# "edge" cases (for the same origin) #


def test_no_release_artifact(requests_mock):
    pass


# no release artifact:
# {visit full, status: uneventful, no contents, etc...}

# problem during loading:
# {visit: partial, status: uneventful, no snapshot}

# problem during loading: failure early enough in between swh contents...
# some contents (contents, directories, etc...) have been written in storage
# {visit: partial, status: eventful, no snapshot}

# problem during loading: failure late enough we can have snapshots (some
# revisions are written in storage already)
# {visit: partial, status: eventful, snapshot}

# "normal" cases (for the same origin) #

def test_release_artifact_no_prior_visit(requests_mock):
    """With no prior visit, load a pypi project ends up with 1 snapshot

    """
    assert 'SWH_CONFIG_FILENAME' in os.environ  # cf. tox.ini

    loader = PyPILoader('https://pypi.org/project/0805nexter')
    requests_mock.get(re.compile('https://'),
                      body=get_response_cb)

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

    assert list(loader.storage.content_missing_per_sha1(expected_contents)) == []

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

    # self.assertEqual(self.loader.load_status(), {'status': 'eventful'})
    # self.assertEqual(self.loader.visit_status(), 'full')

# release artifact, no new artifact
# {visit full, status uneventful, same snapshot as before}

# release artifact, new artifact
# {visit full, status full, new snapshot with shared history as prior snapshot}

# release artifact, old artifact with different checksums
# {visit full, status full, new snapshot with shared history and some new
# different history}


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
