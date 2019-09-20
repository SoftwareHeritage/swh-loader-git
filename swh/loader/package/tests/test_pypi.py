# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

import pytest

from os import path

from swh.core.tarball import uncompress
from swh.loader.package.pypi import PyPILoader, PyPIClient, author, sdist_parse


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

def test_badly_configured_loader_raise():
    """Badly configured loader should raise"""
    assert 'SWH_CONFIG_FILENAME' in os.environ  # cf. tox.ini
    del os.environ['SWH_CONFIG_FILENAME']
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

    expected_status_code = 404
    release_url = '%s/3.0.0/json' % pypi_client.url
    requests_mock.get(release_url, status_code=expected_status_code)

    with pytest.raises(ValueError) as e1:
        pypi_client.info_release("3.0.0")

    assert e1.value.args[0] == "Fail to query '%s'. Reason: %s" % (
        release_url, expected_status_code
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

    release_url = '%s/2.0.0/json' % pypi_client.url
    requests_mock.get(release_url, text='{"version": "2.0.0"}')
    actual_release_info = pypi_client.info_release("2.0.0")
    assert actual_release_info == {
        'version': '2.0.0',
    }


resources = path.abspath(path.dirname(__file__))
resource_json = path.join(resources, 'resources/json')
resource_archives = path.join(resources, 'resources/tarballs')


@pytest.mark.fs
def test_sdist_parse(tmp_path):
    """Parsing existing archive's PKG-INFO should yield results"""
    uncompressed_archive_path = str(tmp_path)
    archive_path = path.join(resource_archives, '0805nexter-1.1.0.zip')
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


def test_sdist_parse_ko(tmp_path):
    """Parsing inexistant path/archive/PKG-INFO yield None"""
    # inexistant first level path
    assert sdist_parse('/something-inexistant') is None
    # inexistant second level path (as expected by pypi archives)
    assert sdist_parse(tmp_path) is None
    # inexistant PKG-INFO within second level path
    existing_path_no_pkginfo = str(tmp_path / 'something')
    os.mkdir(existing_path_no_pkginfo)
    assert sdist_parse(tmp_path) is None

# "edge" cases (for the same origin) #

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

# release artifact, no prior visit
# {visit full, status eventful, snapshot}

# release artifact, no new artifact
# {visit full, status uneventful, same snapshot as before}

# release artifact, new artifact
# {visit full, status full, new snapshot with shared history as prior snapshot}

# release artifact, old artifact with different checksums
# {visit full, status full, new snapshot with shared history and some new
# different history}
