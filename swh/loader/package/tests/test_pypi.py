# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest

from swh.loader.package.pypi import PyPILoader, author


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
