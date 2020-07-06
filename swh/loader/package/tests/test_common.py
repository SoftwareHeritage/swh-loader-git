# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.loader.package.tests.common import (
    check_metadata,
    check_metadata_paths,
)


def test_check_metadata():
    metadata = {
        "a": {"raw": {"time": "something",},},
        "b": [],
        "c": 1,
    }

    for raw_path, raw_type in [
        ("a.raw", dict),
        ("a.raw.time", str),
        ("b", list),
        ("c", int),
    ]:
        check_metadata(metadata, raw_path, raw_type)


def test_check_metadata_ko():
    metadata = {
        "a": {"raw": "hello",},
        "b": [],
        "c": 1,
    }

    for raw_path, raw_type in [
        ("a.b", dict),
        ("a.raw.time", str),
    ]:
        with pytest.raises(AssertionError):
            check_metadata(metadata, raw_path, raw_type)


def test_check_metadata_paths():
    metadata = {
        "a": {"raw": {"time": "something",},},
        "b": [],
        "c": 1,
    }

    check_metadata_paths(
        metadata, [("a.raw", dict), ("a.raw.time", str), ("b", list), ("c", int),]
    )


def test_check_metadata_paths_ko():
    metadata = {
        "a": {"raw": "hello",},
        "b": [],
        "c": 1,
    }

    with pytest.raises(AssertionError):
        check_metadata_paths(metadata, [("a.b", dict), ("a.raw.time", str),])
