# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import subprocess

import pytest

from swh.loader.tests import prepare_repository_from_archive


def test_prepare_repository_from_archive_failure():
    # does not deal with inexistent archive so raise
    assert os.path.exists("unknown-archive") is False
    with pytest.raises(subprocess.CalledProcessError, match="exit status 2"):
        prepare_repository_from_archive("unknown-archive")


def test_prepare_repository_from_archive(datadir, tmp_path):
    archive_name = "0805nexter-1.1.0"
    archive_path = os.path.join(str(datadir), f"{archive_name}.tar.gz")
    assert os.path.exists(archive_path) is True

    tmp_path = str(tmp_path)  # deals with path string
    repo_url = prepare_repository_from_archive(
        archive_path, filename=archive_name, tmp_path=tmp_path
    )
    expected_uncompressed_archive_path = os.path.join(tmp_path, archive_name)
    assert repo_url == f"file://{expected_uncompressed_archive_path}"
    assert os.path.exists(expected_uncompressed_archive_path)


def test_prepare_repository_from_archive_no_filename(datadir, tmp_path):
    archive_name = "0805nexter-1.1.0"
    archive_path = os.path.join(str(datadir), f"{archive_name}.tar.gz")
    assert os.path.exists(archive_path) is True

    # deals with path as posix path (for tmp_path)
    repo_url = prepare_repository_from_archive(archive_path, tmp_path=tmp_path)

    tmp_path = str(tmp_path)
    expected_uncompressed_archive_path = os.path.join(tmp_path, archive_name)
    expected_repo_url = os.path.join(tmp_path, f"{archive_name}.tar.gz")
    assert repo_url == f"file://{expected_repo_url}"

    # passing along the filename does not influence the on-disk extraction
    # just the repo-url computation
    assert os.path.exists(expected_uncompressed_archive_path)
