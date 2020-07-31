# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import json
import os

import pytest

import swh.loader.package
from swh.loader.package.utils import download, api_info, release_name


def test_version_generation():
    assert (
        swh.loader.package.__version__ != "devel"
    ), "Make sure swh.loader.core is installed (e.g. pip install -e .)"


@pytest.mark.fs
def test_download_fail_to_download(tmp_path, requests_mock):
    url = "https://pypi.org/pypi/arrow/json"
    status_code = 404
    requests_mock.get(url, status_code=status_code)

    with pytest.raises(ValueError) as e:
        download(url, tmp_path)

    assert e.value.args[0] == "Fail to query '%s'. Reason: %s" % (url, status_code)


@pytest.mark.fs
def test_download_ok(tmp_path, requests_mock):
    """Download without issue should provide filename and hashes"""
    filename = "requests-0.0.1.tar.gz"
    url = "https://pypi.org/pypi/requests/%s" % filename
    data = "this is something"
    requests_mock.get(url, text=data, headers={"content-length": str(len(data))})

    actual_filepath, actual_hashes = download(url, dest=str(tmp_path))

    actual_filename = os.path.basename(actual_filepath)
    assert actual_filename == filename
    assert actual_hashes["length"] == len(data)
    assert (
        actual_hashes["checksums"]["sha1"] == "fdd1ce606a904b08c816ba84f3125f2af44d92b2"
    )  # noqa
    assert (
        actual_hashes["checksums"]["sha256"]
        == "1d9224378d77925d612c9f926eb9fb92850e6551def8328011b6a972323298d5"
    )


@pytest.mark.fs
def test_download_ok_no_header(tmp_path, requests_mock):
    """Download without issue should provide filename and hashes"""
    filename = "requests-0.0.1.tar.gz"
    url = "https://pypi.org/pypi/requests/%s" % filename
    data = "this is something"
    requests_mock.get(url, text=data)  # no header information

    actual_filepath, actual_hashes = download(url, dest=str(tmp_path))

    actual_filename = os.path.basename(actual_filepath)
    assert actual_filename == filename
    assert actual_hashes["length"] == len(data)
    assert (
        actual_hashes["checksums"]["sha1"] == "fdd1ce606a904b08c816ba84f3125f2af44d92b2"
    )  # noqa
    assert (
        actual_hashes["checksums"]["sha256"]
        == "1d9224378d77925d612c9f926eb9fb92850e6551def8328011b6a972323298d5"
    )


@pytest.mark.fs
def test_download_ok_with_hashes(tmp_path, requests_mock):
    """Download without issue should provide filename and hashes"""
    filename = "requests-0.0.1.tar.gz"
    url = "https://pypi.org/pypi/requests/%s" % filename
    data = "this is something"
    requests_mock.get(url, text=data, headers={"content-length": str(len(data))})

    # good hashes for such file
    good = {
        "sha1": "fdd1ce606a904b08c816ba84f3125f2af44d92b2",
        "sha256": "1d9224378d77925d612c9f926eb9fb92850e6551def8328011b6a972323298d5",  # noqa
    }

    actual_filepath, actual_hashes = download(url, dest=str(tmp_path), hashes=good)

    actual_filename = os.path.basename(actual_filepath)
    assert actual_filename == filename
    assert actual_hashes["length"] == len(data)
    assert actual_hashes["checksums"]["sha1"] == good["sha1"]
    assert actual_hashes["checksums"]["sha256"] == good["sha256"]


@pytest.mark.fs
def test_download_fail_hashes_mismatch(tmp_path, requests_mock):
    """Mismatch hash after download should raise

    """
    filename = "requests-0.0.1.tar.gz"
    url = "https://pypi.org/pypi/requests/%s" % filename
    data = "this is something"
    requests_mock.get(url, text=data, headers={"content-length": str(len(data))})

    # good hashes for such file
    good = {
        "sha1": "fdd1ce606a904b08c816ba84f3125f2af44d92b2",
        "sha256": "1d9224378d77925d612c9f926eb9fb92850e6551def8328011b6a972323298d5",  # noqa
    }

    for hash_algo in good.keys():
        wrong_hash = good[hash_algo].replace("1", "0")
        expected_hashes = good.copy()
        expected_hashes[hash_algo] = wrong_hash  # set the wrong hash

        expected_msg = "Failure when fetching %s. " "Checksum mismatched: %s != %s" % (
            url,
            wrong_hash,
            good[hash_algo],
        )

        with pytest.raises(ValueError, match=expected_msg):
            download(url, dest=str(tmp_path), hashes=expected_hashes)


def test_api_info_failure(requests_mock):
    """Failure to fetch info/release information should raise"""
    url = "https://pypi.org/pypi/requests/json"
    status_code = 400
    requests_mock.get(url, status_code=status_code)

    with pytest.raises(ValueError) as e0:
        api_info(url)

    assert e0.value.args[0] == "Fail to query '%s'. Reason: %s" % (url, status_code)


def test_api_info(requests_mock):
    """Fetching json info from pypi project should be ok"""
    url = "https://pypi.org/pypi/requests/json"
    requests_mock.get(url, text='{"version": "0.0.1"}')
    actual_info = json.loads(api_info(url))
    assert actual_info == {
        "version": "0.0.1",
    }


def test_release_name():
    for version, filename, expected_release in [
        ("0.0.1", None, "releases/0.0.1"),
        ("0.0.2", "something", "releases/0.0.2/something"),
    ]:
        assert release_name(version, filename) == expected_release
