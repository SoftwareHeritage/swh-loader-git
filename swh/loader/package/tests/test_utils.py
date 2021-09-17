# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import json
import os
from unittest.mock import MagicMock
from urllib.error import URLError
from urllib.parse import quote

import pytest

from swh.loader.exception import NotFound
import swh.loader.package
from swh.loader.package.utils import api_info, download, release_name


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


_filename = "requests-0.0.1.tar.gz"
_data = "this is something"


def _check_download_ok(url, dest, filename=_filename, hashes=None):
    actual_filepath, actual_hashes = download(url, dest, hashes=hashes)

    actual_filename = os.path.basename(actual_filepath)
    assert actual_filename == filename
    assert actual_hashes["length"] == len(_data)
    assert (
        actual_hashes["checksums"]["sha1"] == "fdd1ce606a904b08c816ba84f3125f2af44d92b2"
    )
    assert (
        actual_hashes["checksums"]["sha256"]
        == "1d9224378d77925d612c9f926eb9fb92850e6551def8328011b6a972323298d5"
    )


@pytest.mark.fs
def test_download_ok(tmp_path, requests_mock):
    """Download without issue should provide filename and hashes"""
    url = f"https://pypi.org/pypi/requests/{_filename}"
    requests_mock.get(url, text=_data, headers={"content-length": str(len(_data))})
    _check_download_ok(url, dest=str(tmp_path))


@pytest.mark.fs
def test_download_ok_no_header(tmp_path, requests_mock):
    """Download without issue should provide filename and hashes"""
    url = f"https://pypi.org/pypi/requests/{_filename}"
    requests_mock.get(url, text=_data)  # no header information
    _check_download_ok(url, dest=str(tmp_path))


@pytest.mark.fs
def test_download_ok_with_hashes(tmp_path, requests_mock):
    """Download without issue should provide filename and hashes"""
    url = f"https://pypi.org/pypi/requests/{_filename}"
    requests_mock.get(url, text=_data, headers={"content-length": str(len(_data))})

    # good hashes for such file
    good = {
        "sha1": "fdd1ce606a904b08c816ba84f3125f2af44d92b2",
        "sha256": "1d9224378d77925d612c9f926eb9fb92850e6551def8328011b6a972323298d5",  # noqa
    }

    _check_download_ok(url, dest=str(tmp_path), hashes=good)


@pytest.mark.fs
def test_download_fail_hashes_mismatch(tmp_path, requests_mock):
    """Mismatch hash after download should raise

    """
    url = f"https://pypi.org/pypi/requests/{_filename}"
    requests_mock.get(url, text=_data, headers={"content-length": str(len(_data))})

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


@pytest.mark.fs
def test_ftp_download_ok(tmp_path, mocker):
    """Download without issue should provide filename and hashes"""
    url = f"ftp://pypi.org/pypi/requests/{_filename}"

    cm = MagicMock()
    cm.getstatus.return_value = 200
    cm.read.side_effect = [_data.encode(), b""]
    cm.__enter__.return_value = cm
    mocker.patch("swh.loader.package.utils.urlopen").return_value = cm

    _check_download_ok(url, dest=str(tmp_path))


@pytest.mark.fs
def test_ftp_download_ko(tmp_path, mocker):
    """Download without issue should provide filename and hashes"""
    filename = "requests-0.0.1.tar.gz"
    url = "ftp://pypi.org/pypi/requests/%s" % filename

    mocker.patch("swh.loader.package.utils.urlopen").side_effect = URLError("FTP error")

    with pytest.raises(URLError):
        download(url, dest=str(tmp_path))


@pytest.mark.fs
def test_download_with_redirection(tmp_path, requests_mock):
    """Download with redirection should use the targeted URL to extract filename"""
    url = "https://example.org/project/requests/download"
    redirection_url = f"https://example.org/project/requests/files/{_filename}"

    requests_mock.get(url, status_code=302, headers={"location": redirection_url})
    requests_mock.get(
        redirection_url, text=_data, headers={"content-length": str(len(_data))}
    )

    _check_download_ok(url, dest=str(tmp_path))


def test_download_extracting_filename_from_url(tmp_path, requests_mock):
    """Extracting filename from url must sanitize the filename first"""
    url = "https://example.org/project/requests-0.0.1.tar.gz?a=b&c=d&foo=bar"

    requests_mock.get(
        url, status_code=200, text=_data, headers={"content-length": str(len(_data))}
    )

    _check_download_ok(url, dest=str(tmp_path))


@pytest.mark.fs
@pytest.mark.parametrize(
    "filename", [f'"{_filename}"', _filename, '"filename with spaces.tar.gz"']
)
def test_download_filename_from_content_disposition(tmp_path, requests_mock, filename):
    """Filename should be extracted from content-disposition request header
    when available."""
    url = "https://example.org/download/requests/tar.gz/v0.0.1"

    requests_mock.get(
        url,
        text=_data,
        headers={
            "content-length": str(len(_data)),
            "content-disposition": f"attachment; filename={filename}",
        },
    )

    _check_download_ok(url, dest=str(tmp_path), filename=filename.strip('"'))


@pytest.mark.fs
@pytest.mark.parametrize("filename", ['"archive école.tar.gz"', "archive_école.tgz"])
def test_download_utf8_filename_from_content_disposition(
    tmp_path, requests_mock, filename
):
    """Filename should be extracted from content-disposition request header
    when available."""
    url = "https://example.org/download/requests/tar.gz/v0.0.1"
    data = "this is something"

    requests_mock.get(
        url,
        text=data,
        headers={
            "content-length": str(len(data)),
            "content-disposition": f"attachment; filename*=utf-8''{quote(filename)}",
        },
    )

    _check_download_ok(url, dest=str(tmp_path), filename=filename.strip('"'))


def test_api_info_failure(requests_mock):
    """Failure to fetch info/release information should raise"""
    url = "https://pypi.org/pypi/requests/json"
    status_code = 400
    requests_mock.get(url, status_code=status_code)

    with pytest.raises(NotFound) as e0:
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
