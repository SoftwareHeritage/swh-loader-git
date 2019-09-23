# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import os
import pytest

from swh.loader.package.utils import download


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
