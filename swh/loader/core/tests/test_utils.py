# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

from unittest.mock import patch

from swh.loader.core.utils import clean_dangling_folders


def prepare_arborescence_from(tmpdir, folder_names):
    """Prepare arborescence tree with folders

    Args:
        tmpdir (Either[LocalPath, str]): Root temporary directory
        folder_names (List[str]): List of folder names

    Returns:
        List of folders
    """
    dangling_folders = []
    for dname in folder_names:
        d = str(tmpdir / dname)
        os.mkdir(d)
        dangling_folders.append(d)
    return str(tmpdir), dangling_folders


def assert_dirs(actual_dirs, expected_dirs):
    """Assert that the directory actual and expected match

    """
    for d in actual_dirs:
        assert d in expected_dirs
    assert len(actual_dirs) == len(expected_dirs)


def test_clean_dangling_folders_0(tmpdir):
    """Folder does not exist, do nothing"""
    r = clean_dangling_folders("/path/does/not/exist", "unused-pattern")
    assert r is None


@patch("swh.loader.core.utils.psutil.pid_exists", return_value=False)
def test_clean_dangling_folders_1(mock_pid_exists, tmpdir):
    """Folder which matches pattern with dead pid are cleaned up

    """
    rootpath, dangling = prepare_arborescence_from(
        tmpdir, ["something", "swh.loader.svn-4321.noisynoise",]
    )

    clean_dangling_folders(rootpath, "swh.loader.svn")

    actual_dirs = os.listdir(rootpath)
    mock_pid_exists.assert_called_once_with(4321)
    assert_dirs(actual_dirs, ["something"])


@patch("swh.loader.core.utils.psutil.pid_exists", return_value=True)
def test_clean_dangling_folders_2(mock_pid_exists, tmpdir):
    """Folder which matches pattern with live pid are skipped

    """
    rootpath, dangling = prepare_arborescence_from(
        tmpdir, ["something", "swh.loader.hg-1234.noisynoise",]
    )

    clean_dangling_folders(rootpath, "swh.loader.hg")

    actual_dirs = os.listdir(rootpath)
    mock_pid_exists.assert_called_once_with(1234)
    assert_dirs(actual_dirs, ["something", "swh.loader.hg-1234.noisynoise",])


@patch("swh.loader.core.utils.psutil.pid_exists", return_value=False)
@patch(
    "swh.loader.core.utils.shutil.rmtree",
    side_effect=ValueError("Could not remove for reasons"),
)
def test_clean_dangling_folders_3(mock_rmtree, mock_pid_exists, tmpdir):
    """Error in trying to clean dangling folders are skipped

    """
    path1 = "thingy"
    path2 = "swh.loader.git-1468.noisy"
    rootpath, dangling = prepare_arborescence_from(tmpdir, [path1, path2,])

    clean_dangling_folders(rootpath, "swh.loader.git")

    actual_dirs = os.listdir(rootpath)
    mock_pid_exists.assert_called_once_with(1468)
    mock_rmtree.assert_called_once_with(os.path.join(rootpath, path2))
    assert_dirs(actual_dirs, [path2, path1])
