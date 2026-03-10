# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import tarfile
from tempfile import SpooledTemporaryFile

from click.testing import CliRunner
import pytest

from swh.loader.git.loader import FetchPackReturn
from swh.loader.git.tar_out import GitTarOutLoader
from swh.loader.git.tar_out_cli import main
from swh.storage import get_storage


def test_git_tar_out_cli(mocker, tmp_path):
    repo_url = "https://example.com/repo.git"
    tar_output_path = str(tmp_path / "cli_repo.tar")

    # Mock the loader to avoid actual network/processing
    mock_loader = mocker.patch("swh.loader.git.tar_out_cli.GitTarOutLoader")
    mock_loader.return_value.load.return_value = {"status": "eventful"}

    runner = CliRunner()
    result = runner.invoke(main, [repo_url, tar_output_path])

    assert result.exit_code == 0
    assert f"Successfully created {tar_output_path}" in result.output
    mock_loader.assert_called_once()
    args, kwargs = mock_loader.call_args
    assert kwargs["url"] == repo_url
    assert kwargs["tar_output_path"] == tar_output_path
    assert kwargs["incremental"] is False


def test_git_tar_loader_default_prefix(mocker, tmp_path):
    storage = get_storage(cls="memory")
    repo_url = "https://example.com/some-repo.git"
    tar_output_path = str(tmp_path / "repo_default_prefix.tar")
    expected_prefix = "some-repo"

    pack_content = b"fake pack content"
    pack_buffer = SpooledTemporaryFile()
    pack_buffer.write(pack_content)
    pack_buffer.seek(0)

    # Mock fetch_pack_from_origin
    mock_fetch = mocker.patch("swh.loader.git.loader.GitLoader.fetch_pack_from_origin")
    mock_fetch.return_value = FetchPackReturn(
        remote_refs={b"refs/heads/master": b"deadbeef" * 5},
        symbolic_refs={b"HEAD": b"refs/heads/master"},
        pack_buffer=pack_buffer,
        pack_size=len(pack_content),
    )

    # Mock PackData and its methods
    mock_pack_data = mocker.patch("swh.loader.git.loader.PackData.from_file")
    mock_pack_data.return_value.get_stored_checksum.return_value = b"a" * 20

    loader = GitTarOutLoader(
        storage=storage,
        url=repo_url,
        tar_output_path=tar_output_path,
        # prefix is None by default
    )
    res = loader.load()

    assert res["status"] == "eventful"
    assert os.path.exists(tar_output_path)

    with tarfile.open(tar_output_path, "r") as tar:
        names = tar.getnames()
        assert f"{expected_prefix}/HEAD" in names
        assert f"{expected_prefix}/config" in names


def test_git_tar_loader_prefix(mocker, tmp_path):
    storage = get_storage(cls="memory")
    repo_url = "https://example.com/repo.git"
    tar_output_path = str(tmp_path / "repo_prefix.tar")
    prefix = "my-project"

    pack_content = b"fake pack content"
    pack_buffer = SpooledTemporaryFile()
    pack_buffer.write(pack_content)
    pack_buffer.seek(0)

    # Mock fetch_pack_from_origin
    mock_fetch = mocker.patch("swh.loader.git.loader.GitLoader.fetch_pack_from_origin")
    mock_fetch.return_value = FetchPackReturn(
        remote_refs={b"refs/heads/master": b"deadbeef" * 5},
        symbolic_refs={b"HEAD": b"refs/heads/master"},
        pack_buffer=pack_buffer,
        pack_size=len(pack_content),
    )

    # Mock PackData and its methods
    mock_pack_data = mocker.patch("swh.loader.git.loader.PackData.from_file")
    mock_pack_data.return_value.get_stored_checksum.return_value = b"a" * 20

    loader = GitTarOutLoader(
        storage=storage,
        url=repo_url,
        tar_output_path=tar_output_path,
        prefix=prefix,
    )
    res = loader.load()

    assert res["status"] == "eventful"
    assert os.path.exists(tar_output_path)

    with tarfile.open(tar_output_path, "r") as tar:
        names = tar.getnames()
        assert f"{prefix}/HEAD" in names
        assert f"{prefix}/config" in names
        assert f"{prefix}/description" in names
        assert f"{prefix}/refs/heads/master" in names
        assert any(
            name.startswith(f"{prefix}/objects/pack/pack-") and name.endswith(".pack")
            for name in names
        )


def test_git_tar_loader_no_index(mocker, tmp_path):
    storage = get_storage(cls="memory")
    repo_url = "https://example.com/repo.git"
    tar_output_path = str(tmp_path / "repo_no_index.tar")

    pack_content = b"fake pack content"
    pack_buffer = SpooledTemporaryFile()
    pack_buffer.write(pack_content)
    pack_buffer.seek(0)

    # Mock fetch_pack_from_origin
    mock_fetch = mocker.patch("swh.loader.git.loader.GitLoader.fetch_pack_from_origin")
    mock_fetch.return_value = FetchPackReturn(
        remote_refs={b"refs/heads/master": b"deadbeef" * 5},
        symbolic_refs={b"HEAD": b"refs/heads/master"},
        pack_buffer=pack_buffer,
        pack_size=len(pack_content),
    )

    # Mock PackData and its methods
    mock_pack_data = mocker.patch("swh.loader.git.loader.PackData.from_file")
    mock_pack_data.return_value.get_stored_checksum.return_value = b"a" * 20

    loader = GitTarOutLoader(
        storage=storage,
        url=repo_url,
        tar_output_path=tar_output_path,
        generate_index=False,
        prefix="",
    )
    res = loader.load()

    assert res["status"] == "eventful"
    assert os.path.exists(tar_output_path)

    with tarfile.open(tar_output_path, "r") as tar:
        names = tar.getnames()
        assert "HEAD" in names
        # Check that .pack is present but .idx is NOT
        assert any(
            name.startswith("objects/pack/pack-") and name.endswith(".pack")
            for name in names
        )
        assert not any(
            name.startswith("objects/pack/pack-") and name.endswith(".idx")
            for name in names
        )


def test_git_tar_loader(mocker, tmp_path):
    storage = get_storage(cls="memory")
    repo_url = "https://example.com/repo.git"
    tar_output_path = str(tmp_path / "repo.tar")

    pack_content = b"fake pack content"
    pack_buffer = SpooledTemporaryFile()
    pack_buffer.write(pack_content)
    pack_buffer.seek(0)

    # Mock fetch_pack_from_origin
    mock_fetch = mocker.patch("swh.loader.git.loader.GitLoader.fetch_pack_from_origin")
    mock_fetch.return_value = FetchPackReturn(
        remote_refs={b"refs/heads/master": b"deadbeef" * 5},
        symbolic_refs={b"HEAD": b"refs/heads/master"},
        pack_buffer=pack_buffer,
        pack_size=len(pack_content),
    )

    # Mock PackData and its methods
    mock_pack_data = mocker.patch("swh.loader.git.loader.PackData.from_file")
    mock_pack_data.return_value.get_stored_checksum.return_value = b"a" * 20
    # create_index is a method of PackData

    loader = GitTarOutLoader(
        storage=storage, url=repo_url, tar_output_path=tar_output_path, prefix=""
    )
    res = loader.load()

    assert res["status"] == "eventful"
    assert os.path.exists(tar_output_path)

    with tarfile.open(tar_output_path, "r") as tar:
        names = tar.getnames()
        assert "HEAD" in names
        assert "config" in names
        assert "description" in names
        assert "refs/heads/master" in names
        assert any(
            name.startswith("objects/pack/pack-") and name.endswith(".pack")
            for name in names
        )
        assert any(
            name.startswith("objects/pack/pack-") and name.endswith(".idx")
            for name in names
        )

        # Check HEAD content
        f = tar.extractfile("HEAD")
        assert f is not None
        assert f.read() == b"ref: refs/heads/master\n"

        # Check refs/heads/master content
        f = tar.extractfile("refs/heads/master")
        assert f is not None
        assert f.read() == (b"deadbeef" * 5) + b"\n"

        # Check config content
        f = tar.extractfile("config")
        assert f is not None
        config_content = f.read()
        assert b"bare = true" in config_content
