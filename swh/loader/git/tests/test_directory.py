# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from pathlib import Path
from subprocess import run
import threading
from typing import Tuple

from dulwich.client import LocalGitClient
from dulwich.repo import Repo
from dulwich.server import DictBackend, TCPGitServer
import pytest

from swh.loader.core.nar import Nar
from swh.loader.exception import NotFound
from swh.loader.git.directory import (
    GitCheckoutLoader,
    checkout_repository_ref,
    list_git_tree,
)
from swh.loader.tests import (
    assert_last_visit_matches,
    fetch_extids_from_checksums,
    get_stats,
    prepare_repository_from_archive,
)


def test_list_git_tree(datadir, tmp_path):
    """Listing a git tree should not list any .git paths nor empty folders."""
    archive_name = "testrepo"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    repo, repo_url = prepare_test_git_checkout(
        archive_path, archive_name, tmp_path, "branch2-after-delete"
    )

    from swh.model.from_disk import Directory

    repo_dir = repo.path

    # Create an empty dir within the repository
    os.makedirs(os.path.join(repo_dir, "empty-foo"), exist_ok=True)
    os.makedirs(os.path.join(repo_dir, ".git", "empty-foobar"), exist_ok=True)

    repo_path = repo_dir.encode()
    dir1 = Directory.from_disk(path=repo_path)

    def names(entries):
        return [d["name"] for d in entries]

    dir1_entries = names(dir1.entries)
    assert b".git" in dir1_entries
    assert b"empty-foo" in dir1_entries

    # Let's find empty-foobar
    empty_bar_found = False
    all_nodes = dir1.collect()
    assert len(all_nodes) > 0
    for entry in all_nodes:
        if entry.object_type == "content":
            continue
        dir_entries = names(entry.entries)
        if b"empty-foobar" in dir_entries:
            empty_bar_found = True
            break

    assert empty_bar_found is True

    dir2 = Directory.from_disk(path=repo_path, path_filter=list_git_tree)
    dir2_entries = [d["name"] for d in dir2.entries]
    assert b".git" not in dir2_entries
    assert b"empty-foo" not in dir2_entries

    # Check .git folder and empty folders have not been collected.
    all_nodes = dir2.collect()
    assert len(all_nodes) > 0
    for entry in all_nodes:
        if entry.object_type == "content":
            continue
        dir_entries = names(entry.entries)
        assert b"empty" not in dir_entries
        assert b".git" not in dir_entries


def compute_nar_hash_for_ref(
    repo_url: str, ref: str, hash_name: str = "sha256", temp_dir: str = "/tmp"
) -> str:
    """Compute the nar from a git checked out by git."""
    tmp_path = Path(os.path.join(temp_dir, "compute-nar"))
    tmp_path.mkdir(exist_ok=True)
    git_repo_path = checkout_repository_ref(repo_url, ref, tmp_path)
    nar = Nar(hash_names=[hash_name], exclude_vcs=True)
    nar.serialize(git_repo_path)
    return nar.hexdigest()[hash_name]


def prepare_test_git_checkout(
    archive_path: str, archive_name: str, tmp_path: str, ref: str
) -> Tuple:
    repo_url = prepare_repository_from_archive(
        archive_path, archive_name, tmp_path=tmp_path
    )

    temp_dir = Path(tmp_path) / "checkout"
    os.makedirs(temp_dir)
    repo_path = checkout_repository_ref(repo_url, ref, temp_dir)
    assert repo_path and repo_path.exists()
    expected_path = temp_dir / os.path.basename(repo_url)
    assert str(repo_path) == str(expected_path)
    return Repo(str(repo_path)), repo_url


@pytest.mark.parametrize(
    "reference_type,reference",
    [
        ("branch", "master"),
        ("tag", "branch2-after-delete"),
        ("commit", "bd746cd1913721b269b395a56a97baf6755151c2"),
        ("commit", "bd746cd"),
    ],
)
def test_checkout_repository_ref_from(datadir, tmp_path, reference, reference_type):
    """Cloning a repository from a branch, tag or commit should be ok"""
    archive_name = "testrepo"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    # reference is either: branch, tags, or commit
    checkout, repo_url = prepare_test_git_checkout(
        archive_path, archive_name, tmp_path, reference
    )
    refs = LocalGitClient().get_refs(repo_url.replace("file://", ""))
    # Ensure the repository exists and is at the branch_name required
    if reference_type == "branch":
        ref = f"refs/heads/{reference}".encode()
        expected_head = refs[ref]
    elif reference_type == "tag":
        ref = f"refs/tags/{reference}".encode()
        expected_head = refs[ref]
    else:
        expected_head = reference.encode()
    assert checkout.head().startswith(expected_head)


def test_checkout_repository_ref_not_found(tmp_path):
    with pytest.raises(NotFound, match=r"Repository <file://.*not found"):
        checkout_repository_ref(
            "file:///home/origin/does/not/exist", "not-important", tmp_path
        )


@pytest.mark.parametrize(
    "reference",
    [
        "master",
        "branch2-after-delete",
        "bd746cd1913721b269b395a56a97baf6755151c2",
    ],
)
def test_git_loader_directory(swh_storage, datadir, tmp_path, reference):
    """Loading a git directory should be eventful"""
    archive_name = "testrepo"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    _, repo_url = prepare_test_git_checkout(
        archive_path, archive_name, tmp_path, reference
    )

    checksums = {
        "sha256": compute_nar_hash_for_ref(repo_url, reference, "sha256", tmp_path)
    }
    loader = GitCheckoutLoader(
        swh_storage,
        repo_url,
        ref=reference,
        checksum_layout="nar",
        checksums=checksums,
    )

    actual_result = loader.load()

    assert actual_result == {"status": "eventful"}

    actual_visit = assert_last_visit_matches(
        swh_storage,
        repo_url,
        status="full",
        type="git-checkout",
    )

    snapshot = swh_storage.snapshot_get(actual_visit.snapshot)
    assert snapshot is not None

    branches = snapshot["branches"].keys()
    assert set(branches) == {b"HEAD", reference.encode()}

    # Ensure the extids got stored as well
    extids = fetch_extids_from_checksums(
        loader.storage,
        checksum_layout="nar",
        checksums=checksums,
        extid_version=loader.extid_version,
    )
    assert len(extids) == len(checksums)


def test_loader_git_directory_hash_mismatch(swh_storage, datadir, tmp_path):
    """Loading a git tree with faulty checksums should fail"""
    archive_name = "testrepo"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    release_name = "branch2-before-delete"
    _, repo_url = prepare_test_git_checkout(
        archive_path, archive_name, tmp_path, release_name
    )

    reference = "branch2-before-delete"
    truthy_checksums = compute_nar_hash_for_ref(repo_url, reference, "sha256", tmp_path)
    faulty_checksums = {"sha256": truthy_checksums.replace("5", "0")}
    loader = GitCheckoutLoader(
        swh_storage,
        repo_url,
        ref=reference,
        checksum_layout="nar",
        checksums=faulty_checksums,
    )

    actual_result = loader.load()

    # Ingestion fails because the checks failed
    assert actual_result == {"status": "failed"}
    assert get_stats(swh_storage) == {
        "content": 0,
        "directory": 0,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 0,
        "skipped_content": 0,
        "snapshot": 0,
    }

    # Ensure no extids got stored
    extids = fetch_extids_from_checksums(
        loader.storage,
        checksum_layout="nar",
        checksums=faulty_checksums,
        extid_version=loader.extid_version,
    )
    assert len(extids) == 0


def test_loader_git_directory_not_found(swh_storage, datadir, tmp_path):
    """Loading a git tree from an unknown origin should fail"""
    loader = GitCheckoutLoader(
        swh_storage,
        "file:///home/origin/does/not/exist/",
        ref="not-important",
        checksum_layout="standard",
        checksums={},
    )

    actual_result = loader.load()

    # Ingestion fails because the checks failed
    assert actual_result == {"status": "uneventful"}
    assert get_stats(swh_storage) == {
        "content": 0,
        "directory": 0,
        "origin": 1,
        "origin_visit": 1,
        "release": 0,
        "revision": 0,
        "skipped_content": 0,
        "snapshot": 0,
    }


@pytest.fixture
def simple_git_repository_url(tmp_path):
    """Create a simple git repository containing one file.
    This repository will be added as a submodule in another git repository.
    As git forbids to add a submodule from the local filesystem, we serve
    the repository using TCP."""
    git_repo_path = os.path.join(tmp_path, "git_repo")
    git_repo = Repo.init(git_repo_path, mkdir=True)

    with open(os.path.join(git_repo_path, "file"), "w") as f:
        f.write("foo")

    git_repo.stage(["file"])
    git_repo.do_commit(
        b"file added",
        committer=b"Test Committer <test@example.org>",
        author=b"Test Author <test@example.org>",
        commit_timestamp=12395,
        commit_timezone=0,
        author_timestamp=12395,
        author_timezone=0,
    )

    backend = DictBackend({b"/": git_repo})
    git_server = TCPGitServer(backend, b"localhost", 0)

    git_server_thread = threading.Thread(target=git_server.serve)
    git_server_thread.start()

    _, port = git_server.socket.getsockname()

    yield f"git://localhost:{port}/"

    git_server.shutdown()
    git_server.server_close()
    git_server_thread.join()


@pytest.mark.parametrize("with_submodule", [False, True])
def test_loader_git_directory_without_or_with_submodule(
    swh_storage, datadir, tmp_path, simple_git_repository_url, with_submodule
):
    archive_name = "testrepo"
    archive_path = os.path.join(datadir, f"{archive_name}.tgz")
    release_name = "branch2-before-delete"
    repo, _ = prepare_test_git_checkout(
        archive_path, archive_name, tmp_path, release_name
    )
    repo_url = f"file://{repo.path}"

    if with_submodule:
        run(
            ["git", "config", "user.email", "foo@example.org"],
            check=True,
            cwd=repo.path,
        )
        run(
            ["git", "config", "user.name", "Foo"],
            check=True,
            cwd=repo.path,
        )
        # add the repository served by the simple_git_repository_url fixture as a
        # submodule in it
        run(
            ["git", "submodule", "add", simple_git_repository_url, "submodule"],
            check=True,
            cwd=repo.path,
        )
        run(
            ["git", "commit", "-m", "submodule added"],
            check=True,
            cwd=repo.path,
        )

    tmp_clone_path = os.path.join(tmp_path, "repo_clone")

    run(["git", "clone", repo_url, tmp_clone_path], check=True)
    if with_submodule:
        run(["git", "submodule", "init"], check=True, cwd=tmp_clone_path)
        run(["git", "submodule", "update"], check=True, cwd=tmp_clone_path)

    nar = Nar(hash_names=["sha1", "sha256"], exclude_vcs=True)
    nar.serialize(Path(tmp_clone_path))
    nar_hashes = nar.hexdigest()

    loader = GitCheckoutLoader(
        swh_storage,
        repo_url,
        ref=repo.head().decode(),
        checksum_layout="nar",
        checksums=nar_hashes,
        submodules=with_submodule,
    )

    result = loader.load()

    assert result == {"status": "eventful"}

    assert_last_visit_matches(
        swh_storage,
        repo_url,
        status="full",
        type="git-checkout",
    )
