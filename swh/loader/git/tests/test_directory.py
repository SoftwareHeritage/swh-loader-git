# Copyright (C) 2023-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from pathlib import Path
from subprocess import CalledProcessError, TimeoutExpired, run
import threading
import time
from typing import Tuple

from dulwich.client import LocalGitClient
from dulwich.repo import Repo
from dulwich.server import DictBackend, TCPGitServer
import pytest

from swh.core.nar import Nar, NarHashAlgo
from swh.loader.exception import NotFound
from swh.loader.git import directory as git_directory
from swh.loader.git.directory import (
    GitCheckoutLoader,
    TreeTooLarge,
    checkout_repository_ref,
    count_tree_entries,
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
    dir1 = Directory.from_disk(path=repo_path, max_content_length=None)

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

    dir2 = Directory.from_disk(
        path=repo_path, path_filter=list_git_tree, max_content_length=None
    )
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
    repo_url: str, ref: str, hash_name: NarHashAlgo = "sha256", temp_dir: str = "/tmp"
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
    refs = LocalGitClient().get_refs(repo_url.replace("file://", "")).refs
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
    assert actual_result["status"] == "failed"
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

    git_repo.get_worktree().stage(["file"])
    git_repo.get_worktree().commit(
        b"file added",
        committer=b"Test Committer <test@example.org>",
        author=b"Test Author <test@example.org>",
        commit_timestamp=12395,
        commit_timezone=0,
        author_timestamp=12395,
        author_timezone=0,
        sign=False,
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
        run(
            ["git", "config", "commit.gpgsign", "false"],
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


def _git(cmd, cwd, stdin=None) -> str:
    """Run a git command and return its stripped standard output."""
    proc = run(["git"] + cmd, cwd=cwd, input=stdin, check=True, capture_output=True)
    return proc.stdout.decode().strip()


def _make_deep_shared_tree(path: Path, levels: int = 6, fanout: int = 4) -> str:
    """Build a small, deeply-shared tree and return its commit id.

    Each level is a tree whose ``fanout`` entries all name the *same*
    next-level tree, so the repository stays a handful of objects while the
    checked-out tree is ``fanout ** levels`` files, plus the directories that
    hold them. Kept small enough to check out safely if the guard fails, so a
    regression is a failing assertion rather than a filled disk.
    """
    _git(["init", "--initial-branch=main", str(path)], cwd=path.parent)
    _git(["config", "user.email", "t@example.org"], cwd=path)
    _git(["config", "user.name", "t"], cwd=path)

    # bottom of the pyramid: a single blob, reached by every path
    blob = _git(["hash-object", "-w", "--stdin"], cwd=path, stdin=b"boom\n")
    level = _git(["mktree"], cwd=path, stdin=f"100644 blob {blob}\tf\n".encode())

    for _ in range(levels):
        spec = "".join(f"040000 tree {level}\td{i}\n" for i in range(fanout))
        level = _git(["mktree"], cwd=path, stdin=spec.encode())

    commit = _git(["commit-tree", level, "-m", "tree"], cwd=path)
    _git(["update-ref", "refs/heads/main", commit], cwd=path)
    return commit


def _inodes_on_disk(root: Path) -> int:
    """Count what is actually on disk under ``root``, ignoring ``.git``.

    The ground truth the counter is checked against: every file and every
    directory a checkout leaves behind is one inode.
    """
    return sum(1 for p in root.rglob("*") if ".git" not in p.relative_to(root).parts)


def test_count_tree_entries_counts_exactly(tmp_path):
    """The counter must agree with what a checkout would actually write.

    Asserted against the working tree itself rather than against arithmetic, so
    the test cannot drift with the implementation: four files and one directory
    are five inodes, and five is what the counter has to say.

    The pathname with an embedded newline is the reason ``-z`` is used: without
    it git quotes such a name and the record separator becomes ambiguous.
    """
    repo = tmp_path / "small"
    repo.mkdir()
    _git(["init", "--initial-branch=main", "."], cwd=repo)
    _git(["config", "user.email", "t@example.org"], cwd=repo)
    _git(["config", "user.name", "t"], cwd=repo)
    (repo / "a").write_text("a")
    (repo / "we\nird").write_text("w")
    (repo / "sub").mkdir()
    (repo / "sub" / "b").write_text("b")
    (repo / "sub" / "c").write_text("c")
    _git(["add", "-A"], cwd=repo)
    _git(["commit", "-m", "x"], cwd=repo)

    assert _inodes_on_disk(repo) == 5
    assert count_tree_entries(str(repo), "HEAD", limit=100) == 5


def test_count_tree_entries_times_out_rather_than_undercounting(tmp_path):
    """A git that stops emitting must raise, never return a short count.

    The subtle half: killing git closes the pipe, so the read returns b"" just
    as it does at a legitimate end of output. Believing that would hand back
    whatever had been counted so far -- an undercount, which is the one
    direction this guard may never fail in. So a timeout has to be
    distinguishable from EOF, and it has to raise.

    Simulated with a timeout of zero rather than a wedged git, so the test is
    deterministic and costs nothing.
    """
    repo = tmp_path / "shared"
    repo.mkdir()
    _make_deep_shared_tree(repo, levels=6, fanout=4)

    with pytest.raises(TimeoutExpired):
        count_tree_entries(str(repo), "HEAD", limit=1_000_000, timeout=0.000001)


def test_count_tree_entries_reports_a_git_failure(tmp_path):
    """An unresolvable revision must raise, not silently count zero."""
    repo = tmp_path / "empty"
    repo.mkdir()
    _git(["init", "--initial-branch=main", "."], cwd=repo)

    with pytest.raises(CalledProcessError):
        count_tree_entries(str(repo), "no-such-ref", limit=100)


def test_count_tree_entries_rejects_an_oversized_tree(tmp_path):
    """9556 inodes from ~8 objects; the limit must bite.

    4**6 = 4096 of those are files and the remaining 5460 are the directories
    holding them, which is why the golden value is not a power of four.
    """
    repo = tmp_path / "shared"
    repo.mkdir()
    _make_deep_shared_tree(repo, levels=6, fanout=4)

    # exact count, when we allow it
    assert count_tree_entries(str(repo), "HEAD", limit=100_000) == 9556

    # and refused, when we do not
    with pytest.raises(TreeTooLarge):
        count_tree_entries(str(repo), "HEAD", limit=100)


def test_count_tree_entries_counts_directories_not_only_files(tmp_path):
    """A tree whose cost is carried by directories must still be counted.

    This is the test that fails if ``-t`` is ever dropped from the ``ls-tree``
    invocation. The tree below holds a single file at the bottom of a chain of
    sixty directories: leaves-only counting calls it 1, a checkout creates 61
    inodes, and a limit of 50 has to refuse it.

    Nothing about "files" bounds this shape, which is why the guard counts
    inodes rather than blobs.
    """
    repo = tmp_path / "chain"
    repo.mkdir()
    _git(["init", "--initial-branch=main", "."], cwd=repo)
    _git(["config", "user.email", "t@example.org"], cwd=repo)
    _git(["config", "user.name", "t"], cwd=repo)

    deep = repo.joinpath(*(f"d{i}" for i in range(60)))
    deep.mkdir(parents=True)
    (deep / "f").write_text("x")
    _git(["add", "-A"], cwd=repo)
    _git(["commit", "-m", "chain"], cwd=repo)

    assert _inodes_on_disk(repo) == 61
    assert count_tree_entries(str(repo), "HEAD", limit=1000) == 61

    with pytest.raises(TreeTooLarge):
        count_tree_entries(str(repo), "HEAD", limit=50)


def test_count_tree_entries_is_cheap_regardless_of_expansion(tmp_path):
    """The whole point: rejection must not depend on the expanded size.

    A deeper tree has vastly more entries but the counter still stops after
    `limit` of them, so the two rejections cost about the same.
    """
    repo = tmp_path / "deep"
    repo.mkdir()
    _make_deep_shared_tree(repo, levels=12, fanout=4)  # 39M inodes, 8 objects

    t0 = time.monotonic()
    with pytest.raises(TreeTooLarge):
        count_tree_entries(str(repo), "HEAD", limit=1000)
    elapsed = time.monotonic() - t0
    # generous bound: this is about not being O(expanded), not a benchmark
    assert elapsed < 30, f"rejection took {elapsed:.1f}s -- is it streaming?"


def test_checkout_refuses_an_oversized_tree_without_writing_it(tmp_path):
    """End to end: the tree is refused and nothing is materialised."""
    remote = tmp_path / "remote"
    remote.mkdir()
    _make_deep_shared_tree(remote, levels=6, fanout=4)

    target = tmp_path / "target"
    target.mkdir()
    with pytest.raises(TreeTooLarge):
        checkout_repository_ref(
            f"file://{remote}", "main", target=target, max_tree_entries=100
        )

    # the working tree was never written: only .git exists under the clone
    clone = target / "remote"
    materialised = [p for p in clone.rglob("*") if ".git" not in p.parts]
    assert materialised == [], f"checkout wrote {len(materialised)} paths anyway"


def test_checkout_still_works_under_the_limit(tmp_path):
    """The guard must not break ordinary repositories."""
    remote = tmp_path / "ok-remote"
    remote.mkdir()
    _make_deep_shared_tree(remote, levels=3, fanout=2)  # 22 inodes

    target = tmp_path / "ok-target"
    target.mkdir()
    path = checkout_repository_ref(
        f"file://{remote}", "main", target=target, max_tree_entries=1000
    )
    assert (path / "d0" / "d0" / "d0" / "f").exists()


def test_checkout_still_works_when_the_shallow_fetch_fails(tmp_path, mocker):
    """The full-fetch fallback must keep working.

    On that path no local branch exists yet, only ``refs/remotes/origin/main``.
    ``git checkout main`` resolves it anyway, but plumbing such as
    ``git ls-tree`` does not, so the entry count has to resolve the ref first.
    """
    remote = tmp_path / "fallback-remote"
    remote.mkdir()
    _make_deep_shared_tree(remote, levels=3, fanout=2)  # 22 inodes

    # force the `git fetch --depth 1` attempt to fail, without touching the
    # other git invocations
    real_check_output = git_directory.check_output

    def fail_shallow_fetch(cmd, *args, **kwargs):
        if "--depth" in cmd:
            raise CalledProcessError(1, cmd, stderr=b"shallow not supported\n")
        return real_check_output(cmd, *args, **kwargs)

    mocker.patch.object(git_directory, "check_output", side_effect=fail_shallow_fetch)

    target = tmp_path / "fallback-target"
    target.mkdir()
    path = checkout_repository_ref(f"file://{remote}", "main", target=target)
    assert (path / "d0" / "d0" / "d0" / "f").exists()


def test_checkout_does_not_report_a_timeout_as_not_found(tmp_path, mocker):
    """A timeout must surface as a failure, never as a ``not_found`` visit.

    ``subprocess.TimeoutExpired`` is a sibling of ``CalledProcessError``, not a
    subclass, so it flows past the ``except CalledProcessError`` handler that
    translates unreachable remotes into :exc:`NotFound`.
    """
    remote = tmp_path / "slow-remote"
    remote.mkdir()
    _make_deep_shared_tree(remote, levels=3, fanout=2)

    mocker.patch.object(
        git_directory,
        "check_output",
        side_effect=TimeoutExpired(cmd=["git"], timeout=1),
    )

    target = tmp_path / "slow-target"
    target.mkdir()
    with pytest.raises(TimeoutExpired):
        checkout_repository_ref(f"file://{remote}", "main", target=target)


def test_loader_threads_the_limits_down_to_the_checkout(swh_storage, tmp_path, mocker):
    """The constructor kwargs must reach `checkout_repository_ref`.

    They are how an operator tunes the guard: `from_configfile` passes anything
    in the loader configuration through to `__init__`.
    """
    remote = tmp_path / "kwargs-remote"
    remote.mkdir()
    _make_deep_shared_tree(remote, levels=6, fanout=4)  # 9556 inodes

    loader = GitCheckoutLoader(
        swh_storage,
        f"file://{remote}",
        ref="main",
        checksum_layout="standard",
        checksums={},
        max_tree_entries="100",  # a string, as a YAML config would give it
        git_timeout="120",
    )
    assert loader.max_tree_entries == 100
    assert loader.git_timeout == 120

    spy = mocker.spy(git_directory, "checkout_repository_ref")

    result = loader.load()
    assert result["status"] == "failed"
    assert spy.call_args.kwargs["max_tree_entries"] == 100
    assert spy.call_args.kwargs["timeout"] == 120
