# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Tests for the gitoxide (gix) Rust extension module.

This file grows with the migration phases:
  Phase 1: build infrastructure smoke test + fetch_pack smoke test.
  Phase 2 (current): pack inflation unit tests.
  Phase 3: converter compatibility tests (no dulwich).
"""

import hashlib
import socket
import subprocess

import pytest

import swh.loader.git._gix as _gix


def _http_reachable(host: str, port: int = 443, timeout: float = 3.0) -> bool:
    """Return True if a TCP connection to host:port succeeds within timeout."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


network = pytest.mark.skipif(
    not _http_reachable("gitlab.softwareheritage.org", 443),
    reason="network not available",
)


def test_extension_loads():
    """The compiled Rust extension imports without error."""
    assert _gix is not None


def test_version():
    """version() returns a non-empty string."""
    v = _gix.version()
    assert isinstance(v, str)
    assert len(v) > 0


def test_fetch_pack_invalid_url():
    """fetch_pack raises ValueError for a bogus URL."""
    with pytest.raises(ValueError):
        _gix.fetch_pack("http://localhost:1/nonexistent.git", [], [])


@network
def test_fetch_pack_list_refs_only(monkeypatch):
    """fetch_pack with no wants returns remote_refs without transferring a pack.

    Uses the public swh-py-template repo (very small, stable).

    The monkeypatch is needed to remove the ``http_proxy``/``https_proxy``
    environment variables injected by the ``swh_proxy`` session fixture
    (swh.loader.pytest_plugin), which sets them to ``http://localhost:999``
    to block accidental network access in other tests.  These network tests
    explicitly need real connectivity, so we clear the proxy for the duration
    of the test.
    """
    monkeypatch.delenv("http_proxy", raising=False)
    monkeypatch.delenv("https_proxy", raising=False)
    monkeypatch.delenv("HTTP_PROXY", raising=False)
    monkeypatch.delenv("HTTPS_PROXY", raising=False)

    url = "https://gitlab.softwareheritage.org/swh/devel/swh-py-template.git"
    remote_refs, symbolic_refs, pack_data = _gix.fetch_pack(url, [], [])

    # Should have at least one ref (HEAD or a branch)
    assert len(remote_refs) > 0

    # All keys are bytes, all values are 40-char hex strings
    for ref_name, sha in remote_refs.items():
        assert isinstance(ref_name, bytes), f"ref name must be bytes: {ref_name!r}"
        assert isinstance(sha, str), f"SHA must be str: {sha!r}"
        assert len(sha) == 40, f"SHA must be 40 hex chars: {sha!r}"
        assert all(c in "0123456789abcdef" for c in sha), f"non-hex SHA: {sha!r}"

    # Symbolic refs keys/values are both bytes
    for src, dst in symbolic_refs.items():
        assert isinstance(src, bytes)
        assert isinstance(dst, bytes)

    # No wants → no pack data transferred
    assert pack_data == b""


# ── Phase 2: iter_pack_objects ────────────────────────────────────────────────


def _git_sha1(type_str: str, data: bytes) -> bytes:
    """Reproduce git's object SHA-1: sha1("<type> <len>\\0<data>")."""
    header = f"{type_str} {len(data)}\0".encode()
    return hashlib.sha1(header + data).digest()


_TYPE_NUM_TO_NAME = {1: "commit", 2: "tree", 3: "blob", 4: "tag"}


@pytest.fixture
def tiny_pack(tmp_path):
    """Create a tiny git repository with one blob, one tree and one commit,
    then return its pack bytes (via ``git pack-objects --stdout --all``).

    The fixture uses git directly so it does not depend on dulwich.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", str(repo)], check=True, capture_output=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@example.com"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "Test"],
        check=True,
        capture_output=True,
    )
    (repo / "hello.txt").write_bytes(b"Hello, gix!\n")
    subprocess.run(
        ["git", "-C", str(repo), "add", "."], check=True, capture_output=True
    )
    subprocess.run(
        ["git", "-C", str(repo), "commit", "--no-gpg-sign", "-m", "initial"],
        check=True,
        capture_output=True,
    )
    result = subprocess.run(
        ["git", "-C", str(repo), "pack-objects", "--stdout", "--all"],
        check=True,
        capture_output=True,
    )
    return result.stdout


def test_iter_pack_objects_returns_three_object_types(tiny_pack):
    """iter_pack_objects yields blob, tree and commit objects for a tiny repo."""
    objects = _gix.iter_pack_objects(tiny_pack)

    assert len(objects) == 3, f"expected 3 objects, got {len(objects)}"

    type_nums = {t for t, _sha, _data in objects}
    assert type_nums == {1, 2, 3}, f"missing object types: {type_nums}"


def test_iter_pack_objects_sha1_identity(tiny_pack):
    """SHA-1 returned by iter_pack_objects matches the git SHA-1 computed from
    the type and content — the fundamental correctness invariant."""
    objects = _gix.iter_pack_objects(tiny_pack)

    for type_num, sha1, data in objects:
        type_name = _TYPE_NUM_TO_NAME[type_num]
        expected_sha1 = _git_sha1(type_name, data)
        assert sha1 == expected_sha1, (
            f"SHA-1 mismatch for {type_name}: "
            f"gix={sha1.hex()!r}  expected={expected_sha1.hex()!r}"
        )


def test_iter_pack_objects_sha1_fields_are_20_bytes(tiny_pack):
    """All sha1 values are exactly 20 bytes."""
    objects = _gix.iter_pack_objects(tiny_pack)
    for _type_num, sha1, _data in objects:
        assert len(sha1) == 20


def test_iter_pack_objects_invalid_input():
    """iter_pack_objects raises ValueError on garbage input."""
    with pytest.raises(ValueError):
        _gix.iter_pack_objects(b"not a pack file at all")


def test_iter_pack_objects_empty_pack():
    """iter_pack_objects on an empty pack (zero objects) returns an empty list."""
    import hashlib
    import struct

    # Minimal valid pack: header + SHA-1 trailer (no objects)
    num_objects = 0
    header = b"PACK" + struct.pack(">I", 2) + struct.pack(">I", num_objects)
    # The 20-byte SHA-1 of the header bytes is the pack checksum
    checksum = hashlib.sha1(header).digest()
    pack_bytes = header + checksum

    objects = _gix.iter_pack_objects(pack_bytes)
    assert objects == []


# ── Phase 4A: PackReader streaming iterator ──────────────────────────────────


def _pack_reader_sha1_git(obj_tuple):
    """Extract sha1_git from a PackReader tuple (handles both tree formats)."""
    from swh.model.model import Directory

    if obj_tuple[0] == 2 and isinstance(obj_tuple[1], Directory):
        return obj_tuple[1].id
    return obj_tuple[1]


def test_pack_reader_matches_iter_pack_objects(tiny_pack, tmp_path):
    """PackReader yields the same sha1_git set as iter_pack_objects."""
    pack_file = tmp_path / "test.pack"
    pack_file.write_bytes(tiny_pack)

    batch_shas = {sha.hex() for (_, sha, _) in _gix.iter_pack_objects(tiny_pack)}

    stream_shas = set()
    for obj_tuple in _gix.PackReader(str(pack_file)):
        stream_shas.add(_pack_reader_sha1_git(obj_tuple).hex())

    assert (
        stream_shas == batch_shas
    ), "PackReader and iter_pack_objects must yield same objects"


def test_pack_reader_blob_hashes(tiny_pack, tmp_path):
    """PackReader computes correct blob hashes."""
    pack_file = tmp_path / "test.pack"
    pack_file.write_bytes(tiny_pack)

    for obj_tuple in _gix.PackReader(str(pack_file)):
        if obj_tuple[0] == 3:  # blob
            _type, sha1_git, sha1, sha256, blake2s256, data = obj_tuple
            expected = _git_sha1("blob", data)
            assert sha1_git == expected, "sha1_git mismatch for blob"
            assert len(sha1) == 20
            assert len(sha256) == 32
            assert len(blake2s256) == 32


def test_pack_reader_tree_verified(tiny_pack, tmp_path):
    """PackReader returns Directory objects for verified trees."""
    from swh.model.model import Directory

    pack_file = tmp_path / "test.pack"
    pack_file.write_bytes(tiny_pack)

    for obj_tuple in _gix.PackReader(str(pack_file)):
        if obj_tuple[0] == 2:  # tree
            assert isinstance(
                obj_tuple[1], Directory
            ), "verified tree should return Directory object"
            dir_obj = obj_tuple[1]
            assert len(dir_obj.id) == 20
            assert isinstance(dir_obj.entries, tuple)
            for entry in dir_obj.entries:
                assert isinstance(entry.name, bytes)
                assert isinstance(entry.perms, int)
                assert len(entry.target) == 20
                assert entry.type in ("file", "dir", "rev")


def test_parallel_pack_reader_matches_pack_reader(tiny_pack, tmp_path):
    """ParallelPackReader yields the same sha1_git set as PackReader."""
    pack_file = tmp_path / "test.pack"
    pack_file.write_bytes(tiny_pack)

    sequential_shas = set()
    for obj_tuple in _gix.PackReader(str(pack_file)):
        sequential_shas.add(_pack_reader_sha1_git(obj_tuple).hex())

    parallel_shas = set()
    for obj_tuple in _gix.ParallelPackReader(str(pack_file)):
        parallel_shas.add(_pack_reader_sha1_git(obj_tuple).hex())

    assert (
        parallel_shas == sequential_shas
    ), "ParallelPackReader and PackReader must yield same objects"


def test_parallel_pack_reader_blob_hashes(tiny_pack, tmp_path):
    """ParallelPackReader computes correct blob hashes."""
    pack_file = tmp_path / "test.pack"
    pack_file.write_bytes(tiny_pack)

    for obj_tuple in _gix.ParallelPackReader(str(pack_file)):
        if obj_tuple[0] == 3:  # blob
            _type, sha1_git, sha1, sha256, blake2s256, data = obj_tuple
            expected = _git_sha1("blob", data)
            assert sha1_git == expected, "sha1_git mismatch for blob"
            assert len(sha1) == 20
            assert len(sha256) == 32
            assert len(blake2s256) == 32


@network
def test_fetch_pack_size_limit(monkeypatch):
    """fetch_pack raises ValueError when the pack exceeds size_limit."""
    monkeypatch.delenv("http_proxy", raising=False)
    monkeypatch.delenv("https_proxy", raising=False)
    monkeypatch.delenv("HTTP_PROXY", raising=False)
    monkeypatch.delenv("HTTPS_PROXY", raising=False)

    url = "https://gitlab.softwareheritage.org/swh/devel/swh-py-template.git"

    # First get a ref to want something real
    remote_refs, _, _ = _gix.fetch_pack(url, [], [])
    if not remote_refs:
        pytest.skip("repo returned no refs")

    # Pick one SHA to want and set an absurdly low size limit (1 byte)
    sha_hex = next(iter(remote_refs.values()))
    sha_bytes = bytes.fromhex(sha_hex)

    with pytest.raises(ValueError, match="size"):
        _gix.fetch_pack(url, [sha_bytes], [], size_limit=1)
