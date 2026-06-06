# Copyright (C) 2015-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Convert git objects to dictionaries suitable for swh.storage.

The primary converter functions (blob_to_content, tree_to_directory,
commit_to_revision, tag_to_release) accept raw bytes and a pre-computed
sha1_git, with no dependency on dulwich.

Backward-compatible ``dulwich_*`` wrappers are provided so that callers
still passing dulwich ShaFile objects (e.g. from_disk.py) continue to work.
"""

from __future__ import annotations

from io import BytesIO
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import attr

from swh.model.hashutil import (
    DEFAULT_ALGORITHMS,
    MultiHash,
    git_object_header,
    hash_to_hex,
)
from swh.model.model import (
    BaseContent,
    Content,
    Directory,
    DirectoryEntry,
    HashableObject,
    ObjectType,
    Person,
    Release,
    Revision,
    RevisionType,
    SkippedContent,
    SnapshotTargetType,
    Timestamp,
    TimestampOverflowException,
    TimestampWithTimezone,
)

COMMIT_MODE_MASK = 0o160000
"""Mode/perms of tree entries that point to a commit.
They are normally equal to this mask, but may have more bits set to 1."""
TREE_MODE_MASK = 0o040000
"""Mode/perms of tree entries that point to a tree.
They are normally equal to this mask, but may have more bits set to 1."""

AUTHORSHIP_LINE_RE = re.compile(rb"^.*> (?P<timestamp>\S+) (?P<timezone>\S+)$")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Target / object type maps (keyed by the ASCII type name used in git objects)
# ---------------------------------------------------------------------------

DULWICH_TARGET_TYPES = {
    b"blob": SnapshotTargetType.CONTENT,
    b"tree": SnapshotTargetType.DIRECTORY,
    b"commit": SnapshotTargetType.REVISION,
    b"tag": SnapshotTargetType.RELEASE,
}

DULWICH_OBJECT_TYPES = {
    b"blob": ObjectType.CONTENT,
    b"tree": ObjectType.DIRECTORY,
    b"commit": ObjectType.REVISION,
    b"tag": ObjectType.RELEASE,
}


# ---------------------------------------------------------------------------
# Hash-checking helpers
# ---------------------------------------------------------------------------


class HashMismatch(Exception):
    pass


def check_id(obj: HashableObject) -> None:
    real_id = obj.compute_hash()
    if obj.id != real_id:
        raise HashMismatch(
            f"Expected {type(obj).__name__} hash to be {obj.id.hex()}, "
            f"got {real_id.hex()}"
        )


# ---------------------------------------------------------------------------
# Low-level git format parsers
# ---------------------------------------------------------------------------


def parse_git_headers(
    raw: bytes,
) -> Tuple[List[Tuple[bytes, bytes]], Optional[bytes]]:
    """Parse the key-value header format used by git commits and tags.

    Returns ``(headers_list, message_body)`` where *headers_list* is an
    ordered list of ``(field, value)`` tuples.  Multi-line continuation
    lines (starting with a space) are joined.  *message_body* is
    everything after the first blank line (``\\n\\n``), or ``None`` when
    there is no body.
    """
    f = BytesIO(raw)
    headers: List[Tuple[bytes, bytes]] = []
    k: Optional[bytes] = None
    v = b""
    eof = False

    def _strip_last_newline(value: bytes) -> bytes:
        if value and value.endswith(b"\n"):
            return value[:-1]
        return value

    for line in f:
        if line.startswith(b" "):
            # Continuation of previous header value
            v += line[1:]
        else:
            if k is not None:
                headers.append((k, _strip_last_newline(v)))
            if line == b"\n":
                # Blank line → end of headers
                break
            (k, v) = line.split(b" ", 1)
    else:
        # Reached EOF before a blank-line separator
        eof = True
        if k is not None:
            headers.append((k, _strip_last_newline(v)))
        return (headers, None)

    if not eof:
        body = f.read()
        return (headers, body if body else None)

    return (headers, None)  # pragma: no cover


def parse_tree_entries(raw: bytes) -> List[Tuple[int, bytes, bytes]]:
    """Parse the binary tree format.

    Each entry is ``<mode_octal_ascii> <name>\\x00<20-byte-sha>``.

    Returns a list of ``(mode_int, name_bytes, sha_20bytes)``.
    """
    entries: List[Tuple[int, bytes, bytes]] = []
    pos = 0
    length = len(raw)
    while pos < length:
        # Find the space separating mode from name
        space = raw.index(b" ", pos)
        mode = int(raw[pos:space], 8)
        # Find the NUL terminating the name
        nul = raw.index(b"\x00", space + 1)
        name = raw[space + 1 : nul]
        # The next 20 bytes are the SHA-1
        sha = raw[nul + 1 : nul + 21]
        entries.append((mode, name, sha))
        pos = nul + 21
    return entries


def parse_authorship_line(
    line: bytes,
) -> Tuple[bytes, Optional[int], Optional[bytes]]:
    """Parse a git author/committer/tagger line.

    Format: ``Name <email> timestamp timezone``

    Returns ``(fullname_bytes, timestamp_or_None, timezone_bytes_or_None)``.
    The *fullname* is everything up to and including the closing ``>``.
    """
    try:
        sep = line.rindex(b"> ")
    except ValueError:
        # No "> " found — the whole line is the identity, no timestamp
        return (line, None, None)
    fullname = line[: sep + 1]
    m = AUTHORSHIP_LINE_RE.match(line)
    if m:
        try:
            timestamp = int(m.group("timestamp"))
        except (ValueError, OverflowError):
            return (fullname, None, None)
        timezone_bytes = m.group("timezone")
        return (fullname, timestamp, timezone_bytes)
    # Could not match the RE — treat as no timestamp
    return (fullname, None, None)


def _parse_timezone(text: bytes) -> Tuple[int, bool]:
    """Parse a timezone fragment like ``+0100`` or ``-0000``.

    Returns ``(offset_seconds, negative_utc_flag)``.

    Compatible with dulwich's ``parse_timezone``.
    """
    if not text or text[0:1] not in (b"+", b"-"):
        raise ValueError(f"Timezone must start with + or - ({text!r})")
    sign = text[:1]
    offset = int(text[1:])
    if sign == b"-":
        offset = -offset
    unnecessary_negative = offset >= 0 and sign == b"-"
    signum = -1 if offset < 0 else 1
    offset = abs(offset)
    hours = offset // 100
    minutes = offset % 100
    return (signum * (hours * 3600 + minutes * 60), unnecessary_negative)


def _make_timestamp(
    timestamp: Optional[int],
    timezone_bytes: Optional[bytes],
) -> TimestampWithTimezone:
    """Build a :class:`TimestampWithTimezone` from parsed authorship info.

    If *timezone_bytes* is available the raw offset string is preserved;
    otherwise we fall back to numeric conversion.
    """
    if timestamp is None:
        ts = Timestamp(seconds=0, microseconds=0)
    else:
        try:
            ts = Timestamp(seconds=int(timestamp), microseconds=0)
        except TimestampOverflowException:
            ts = Timestamp(seconds=0, microseconds=0)

    if timezone_bytes is None:
        # No parseable timezone — use zero offset
        return TimestampWithTimezone.from_numeric_offset(
            timestamp=ts,
            offset=0,
            negative_utc=False,
        )
    else:
        return TimestampWithTimezone(timestamp=ts, offset_bytes=timezone_bytes)


# Keep the old name as an alias for callers that still use it directly
def dulwich_tsinfo_to_timestamp(
    timestamp,
    timezone: int,
    timezone_neg_utc: bool,
    timezone_bytes: Optional[bytes],
) -> TimestampWithTimezone:
    """Convert dulwich timestamp information to SWH format.

    Retained for backward compatibility with from_disk.py and tests.
    """
    try:
        ts = Timestamp(seconds=int(timestamp), microseconds=0)
    except TimestampOverflowException:
        ts = Timestamp(seconds=0, microseconds=0)
    if timezone_bytes is None:
        return TimestampWithTimezone.from_numeric_offset(
            timestamp=ts,
            offset=timezone // 60,
            negative_utc=timezone_neg_utc,
        )
    else:
        return TimestampWithTimezone(timestamp=ts, offset_bytes=timezone_bytes)


# ---------------------------------------------------------------------------
# Author parsing helper
# ---------------------------------------------------------------------------


def parse_author(name_email: bytes) -> Person:
    """Parse an author line"""
    return Person.from_fullname(name_email)


# ---------------------------------------------------------------------------
# Primary converter functions (dulwich-free)
# ---------------------------------------------------------------------------


def blob_to_content_id(sha1_git: bytes, data: bytes) -> Dict[str, Any]:
    """Convert raw blob bytes to a Software Heritage content id dict."""
    hashes = MultiHash.from_data(data, DEFAULT_ALGORITHMS).digest()
    if hashes["sha1_git"] != sha1_git:
        raise HashMismatch(
            f"Expected Content hash to be {sha1_git.hex()}, "
            f"got {hashes['sha1_git'].hex()}"
        )
    hashes["length"] = len(data)
    return hashes


def blob_to_content(sha1_git: bytes, data: bytes, max_content_size=None) -> BaseContent:
    """Convert raw blob bytes to a Software Heritage content object."""
    hashes = blob_to_content_id(sha1_git, data)
    if max_content_size is not None and hashes["length"] >= max_content_size:
        return SkippedContent(
            status="absent",
            reason="Content too large",
            **hashes,
        )
    else:
        return Content(
            data=data,
            status="visible",
            **hashes,
        )


def blob_to_content_precomputed(
    sha1_git: bytes,
    sha1: bytes,
    sha256: bytes,
    blake2s256: bytes,
    data: bytes,
    max_content_size=None,
) -> BaseContent:
    """Convert a blob to a :class:`Content` using pre-computed hashes.

    All four hashes must be 20/32-byte binary digests computed by the Rust
    layer.  Avoids Python-side ``MultiHash`` computation entirely.
    """
    length = len(data)
    if max_content_size is not None and length >= max_content_size:
        return SkippedContent(
            status="absent",
            reason="Content too large",
            sha1=sha1,
            sha1_git=sha1_git,
            sha256=sha256,
            blake2s256=blake2s256,
            length=length,
        )
    return Content(
        data=data,
        status="visible",
        sha1=sha1,
        sha1_git=sha1_git,
        sha256=sha256,
        blake2s256=blake2s256,
        length=length,
    )


def _tree_sort_key(entry: Tuple[int, bytes, bytes]) -> bytes:
    """Sort key matching git's tree entry ordering.

    Git sorts tree entries lexicographically, but appends ``/`` to directory
    names for comparison purposes."""
    mode, name, _sha = entry
    if mode & TREE_MODE_MASK == TREE_MODE_MASK:
        return name + b"/"
    return name


def tree_to_directory(sha1_git: bytes, raw_data: bytes) -> Directory:
    """Convert raw tree bytes to a Software Heritage :class:`Directory`."""
    entries = []
    for mode, name, sha in sorted(parse_tree_entries(raw_data), key=_tree_sort_key):
        if mode & COMMIT_MODE_MASK == COMMIT_MODE_MASK:
            type_ = "rev"
        elif mode & TREE_MODE_MASK == TREE_MODE_MASK:
            type_ = "dir"
        else:
            type_ = "file"

        entries.append(
            DirectoryEntry(
                type=type_,
                perms=mode,
                name=name.replace(b"/", b"_"),  # '/' is very rare, and invalid in SWH.
                target=sha,
            )
        )

    dir_ = Directory(
        id=sha1_git,
        entries=tuple(entries),
    )

    computed = dir_.compute_hash()
    if computed != dir_.id:
        logger.warning(
            "Expected directory to have id %s, but got %s. Recording raw_manifest.",
            hash_to_hex(dir_.id),
            hash_to_hex(computed),
        )
        dir_ = attr.evolve(
            dir_,
            raw_manifest=git_object_header("tree", len(raw_data)) + raw_data,
        )
        computed = dir_.compute_hash()

    if computed != dir_.id:
        raise HashMismatch(
            f"Expected Directory hash to be {dir_.id.hex()}, got {computed.hex()}"
        )
    return dir_


def _entries_to_directory_entries(
    parsed_entries: List[Tuple[int, bytes, bytes]],
) -> Tuple[DirectoryEntry, ...]:
    """Convert pre-parsed tree entries to sorted DirectoryEntry tuple.

    Uses ``__new__`` + ``object.__setattr__`` to bypass the attrs frozen-class
    validators.  This is ~2.7x faster than the regular ``DirectoryEntry(...)``
    constructor path — 0.32 us/entry vs 0.81 us/entry on a typical workload
    (measured with ``benchmarks/bench_direct_tree.py``).  Validation is safe
    to skip here: ``type`` is derived from the mode mask (always one of the
    three valid values), ``target`` and ``name`` come from gix's tree parser
    which already validated them.
    """
    entries = []
    for mode, name, sha in sorted(parsed_entries, key=_tree_sort_key):
        if mode & COMMIT_MODE_MASK == COMMIT_MODE_MASK:
            type_ = "rev"
        elif mode & TREE_MODE_MASK == TREE_MODE_MASK:
            type_ = "dir"
        else:
            type_ = "file"
        de = DirectoryEntry.__new__(DirectoryEntry)
        object.__setattr__(de, "name", name.replace(b"/", b"_"))
        object.__setattr__(de, "type", type_)
        object.__setattr__(de, "target", sha)
        object.__setattr__(de, "perms", mode)
        entries.append(de)
    return tuple(entries)


def tree_to_directory_preparsed(
    sha1_git: bytes,
    raw_data: bytes,
    parsed_entries: List[Tuple[int, bytes, bytes]],
    hash_match: bool = False,
) -> Directory:
    """Convert pre-parsed tree entries to a :class:`Directory`.

    ``parsed_entries`` is a list of ``(mode, name, target_sha)`` tuples
    produced by the Rust layer.  ``raw_data`` is retained for the
    raw_manifest fallback when the hash doesn't match.

    If *hash_match* is True (verified by Rust), the SWH model's
    re-serialization is known to produce identical bytes, so
    ``compute_hash()`` is skipped entirely.

    Bypass attrs validators (``__new__`` + ``object.__setattr__``) for the
    Directory instance as well — the entries have just been built and
    sorted, the id is precomputed by gix.  This mirrors
    ``_entries_to_directory_entries`` below and halves the hot-path cost
    (measured with ``benchmarks/bench_direct_tree.py``).
    """
    entries = _entries_to_directory_entries(parsed_entries)
    if hash_match:
        dir_ = Directory.__new__(Directory)
        object.__setattr__(dir_, "entries", entries)
        object.__setattr__(dir_, "id", sha1_git)
        object.__setattr__(dir_, "raw_manifest", None)
        return dir_

    dir_ = Directory(id=sha1_git, entries=entries)

    computed = dir_.compute_hash()
    if computed != dir_.id:
        logger.warning(
            "Expected directory to have id %s, but got %s. Recording raw_manifest.",
            hash_to_hex(dir_.id),
            hash_to_hex(computed),
        )
        dir_ = attr.evolve(
            dir_,
            raw_manifest=git_object_header("tree", len(raw_data)) + raw_data,
        )
        computed = dir_.compute_hash()

    if computed != dir_.id:
        raise HashMismatch(
            f"Expected Directory hash to be {dir_.id.hex()}, got {computed.hex()}"
        )
    return dir_


def commit_to_revision(
    sha1_git: bytes, raw_data: bytes, hash_match: bool = False
) -> Revision:
    """Convert raw commit bytes to a Software Heritage :class:`Revision`."""
    headers, body = parse_git_headers(raw_data)

    tree_sha: Optional[bytes] = None
    parents: List[bytes] = []
    author_person: Optional[Person] = None
    author_date: Optional[TimestampWithTimezone] = None
    committer_person: Optional[Person] = None
    committer_date: Optional[TimestampWithTimezone] = None
    extra_headers: List[Tuple[bytes, bytes]] = []

    for field, value in headers:
        if field == b"tree":
            tree_sha = bytes.fromhex(value.decode("ascii"))
        elif field == b"parent":
            parents.append(bytes.fromhex(value.decode("ascii")))
        elif field == b"author":
            fullname, timestamp, tz_bytes = parse_authorship_line(value)
            author_person = parse_author(fullname)
            author_date = _make_timestamp(timestamp, tz_bytes)
        elif field == b"committer":
            fullname, timestamp, tz_bytes = parse_authorship_line(value)
            committer_person = parse_author(fullname)
            committer_date = _make_timestamp(timestamp, tz_bytes)
        elif field == b"encoding":
            extra_headers.append((b"encoding", value))
        elif field == b"mergetag":
            # The raw mergetag value ends with \n in the parsed form;
            # dulwich strips that trailing newline when storing.
            v = value
            if v.endswith(b"\n"):
                v = v[:-1]
            extra_headers.append((b"mergetag", v))
        elif field == b"gpgsig":
            extra_headers.append((b"gpgsig", value))
        else:
            extra_headers.append((field, value))

    rev = Revision(
        id=sha1_git,
        author=author_person,
        date=author_date,
        committer=committer_person,
        committer_date=committer_date,
        type=RevisionType.GIT,
        directory=tree_sha or b"",
        message=body,
        metadata=None,
        extra_headers=tuple(extra_headers),
        synthetic=False,
        parents=tuple(parents),
    )

    if hash_match:
        return rev

    computed = rev.compute_hash()
    if computed != rev.id:
        logger.warning(
            "Expected revision to have id %s, but got %s. Recording raw_manifest.",
            hash_to_hex(rev.id),
            hash_to_hex(computed),
        )
        rev = attr.evolve(
            rev,
            raw_manifest=git_object_header("commit", len(raw_data)) + raw_data,
        )
        computed = rev.compute_hash()

    if computed != rev.id:
        raise HashMismatch(
            f"Expected Revision hash to be {rev.id.hex()}, got {computed.hex()}"
        )
    return rev


def tag_to_release(
    sha1_git: bytes, raw_data: bytes, hash_match: bool = False
) -> Release:
    """Convert raw tag bytes to a Software Heritage :class:`Release`."""
    headers, body = parse_git_headers(raw_data)

    target_sha: Optional[bytes] = None
    target_type: Optional[ObjectType] = None
    tag_name: Optional[bytes] = None
    author: Optional[Person] = None
    date: Optional[TimestampWithTimezone] = None

    for field, value in headers:
        if field == b"object":
            target_sha = bytes.fromhex(value.decode("ascii"))
        elif field == b"type":
            target_type = DULWICH_OBJECT_TYPES[value]
        elif field == b"tag":
            tag_name = value
        elif field == b"tagger":
            fullname, timestamp, tz_bytes = parse_authorship_line(value)
            author = parse_author(fullname)
            if timestamp is not None:
                date = _make_timestamp(timestamp, tz_bytes)
            else:
                date = None

    # In dulwich, the body is split at the PGP/SSH signature boundary into
    # message and signature.  The old converter then reconstituted the full
    # body: ``message = tag.message; if tag.signature: message += tag.signature``
    # We simply use the entire body as the message (equivalent result).
    message = body

    rel = Release(
        id=sha1_git,
        author=author,
        date=date,
        name=tag_name or b"",
        target=target_sha or b"",
        target_type=target_type or ObjectType.REVISION,
        message=message,
        metadata=None,
        synthetic=False,
    )

    if hash_match:
        return rel

    computed = rel.compute_hash()
    if computed != rel.id:
        logger.warning(
            "Expected release to have id %s, but got %s. Recording raw_manifest.",
            hash_to_hex(rel.id),
            hash_to_hex(computed),
        )
        rel = attr.evolve(
            rel,
            raw_manifest=git_object_header("tag", len(raw_data)) + raw_data,
        )
        computed = rel.compute_hash()

    if computed != rel.id:
        raise HashMismatch(
            f"Expected Release hash to be {rel.id.hex()}, got {computed.hex()}"
        )
    return rel


# ---------------------------------------------------------------------------
# Backward-compatible dulwich wrappers (used by from_disk.py)
# ---------------------------------------------------------------------------


def dulwich_blob_to_content_id(obj) -> Dict[str, Any]:
    """Convert a dulwich blob to a Software Heritage content id."""
    if obj.type_name != b"blob":
        raise ValueError("Argument is not a blob.")
    return blob_to_content_id(obj.sha().digest(), obj.as_raw_string())


def dulwich_blob_to_content(obj, max_content_size=None) -> BaseContent:
    """Convert a dulwich blob to a Software Heritage content."""
    if obj.type_name != b"blob":
        raise ValueError("Argument is not a blob.")
    return blob_to_content(obj.sha().digest(), obj.as_raw_string(), max_content_size)


def dulwich_tree_to_directory(obj) -> Directory:
    """Format a dulwich tree as a directory."""
    if obj.type_name != b"tree":
        raise ValueError("Argument is not a tree.")
    return tree_to_directory(obj.sha().digest(), obj.as_raw_string())


def dulwich_commit_to_revision(obj) -> Revision:
    """Convert a dulwich commit to a Software Heritage revision."""
    if obj.type_name != b"commit":
        raise ValueError("Argument is not a commit.")
    return commit_to_revision(obj.sha().digest(), obj.as_raw_string())


def dulwich_tag_to_release(obj) -> Release:
    """Convert a dulwich tag to a Software Heritage release."""
    if obj.type_name != b"tag":
        raise ValueError("Argument is not a tag.")
    return tag_to_release(obj.sha().digest(), obj.as_raw_string())
