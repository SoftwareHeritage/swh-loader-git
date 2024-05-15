# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Convert dulwich objects to dictionaries suitable for swh.storage"""

import logging
import re
from typing import Any, Dict, Optional, cast

import attr
from dulwich.objects import Blob, Commit, ShaFile, Tag, Tree, _parse_message

from swh.model.hashutil import (
    DEFAULT_ALGORITHMS,
    MultiHash,
    git_object_header,
    hash_to_bytes,
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


class HashMismatch(Exception):
    pass


def check_id(obj: HashableObject) -> None:
    real_id = obj.compute_hash()
    if obj.id != real_id:
        raise HashMismatch(
            f"Expected {type(obj).__name__} hash to be {obj.id.hex()}, "
            f"got {real_id.hex()}"
        )


def dulwich_blob_to_content_id(obj: ShaFile) -> Dict[str, Any]:
    """Convert a dulwich blob to a Software Heritage content id"""
    if obj.type_name != b"blob":
        raise ValueError("Argument is not a blob.")
    blob = cast(Blob, obj)

    size = blob.raw_length()
    data = blob.as_raw_string()
    hashes = MultiHash.from_data(data, DEFAULT_ALGORITHMS).digest()
    if hashes["sha1_git"] != blob.sha().digest():
        raise HashMismatch(
            f"Expected Content hash to be {blob.sha().digest().hex()}, "
            f"got {hashes['sha1_git'].hex()}"
        )
    hashes["length"] = size
    return hashes


def dulwich_blob_to_content(obj: ShaFile, max_content_size=None) -> BaseContent:
    """Convert a dulwich blob to a Software Heritage content"""
    if obj.type_name != b"blob":
        raise ValueError("Argument is not a blob.")
    blob = cast(Blob, obj)

    hashes = dulwich_blob_to_content_id(blob)
    if max_content_size is not None and hashes["length"] >= max_content_size:
        return SkippedContent(
            status="absent",
            reason="Content too large",
            **hashes,
        )
    else:
        return Content(
            data=blob.as_raw_string(),
            status="visible",
            **hashes,
        )


def dulwich_tree_to_directory(obj: ShaFile) -> Directory:
    """Format a tree as a directory"""
    if obj.type_name != b"tree":
        raise ValueError("Argument is not a tree.")
    tree = cast(Tree, obj)

    entries = []

    for entry in tree.iteritems():
        if entry.mode & COMMIT_MODE_MASK == COMMIT_MODE_MASK:
            type_ = "rev"
        elif entry.mode & TREE_MODE_MASK == TREE_MODE_MASK:
            type_ = "dir"
        else:
            type_ = "file"

        entries.append(
            DirectoryEntry(
                type=type_,
                perms=entry.mode,
                name=entry.path.replace(
                    b"/", b"_"
                ),  # '/' is very rare, and invalid in SWH.
                target=hash_to_bytes(entry.sha.decode("ascii")),
            )
        )

    dir_ = Directory(
        id=tree.sha().digest(),
        entries=tuple(entries),
    )

    if dir_.compute_hash() != dir_.id:
        expected_id = dir_.id
        actual_id = dir_.compute_hash()
        logger.warning(
            "Expected directory to have id %s, but got %s. Recording raw_manifest.",
            hash_to_hex(expected_id),
            hash_to_hex(actual_id),
        )
        raw_string = tree.as_raw_string()
        dir_ = attr.evolve(
            dir_, raw_manifest=git_object_header("tree", len(raw_string)) + raw_string
        )

    check_id(dir_)
    return dir_


def parse_author(name_email: bytes) -> Person:
    """Parse an author line"""
    return Person.from_fullname(name_email)


def dulwich_tsinfo_to_timestamp(
    timestamp,
    timezone: int,
    timezone_neg_utc: bool,
    timezone_bytes: Optional[bytes],
) -> TimestampWithTimezone:
    """Convert the dulwich timestamp information to a structure compatible with
    Software Heritage."""
    ts = Timestamp(
        seconds=int(timestamp),
        microseconds=0,
    )
    if timezone_bytes is None:
        # Failed to parse from the raw manifest, fallback to what Dulwich managed to
        # parse.
        return TimestampWithTimezone.from_numeric_offset(
            timestamp=ts,
            offset=timezone // 60,
            negative_utc=timezone_neg_utc,
        )
    else:
        return TimestampWithTimezone(timestamp=ts, offset_bytes=timezone_bytes)


def dulwich_commit_to_revision(obj: ShaFile) -> Revision:
    if obj.type_name != b"commit":
        raise ValueError("Argument is not a commit.")
    commit = cast(Commit, obj)

    author_timezone = None
    committer_timezone = None
    assert commit._chunked_text is not None  # to keep mypy happy
    for field, value in _parse_message(commit._chunked_text):
        if field == b"author":
            assert value is not None
            m = AUTHORSHIP_LINE_RE.match(value)
            if m:
                author_timezone = m.group("timezone")
        elif field == b"committer":
            assert value is not None
            m = AUTHORSHIP_LINE_RE.match(value)
            if m:
                committer_timezone = m.group("timezone")

    extra_headers = []
    if commit.encoding is not None:
        extra_headers.append((b"encoding", commit.encoding))
    if commit.mergetag:
        for mergetag in commit.mergetag:
            raw_string = mergetag.as_raw_string()
            assert raw_string.endswith(b"\n")
            extra_headers.append((b"mergetag", raw_string[:-1]))

    if commit.extra:
        extra_headers.extend((k, v) for k, v in commit.extra)

    if commit.gpgsig:
        extra_headers.append((b"gpgsig", commit.gpgsig))

    rev = Revision(
        id=commit.sha().digest(),
        author=parse_author(commit.author),
        date=dulwich_tsinfo_to_timestamp(
            commit.author_time,
            commit.author_timezone,
            commit._author_timezone_neg_utc,
            author_timezone,
        ),
        committer=parse_author(commit.committer),
        committer_date=dulwich_tsinfo_to_timestamp(
            commit.commit_time,
            commit.commit_timezone,
            commit._commit_timezone_neg_utc,
            committer_timezone,
        ),
        type=RevisionType.GIT,
        directory=bytes.fromhex(commit.tree.decode()),
        message=commit.message,
        metadata=None,
        extra_headers=tuple(extra_headers),
        synthetic=False,
        parents=tuple(bytes.fromhex(p.decode()) for p in commit.parents),
    )

    if rev.compute_hash() != rev.id:
        expected_id = rev.id
        actual_id = rev.compute_hash()
        logger.warning(
            "Expected revision to have id %s, but got %s. Recording raw_manifest.",
            hash_to_hex(expected_id),
            hash_to_hex(actual_id),
        )
        raw_string = commit.as_raw_string()
        rev = attr.evolve(
            rev, raw_manifest=git_object_header("commit", len(raw_string)) + raw_string
        )

    check_id(rev)
    return rev


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


def dulwich_tag_to_release(obj: ShaFile) -> Release:
    if obj.type_name != b"tag":
        raise ValueError("Argument is not a tag.")
    tag = cast(Tag, obj)

    tagger_timezone = None
    # FIXME: _parse_message is a private function from Dulwich.
    for field, value in _parse_message(tag.as_raw_chunks()):
        if field == b"tagger":
            assert value is not None
            m = AUTHORSHIP_LINE_RE.match(value)
            if m:
                tagger_timezone = m.group("timezone")

    target_type, target = tag.object
    if tag.tagger:
        author: Optional[Person] = parse_author(tag.tagger)
        if tag.tag_time is None:
            date = None
        else:
            date = dulwich_tsinfo_to_timestamp(
                tag.tag_time,
                tag.tag_timezone,
                tag._tag_timezone_neg_utc,
                tagger_timezone,
            )
    else:
        author = date = None

    message = tag.message
    if tag.signature:
        message += tag.signature

    rel = Release(
        id=tag.sha().digest(),
        author=author,
        date=date,
        name=tag.name,
        target=bytes.fromhex(target.decode()),
        target_type=DULWICH_OBJECT_TYPES[target_type.type_name],
        message=message,
        metadata=None,
        synthetic=False,
    )

    if rel.compute_hash() != rel.id:
        expected_id = rel.id
        actual_id = rel.compute_hash()
        logger.warning(
            "Expected release to have id %s, but got %s. Recording raw_manifest.",
            hash_to_hex(expected_id),
            hash_to_hex(actual_id),
        )
        raw_string = tag.as_raw_string()
        rel = attr.evolve(
            rel, raw_manifest=git_object_header("tag", len(raw_string)) + raw_string
        )

    check_id(rel)
    return rel
