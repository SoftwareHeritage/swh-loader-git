# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Convert dulwich objects to dictionaries suitable for swh.storage"""

from typing import Any, Dict, Optional, cast

from dulwich.objects import Blob, Commit, ShaFile, Tag, Tree

from swh.model.hashutil import DEFAULT_ALGORITHMS, MultiHash, hash_to_bytes
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
    TargetType,
    Timestamp,
    TimestampWithTimezone,
)


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
    """Convert a dulwich blob to a Software Heritage content

    """
    if obj.type_name != b"blob":
        raise ValueError("Argument is not a blob.")
    blob = cast(Blob, obj)

    hashes = dulwich_blob_to_content_id(blob)
    if max_content_size is not None and hashes["length"] >= max_content_size:
        return SkippedContent(status="absent", reason="Content too large", **hashes,)
    else:
        return Content(data=blob.as_raw_string(), status="visible", **hashes,)


def dulwich_tree_to_directory(obj: ShaFile, log=None) -> Directory:
    """Format a tree as a directory"""
    if obj.type_name != b"tree":
        raise ValueError("Argument is not a tree.")
    tree = cast(Tree, obj)

    entries = []

    entry_mode_map = {
        0o040000: "dir",
        0o160000: "rev",
        0o100644: "file",
        0o100755: "file",
        0o120000: "file",
    }

    for entry in tree.iteritems():
        entries.append(
            DirectoryEntry(
                type=entry_mode_map.get(entry.mode, "file"),
                perms=entry.mode,
                name=entry.path,
                target=hash_to_bytes(entry.sha.decode("ascii")),
            )
        )

    dir_ = Directory(id=tree.sha().digest(), entries=tuple(entries),)
    check_id(dir_)
    return dir_


def parse_author(name_email: bytes) -> Person:
    """Parse an author line"""
    return Person.from_fullname(name_email)


def dulwich_tsinfo_to_timestamp(
    timestamp, timezone, timezone_neg_utc
) -> TimestampWithTimezone:
    """Convert the dulwich timestamp information to a structure compatible with
    Software Heritage"""
    return TimestampWithTimezone(
        timestamp=Timestamp(seconds=int(timestamp), microseconds=0,),
        offset=timezone // 60,
        negative_utc=timezone_neg_utc if timezone == 0 else False,
    )


def dulwich_commit_to_revision(obj: ShaFile, log=None) -> Revision:
    if obj.type_name != b"commit":
        raise ValueError("Argument is not a commit.")
    commit = cast(Commit, obj)

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
            commit.author_time, commit.author_timezone, commit._author_timezone_neg_utc,
        ),
        committer=parse_author(commit.committer),
        committer_date=dulwich_tsinfo_to_timestamp(
            commit.commit_time, commit.commit_timezone, commit._commit_timezone_neg_utc,
        ),
        type=RevisionType.GIT,
        directory=bytes.fromhex(commit.tree.decode()),
        message=commit.message,
        metadata=None,
        extra_headers=tuple(extra_headers),
        synthetic=False,
        parents=tuple(bytes.fromhex(p.decode()) for p in commit.parents),
    )
    check_id(rev)
    return rev


DULWICH_TARGET_TYPES = {
    b"blob": TargetType.CONTENT,
    b"tree": TargetType.DIRECTORY,
    b"commit": TargetType.REVISION,
    b"tag": TargetType.RELEASE,
}


DULWICH_OBJECT_TYPES = {
    b"blob": ObjectType.CONTENT,
    b"tree": ObjectType.DIRECTORY,
    b"commit": ObjectType.REVISION,
    b"tag": ObjectType.RELEASE,
}


def dulwich_tag_to_release(obj: ShaFile, log=None) -> Release:
    if obj.type_name != b"tag":
        raise ValueError("Argument is not a tag.")
    tag = cast(Tag, obj)

    target_type, target = tag.object
    if tag.tagger:
        author: Optional[Person] = parse_author(tag.tagger)
        if not tag.tag_time:
            date = None
        else:
            date = dulwich_tsinfo_to_timestamp(
                tag.tag_time, tag.tag_timezone, tag._tag_timezone_neg_utc,
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
    check_id(rel)
    return rel
