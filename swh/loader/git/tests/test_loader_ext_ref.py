# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""``_resolve_ext_ref`` must hand dulwich the exact bytes of the object asked
for, which are the ones in ``raw_manifest`` when that field is set."""

import datetime
import hashlib

import pytest

from swh.loader.git.loader import GitLoader
from swh.model.model import (
    Directory,
    DirectoryEntry,
    Person,
    Revision,
    RevisionType,
    TimestampWithTimezone,
)


def _revision_with_raw_manifest() -> Revision:
    """A revision carrying a header the model does not keep, so the rebuilt
    manifest is shorter than the original."""
    person = Person.from_fullname(b"John Doe <john.doe@example.org>")
    ts = TimestampWithTimezone.from_datetime(
        datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
    )
    body = (
        b"tree " + b"0" * 40 + b"\n"
        b"author John Doe <john.doe@example.org> 1577836800 +0000\n"
        b"encoding ISO-8859-1\n"
        b"committer John Doe <john.doe@example.org> 1577836800 +0000\n"
        b"\n"
        b"x\n"
    )
    raw = b"commit %d\x00" % len(body) + body
    revision = Revision(
        message=b"x\n",
        author=person,
        committer=person,
        date=ts,
        committer_date=ts,
        type=RevisionType.GIT,
        directory=bytes.fromhex("0" * 40),
        synthetic=False,
        parents=(),
        raw_manifest=raw,
        id=hashlib.sha1(raw).digest(),
    )
    revision.check()
    return revision


def _directory_with_raw_manifest() -> Directory:
    """A tree whose entries are stored in non-canonical order.

    The model sorts entries, so the rebuilt manifest has the SAME length as
    the original and different bytes.
    """
    target = b"\x01" * 20
    entry = b"100644 %s\x00" + target
    body = (entry % b"b") + (entry % b"a")
    raw = b"tree %d\x00" % len(body) + body
    entries = tuple(
        DirectoryEntry(name=name, type="file", target=target, perms=0o100644)
        for name in (b"a", b"b")
    )
    directory = Directory(
        entries=entries, raw_manifest=raw, id=hashlib.sha1(raw).digest()
    )
    directory.check()
    return directory


@pytest.mark.parametrize("object_type", ["revision", "directory"])
def test_resolve_ext_ref_returns_the_archived_bytes(swh_storage, object_type):
    if object_type == "revision":
        obj = _revision_with_raw_manifest()
        swh_storage.revision_add([obj])
        expected_type_num, header = 1, b"commit"
    else:
        obj = _directory_with_raw_manifest()
        swh_storage.directory_add([obj])
        expected_type_num, header = 2, b"tree"
    swh_storage.flush()

    loader = GitLoader(swh_storage, "https://example.org/unused")

    type_num, chunks = loader._resolve_ext_ref(obj.id)

    assert type_num == expected_type_num
    body = b"".join(chunks)
    manifest = header + b" %d\x00" % len(body) + body
    assert manifest == obj.raw_manifest
    assert hashlib.sha1(manifest).digest() == obj.id
