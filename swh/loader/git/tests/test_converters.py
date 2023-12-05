# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import datetime
import os
import shutil
import subprocess
import tempfile

import dulwich.objects
import dulwich.repo
import pytest

import swh.loader.git.converters as converters
from swh.model.hashutil import bytehex_to_hash, hash_to_bytehex, hash_to_bytes
from swh.model.model import (
    Content,
    Directory,
    DirectoryEntry,
    ObjectType,
    Person,
    Release,
    Revision,
    RevisionType,
    Timestamp,
    TimestampWithTimezone,
)

TEST_DATA = os.path.join(os.path.dirname(__file__), "data")
GPGSIG = (
    b"-----BEGIN PGP SIGNATURE-----\n"
    b"\n"
    b"iQJLBAABCAA1FiEEAOWDevQbOk/9ITMF6ImSleOlnUcFAl8EnS4XHGRhdmlkLmRv\n"
    b"dWFyZEBzZGZhMy5vcmcACgkQ6ImSleOlnUdrqQ/8C5RO4NZ5Qr/dwAy2cPA7ktkY\n"
    b"1oUjKtspQoPbC1X3MXVa1aWo9B3KuOMR2URw44RhMNFwjccLOhfss06E8p7CZr2H\n"
    b"uR3CzdDw7i52jHLCL2M2ZMaPAEbQuHjXWiUWIUXz9So8YwpTyd2XQneyOC2RDDEI\n"
    b"I2NVbmiMeDz33jJYPrQO0QayW+ErW+xgBF7N/qS9jFWsdV1ZNfn9NxkTH8UdGuAX\n"
    b"583P+0tVC2DjXc6vORVhyFzyfn1A9wHosbtWI2Mpa+zezPjoPSkcyQAJu2GyOkMC\n"
    b"YzSjJdQVqyovo+INkIf6PuUNdp41886BG/06xwT8fl4sVsyO51lNIfgH0DMwfTTB\n"
    b"ZgThYnvvO7SrXDm3QzBTXkvAiHiFFl3iNyGkCyxvgVmaTntuFT+cP+HD/pCiGaC+\n"
    b"jHzRwfUrmuLd/lLPyq3JXBibyjnfd3SVS+7q1NZHJ4WUmCboZ0+pfrEl65mEQ/Hz\n"
    b"J1qCwQ/3SsTB77ANf6lLzGSowjjrtHcBTkTbFxR4ACUhiBbosyDKpHTM7fzGFGjo\n"
    b"EIjohzrEnqR3bbyxJkK+nxoOByhIRdowgyeJ02I4neMyLJqcaup8NMWCddxqjaPt\n"
    b"YobghnjaDqEd+suL/v83hbZUAZHNO3i1OZYGMqzp1WHikDPoTwGP76baqBoXi56T\n"
    b"4WSpxCAJRDODHLk1HgU=\n"
    b"=73wF"
    b"\n"
    b"-----END PGP SIGNATURE-----"
)

MERGETAG = (
    b"object 9768d0b576dbaaecd80abedad6dfd0d72f1476da\n"
    b"type commit\n"
    b"tag v0.0.1\n"
    b"tagger David Douard <david.douard@sdfa3.org> 1594138133 +0200\n"
    b"\n"
    b"v0.0.1\n"
    b"-----BEGIN PGP SIGNATURE-----\n"
    b"\n"
    b"iQJLBAABCAA1FiEEAOWDevQbOk/9ITMF6ImSleOlnUcFAl8EnhkXHGRhdmlkLmRv\n"
    b"dWFyZEBzZGZhMy5vcmcACgkQ6ImSleOlnUcdzg//ZW9y2xU5JFQuUsBe/LfKrs+m\n"
    b"0ohVInPKXwAfpB3+gn/XtTSLe+Nnr8+QEZyVRCUz2gpGZ2tNqRjhYLIX4x5KKlaV\n"
    b"rfl/6Cy7zibsxxuzA1h7HylCs3IPsueQpznVHUwD9jQ5baGJSc2Lt1LufXTueHZJ\n"
    b"Oc0oLiP5xCZcPqeX8R/4zUUImJZ1QrPeKmQ/3F+Iq62iWp7nWDp8PtwpykSiYlNf\n"
    b"KrJM8omGvrlrWLtfPNUaQFClXwnwK1/HyNY2kYan6K5NtsIl2UX0LZ42GkRjJIrb\n"
    b"q4TFIZWZ6xndtEhHEX6B8Q5TZV6sqPgNnfGpbhj8BDoZgjD0Y43fzfDiZ0Bl2tph\n"
    b"tXaLg3SX/UUjFVzC1zkoQ2MR7+j8NVKauAsBINpKF4pMGsrsVRk8764pgO49iQ+S\n"
    b"8JVCVV76dNNm1gd7BbhFAdIAiegBtsEF69niJBoHKYLlrT8E8hDkF/gk4IkimPqf\n"
    b"UHtw/fPhVW3B4G2skd013NJGcnRj5oKtaM99d2Roxc3vhSRiTsoaM8BM9NDvLmJg\n"
    b"35rWEOnet39iJIMCHk3AYaJl8QmUhllDdr6vygaBVeVEf27m2c3NzONmIKpWqa2J\n"
    b"kTpF4cmzHYro34G7WuJ1bYvmLb6qWNQt9wd8RW+J1kVm5I8dkjPzLUougBpOd0YL\n"
    b"Bl5UTQILbV4Tv8ZlmJM=\n"
    b"=s1lv\n"
    b"-----END PGP SIGNATURE-----"
)


class SWHObjectType:
    """Dulwich lookalike ObjectType class"""

    def __init__(self, type_name):
        self.type_name = type_name


@pytest.mark.fs
class TestConverters:
    @classmethod
    def setup_class(cls):
        cls.repo_path = tempfile.mkdtemp()

        bundle = os.path.join(TEST_DATA, "git-repos", "example-submodule.bundle")

        git = subprocess.Popen(
            ["git", "clone", "--quiet", "--bare", "--mirror", bundle, cls.repo_path],
            cwd=TEST_DATA,
        )

        # flush stdout of xz
        git.communicate()
        cls.repo = dulwich.repo.Repo(cls.repo_path)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

        shutil.rmtree(cls.repo_path)

    def test_blob_to_content(self):
        content_id = b"28c6f4023d65f74e3b59a2dea3c4277ed9ee07b0"
        content = converters.dulwich_blob_to_content(self.repo[content_id])
        expected_content = Content(
            sha1_git=bytehex_to_hash(content_id),
            sha1=hash_to_bytes("4850a3420a2262ff061cb296fb915430fa92301c"),
            sha256=hash_to_bytes(
                "fee7c8a485a10321ad94b64135073cb5" "5f22cb9f57fa2417d2adfb09d310adef"
            ),
            blake2s256=hash_to_bytes(
                "5d71873f42a137f6d89286e43677721e574" "1fa05ce4cd5e3c7ea7c44d4c2d10b"
            ),
            data=(
                b'[submodule "example-dependency"]\n'
                b"\tpath = example-dependency\n"
                b"\turl = https://github.com/githubtraining/"
                b"example-dependency.git\n"
            ),
            length=124,
            status="visible",
        )
        assert content == expected_content

    def test_corrupt_blob(self, mocker):
        # has a signature
        sha1 = hash_to_bytes("28c6f4023d65f74e3b59a2dea3c4277ed9ee07b0")

        blob = copy.deepcopy(self.repo[hash_to_bytehex(sha1)])

        class hasher:
            def digest():
                return sha1

        blob._sha = hasher

        converters.dulwich_blob_to_content(blob)
        converters.dulwich_blob_to_content_id(blob)

        sha1 = hash_to_bytes("1234" * 10)

        with pytest.raises(converters.HashMismatch):
            converters.dulwich_blob_to_content(blob)
        with pytest.raises(converters.HashMismatch):
            converters.dulwich_blob_to_content_id(blob)

    def test_convertion_wrong_input(self):
        class Something:
            type_name = b"something-not-the-right-type"

        m = {
            "blob": converters.dulwich_blob_to_content,
            "tree": converters.dulwich_tree_to_directory,
            "commit": converters.dulwich_tree_to_directory,
            "tag": converters.dulwich_tag_to_release,
        }

        for _callable in m.values():
            with pytest.raises(ValueError):
                _callable(Something())

    def test_corrupt_tree(self):
        sha1 = b"a9b41fc6347d778f16c4380b598d8083e9b4c1fb"
        target = b"641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"
        tree = dulwich.objects.Tree()
        tree.add(b"file1", 0o644, target)
        assert tree.sha().hexdigest() == sha1.decode()
        converters.dulwich_tree_to_directory(tree)

        original_sha = tree.sha()

        tree.add(b"file2", 0o644, target)
        tree.sha()  # reset tree._needs_serialization
        tree._sha = original_sha  # force the wrong hash
        assert tree.sha().hexdigest() == sha1.decode()

        with pytest.raises(converters.HashMismatch):
            converters.dulwich_tree_to_directory(tree)

    def test_weird_tree(self):
        """Tests a tree with entries the wrong order"""

        raw_string = (
            b"0644 file2\x00"
            b"d\x1f\xb6\xe0\x8d\xdb.O\xd0\x96\xdc\xf1\x8e\x80\xb8\x94\xbf~%\xce"
            b"0644 file1\x00"
            b"d\x1f\xb6\xe0\x8d\xdb.O\xd0\x96\xdc\xf1\x8e\x80\xb8\x94\xbf~%\xce"
        )

        tree = dulwich.objects.Tree.from_raw_string(b"tree", raw_string)

        assert converters.dulwich_tree_to_directory(tree) == Directory(
            entries=(
                # in alphabetical order, as it should be
                DirectoryEntry(
                    name=b"file1",
                    type="file",
                    target=hash_to_bytes("641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"),
                    perms=0o644,
                ),
                DirectoryEntry(
                    name=b"file2",
                    type="file",
                    target=hash_to_bytes("641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"),
                    perms=0o644,
                ),
            ),
            raw_manifest=b"tree 62\x00" + raw_string,
        )

    def test_tree_perms(self):
        entries = [
            (b"blob_100644", 0o100644, "file"),
            (b"blob_100664", 0o100664, "file"),
            (b"blob_100666", 0o100666, "file"),
            (b"blob_120000", 0o120000, "file"),
            (b"commit_160644", 0o160644, "rev"),
            (b"commit_160664", 0o160664, "rev"),
            (b"commit_160666", 0o160666, "rev"),
            (b"commit_normal", 0o160000, "rev"),
            (b"tree_040644", 0o040644, "dir"),
            (b"tree_040664", 0o040664, "dir"),
            (b"tree_040666", 0o040666, "dir"),
            (b"tree_normal", 0o040000, "dir"),
        ]

        tree = dulwich.objects.Tree()
        for name, mode, _ in entries:
            tree.add(name, mode, b"00" * 20)

        assert converters.dulwich_tree_to_directory(tree) == Directory(
            entries=tuple(
                DirectoryEntry(type=type, perms=perms, name=name, target=b"\x00" * 20)
                for (name, perms, type) in entries
            )
        )

    def test_tree_with_slashes(self):
        raw_string = (
            b"100775 AUTHORS\x00"
            b"\x7f\xde\x98Av\x81I\xbb\x19\x88N\xffu\xed\xca\x01\xe1\x04\xb1\x81"
            b"100775 COPYING\x00"
            b'\xd5\n\x11\xd6O\xa5(\xfcv\xb3\x81\x92\xd1\x8c\x05?\xe8"A\xda'
            b"100775 README.markdown\x00"
            b"X-c\xd6\xb7\xa8*\xfa\x13\x9e\xef\x83q\xbf^\x90\xe9UVQ"
            b"100775 gitter/gitter.xml\x00"
            b"\xecJ\xfa\xa3\\\xe1\x9fo\x93\x131I\xcb\xbf1h2\x00}n"
            b"100775 gitter/script.py\x00"
            b"\x1d\xd3\xec\x83\x94+\xbc\x04\xde\xee\x7f\xc6\xbe\x8b\x9cnp=W\xf9"
        )

        tree = dulwich.objects.Tree.from_raw_string(b"tree", raw_string)

        dir_ = Directory(
            entries=(
                DirectoryEntry(
                    name=b"AUTHORS",
                    type="file",
                    target=hash_to_bytes("7fde9841768149bb19884eff75edca01e104b181"),
                    perms=0o100775,
                ),
                DirectoryEntry(
                    name=b"COPYING",
                    type="file",
                    target=hash_to_bytes("d50a11d64fa528fc76b38192d18c053fe82241da"),
                    perms=0o100775,
                ),
                DirectoryEntry(
                    name=b"README.markdown",
                    type="file",
                    target=hash_to_bytes("582d63d6b7a82afa139eef8371bf5e90e9555651"),
                    perms=0o100775,
                ),
                DirectoryEntry(
                    name=b"gitter_gitter.xml",  # changed
                    type="file",
                    target=hash_to_bytes("ec4afaa35ce19f6f93133149cbbf316832007d6e"),
                    perms=0o100775,
                ),
                DirectoryEntry(
                    name=b"gitter_script.py",  # changed
                    type="file",
                    target=hash_to_bytes("1dd3ec83942bbc04deee7fc6be8b9c6e703d57f9"),
                    perms=0o100775,
                ),
            ),
            raw_manifest=b"tree 202\x00" + raw_string,
        )
        assert converters.dulwich_tree_to_directory(tree) == dir_

    def test_commit_to_revision(self):
        sha1 = b"9768d0b576dbaaecd80abedad6dfd0d72f1476da"

        revision = converters.dulwich_commit_to_revision(self.repo[sha1])
        expected_revision = Revision(
            id=hash_to_bytes("9768d0b576dbaaecd80abedad6dfd0d72f1476da"),
            directory=b"\xf0i\\./\xa7\xce\x9dW@#\xc3A7a\xa4s\xe5\x00\xca",
            type=RevisionType.GIT,
            committer=Person(
                name=b"Stefano Zacchiroli",
                fullname=b"Stefano Zacchiroli <zack@upsilon.cc>",
                email=b"zack@upsilon.cc",
            ),
            author=Person(
                name=b"Stefano Zacchiroli",
                fullname=b"Stefano Zacchiroli <zack@upsilon.cc>",
                email=b"zack@upsilon.cc",
            ),
            committer_date=TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=1443083765,
                    microseconds=0,
                ),
                offset_bytes=b"+0200",
            ),
            message=b"add submodule dependency\n",
            metadata=None,
            extra_headers=(),
            date=TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=1443083765,
                    microseconds=0,
                ),
                offset_bytes=b"+0200",
            ),
            parents=(b"\xc3\xc5\x88q23`\x9f[\xbb\xb2\xd9\xe7\xf3\xfbJf\x0f?r",),
            synthetic=False,
        )

        assert revision == expected_revision

    def test_commit_to_revision_with_extra_headers(self):
        sha1 = b"322f5bc915e50fc25e85226b5a182bded0e98e4b"

        revision = converters.dulwich_commit_to_revision(self.repo[sha1])
        expected_revision = Revision(
            id=hash_to_bytes(sha1.decode()),
            directory=bytes.fromhex("f8ec06e4ed7b9fff4918a0241a48023143f30000"),
            type=RevisionType.GIT,
            committer=Person(
                name=b"David Douard",
                fullname=b"David Douard <david.douard@sdfa3.org>",
                email=b"david.douard@sdfa3.org",
            ),
            author=Person(
                name=b"David Douard",
                fullname=b"David Douard <david.douard@sdfa3.org>",
                email=b"david.douard@sdfa3.org",
            ),
            committer_date=TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=1594137902,
                    microseconds=0,
                ),
                offset_bytes=b"+0200",
            ),
            message=b"Am\xe9lioration du fichier READM\xa4\n",
            metadata=None,
            extra_headers=((b"encoding", b"ISO-8859-15"), (b"gpgsig", GPGSIG)),
            date=TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=1594136900,
                    microseconds=0,
                ),
                offset_bytes=b"+0200",
            ),
            parents=(bytes.fromhex("c730509025c6e81947102b2d77bc4dc1cade9489"),),
            synthetic=False,
        )

        assert revision == expected_revision

    def test_commit_without_manifest(self):
        """Tests a Release can still be produced when the manifest is not understood
        by the custom parser in dulwich_commit_to_revision."""
        target = b"641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"
        message = b"some commit message"
        author = Person(
            fullname=b"Foo <foo@example.org>", name=b"Foo", email=b"foo@example.org"
        )
        commit = dulwich.objects.Commit()
        commit.tree = target
        commit.message = message
        commit.author = commit.committer = b"Foo <foo@example.org>"
        commit.author_time = commit.commit_time = 1641980946
        commit.author_timezone = commit.commit_timezone = 3600
        assert converters.dulwich_commit_to_revision(commit) == Revision(
            message=b"some commit message",
            author=author,
            committer=author,
            date=TimestampWithTimezone(
                timestamp=Timestamp(seconds=1641980946, microseconds=0),
                offset_bytes=b"+0100",
            ),
            committer_date=TimestampWithTimezone(
                timestamp=Timestamp(seconds=1641980946, microseconds=0),
                offset_bytes=b"+0100",
            ),
            type=RevisionType.GIT,
            directory=hash_to_bytes(target.decode()),
            synthetic=False,
            metadata=None,
            parents=(),
        )

    @pytest.mark.parametrize("attribute", ["message", "encoding", "author", "gpgsig"])
    def test_corrupt_commit(self, attribute):
        sha = hash_to_bytes("3f0ac5a6d15d89cf928209a57334e3b77c5651b9")
        target = b"641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"
        message = b"some commit message"
        commit = dulwich.objects.Commit()
        commit.tree = target
        commit.message = message
        commit.gpgsig = GPGSIG
        commit.author = commit.committer = b"Foo <foo@example.org>"
        commit.author_time = commit.commit_time = 1641980946
        commit.author_timezone = commit.commit_timezone = 3600
        converters.dulwich_commit_to_revision(commit)
        assert commit.sha().digest() == sha

        original_sha = commit.sha()

        setattr(commit, attribute, b"abcde")
        commit.sha()  # reset tag._needs_serialization
        commit._sha = original_sha  # force the wrong hash

        with pytest.raises(converters.HashMismatch):
            converters.dulwich_commit_to_revision(commit)

        if attribute == "_gpgsig":
            setattr(commit, attribute, None)
            commit.sha()  # reset tag._needs_serialization
            commit._sha = original_sha  # force the wrong hash
            with pytest.raises(converters.HashMismatch):
                converters.dulwich_commit_to_revision(commit)

    def test_commit_to_revision_with_extra_headers_mergetag(self):
        sha1 = b"3ab3da4bf0f81407be16969df09cd1c8af9ac703"

        revision = converters.dulwich_commit_to_revision(self.repo[sha1])
        expected_revision = Revision(
            id=hash_to_bytes(sha1.decode()),
            directory=bytes.fromhex("faa4b64a841ca3e3f07d6501caebda2e3e8e544e"),
            type=RevisionType.GIT,
            committer=Person(
                name=b"David Douard",
                fullname=b"David Douard <david.douard@sdfa3.org>",
                email=b"david.douard@sdfa3.org",
            ),
            author=Person(
                name=b"David Douard",
                fullname=b"David Douard <david.douard@sdfa3.org>",
                email=b"david.douard@sdfa3.org",
            ),
            committer_date=TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=1594138183,
                    microseconds=0,
                ),
                offset_bytes=b"+0200",
            ),
            message=b"Merge tag 'v0.0.1' into readme\n\nv0.0.1\n",
            metadata=None,
            extra_headers=((b"encoding", b"ISO-8859-15"), (b"mergetag", MERGETAG)),
            date=TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=1594138183,
                    microseconds=0,
                ),
                offset_bytes=b"+0200",
            ),
            parents=(
                bytes.fromhex("322f5bc915e50fc25e85226b5a182bded0e98e4b"),
                bytes.fromhex("9768d0b576dbaaecd80abedad6dfd0d72f1476da"),
            ),
            synthetic=False,
        )

        assert revision == expected_revision

    def test_weird_commit(self):
        """Checks raw_manifest is set when the commit cannot fit the data model"""

        # Well-formed manifest
        raw_string = (
            b"tree 641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce\n"
            b"author Foo <foo@example.org> 1640191028 +0200\n"
            b"committer Foo <foo@example.org> 1640191028 +0200\n\n"
            b"some commit message"
        )
        commit = dulwich.objects.Commit.from_raw_string(b"commit", raw_string)
        date = TimestampWithTimezone(
            timestamp=Timestamp(seconds=1640191028, microseconds=0),
            offset_bytes=b"+0200",
        )
        assert converters.dulwich_commit_to_revision(commit) == Revision(
            message=b"some commit message",
            directory=hash_to_bytes("641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"),
            synthetic=False,
            author=Person.from_fullname(
                b"Foo <foo@example.org>",
            ),
            committer=Person.from_fullname(
                b"Foo <foo@example.org>",
            ),
            date=date,
            committer_date=date,
            type=RevisionType.GIT,
            raw_manifest=None,
        )

        # Mess with the offset
        raw_string2 = raw_string.replace(b"+0200", b"+200")
        commit = dulwich.objects.Commit.from_raw_string(b"commit", raw_string2)
        date = TimestampWithTimezone(
            timestamp=Timestamp(seconds=1640191028, microseconds=0),
            offset_bytes=b"+200",
        )
        assert converters.dulwich_commit_to_revision(commit) == Revision(
            message=b"some commit message",
            directory=hash_to_bytes("641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"),
            synthetic=False,
            author=Person.from_fullname(
                b"Foo <foo@example.org>",
            ),
            committer=Person.from_fullname(
                b"Foo <foo@example.org>",
            ),
            date=date,
            committer_date=date,
            type=RevisionType.GIT,
            raw_manifest=None,
        )

        # Mess with the rest of the manifest
        raw_string2 = raw_string.replace(
            b"641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce",
            b"641FB6E08DDB2E4FD096DCF18E80B894BF7E25CE",
        )
        commit = dulwich.objects.Commit.from_raw_string(b"commit", raw_string2)
        date = TimestampWithTimezone(
            timestamp=Timestamp(seconds=1640191028, microseconds=0),
            offset_bytes=b"+0200",
        )
        assert converters.dulwich_commit_to_revision(commit) == Revision(
            message=b"some commit message",
            directory=hash_to_bytes("641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"),
            synthetic=False,
            author=Person.from_fullname(
                b"Foo <foo@example.org>",
            ),
            committer=Person.from_fullname(
                b"Foo <foo@example.org>",
            ),
            date=date,
            committer_date=date,
            type=RevisionType.GIT,
            raw_manifest=b"commit 161\x00" + raw_string2,
        )

    def test_author_line_to_author(self):
        # edge case out of the way
        with pytest.raises(TypeError):
            converters.parse_author(None)

        tests = {
            b"a <b@c.com>": Person(
                name=b"a",
                email=b"b@c.com",
                fullname=b"a <b@c.com>",
            ),
            b"<foo@bar.com>": Person(
                name=None,
                email=b"foo@bar.com",
                fullname=b"<foo@bar.com>",
            ),
            b"malformed <email": Person(
                name=b"malformed", email=b"email", fullname=b"malformed <email"
            ),
            b"trailing <sp@c.e> ": Person(
                name=b"trailing",
                email=b"sp@c.e",
                fullname=b"trailing <sp@c.e> ",
            ),
            b"no<sp@c.e>": Person(
                name=b"no",
                email=b"sp@c.e",
                fullname=b"no<sp@c.e>",
            ),
            b" <>": Person(
                name=None,
                email=None,
                fullname=b" <>",
            ),
            b"something": Person(name=b"something", email=None, fullname=b"something"),
        }

        for author in sorted(tests):
            parsed_author = tests[author]
            assert parsed_author == converters.parse_author(author)

    def test_dulwich_tag_to_release_no_author_no_date(self):
        sha = hash_to_bytes("f6e367357b446bd1315276de5e88ba3d0d99e136")
        target = b"641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"
        message = b"some release message"
        tag = dulwich.objects.Tag()
        tag.name = b"blah"
        tag.object = (dulwich.objects.Commit, target)
        tag.message = message
        tag.signature = None
        tag.tagger = None
        tag.tag_time = None
        tag.tag_timezone = None
        assert tag.sha().digest() == sha

        # when
        actual_release = converters.dulwich_tag_to_release(tag)

        # then
        expected_release = Release(
            author=None,
            date=None,
            id=sha,
            message=message,
            metadata=None,
            name=b"blah",
            synthetic=False,
            target=hash_to_bytes(target.decode()),
            target_type=ObjectType.REVISION,
        )

        assert actual_release == expected_release

    def test_dulwich_tag_to_release_author_and_date(self):
        sha = hash_to_bytes("fc1e6a4f1e37e93e28e78560e73efd0b12f616ef")
        tagger = b"hey dude <hello@mail.org>"
        target = b"641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"
        message = b"some release message"

        date = int(
            datetime.datetime(2007, 12, 5, tzinfo=datetime.timezone.utc).timestamp()
        )

        tag = dulwich.objects.Tag()
        tag.name = b"blah"
        tag.object = (dulwich.objects.Commit, target)
        tag.message = message
        tag.signature = None
        tag.tagger = tagger
        tag.tag_time = date
        tag.tag_timezone = 0
        assert tag.sha().digest() == sha

        # when
        actual_release = converters.dulwich_tag_to_release(tag)

        # then
        expected_release = Release(
            author=Person(
                email=b"hello@mail.org",
                fullname=b"hey dude <hello@mail.org>",
                name=b"hey dude",
            ),
            date=TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=1196812800,
                    microseconds=0,
                ),
                offset_bytes=b"+0000",
            ),
            id=sha,
            message=message,
            metadata=None,
            name=b"blah",
            synthetic=False,
            target=hash_to_bytes(target.decode()),
            target_type=ObjectType.REVISION,
        )

        assert actual_release == expected_release

    def test_dulwich_tag_to_release_author_no_date(self):
        # to reproduce bug T815 (fixed)
        sha = hash_to_bytes("41076e970975122dc6b2a878aa9797960bc4781d")
        tagger = b"hey dude <hello@mail.org>"
        target = b"641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"
        message = b"some release message"
        tag = dulwich.objects.Tag()
        tag.name = b"blah"
        tag.object = (dulwich.objects.Commit, target)
        tag.message = message
        tag.signature = None
        tag.tagger = tagger
        tag.tag_time = None
        tag.tag_timezone = None
        assert tag.sha().digest() == sha

        # when
        actual_release = converters.dulwich_tag_to_release(tag)

        # then
        expected_release = Release(
            author=Person(
                email=b"hello@mail.org",
                fullname=b"hey dude <hello@mail.org>",
                name=b"hey dude",
            ),
            date=None,
            id=sha,
            message=message,
            metadata=None,
            name=b"blah",
            synthetic=False,
            target=hash_to_bytes(target.decode()),
            target_type=ObjectType.REVISION,
        )

        assert actual_release == expected_release

    def test_dulwich_tag_to_release_author_zero_date(self):
        # to reproduce bug T815 (fixed)
        sha = hash_to_bytes("6cc1deff5cdcd853428bb63b937f43dd2566c36f")
        tagger = b"hey dude <hello@mail.org>"
        target = b"641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"
        message = b"some release message"
        date = int(
            datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc).timestamp()
        )
        tag = dulwich.objects.Tag()
        tag.name = b"blah"
        tag.object = (dulwich.objects.Commit, target)
        tag.message = message
        tag.signature = None
        tag.tagger = tagger
        tag.tag_time = date
        tag.tag_timezone = 0
        assert tag.sha().digest() == sha

        # when
        actual_release = converters.dulwich_tag_to_release(tag)

        # then
        expected_release = Release(
            author=Person(
                email=b"hello@mail.org",
                fullname=b"hey dude <hello@mail.org>",
                name=b"hey dude",
            ),
            date=TimestampWithTimezone(
                timestamp=Timestamp(
                    seconds=0,
                    microseconds=0,
                ),
                offset_bytes=b"+0000",
            ),
            id=sha,
            message=message,
            metadata=None,
            name=b"blah",
            synthetic=False,
            target=hash_to_bytes(target.decode()),
            target_type=ObjectType.REVISION,
        )

        assert actual_release == expected_release

    def test_dulwich_tag_to_release_signature(self):
        target = b"641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"
        message = b"some release message"
        sha = hash_to_bytes("46fff489610ed733d2cc904e363070dadee05c71")
        tag = dulwich.objects.Tag()
        tag.name = b"blah"
        tag.object = (dulwich.objects.Commit, target)
        tag.message = message
        tag.signature = GPGSIG
        tag.tagger = None
        tag.tag_time = None
        tag.tag_timezone = None
        assert tag.sha().digest() == sha

        # when
        actual_release = converters.dulwich_tag_to_release(tag)

        # then
        expected_release = Release(
            author=None,
            date=None,
            id=sha,
            message=message + GPGSIG,
            metadata=None,
            name=b"blah",
            synthetic=False,
            target=hash_to_bytes(target.decode()),
            target_type=ObjectType.REVISION,
        )

        assert actual_release == expected_release

    @pytest.mark.parametrize("attribute", ["name", "message", "signature"])
    def test_corrupt_tag(self, attribute):
        sha = hash_to_bytes("46fff489610ed733d2cc904e363070dadee05c71")
        target = b"641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"
        message = b"some release message"
        tag = dulwich.objects.Tag()
        tag.name = b"blah"
        tag.object = (dulwich.objects.Commit, target)
        tag.message = message
        tag.signature = GPGSIG
        tag.tagger = None
        tag.tag_time = None
        tag.tag_timezone = None
        assert tag.sha().digest() == sha
        converters.dulwich_tag_to_release(tag)

        original_sha = tag.sha()

        setattr(tag, attribute, b"abcde")
        tag.sha()  # reset tag._needs_serialization
        tag._sha = original_sha  # force the wrong hash
        with pytest.raises(converters.HashMismatch):
            converters.dulwich_tag_to_release(tag)

        if attribute == "signature":
            setattr(tag, attribute, None)
            tag.sha()  # reset tag._needs_serialization
            tag._sha = original_sha  # force the wrong hash
            with pytest.raises(converters.HashMismatch):
                converters.dulwich_tag_to_release(tag)

    def test_weird_tag(self):
        """Checks raw_manifest is set when the tag cannot fit the data model"""

        # Well-formed manifest
        raw_string = (
            b"object 641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce\n"
            b"type commit\n"
            b"tag blah\n"
            b"tagger Foo <foo@example.org> 1640191027 +0200\n\n"
            b"some release message"
        )
        tag = dulwich.objects.Tag.from_raw_string(b"tag", raw_string)
        assert converters.dulwich_tag_to_release(tag) == Release(
            name=b"blah",
            message=b"some release message",
            target=hash_to_bytes("641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"),
            target_type=ObjectType.REVISION,
            synthetic=False,
            author=Person.from_fullname(
                b"Foo <foo@example.org>",
            ),
            date=TimestampWithTimezone(
                timestamp=Timestamp(seconds=1640191027, microseconds=0),
                offset_bytes=b"+0200",
            ),
            raw_manifest=None,
        )

        # Mess with the offset (negative UTC)
        raw_string2 = raw_string.replace(b"+0200", b"-0000")
        tag = dulwich.objects.Tag.from_raw_string(b"tag", raw_string2)
        assert converters.dulwich_tag_to_release(tag) == Release(
            name=b"blah",
            message=b"some release message",
            target=hash_to_bytes("641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"),
            target_type=ObjectType.REVISION,
            synthetic=False,
            author=Person.from_fullname(
                b"Foo <foo@example.org>",
            ),
            date=TimestampWithTimezone(
                timestamp=Timestamp(seconds=1640191027, microseconds=0),
                offset_bytes=b"-0000",
            ),
        )

        # Mess with the offset (other)
        raw_string2 = raw_string.replace(b"+0200", b"+200")
        tag = dulwich.objects.Tag.from_raw_string(b"tag", raw_string2)
        assert converters.dulwich_tag_to_release(tag) == Release(
            name=b"blah",
            message=b"some release message",
            target=hash_to_bytes("641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"),
            target_type=ObjectType.REVISION,
            synthetic=False,
            author=Person.from_fullname(
                b"Foo <foo@example.org>",
            ),
            date=TimestampWithTimezone(
                timestamp=Timestamp(seconds=1640191027, microseconds=0),
                offset_bytes=b"+200",
            ),
        )

        # Mess with the rest of the manifest
        raw_string2 = raw_string.replace(
            b"641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce",
            b"641FB6E08DDB2E4FD096DCF18E80B894BF7E25CE",
        )
        tag = dulwich.objects.Tag.from_raw_string(b"tag", raw_string2)
        assert converters.dulwich_tag_to_release(tag) == Release(
            name=b"blah",
            message=b"some release message",
            target=hash_to_bytes("641fb6e08ddb2e4fd096dcf18e80b894bf7e25ce"),
            target_type=ObjectType.REVISION,
            synthetic=False,
            author=Person.from_fullname(
                b"Foo <foo@example.org>",
            ),
            date=TimestampWithTimezone(
                timestamp=Timestamp(seconds=1640191027, microseconds=0),
                offset_bytes=b"+0200",
            ),
            raw_manifest=b"tag 136\x00" + raw_string2,
        )
