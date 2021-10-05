# Copyright (C) 2018-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os
import subprocess
from tempfile import SpooledTemporaryFile
from threading import Thread

from dulwich.errors import GitProtocolError, NotGitRepository, ObjectFormatException
from dulwich.porcelain import push
import dulwich.repo
import pytest

from swh.loader.git import dumb
from swh.loader.git.loader import GitLoader
from swh.loader.git.tests.test_from_disk import FullGitLoaderTests
from swh.loader.tests import (
    assert_last_visit_matches,
    get_stats,
    prepare_repository_from_archive,
)


class CommonGitLoaderNotFound:
    @pytest.fixture(autouse=True)
    def __inject_fixtures(self, mocker):
        """Inject required fixtures in unittest.TestCase class

        """
        self.mocker = mocker

    @pytest.mark.parametrize(
        "failure_exception",
        [
            GitProtocolError("Repository unavailable"),  # e.g DMCA takedown
            GitProtocolError("Repository not found"),
            GitProtocolError("unexpected http resp 401"),
            NotGitRepository("not a git repo"),
        ],
    )
    def test_load_visit_not_found(self, failure_exception):
        """Ingesting an unknown url result in a visit with not_found status

        """
        # simulate an initial communication error (e.g no repository found, ...)
        mock = self.mocker.patch(
            "swh.loader.git.loader.GitLoader.fetch_pack_from_origin"
        )
        mock.side_effect = failure_exception

        res = self.loader.load()
        assert res == {"status": "uneventful"}

        assert_last_visit_matches(
            self.loader.storage,
            self.repo_url,
            status="not_found",
            type="git",
            snapshot=None,
        )

    @pytest.mark.parametrize(
        "failure_exception",
        [IOError, ObjectFormatException, OSError, ValueError, GitProtocolError,],
    )
    def test_load_visit_failure(self, failure_exception):
        """Failing during the fetch pack step result in failing visit

        """
        # simulate a fetch communication error after the initial connection
        # server error (e.g IOError, ObjectFormatException, ...)
        mock = self.mocker.patch(
            "swh.loader.git.loader.GitLoader.fetch_pack_from_origin"
        )

        mock.side_effect = failure_exception("failure")

        res = self.loader.load()
        assert res == {"status": "failed"}

        assert_last_visit_matches(
            self.loader.storage,
            self.repo_url,
            status="failed",
            type="git",
            snapshot=None,
        )


class TestGitLoader(FullGitLoaderTests, CommonGitLoaderNotFound):
    """Prepare a git directory repository to be loaded through a GitLoader.
    This tests all git loader scenario.

    """

    @pytest.fixture(autouse=True)
    def init(self, swh_storage, datadir, tmp_path):
        archive_name = "testrepo"
        archive_path = os.path.join(datadir, f"{archive_name}.tgz")
        tmp_path = str(tmp_path)
        self.repo_url = prepare_repository_from_archive(
            archive_path, archive_name, tmp_path=tmp_path
        )
        self.destination_path = os.path.join(tmp_path, archive_name)
        self.loader = GitLoader(swh_storage, self.repo_url)
        self.repo = dulwich.repo.Repo(self.destination_path)


class TestGitLoader2(FullGitLoaderTests, CommonGitLoaderNotFound):
    """Mostly the same loading scenario but with a base-url different than the repo-url.
    To walk slightly different paths, the end result should stay the same.

    """

    @pytest.fixture(autouse=True)
    def init(self, swh_storage, datadir, tmp_path):
        archive_name = "testrepo"
        archive_path = os.path.join(datadir, f"{archive_name}.tgz")
        tmp_path = str(tmp_path)
        self.repo_url = prepare_repository_from_archive(
            archive_path, archive_name, tmp_path=tmp_path
        )
        self.destination_path = os.path.join(tmp_path, archive_name)
        base_url = f"base://{self.repo_url}"
        self.loader = GitLoader(swh_storage, self.repo_url, base_url=base_url)
        self.repo = dulwich.repo.Repo(self.destination_path)


class DumbGitLoaderTestBase(FullGitLoaderTests):
    """Prepare a git repository to be loaded using the HTTP dumb transfer protocol.
    """

    @pytest.fixture(autouse=True)
    def init(self, swh_storage, datadir, tmp_path):
        # remove any proxy settings in order to successfully spawn a local HTTP server
        http_proxy = os.environ.get("http_proxy")
        https_proxy = os.environ.get("https_proxy")
        if http_proxy:
            del os.environ["http_proxy"]
        if http_proxy:
            del os.environ["https_proxy"]

        # prepare test base repository using smart transfer protocol
        archive_name = "testrepo"
        archive_path = os.path.join(datadir, f"{archive_name}.tgz")
        tmp_path = str(tmp_path)
        base_repo_url = prepare_repository_from_archive(
            archive_path, archive_name, tmp_path=tmp_path
        )
        destination_path = os.path.join(tmp_path, archive_name)
        self.destination_path = destination_path
        with_pack_files = self.with_pack_files

        if with_pack_files:
            # create a bare clone of that repository in another folder,
            # all objects will be contained in one or two pack files in that case
            bare_repo_path = os.path.join(tmp_path, archive_name + "_bare")
            subprocess.run(
                ["git", "clone", "--bare", base_repo_url, bare_repo_path], check=True,
            )
        else:
            # otherwise serve objects from the bare repository located in
            # the .git folder of the base repository
            bare_repo_path = os.path.join(destination_path, ".git")

        # spawn local HTTP server that will serve the bare repository files
        hostname = "localhost"
        handler = partial(SimpleHTTPRequestHandler, directory=bare_repo_path)
        httpd = HTTPServer((hostname, 0), handler, bind_and_activate=True)

        def serve_forever(httpd):
            with httpd:
                httpd.serve_forever()

        thread = Thread(target=serve_forever, args=(httpd,))
        thread.start()

        repo = dulwich.repo.Repo(self.destination_path)

        class DumbGitLoaderTest(GitLoader):
            def load(self):
                """
                Override load method to ensure the bare repository will be synchronized
                with the base one as tests can modify its content.
                """
                if with_pack_files:
                    # ensure HEAD ref will be the same for both repositories
                    with open(os.path.join(bare_repo_path, "HEAD"), "wb") as fw:
                        with open(
                            os.path.join(destination_path, ".git/HEAD"), "rb"
                        ) as fr:
                            head_ref = fr.read()
                            fw.write(head_ref)

                    # push possibly modified refs in the base repository to the bare one
                    for ref in repo.refs.allkeys():
                        if ref != b"HEAD" or head_ref in repo.refs:
                            push(
                                repo,
                                remote_location=f"file://{bare_repo_path}",
                                refspecs=ref,
                            )

                # generate or update the info/refs file used in dumb protocol
                subprocess.run(
                    ["git", "-C", bare_repo_path, "update-server-info"], check=True,
                )

                return super().load()

        # bare repository with dumb protocol only URL
        self.repo_url = f"http://{httpd.server_name}:{httpd.server_port}"
        self.loader = DumbGitLoaderTest(swh_storage, self.repo_url)
        self.repo = repo

        yield

        # shutdown HTTP server
        httpd.shutdown()
        thread.join()

        # restore HTTP proxy settings if any
        if http_proxy:
            os.environ["http_proxy"] = http_proxy
        if https_proxy:
            os.environ["https_proxy"] = https_proxy

    @pytest.mark.parametrize(
        "failure_exception", [AttributeError, NotImplementedError, ValueError]
    )
    def test_load_despite_dulwich_exception(self, mocker, failure_exception):
        """Checks repository can still be loaded when dulwich raises exception
        when encountering a repository with dumb transfer protocol.
        """

        fetch_pack_from_origin = mocker.patch(
            "swh.loader.git.loader.GitLoader.fetch_pack_from_origin"
        )

        fetch_pack_from_origin.side_effect = failure_exception("failure")

        res = self.loader.load()

        assert res == {"status": "eventful"}

        stats = get_stats(self.loader.storage)
        assert stats == {
            "content": 4,
            "directory": 7,
            "origin": 1,
            "origin_visit": 1,
            "release": 0,
            "revision": 7,
            "skipped_content": 0,
            "snapshot": 1,
        }

    def test_load_empty_repository(self, mocker):
        class GitObjectsFetcherNoRefs(dumb.GitObjectsFetcher):
            def _get_refs(self):
                return {}

        mocker.patch.object(dumb, "GitObjectsFetcher", GitObjectsFetcherNoRefs)

        res = self.loader.load()

        assert res == {"status": "uneventful"}

        stats = get_stats(self.loader.storage)
        assert stats == {
            "content": 0,
            "directory": 0,
            "origin": 1,
            "origin_visit": 1,
            "release": 0,
            "revision": 0,
            "skipped_content": 0,
            "snapshot": 1,
        }


class TestDumbGitLoaderWithPack(DumbGitLoaderTestBase):
    @classmethod
    def setup_class(cls):
        cls.with_pack_files = True

    def test_load_with_missing_pack(self, mocker):
        """Some dumb git servers might reference a no longer existing pack file
        while it is possible to load a repository without it.
        """

        class GitObjectsFetcherMissingPack(dumb.GitObjectsFetcher):
            def _http_get(self, path: str) -> SpooledTemporaryFile:
                buffer = super()._http_get(path)
                if path == "objects/info/packs":
                    # prepend a non existing pack to the returned packs list
                    packs = buffer.read().decode("utf-8")
                    buffer.seek(0)
                    buffer.write(
                        (
                            "P pack-a70762ba1a901af3a0e76de02fc3a99226842745.pack\n"
                            + packs
                        ).encode()
                    )
                    buffer.flush()
                    buffer.seek(0)
                return buffer

        mocker.patch.object(dumb, "GitObjectsFetcher", GitObjectsFetcherMissingPack)

        res = self.loader.load()

        assert res == {"status": "eventful"}


class TestDumbGitLoaderWithoutPack(DumbGitLoaderTestBase):
    @classmethod
    def setup_class(cls):
        cls.with_pack_files = False
