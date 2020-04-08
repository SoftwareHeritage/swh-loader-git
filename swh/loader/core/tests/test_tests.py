# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os.path
from os.path import join
import pytest
import re
import tarfile
import tempfile
from unittest import TestCase

from . import BaseLoaderStorageTest

from swh.loader.core.tests import BaseLoaderTest


class TestsTest(BaseLoaderStorageTest, TestCase):
    """Test the helpers provided to other loaders' tests."""

    def _build_archive(self, fd):
        with tarfile.open(mode="w", fileobj=fd) as tf:
            with tempfile.TemporaryDirectory() as src_dir:
                with open(join(src_dir, "hello.txt"), "a") as src_file:
                    src_file.write("world\n")
                tf.add(src_dir, arcname="test_dir")

    def _build_workdir(self, workdir):
        os.mkdir(join(workdir, "resources"))
        tarball_path = join(workdir, "resources", "test_archive.tar")
        with open(tarball_path, "a+b") as tar_fd:
            self._build_archive(tar_fd)

    @pytest.mark.fs
    def test_uncompress_setup_auto_name(self):
        loader_test = BaseLoaderTest()
        with tempfile.TemporaryDirectory() as workdir:
            self._build_workdir(workdir)

            loader_test.setUp("test_archive.tar", start_path=workdir)

        self.assertTrue(
            re.match("^file://.*-tests/test_archive.tar$", loader_test.repo_url),
            msg=loader_test.repo_url,
        )
        self.assertTrue(os.path.isdir(loader_test.destination_path))
        self.assertTrue(os.path.isdir(join(loader_test.destination_path, "test_dir")))
        self.assertTrue(
            os.path.isfile(join(loader_test.destination_path, "test_dir", "hello.txt"))
        )

        loader_test.tearDown()
        self.assertFalse(os.path.isdir(loader_test.destination_path))

    @pytest.mark.fs
    def test_uncompress_setup_provided_name(self):
        loader_test = BaseLoaderTest()
        with tempfile.TemporaryDirectory() as workdir:
            self._build_workdir(workdir)

            loader_test.setUp(
                "test_archive.tar", start_path=workdir, filename="test_dir"
            )

        self.assertTrue(
            re.match("^file://.*-tests/test_dir$", loader_test.repo_url),
            msg=loader_test.repo_url,
        )
        self.assertTrue(os.path.isdir(loader_test.destination_path))
        self.assertTrue(os.path.isfile(join(loader_test.destination_path, "hello.txt")))

        loader_test.tearDown()
        self.assertFalse(os.path.isdir(loader_test.destination_path))

    @pytest.mark.fs
    def test_setup_no_uncompress(self):
        loader_test = BaseLoaderTest()
        with tempfile.TemporaryDirectory() as workdir:
            self._build_workdir(workdir)

            loader_test.setUp(
                "test_archive.tar", start_path=workdir, uncompress_archive=False
            )

        self.assertEqual(
            "file://" + workdir + "/resources/test_archive.tar", loader_test.repo_url
        )
        self.assertEqual(
            workdir + "/resources/test_archive.tar", loader_test.destination_path
        )
