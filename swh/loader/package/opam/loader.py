# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import io
import os
from subprocess import PIPE, Popen, call
from typing import Iterator, List, Optional, Tuple

import attr

from swh.loader.package.loader import BasePackageInfo, PackageLoader
from swh.model.model import Person, Revision, RevisionType, Sha1Git
from swh.storage.interface import StorageInterface


@attr.s
class OpamPackageInfo(BasePackageInfo):
    author = attr.ib(type=Person)
    committer = attr.ib(type=Person)
    version = attr.ib(type=str)


def opam_read(
    cmd: List[str], init_error_msg_if_any: Optional[str] = None
) -> Optional[str]:
    """This executes an opam command and returns the first line of the output.

    Args:
        cmd: Opam command to execute as a list of string
        init_error_msg_if_any: Error message to raise in case a problem occurs
          during initialization

    Raises:
        ValueError with the init_error_msg_if_any content in case stdout is not
        consumable and the variable is provided with non empty value.

    Returns:
        the first line of the executed command output

    """
    with Popen(cmd, stdout=PIPE) as proc:
        if proc.stdout is not None:
            for line in io.TextIOWrapper(proc.stdout):
                # care only for the first line output result (mostly blank separated
                # values, callers will deal with the parsing of the line)
                return line
        elif init_error_msg_if_any:
            raise ValueError(init_error_msg_if_any)
    return None


class OpamLoader(PackageLoader[OpamPackageInfo]):
    """
    Load all versions of a given package in a given opam repository.

    The state of the opam repository is stored in a directory called an
    opam root. Either the opam root has been created by the loader and we
    simply re-use it, either it doesn't exist yet and we create it on the
    first package we try to load (next packages will be able to re-use it).

    Then we just ask the opam binary to give us the list of all versions of
    the given package. For each version, we ask the opam binary to give us
    the url to the tarball to archive.
    """

    visit_type = "opam"

    def __init__(
        self,
        storage: StorageInterface,
        url: str,
        opam_root: str,
        opam_instance: str,
        opam_url: str,
        opam_package: str,
        max_content_size: Optional[int] = None,
    ):
        super().__init__(storage=storage, url=url, max_content_size=max_content_size)

        self.opam_root = opam_root
        self.opam_instance = opam_instance
        self.opam_url = opam_url
        self.opam_package = opam_package

    def get_versions(self) -> List[str]:
        """First initialize the opam root directory if needed the start listing the package
versions.

        """
        if not os.path.isdir(self.opam_root):
            if os.path.isfile(self.opam_root):
                raise ValueError("invalid opam root")
            else:
                call(
                    [
                        "opam",
                        "init",
                        "--reinit",
                        "--bare",
                        "--no-setup",
                        "--root",
                        self.opam_root,
                        self.opam_instance,
                        self.opam_url,
                    ]
                )
        elif not os.path.isfile(os.path.join(self.opam_root, "config")):
            raise ValueError("invalid opam root")

        versions = opam_read(
            [
                "opam",
                "show",
                "--color",
                "never",
                "--normalise",
                "--root",
                self.opam_root,
                "-f",
                "all-versions",
                self.opam_package,
            ],
            init_error_msg_if_any=(
                f"can't get versions for package {self.opam_package} "
                f"(at url {self.url}) from `opam show`"
            ),
        )
        return versions.split() if versions else []

    def get_default_version(self) -> str:

        init_error_msg = f"can't get default version for package {self.opam_package} \
            (at url {self.url}) from `opam show`"
        # we only care about the first element of the first line
        # which is the initial version
        versions_ = opam_read(
            [
                "opam",
                "show",
                "--color",
                "never",
                "--normalise",
                "--root",
                self.opam_root,
                "-f",
                "version",
                self.opam_package,
            ],
            init_error_msg_if_any=init_error_msg,
        )
        if not versions_:
            raise ValueError(init_error_msg)
        versions = versions_.split()
        if len(versions) != 1:
            raise ValueError(init_error_msg)
        return versions[0]

    def get_enclosed_single_line_field(self, field, version) -> Optional[str]:
        result = opam_read(
            [
                "opam",
                "show",
                "--color",
                "never",
                "--normalise",
                "--root",
                self.opam_root,
                "-f",
                field,
                f"{self.opam_package}.{version}",
            ]
        )

        # this needs to be cleaned up a bit (remove enclosing " and the trailing \n)
        return result[1:-2] if result else None

    def get_package_info(self, version: str) -> Iterator[Tuple[str, OpamPackageInfo]]:

        branch_name = f"{self.opam_package}.{version}"
        url = self.get_enclosed_single_line_field("url.src:", version)

        if url is None:
            raise ValueError(
                f"can't get field url.src: for version {version} of package {self.opam_package} \
                (at url {self.url}) from `opam show`"
            )

        authors_field = self.get_enclosed_single_line_field("authors:", version)
        fullname = b"" if authors_field is None else str.encode(authors_field)
        author = Person(fullname=fullname, name=None, email=None)

        maintainer_field = self.get_enclosed_single_line_field("maintainer:", version)
        fullname = b"" if maintainer_field is None else str.encode(maintainer_field)
        committer = Person(fullname=fullname, name=None, email=None)

        yield branch_name, OpamPackageInfo(
            url=url, filename=None, author=author, committer=committer, version=version
        )

    def build_revision(
        self, p_info: OpamPackageInfo, uncompressed_path: str, directory: Sha1Git
    ) -> Optional[Revision]:

        return Revision(
            type=RevisionType.TAR,
            author=p_info.author,
            committer=p_info.committer,
            message=str.encode(p_info.version),
            date=None,
            committer_date=None,
            parents=(),
            directory=directory,
            synthetic=True,
        )
