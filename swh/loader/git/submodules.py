# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from io import BytesIO
from typing import TYPE_CHECKING, Dict, List, Tuple
from urllib.parse import urljoin, urlparse

from dulwich.config import ConfigFile, parse_submodules

from swh.model.model import Content, TargetType
from swh.scheduler.utils import create_oneshot_task_dict

if TYPE_CHECKING:
    from .loader import GitLoader


def process_submodules(loader: GitLoader) -> None:
    """Process git submodules discovered while loading a repository.

    The overall submodules processing is the following:

    1. While loading a repository and before sending a new directory
       to archive in the storage, check if it has a ".gitmodules" file
       in its entries and add the tuple (directory_id, content_sha1git)
       in a global set if it is the case.

    2. During the post_load operation, that function is called to
       process each discovered ".gitmodules" file the following way:

        - retrieve content metadata to get sha1 checksum of file

        - retrieve .gitmodules content bytes in objstorage from sha1

        - parse .gitmodules file content

        - for each submodule definition:

            * get git commit id associated to submodule path

            * check if git commit has been archived by SWH

            * if not, add the submodule repository URL in a set

        - for each submodule detected as not archived or partially
            archived, create a one shot git loading task with high
            priority in the scheduler database
    """
    if loader.scheduler is None:
        return

    submodule_origins = set()
    processed_revisions = set()
    contents: Dict[bytes, Content] = {}

    # when reloading a stale git repo, we might have missed some submodules
    # because the loader did not process them yet at the last loading time,
    # so try to find submodule definition files in the root directories of
    # the current snapshot branches
    for branch in loader.snapshot.branches.values():
        if branch is None:
            continue
        if branch.target_type == TargetType.REVISION:
            rev = loader.storage.revision_get([branch.target])[0]
            if rev is None:
                continue
            gitmodules = loader.storage.directory_entry_get_by_path(
                rev.directory, [b".gitmodules"]
            )
            if gitmodules is None or gitmodules["type"] != "file":
                continue
            loader.gitmodules.add((rev.directory, gitmodules["target"]))

    def _get_content(sha1_git: bytes) -> Content:
        if sha1_git not in contents:
            query = {"sha1_git": sha1_git}
            contents[sha1_git] = loader.storage.content_find(query)[0]
        return contents[sha1_git]

    # get .gitmodules files metadata
    gitmodules_data = (
        (directory, _get_content(sha1_git)) for directory, sha1_git in loader.gitmodules
    )

    parsed_submodules: Dict[bytes, List[Tuple[bytes, bytes, bytes]]] = {}

    # iterate on each .gitmodules files and parse them
    for directory, content in gitmodules_data:
        if content.sha1 in parsed_submodules:
            submodules = parsed_submodules[content.sha1]
        else:
            content_data = loader.storage.content_get_data(content.sha1)
            if content_data is None:
                parsed_submodules[content.sha1] = []
                continue
            try:
                submodules = list(
                    parse_submodules(ConfigFile.from_file(BytesIO(content_data)))
                )
                parsed_submodules[content.sha1] = submodules
            except Exception:
                loader.log.warning(
                    ".gitmodules file with sha1_git %s could not be parsed",
                    content.sha1_git.hex(),
                )
                parsed_submodules[content.sha1] = []
                continue

        # iterate on parsed submodules
        for submodule in submodules:
            # extract submodule path and URL
            path, url = submodule[0], submodule[1].decode("ascii")
            if url.startswith(("./", "../")):
                # location relative to the loaded origin repository,
                # use an heuristic to compute submodule repository URL
                url = urljoin(loader.origin_url.rstrip("/") + "/", url)

            origin_scheme = urlparse(loader.origin_url).scheme
            submodule_scheme = urlparse(url).scheme

            if (
                # submodule origin already marked to be archived
                url in submodule_origins
                # submodule origin URL scheme is not supported by the loader
                or (
                    submodule_scheme not in ("git", "http", "https")
                    # submodule origin URL does not match those of unit tests
                    and not (origin_scheme == "file" and submodule_scheme == "file")
                )
            ):
                continue

            # get directory entry for submodule path
            rev_entry = loader.storage.directory_entry_get_by_path(directory, [path])

            if (
                # submodule path does not exist
                rev_entry is None
                # path is not a submodule
                or rev_entry["type"] != "rev"
                # target revision already processed
                or rev_entry["target"] in processed_revisions
            ):
                continue
            elif loader.storage.revision_get([rev_entry["target"]])[0] is None:
                loader.log.debug(
                    "Target revision %s for submodule %s is not archived, "
                    "origin %s will be loaded afterwards to get it.",
                    rev_entry["target"].hex(),
                    path,
                    url,
                )
                submodule_origins.add(url)
            else:
                loader.log.debug(
                    "Target revision %s for submodule %s is already archived, "
                    "origin %s will not be reloaded",
                    rev_entry["target"].hex(),
                    path,
                    url,
                )
            processed_revisions.add(rev_entry["target"])

    if submodule_origins:
        # create loading tasks for submodules with high priority
        tasks = [
            create_oneshot_task_dict("load-git", priority="high", url=origin_url)
            for origin_url in submodule_origins
        ]
        loader.scheduler.create_tasks(tasks)
