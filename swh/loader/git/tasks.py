# Copyright (C) 2015-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict

from celery import shared_task

from swh.loader.core.utils import parse_visit_date
from swh.loader.git.directory import GitCheckoutLoader
from swh.loader.git.from_disk import GitLoaderFromArchive, GitLoaderFromDisk
from swh.loader.git.loader import GitLoader


def _process_kwargs(kwargs):
    if "visit_date" in kwargs:
        kwargs["visit_date"] = parse_visit_date(kwargs["visit_date"])
    return kwargs


@shared_task(name=__name__ + ".UpdateGitRepository")
def load_git(**kwargs) -> Dict[str, Any]:
    """Import a git repository from a remote location"""
    loader = GitLoader.from_configfile(**_process_kwargs(kwargs))
    return loader.load()


@shared_task(name=__name__ + ".LoadDiskGitRepository")
def load_git_from_dir(**kwargs) -> Dict[str, Any]:
    """Import a git repository from a local repository"""
    loader = GitLoaderFromDisk.from_configfile(**_process_kwargs(kwargs))
    return loader.load()


@shared_task(name=__name__ + ".UncompressAndLoadDiskGitRepository")
def load_git_from_zip(**kwargs) -> Dict[str, Any]:
    """Import a git repository from a zip archive

    1. Uncompress an archive repository in a local and temporary folder
    2. Load it through the git disk loader
    3. Clean up the temporary folder

    """
    loader = GitLoaderFromArchive.from_configfile(**_process_kwargs(kwargs))
    return loader.load()


@shared_task(name=__name__ + ".LoadGitCheckout")
def load_git_checkout(**kwargs) -> Dict[str, Any]:
    """Load a git tree at a specific commit, tag or branch."""
    loader = GitCheckoutLoader.from_configfile(**_process_kwargs(kwargs))
    return loader.load()
