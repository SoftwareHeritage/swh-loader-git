# Copyright (C) 2015-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, Optional

from celery import shared_task

from swh.loader.core.utils import parse_visit_date
from swh.loader.git.directory import GitCheckoutLoader
from swh.loader.git.from_disk import GitLoaderFromArchive, GitLoaderFromDisk
from swh.loader.git.loader import GitLoader


def _process_kwargs(kwargs):
    if "visit_date" in kwargs:
        kwargs["visit_date"] = parse_visit_date(kwargs["visit_date"])
    return kwargs


def _load_git_sized(size_class: Optional[str], **kwargs) -> Dict[str, Any]:
    """Shared body for the size-classed git load tasks.

    When ``size_class`` is None the loader behaves exactly as the legacy
    ``UpdateGitRepository`` task did — no dispatch awareness, no
    safety-net re-queue.  When ``size_class`` is one of ``"small"``,
    ``"large"``, or ``"xl"`` the loader records which queue it is
    running on and activates the safety-net re-dispatch described in
    :meth:`GitLoader._maybe_redispatch_oversized_pack`.
    """
    kwargs = _process_kwargs(kwargs)
    kwargs.setdefault("current_size_class", size_class)
    loader = GitLoader.from_configfile(**kwargs)
    return loader.load()


@shared_task(name=__name__ + ".UpdateGitRepository")
def load_git(**kwargs) -> Dict[str, Any]:
    """Import a git repository from a remote location.

    Legacy entry point, retained for backwards compatibility with the
    current production scheduler configuration.  Equivalent to the
    size-classed tasks with ``size_class=None`` — the safety-net re-queue
    is disabled, matching pre-dispatch behaviour exactly.
    """
    return _load_git_sized(None, **kwargs)


@shared_task(name=__name__ + ".UpdateGitRepositorySmall")
def load_git_small(**kwargs) -> Dict[str, Any]:
    """Size-classed git load task for the ``small`` queue.

    Routed to by the scheduler via ``grab_next_visits(size_class="small")``;
    see ``notes/PLAN-size-based-dispatch.md``.  Small-queue workers
    should be sized for repos with ``pack_size_kb < 100 MB`` and for
    all incremental visits (``origin_visit_stats.last_snapshot IS NOT NULL``).
    If the actual downloaded pack exceeds the small-queue threshold the
    loader re-dispatches to ``UpdateGitRepositoryLarge``.
    """
    return _load_git_sized("small", **kwargs)


@shared_task(name=__name__ + ".UpdateGitRepositoryLarge")
def load_git_large(**kwargs) -> Dict[str, Any]:
    """Size-classed git load task for the ``large`` queue.

    Routed to by the scheduler for first-visit origins with
    ``pack_size_kb`` in ``[100 MB, 2 GB)``.  If the actual pack exceeds
    2 GB the loader re-dispatches to ``UpdateGitRepositoryXl``.
    """
    return _load_git_sized("large", **kwargs)


@shared_task(name=__name__ + ".UpdateGitRepositoryXl")
def load_git_xl(**kwargs) -> Dict[str, Any]:
    """Size-classed git load task for the ``xl`` queue.

    Routed to by the scheduler for first-visit origins with
    ``pack_size_kb >= 2 GB``.  Terminal queue — oversized packs cannot
    be re-dispatched further, so a worker on this queue must either
    complete the load or fail.
    """
    return _load_git_sized("xl", **kwargs)


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
