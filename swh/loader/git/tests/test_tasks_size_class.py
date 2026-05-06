# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Tests for the three size-classed Celery tasks introduced for
size-based dispatch: ``UpdateGitRepositorySmall``,
``UpdateGitRepositoryLarge``, ``UpdateGitRepositoryXl``.

The full Celery-based registration test
(:func:`assert_module_tasks_are_scheduler_ready`) lives in
test_tasks.py and exercises the broader scheduler-side plumbing.  Here
we focus on the size-classed task wrappers themselves: that they exist,
that they propagate ``size_class`` correctly into the loader, and that
the task-name → queue mapping matches the project convention used by
:func:`swh.scheduler.celery_backend.config.route_for_task`.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from swh.loader.git import tasks as git_tasks

# The three new task callables and their expected size_class values.
SIZE_CLASS_TASKS = [
    (git_tasks.load_git_small, "small"),
    (git_tasks.load_git_large, "large"),
    (git_tasks.load_git_xl, "xl"),
]


# Expected fully-qualified task names.  These literals are the queue
# names workers subscribe to (queue name == task name; see
# swh-scheduler/swh/scheduler/celery_backend/config.py route_for_task).
EXPECTED_TASK_NAMES = {
    "small": "swh.loader.git.tasks.UpdateGitRepositorySmall",
    "large": "swh.loader.git.tasks.UpdateGitRepositoryLarge",
    "xl": "swh.loader.git.tasks.UpdateGitRepositoryXl",
}


@pytest.mark.parametrize("task,size_class", SIZE_CLASS_TASKS)
def test_size_classed_task_name_matches_convention(task, size_class):
    """Each new task is registered under the expected swh.loader.git.tasks.*
    name.  Workers subscribe to queues with this exact name."""
    assert task.name == EXPECTED_TASK_NAMES[size_class]


@pytest.mark.parametrize("task,size_class", SIZE_CLASS_TASKS)
def test_size_classed_task_propagates_size_class(task, size_class):
    """The task wrappers pass their size_class into GitLoader.from_configfile
    via kwargs.  We mock from_configfile + load to verify the kwargs
    without standing up a real loader.
    """
    fake_loader = MagicMock()
    fake_loader.load.return_value = {"status": "uneventful"}

    with patch(
        "swh.loader.git.tasks.GitLoader.from_configfile",
        return_value=fake_loader,
    ) as from_configfile:
        result = task(url="https://git.example.org/some/repo")

    from_configfile.assert_called_once()
    call_kwargs = from_configfile.call_args.kwargs
    assert call_kwargs.get("current_size_class") == size_class
    assert call_kwargs.get("url") == "https://git.example.org/some/repo"
    assert result == {"status": "uneventful"}


def test_legacy_task_does_not_set_size_class():
    """The legacy ``UpdateGitRepository`` task — used by current
    production callers — must not set ``current_size_class`` so the
    safety-net stays inert and behaviour is unchanged."""
    fake_loader = MagicMock()
    fake_loader.load.return_value = {"status": "uneventful"}

    with patch(
        "swh.loader.git.tasks.GitLoader.from_configfile",
        return_value=fake_loader,
    ) as from_configfile:
        git_tasks.load_git(url="https://git.example.org/some/repo")

    call_kwargs = from_configfile.call_args.kwargs
    # The legacy task passes size_class=None into the helper; the
    # helper uses ``setdefault`` so an explicit None still appears in
    # the kwargs.  Either "absent" or "is None" is acceptable for the
    # legacy path — both disable the safety net.
    assert call_kwargs.get("current_size_class") is None


def test_size_classed_task_respects_caller_override():
    """If a caller explicitly passes current_size_class in kwargs (e.g.
    a test or a CLI wrapper), the task helper's ``setdefault`` does not
    overwrite it."""
    fake_loader = MagicMock()
    fake_loader.load.return_value = {"status": "uneventful"}

    with patch(
        "swh.loader.git.tasks.GitLoader.from_configfile",
        return_value=fake_loader,
    ) as from_configfile:
        # Use the small task but override to "xl".  Result: the
        # explicit value wins.
        git_tasks.load_git_small(
            url="https://git.example.org/some/repo",
            current_size_class="xl",
        )

    call_kwargs = from_configfile.call_args.kwargs
    assert call_kwargs.get("current_size_class") == "xl"
