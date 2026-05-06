# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Unit tests for the size-based dispatch safety-net in :class:`GitLoader`.

These tests exercise :meth:`GitLoader._maybe_redispatch_oversized_pack`
directly with a stubbed origin and a mocked Celery backend — no real
broker, no real pack fetch, no PostgreSQL.  Higher-level integration
tests (actual Celery routing, actual forges) live in test_tasks.py and
in manual smoke-testing against staging.

See ``notes/PLAN-size-based-dispatch.md`` and
``notes/PLAN-loader-dispatch-implementation.md`` for the design
context.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from swh.loader.git.loader import GitLoader
from swh.model.model import Origin

# ---------------------------------------------------------------------------
# Fixture: a minimal GitLoader with no real storage/network dependencies
# ---------------------------------------------------------------------------


def _make_loader(
    *,
    current_size_class=None,
    safety_net_enabled=True,
    safety_net_oversize_factor=2.0,
    predicted_pack_size_kb=None,
):
    """Build a GitLoader without triggering its heavyweight __init__ path.

    We don't need a real storage to test the routing predicate — we only
    use ``self.origin``, ``self.pack_size``, ``self.statsd`` and the
    size-class attributes.  Using ``GitLoader.__new__`` avoids having to
    stand up a full loader configuration.
    """
    loader = GitLoader.__new__(GitLoader)
    loader.origin = Origin(url="https://git.example.org/some/repo")
    loader.incremental = True
    loader.pack_size = 0  # set per-test
    loader.current_size_class = current_size_class
    loader.safety_net_enabled = safety_net_enabled
    loader.safety_net_oversize_factor = safety_net_oversize_factor
    loader.predicted_pack_size_kb = predicted_pack_size_kb
    loader._delegated = False
    loader._delegated_to = None
    loader.statsd = MagicMock()
    return loader


# ---------------------------------------------------------------------------
# _should_redispatch: pure-predicate tests (no Celery involved)
# ---------------------------------------------------------------------------


def test_no_redispatch_when_size_class_none():
    loader = _make_loader(current_size_class=None)
    assert loader._should_redispatch(actual_pack_kb=10 * 1024 * 1024) is None


def test_no_redispatch_when_safety_net_disabled():
    loader = _make_loader(current_size_class="small", safety_net_enabled=False)
    # Even a 10 GB pack stays on the small worker when the safety net
    # is off — matches pre-dispatch behaviour for ops who prefer OOMs
    # over re-queues.
    assert loader._should_redispatch(actual_pack_kb=10 * 1024 * 1024) is None


def test_no_redispatch_on_xl_terminal_queue():
    loader = _make_loader(current_size_class="xl")
    # xl is terminal — no larger queue exists.
    assert loader._should_redispatch(actual_pack_kb=100 * 1024 * 1024) is None


def test_no_redispatch_when_under_hard_threshold_and_no_prediction():
    loader = _make_loader(current_size_class="small", predicted_pack_size_kb=None)
    # Pack is 50 MB, under the small→large 100 MB hard threshold, no
    # soft threshold because we have no prediction.
    assert loader._should_redispatch(actual_pack_kb=50 * 1024) is None


def test_redispatch_small_to_large_on_hard_threshold():
    loader = _make_loader(current_size_class="small")
    # 200 MB > small-queue's 100 MB hard threshold.
    target = loader._should_redispatch(actual_pack_kb=200 * 1024)
    assert target == "swh.loader.git.tasks.UpdateGitRepositoryLarge"


def test_redispatch_large_to_xl_on_hard_threshold():
    loader = _make_loader(current_size_class="large")
    # 3 GB > large-queue's 2 GB hard threshold.
    target = loader._should_redispatch(actual_pack_kb=3 * 1024 * 1024)
    assert target == "swh.loader.git.tasks.UpdateGitRepositoryXl"


def test_soft_threshold_triggers_redispatch_under_hard_threshold():
    loader = _make_loader(
        current_size_class="small",
        predicted_pack_size_kb=10 * 1024,  # 10 MB predicted
        safety_net_oversize_factor=2.0,
    )
    # Actual 50 MB is 5× the prediction — soft threshold triggers
    # even though 50 MB < 100 MB hard threshold.
    target = loader._should_redispatch(actual_pack_kb=50 * 1024)
    assert target == "swh.loader.git.tasks.UpdateGitRepositoryLarge"


def test_soft_threshold_with_tight_prediction_does_not_trigger():
    loader = _make_loader(
        current_size_class="small",
        predicted_pack_size_kb=30 * 1024,  # 30 MB predicted
        safety_net_oversize_factor=2.0,
    )
    # Actual 50 MB is only 1.67× the prediction — under 2× factor
    # and under hard threshold.
    assert loader._should_redispatch(actual_pack_kb=50 * 1024) is None


# ---------------------------------------------------------------------------
# _maybe_redispatch_oversized_pack: end-to-end with mocked Celery
# ---------------------------------------------------------------------------


def test_maybe_redispatch_no_op_when_under_thresholds():
    loader = _make_loader(current_size_class="small")
    loader.pack_size = 50 * 1024 * 1024  # 50 MB in bytes
    with patch("celery.current_app") as current_app_mock:
        assert loader._maybe_redispatch_oversized_pack() is False
    current_app_mock.signature.assert_not_called()
    assert loader._delegated is False
    assert loader._delegated_to is None


def test_maybe_redispatch_small_to_large_calls_apply_async():
    loader = _make_loader(current_size_class="small")
    loader.pack_size = 200 * 1024 * 1024  # 200 MB in bytes → 200 KB over threshold
    with patch("celery.current_app") as current_app_mock:
        sig = MagicMock()
        current_app_mock.signature.return_value = sig
        assert loader._maybe_redispatch_oversized_pack() is True

    # Verify the Celery signature was built with the right task name
    # and the forwarded kwargs match the design.
    assert current_app_mock.signature.call_count == 1
    call_args, call_kwargs = current_app_mock.signature.call_args
    assert call_args[0] == "swh.loader.git.tasks.UpdateGitRepositoryLarge"
    forwarded_kwargs = call_kwargs["kwargs"]
    assert forwarded_kwargs["url"] == "https://git.example.org/some/repo"
    assert forwarded_kwargs["incremental"] is True
    assert forwarded_kwargs["predicted_pack_size_kb"] == 200 * 1024

    # Verify apply_async was actually invoked.
    sig.apply_async.assert_called_once_with()

    # Verify loader state reflects the delegation.
    assert loader._delegated is True
    assert loader._delegated_to == "swh.loader.git.tasks.UpdateGitRepositoryLarge"

    # Verify statsd counter.
    loader.statsd.increment.assert_called_with(
        "git_safety_net_redispatch_total",
        tags={
            "from_queue": "small",
            "to_task": "UpdateGitRepositoryLarge",
        },
    )


def test_maybe_redispatch_large_to_xl():
    loader = _make_loader(current_size_class="large")
    loader.pack_size = 3 * 1024 * 1024 * 1024  # 3 GB in bytes
    with patch("celery.current_app") as current_app_mock:
        sig = MagicMock()
        current_app_mock.signature.return_value = sig
        assert loader._maybe_redispatch_oversized_pack() is True

    assert current_app_mock.signature.call_args[0][0] == (
        "swh.loader.git.tasks.UpdateGitRepositoryXl"
    )
    assert loader._delegated_to == "swh.loader.git.tasks.UpdateGitRepositoryXl"


def test_maybe_redispatch_does_nothing_on_xl():
    loader = _make_loader(current_size_class="xl")
    loader.pack_size = 50 * 1024 * 1024 * 1024  # 50 GB — huge but terminal
    with patch("celery.current_app") as current_app_mock:
        assert loader._maybe_redispatch_oversized_pack() is False
    current_app_mock.signature.assert_not_called()


def test_maybe_redispatch_propagates_apply_async_failure():
    loader = _make_loader(current_size_class="small")
    loader.pack_size = 200 * 1024 * 1024
    with patch("celery.current_app") as current_app_mock:
        sig = MagicMock()
        sig.apply_async.side_effect = RuntimeError("broker unreachable")
        current_app_mock.signature.return_value = sig
        with pytest.raises(RuntimeError, match="broker unreachable"):
            loader._maybe_redispatch_oversized_pack()

    # The failure-metric counter should have been incremented before
    # the exception propagated.
    loader.statsd.increment.assert_called_with(
        "git_safety_net_redispatch_failed_total",
        tags={"from_queue": "small"},
    )
    # And the loader must NOT be in the "delegated" state — the caller
    # (BaseLoader.load) will see the exception and record a failed
    # visit, which is the correct behaviour.
    assert loader._delegated is False


# ---------------------------------------------------------------------------
# load_status / visit_status overrides on the delegated path
# ---------------------------------------------------------------------------


def test_load_status_when_delegated():
    loader = _make_loader(current_size_class="small")
    loader._delegated = True
    loader._delegated_to = "swh.loader.git.tasks.UpdateGitRepositoryLarge"
    # build_snapshot is never called on the delegated path, so
    # prev_snapshot/snapshot don't need to exist — the delegated
    # branch short-circuits.
    result = loader.load_status()
    assert result == {
        "status": "uneventful",
        "delegated_to": "swh.loader.git.tasks.UpdateGitRepositoryLarge",
    }


def test_visit_status_when_delegated():
    loader = _make_loader(current_size_class="small")
    loader._delegated = True
    assert loader.visit_status() == "partial"


def test_visit_status_when_not_delegated_defaults_to_full():
    loader = _make_loader(current_size_class="small")
    loader._delegated = False
    # Base class default is "full".
    assert loader.visit_status() == "full"
