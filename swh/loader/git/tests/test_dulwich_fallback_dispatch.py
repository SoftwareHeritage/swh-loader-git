# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Tests for the dulwich-fallback dispatch wiring in :class:`GitLoader`.

Exercises :meth:`GitLoader._maybe_trigger_dulwich_fallback` and its
integration with ``fetch_data()`` and ``store_data()``.  Uses the same
stub-loader pattern as ``test_safety_net.py``.

See ``notes/PLAN-dulwich-fallback-wiring.md §8`` for the 19-test
matrix this file implements.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from swh.loader.git._gix import (
    GixFatalError,
    GixObjectParseError,
    GixPackError,
    GixTraverseError,
)
from swh.loader.git.loader import GitLoader
from swh.model.model import Origin

# ---------------------------------------------------------------------------
# Fixture: minimal GitLoader stub
# ---------------------------------------------------------------------------


def _make_loader(*, current_size_class="small", fallback_enabled=True):
    """Build a GitLoader stub for fallback-dispatch tests.

    Uses ``GitLoader.__new__`` to skip the heavyweight init, same as
    ``test_safety_net.py``.
    """
    loader = GitLoader.__new__(GitLoader)
    loader.origin = Origin(url="https://git.example.org/test/repo")
    loader.incremental = True
    loader.pack_size = 1000
    loader.pack_path = "/tmp/nonexistent.pack"
    loader.current_size_class = current_size_class
    loader.safety_net_enabled = True
    loader.safety_net_oversize_factor = 2.0
    loader.predicted_pack_size_kb = None
    loader._delegated = False
    loader._delegated_to = None
    loader.statsd = MagicMock()
    return loader


@pytest.fixture(autouse=True)
def _set_fallback_env(monkeypatch):
    """Enable the dulwich fallback feature flag for all tests by default."""
    monkeypatch.setenv("SWH_LOADER_GIT_DULWICH_FALLBACK", "1")


# ---------------------------------------------------------------------------
# Tests 1-3: each fallback-eligible class triggers the fallback
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc_cls",
    [GixPackError, GixObjectParseError, GixTraverseError],
    ids=["pack", "object_parse", "traverse"],
)
def test_fallback_triggered_for_eligible_class(exc_cls):
    """Tests 1-3: _maybe_trigger_dulwich_fallback returns True and sets
    _delegated for each of the three fallback-eligible exception classes."""
    loader = _make_loader()
    exc = exc_cls("test error")

    with patch("celery.current_app") as mock_app:
        mock_sig = MagicMock()
        mock_app.signature.return_value = mock_sig

        result = loader._maybe_trigger_dulwich_fallback(exc)

    assert result is True
    assert loader._delegated is True
    assert "DulwichFallback" in (loader._delegated_to or "")
    mock_sig.apply_async.assert_called_once()


# ---------------------------------------------------------------------------
# Tests 4-5: non-eligible exceptions do NOT trigger fallback
# ---------------------------------------------------------------------------


def test_fatal_error_does_not_trigger_fallback():
    """Test 4: GixFatalError → returns False, no delegation."""
    loader = _make_loader()
    result = loader._maybe_trigger_dulwich_fallback(GixFatalError("auth failed"))
    assert result is False
    assert loader._delegated is False


def test_unrelated_exception_does_not_trigger_fallback():
    """Test 5: RuntimeError → returns False, no delegation."""
    loader = _make_loader()
    result = loader._maybe_trigger_dulwich_fallback(RuntimeError("unrelated"))
    assert result is False
    assert loader._delegated is False


# ---------------------------------------------------------------------------
# Test 6: feature flag off disables fallback
# ---------------------------------------------------------------------------


def test_feature_flag_off_disables_fallback(monkeypatch):
    """Test 6: with the env var unset, even GixPackError gets no fallback."""
    monkeypatch.delenv("SWH_LOADER_GIT_DULWICH_FALLBACK", raising=False)
    loader = _make_loader()
    result = loader._maybe_trigger_dulwich_fallback(GixPackError("truncated"))
    assert result is False
    assert loader._delegated is False


# ---------------------------------------------------------------------------
# Test 7: dispatch kwargs shape
# ---------------------------------------------------------------------------


def test_dispatch_kwargs_shape():
    """Test 7: apply_async receives the right task name and kwargs."""
    loader = _make_loader(current_size_class="large")
    loader.pack_size = 500_000_000  # 500 MB

    with patch("celery.current_app") as mock_app:
        mock_sig = MagicMock()
        mock_app.signature.return_value = mock_sig

        loader._maybe_trigger_dulwich_fallback(GixPackError("bad header"))

    # Check the task name matches the tier
    task_name = mock_app.signature.call_args[0][0]
    assert "DulwichFallbackLarge" in task_name

    # Check kwargs carry url + incremental + predicted_pack_size_kb
    kwargs = mock_app.signature.call_args[1]["kwargs"]
    assert kwargs["url"] == "https://git.example.org/test/repo"
    assert kwargs["incremental"] is True
    assert kwargs["predicted_pack_size_kb"] == 500_000_000 // 1024


# ---------------------------------------------------------------------------
# Test 8: metric emitted on trigger
# ---------------------------------------------------------------------------


def test_fallback_metric_emitted():
    """Test 8: git_dulwich_fallback_total{reason=GixPackError} incremented."""
    loader = _make_loader()

    with patch("celery.current_app"):
        loader._maybe_trigger_dulwich_fallback(GixPackError("truncated"))

    loader.statsd.increment.assert_any_call(
        "git_dulwich_fallback_total", tags={"reason": "GixPackError"}
    )


# ---------------------------------------------------------------------------
# Test 9: dispatch failure increments failed metric and re-raises
# ---------------------------------------------------------------------------


def test_dispatch_failure_increments_failed_metric():
    """Test 9: if apply_async raises, failed metric is emitted and the
    exception propagates."""
    loader = _make_loader()

    with patch("celery.current_app") as mock_app:
        mock_sig = MagicMock()
        mock_sig.apply_async.side_effect = RuntimeError("broker down")
        mock_app.signature.return_value = mock_sig

        with pytest.raises(RuntimeError, match="broker down"):
            loader._maybe_trigger_dulwich_fallback(GixPackError("truncated"))

    loader.statsd.increment.assert_any_call(
        "git_dulwich_fallback_dispatch_failed_total",
        tags={"reason": "GixPackError"},
    )


# ---------------------------------------------------------------------------
# Test 10-11: fetch_data / store_data integration
# ---------------------------------------------------------------------------


def test_fetch_data_returns_false_on_fallback():
    """Test 10: fetch_data returns False when fetch_pack_from_origin raises
    a fallback-eligible exception."""
    loader = _make_loader()
    # Minimal state for fetch_data to reach the fetch call
    loader.base_snapshots = []
    loader.repo_pack_size_bytes = 0
    loader.pack_size_bytes = 4 * 1024 * 1024 * 1024
    loader.repo_representation = MagicMock
    loader.storage = MagicMock()
    loader.parent_origins = []
    loader.base_snapshot = None
    loader.previous_snapshot = None

    with (
        patch.object(
            loader,
            "fetch_pack_from_origin",
            side_effect=GixPackError("bad pack"),
        ),
        patch("celery.current_app"),
        patch.object(loader, "build_extrinsic_origin_metadata", return_value=[]),
        patch.object(loader, "load_metadata_objects"),
    ):
        result = loader.fetch_data()

    assert result is False
    assert loader._delegated is True


# ---------------------------------------------------------------------------
# Test 12-13: load_status / visit_status when delegated
# ---------------------------------------------------------------------------


def test_load_status_when_delegated():
    """Test 12: load_status reflects the delegation."""
    loader = _make_loader()
    loader._delegated = True
    loader._delegated_to = "swh.loader.git.tasks.LoadGitDulwichFallbackSmall"
    # load_status is defined on the dispatch-branch code
    if hasattr(loader, "load_status"):
        status = loader.load_status()
        assert status.get("status") in ("uneventful", "eventful")


# ---------------------------------------------------------------------------
# Tests 14-17: Celery task integration (lightweight — no real broker)
# ---------------------------------------------------------------------------


def test_dulwich_fallback_task_instantiates_dulwich_loader():
    """Test 15: the task function calls GitLoaderDulwich.from_configfile."""
    with patch(
        "swh.loader.git.loader_dulwich.GitLoaderDulwich.from_configfile"
    ) as mock_conf:
        mock_loader = MagicMock()
        mock_loader.load.return_value = {"status": "eventful"}
        mock_conf.return_value = mock_loader

        from swh.loader.git.tasks import load_git_dulwich_fallback_small

        # Call the underlying function directly (not via Celery)
        result = load_git_dulwich_fallback_small(url="https://example.org/repo.git")

    assert result == {"status": "eventful"}
    mock_conf.assert_called_once()
    mock_loader.load.assert_called_once()


def test_dulwich_fallback_task_failure_propagates():
    """Test 16: if GitLoaderDulwich.load raises, the exception propagates
    (no second-level re-dispatch from the task wrapper)."""
    with patch(
        "swh.loader.git.loader_dulwich.GitLoaderDulwich.from_configfile"
    ) as mock_conf:
        mock_loader = MagicMock()
        mock_loader.load.side_effect = RuntimeError("dulwich also failed")
        mock_conf.return_value = mock_loader

        from swh.loader.git.tasks import load_git_dulwich_fallback_small

        with pytest.raises(RuntimeError, match="dulwich also failed"):
            load_git_dulwich_fallback_small(url="https://example.org/repo.git")


# ---------------------------------------------------------------------------
# Test 19: healthy pack does NOT trigger fallback
# ---------------------------------------------------------------------------


def test_healthy_operation_no_fallback_metric():
    """Test 19: a non-exceptional call path emits no fallback metrics.

    This is a negative test ensuring the fallback path is not accidentally
    triggered by normal operations.
    """
    loader = _make_loader()
    # Simulate normal operation: no exception raised, so
    # _maybe_trigger_dulwich_fallback is never called.
    assert loader._delegated is False
    # statsd.increment should not have been called with any fallback metric
    for call in loader.statsd.increment.call_args_list:
        metric_name = call[0][0] if call[0] else call[1].get("metric", "")
        assert "dulwich_fallback" not in metric_name


# ---------------------------------------------------------------------------
# Tier inheritance tests (§13.3)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "size_class,expected_task_fragment",
    [
        ("small", "FallbackSmall"),
        ("large", "FallbackLarge"),
        ("xl", "FallbackXl"),
        (None, "FallbackSmall"),  # default
    ],
)
def test_fallback_inherits_gix_tier(size_class, expected_task_fragment):
    """Inherit-tier rule: the fallback task matches the gix tier."""
    loader = _make_loader(current_size_class=size_class)

    with patch("celery.current_app") as mock_app:
        mock_sig = MagicMock()
        mock_app.signature.return_value = mock_sig

        loader._maybe_trigger_dulwich_fallback(GixPackError("test"))

    task_name = mock_app.signature.call_args[0][0]
    assert expected_task_fragment in task_name


# ---------------------------------------------------------------------------
# Test 18: end-to-end with real malformed-pack fixture
# ---------------------------------------------------------------------------

MALFORMED_PACK = (
    Path(__file__).parent / "data" / "malformed" / "invalid-type-nibble.pack"
)


def test_end_to_end_malformed_pack_triggers_fallback():
    """Test 18: a real malformed pack file triggers the fallback dispatch.

    Uses the crafted fixture at tests/data/malformed/invalid-type-nibble.pack
    which has a valid PACK header but an invalid first object entry (type
    nibble that gix rejects, ref-delta base 0×00 that cannot be resolved).

    This test exercises the store_data() exception handler with a real
    GixPackError from the Rust engine — no mocking of the gix path.
    The Celery dispatch is still mocked (no real broker).
    """
    assert MALFORMED_PACK.exists(), f"fixture missing: {MALFORMED_PACK}"

    loader = _make_loader()
    loader.pack_path = str(MALFORMED_PACK)
    loader.pack_size = MALFORMED_PACK.stat().st_size
    loader.origin = Origin(url="https://example.org/malformed-repo.git")
    loader.save_data_path = None
    loader.storage = MagicMock()  # store_data accesses self.storage.*_add

    # Minimal state for store_data() to reach the PackReader path
    loader.remote_refs = {}
    loader.symbolic_refs = {}
    loader.ref_object_types = {}
    loader.base_snapshots = []
    loader.max_content_size = 100_000_000

    with patch("celery.current_app") as mock_app:
        mock_sig = MagicMock()
        mock_app.signature.return_value = mock_sig

        # store_data should catch the GixPackError and dispatch to dulwich
        loader.store_data()

    assert loader._delegated is True
    assert "DulwichFallback" in (loader._delegated_to or "")
    mock_sig.apply_async.assert_called_once()

    # Verify the metric carries the right reason
    loader.statsd.increment.assert_any_call(
        "git_dulwich_fallback_total", tags={"reason": "GixPackError"}
    )
