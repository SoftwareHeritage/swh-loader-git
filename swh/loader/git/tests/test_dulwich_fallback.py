# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Tests for the dulwich-fallback dispatch helpers.

See ``swh/loader/git/dulwich_fallback.py`` and
``notes/DESIGN-dulwich-fallback-signals.md`` in the swh-environment
audit tree for the design rationale.
"""

from unittest.mock import MagicMock

import pytest

from swh.loader.git._gix import (
    GixFatalError,
    GixObjectParseError,
    GixPackError,
    GixTraverseError,
)
from swh.loader.git.dulwich_fallback import (
    DulwichFallbackRequested,
    classify_gix_error,
    record_fallback_metric,
)

# ---------------------------------------------------------------------------
# classify_gix_error
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc_cls,expected",
    [
        (GixPackError, "GixPackError"),
        (GixObjectParseError, "GixObjectParseError"),
        (GixTraverseError, "GixTraverseError"),
    ],
)
def test_classify_returns_class_name_for_fallback_classes(exc_cls, expected):
    """Each of the three fallback-eligible classes yields its class name."""
    assert classify_gix_error(exc_cls("boom")) == expected


def test_classify_returns_none_for_gix_fatal_error():
    """GixFatalError must not trigger fallback (fatal-for-both)."""
    assert classify_gix_error(GixFatalError("handshake failed")) is None


def test_classify_returns_none_for_plain_valueerror():
    """A bare ValueError (not one of the typed subclasses) is not a fallback.

    Defends against the regression where a caller double-wraps a gix
    exception as ``ValueError(str(exc))`` and the classifier would
    incorrectly treat it as non-gix.
    """
    assert classify_gix_error(ValueError("something generic")) is None


def test_classify_returns_none_for_unrelated_exception():
    """Non-gix exceptions never trigger fallback."""
    assert classify_gix_error(RuntimeError("unrelated")) is None
    assert classify_gix_error(KeyError("missing")) is None


def test_classify_is_exception_class_based_not_message_text_based():
    """The classifier keys on exception class, not message substring.

    Two different typed classes with the same message must return
    different reasons. This is the property that makes the classifier
    stable across gitoxide error-text changes.
    """
    msg = "identical message text"
    assert classify_gix_error(GixPackError(msg)) == "GixPackError"
    assert classify_gix_error(GixObjectParseError(msg)) == "GixObjectParseError"


# ---------------------------------------------------------------------------
# DulwichFallbackRequested marker
# ---------------------------------------------------------------------------


def test_fallback_requested_carries_reason():
    exc = DulwichFallbackRequested("GixPackError")
    assert exc.reason == "GixPackError"


def test_fallback_requested_default_message_mentions_reason():
    """Logs / sentry traces must make the reason visible without extra fields."""
    exc = DulwichFallbackRequested("GixObjectParseError")
    assert "GixObjectParseError" in str(exc)


def test_fallback_requested_explicit_message_is_preserved():
    exc = DulwichFallbackRequested("GixTraverseError", message="custom context")
    assert str(exc) == "custom context"
    assert exc.reason == "GixTraverseError"


def test_fallback_requested_can_chain_from_original():
    """``raise DulwichFallbackRequested(...) from gix_exc`` preserves the trace."""
    original = GixPackError("pack truncated")
    try:
        try:
            raise original
        except GixPackError as e:
            raise DulwichFallbackRequested(classify_gix_error(e)) from e
    except DulwichFallbackRequested as exc:
        assert exc.__cause__ is original
        assert exc.reason == "GixPackError"


# ---------------------------------------------------------------------------
# record_fallback_metric
# ---------------------------------------------------------------------------


def test_record_metric_calls_statsd_increment():
    statsd = MagicMock()
    record_fallback_metric(statsd, "GixPackError")
    statsd.increment.assert_called_once_with(
        "git_dulwich_fallback_total", tags={"reason": "GixPackError"}
    )


def test_record_metric_is_noop_when_statsd_is_none():
    """CLI / unit-test invocations have no statsd instance; the call must not fail."""
    record_fallback_metric(None, "GixPackError")  # no exception raised


def test_record_metric_labels_correctly_per_reason():
    """Each distinct reason produces a distinct metric label."""
    statsd = MagicMock()
    for reason in ("GixPackError", "GixObjectParseError", "GixTraverseError"):
        record_fallback_metric(statsd, reason)
    assert statsd.increment.call_count == 3
    call_tags = [c.kwargs["tags"]["reason"] for c in statsd.increment.call_args_list]
    assert call_tags == [
        "GixPackError",
        "GixObjectParseError",
        "GixTraverseError",
    ]
