# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Dulwich-fallback dispatch helpers for the gix-based git loader.

When the gix backend rejects a repository for stricter spec-enforcement
reasons (malformed pack, unparsable object, traverse-path failure)
that dulwich is historically tolerant of, the SWH loader dispatch
layer re-queues the visit to a dulwich-based fallback worker.

This module provides the three building blocks that sit at the
boundary between "gix raised an exception" and "the Celery task
wrapper re-dispatches the visit":

- :class:`DulwichFallbackRequested` — a marker exception that the
  loader raises in its error-handling flow. The task wrapper catches
  it and calls ``apply_async`` on the ``loader.git.dulwich_fallback``
  queue.
- :func:`classify_gix_error` — inspects an exception instance and
  returns the reason tag (the typed-exception class name) if the
  error belongs to one of the three fallback-eligible classes, or
  ``None`` if the error is fatal-for-both (network, auth, policy,
  OOM) or simply not a gix error at all.
- :func:`record_fallback_metric` — increments the Prometheus counter
  ``git_dulwich_fallback_total{reason=<class>}`` via an
  ``swh.core.statsd.Statsd`` instance. Operations uses this metric
  to detect a new class of pathological repos entering the archive.

The actual apply_async re-dispatch path lives alongside the
already-committed safety-net re-queue infrastructure in the
``feat/size-dispatch-safety-net`` branch. This module is the
classifier + marker + metric that both the gix loader and the
dispatch task consume.

See ``notes/DESIGN-dulwich-fallback-signals.md`` in the
swh-environment audit tree for the full architectural decision.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from swh.loader.git._gix import (
    GixFatalError,
    GixObjectParseError,
    GixPackError,
    GixTraverseError,
)

if TYPE_CHECKING:
    from swh.core.statsd import Statsd


logger = logging.getLogger(__name__)


# Classes that trigger fallback to dulwich. Each represents a category of
# gix-strict failure that dulwich's more lenient parsers are likely to
# absorb. GixFatalError is deliberately absent — fatal-for-both errors
# (auth, network, filesystem, OOM, policy) must NOT fall back because
# dulwich will fail identically and we would waste fleet capacity.
_FALLBACK_CLASSES: tuple[type[Exception], ...] = (
    GixPackError,
    GixObjectParseError,
    GixTraverseError,
)


class DulwichFallbackRequested(Exception):
    """Marker: the gix loader has decided the current visit should fall
    back to the dulwich worker.

    Carries the classifier :attr:`reason` so the re-dispatch path can
    emit it as a metric label and so operations can slice the fallback
    rate by cause.
    """

    def __init__(self, reason: str, message: Optional[str] = None) -> None:
        self.reason = reason
        super().__init__(message or f"dulwich fallback requested: {reason}")


def classify_gix_error(exc: BaseException) -> Optional[str]:
    """Return a reason tag if *exc* is a fallback-eligible gix error.

    The reason tag is the Python class name
    (``"GixPackError"`` / ``"GixObjectParseError"`` / ``"GixTraverseError"``)
    suitable for use as a metric label.

    Returns ``None`` for :class:`GixFatalError` (fatal-for-both) and for
    any non-gix exception. Callers should propagate a ``None`` result
    to the base loader's error handling rather than triggering a
    fallback.

    The classifier is exception-class-based, not message-text-based:
    the mapping from gix internals to these three public classes lives
    in ``gix-py/src/exceptions.rs`` and is the only coupling point to
    gitoxide's error text. Adding a new upstream error class means
    touching that mapper; it does not affect this module.
    """
    if isinstance(exc, _FALLBACK_CLASSES):
        return type(exc).__name__
    # GixFatalError lands here explicitly so the branch is visible in
    # coverage: fatal-for-both is a deliberate non-fallback.
    if isinstance(exc, GixFatalError):
        return None
    return None


def record_fallback_metric(statsd: Optional["Statsd"], reason: str) -> None:
    """Increment ``git_dulwich_fallback_total{reason=<class>}``.

    No-op if *statsd* is ``None`` (unit tests, CLI invocations).
    """
    if statsd is None:
        return
    statsd.increment("git_dulwich_fallback_total", tags={"reason": reason})
