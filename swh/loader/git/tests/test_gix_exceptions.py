# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Tests for the typed Python exception classes raised by the gix bindings.

See ``gix-py/src/exceptions.rs`` for the class hierarchy and the design
rationale. The classes let the SWH git loader's dispatch layer decide
per-exception-class whether to re-dispatch to the dulwich-based fallback
worker: pack/object/traverse errors may succeed on the more lenient
backend, while ``GixFatalError`` (auth, network, filesystem) would fail
on any backend and must not be retried.
"""

import pytest

import swh.loader.git._gix as _gix
from swh.loader.git._gix import (
    GixFatalError,
    GixObjectParseError,
    GixPackError,
    GixTraverseError,
)

# ---------------------------------------------------------------------------
# Class-level structural tests (no Rust call required)
# ---------------------------------------------------------------------------


ALL_CLASSES = (GixPackError, GixObjectParseError, GixTraverseError, GixFatalError)


def test_all_classes_importable():
    """All four classes are attributes of the _gix module."""
    for cls in ALL_CLASSES:
        assert getattr(_gix, cls.__name__) is cls


def test_all_classes_subclass_valueerror():
    """All four classes subclass ValueError for backwards compatibility.

    Pre-existing ``except ValueError`` code paths keep working after the
    refactor; new code catches the specific class.
    """
    for cls in ALL_CLASSES:
        assert issubclass(cls, ValueError), f"{cls.__name__} must subclass ValueError"


def test_classes_are_distinct():
    """Catching one typed class does NOT catch the others.

    This is the property the dulwich-fallback predicate relies on:
    ``except GixFatalError`` must not catch a ``GixPackError``.
    """
    for cls_a in ALL_CLASSES:
        for cls_b in ALL_CLASSES:
            if cls_a is cls_b:
                continue
            assert not issubclass(cls_a, cls_b), (
                f"{cls_a.__name__} is a subclass of {cls_b.__name__}; "
                "typed-exception dispatch would misroute"
            )


def test_classes_module_attribute():
    """Each class reports its module as ``_gix``.

    Stable module name matters for logging / metric labels that use
    ``type(e).__module__``.
    """
    for cls in ALL_CLASSES:
        assert (
            cls.__module__ == "_gix"
        ), f"{cls.__name__}.__module__ = {cls.__module__!r}; expected '_gix'"


# ---------------------------------------------------------------------------
# Functional tests: trigger real errors, assert the correct class
# ---------------------------------------------------------------------------


def test_fetch_pack_network_failure_raises_gix_fatal_error():
    """A connection to a closed port triggers GixFatalError (handshake failure).

    Catching as ``GixFatalError`` — not as generic ``ValueError`` — is what
    the dispatch predicate relies on for the fatal-for-both category.
    """
    # Port 1 is TCP "tcpmux"; universally unreachable on dev machines.
    with pytest.raises(GixFatalError):
        _gix.fetch_pack("http://127.0.0.1:1/nonexistent.git", [], [])


def test_fetch_pack_network_failure_still_catchable_as_valueerror():
    """Backwards compatibility: pre-existing ``except ValueError`` works."""
    with pytest.raises(ValueError):
        _gix.fetch_pack("http://127.0.0.1:1/nonexistent.git", [], [])


def test_inflate_types_truncated_pack_raises_gix_pack_error():
    """A truncated pack (under 12 bytes) triggers GixPackError.

    This is the canonical "dulwich can probably do better" signal: the
    header scan fails in gix-pack; dulwich's more lenient parser may
    recover partial objects from such inputs.
    """
    # Deliberately malformed: PACK header prefix only, no version/count/objects.
    truncated = b"PACK"
    with pytest.raises(GixPackError):
        _gix.inflate_types(truncated)


def test_inflate_types_empty_pack_raises_gix_pack_error():
    """An entirely empty byte string is a pack truncation → GixPackError."""
    with pytest.raises(GixPackError):
        _gix.inflate_types(b"")


def test_iter_pack_objects_empty_pack_raises_gix_pack_error():
    """iter_pack_objects on empty bytes is also a truncation → GixPackError."""
    with pytest.raises(GixPackError):
        _gix.iter_pack_objects(b"")


# ---------------------------------------------------------------------------
# Negative assertions: fatal-class signals must NOT be caught as fallback-class
# ---------------------------------------------------------------------------


def test_network_failure_not_misrouted_as_pack_error():
    """A fatal handshake failure must NOT surface as GixPackError.

    If this test ever fails, the dispatch predicate would re-route a
    network/auth problem to the dulwich fallback worker, which would
    fail identically — wasting capacity and possibly masking a real
    infrastructure issue.
    """
    with pytest.raises(GixFatalError) as exc_info:
        _gix.fetch_pack("http://127.0.0.1:1/nonexistent.git", [], [])
    assert not isinstance(exc_info.value, GixPackError)
    assert not isinstance(exc_info.value, GixObjectParseError)
    assert not isinstance(exc_info.value, GixTraverseError)


def test_truncated_pack_not_misrouted_as_fatal():
    """A malformed-pack error must NOT surface as GixFatalError.

    If it did, the dispatch predicate would refuse to re-route to
    dulwich and the visit would fail hard, even though dulwich could
    have handled it.
    """
    with pytest.raises(GixPackError) as exc_info:
        _gix.inflate_types(b"PACK")
    assert not isinstance(exc_info.value, GixFatalError)
