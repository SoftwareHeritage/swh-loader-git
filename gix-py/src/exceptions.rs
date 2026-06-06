// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Typed Python exception classes raised by the gix bindings.
//!
//! These classes let the SWH git loader's dispatch layer decide, on a per-class
//! basis, whether a failed visit should be re-dispatched to the dulwich-based
//! fallback worker (because gix enforces stricter spec compliance than
//! dulwich on malformed packs/objects), retried, or propagated as a hard
//! failure.
//!
//! Why typed classes instead of one exception: without them, gix-py would
//! wrap every anyhow error as a bare `PyValueError` whose message is the
//! stringified error chain, and any dispatch predicate would have to
//! string-match on that message *in Python* — a coupling that gitoxide
//! does not commit to preserving across minor releases.
//!
//! The classification itself (in `map_gix_error` below) is keyword-based
//! on the rendered Rust error chain. That is a deliberate trade-off: the
//! coupling to gitoxide's message wording is contained in this one file,
//! covered by the per-family tests in
//! `swh/loader/git/tests/test_gix_exceptions.py`, and adjusting to an
//! upstream rename is a single-table change. The alternative — matching
//! on gitoxide's concrete error *types* — would couple this crate to many
//! unstable enum variants across a dozen gix-* crates instead. When in
//! doubt the mapping falls through to `GixFatalError`, which callers must
//! treat as non-retriable, so a missed keyword degrades to a conservative
//! outcome, never a wrong retry.
//!
//! All four classes inherit from `ValueError` so existing
//! `except ValueError` code paths keep working; new callers catch the
//! specific class they care about.

use pyo3::create_exception;
use pyo3::exceptions::PyValueError;

create_exception!(
    _gix,
    GixPackError,
    PyValueError,
    "Pack-format error (truncated pack, bad entry header, failed decode, \
     corrupt delta chain, index-pack subprocess rejection). These are \
     typically recoverable by the dulwich backend, which is tolerant of \
     legacy pack deviations that gix rejects."
);

create_exception!(
    _gix,
    GixObjectParseError,
    PyValueError,
    "Object-level parsing error (malformed tree entry, bad mode bits, \
     truncated object, invalid commit header). The dulwich object \
     parsers tolerate inputs gix rejects."
);

create_exception!(
    _gix,
    GixTraverseError,
    PyValueError,
    "Pack-traversal error (parallel traverse processor failure, \
     DirectTreeInflater scan failure, REF_DELTA encountered in a \
     direct-tree pass). Dulwich's sequential traversal may succeed on \
     inputs that trip gix's optimised paths."
);

create_exception!(
    _gix,
    GixFatalError,
    PyValueError,
    "Fatal error that any backend would hit identically: authentication \
     failure, network disconnect, filesystem error, out-of-memory, \
     policy violation (pack size exceeds limit), or subprocess \
     execution failure. Must not trigger dulwich fallback."
);

/// Map an `anyhow::Error` surfaced by `gix-lib` to a typed Python exception
/// class.
///
/// Classification is keyword-based on the rendered error chain. The keyword
/// tables live in this function — all coupling to gitoxide's error message
/// text is localised here, so adjusting to an upstream rename is a
/// single-file change with test coverage.
///
/// Order of checks matters: most-specific patterns first. If no pattern
/// matches, the error is raised as a bare `PyValueError` (the pre-existing
/// behaviour) so that unclassified errors surface instead of being
/// silently misrouted.
pub fn map_gix_error(e: anyhow::Error) -> pyo3::PyErr {
    let msg = format!("{e:#}");
    let lower = msg.to_lowercase();

    // GixFatalError: network / auth / filesystem / policy / OOM / subprocess-exec.
    // These represent "the environment failed" - dulwich will fail identically.
    const FATAL_PATTERNS: &[&str] = &[
        "handshake failed",
        "parse remote refs",
        "send fetch arguments",
        "parse fetch response",
        "read pack data",
        "write pack data",
        "write pack bytes",
        "create empty pack file",
        "create temp file",
        "flush pack",
        "pack size exceeds",
        "allocate delta tree",
        "run git index-pack", // subprocess could not start
    ];

    // GixTraverseError: delta-tree / parallel-traversal / DirectTreeInflater errors.
    const TRAVERSE_PATTERNS: &[&str] = &[
        "ref-delta",
        "direct tree traversal",
        "ofs-delta only packs",
    ];

    // GixObjectParseError: object-level parsing failures.
    const OBJECT_PATTERNS: &[&str] = &[
        "parse tree entries",
        "truncated tree entry",
        "invalid mode bytes",
        "mode is not a valid",
        "compute object sha-1",
    ];

    // GixPackError: pack-format / pack-level failures (header, index, delta chain).
    const PACK_PATTERNS: &[&str] = &[
        "index-pack failed with status", // pack rejected by `git index-pack`
        "corrupt pack header",
        "pack too short",       // upstream gitoxide header scan on truncated input
        "pack signature",       // upstream gitoxide: wrong magic bytes
        "open pack index",
        "open pack data",
        "open pack file",
        "pack entry iterator",
        "decode pack entry",    // defensive: direct-tree path may surface this
    ];

    for p in FATAL_PATTERNS {
        if lower.contains(p) {
            return pyo3::PyErr::new::<GixFatalError, _>(msg);
        }
    }
    for p in TRAVERSE_PATTERNS {
        if lower.contains(p) {
            return pyo3::PyErr::new::<GixTraverseError, _>(msg);
        }
    }
    for p in OBJECT_PATTERNS {
        if lower.contains(p) {
            return pyo3::PyErr::new::<GixObjectParseError, _>(msg);
        }
    }
    for p in PACK_PATTERNS {
        if lower.contains(p) {
            return pyo3::PyErr::new::<GixPackError, _>(msg);
        }
    }

    // Unclassified: fall back to bare PyValueError. Staging should collect
    // these and promote them into one of the explicit buckets above.
    PyValueError::new_err(msg)
}
