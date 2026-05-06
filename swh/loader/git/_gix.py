# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Development-mode shim for the gitoxide Rust bindings.

Why this file exists
--------------------
The gitoxide bindings live in the ``gix-py`` Rust crate.  The production
build (``pip install -e .`` from the repository root) uses
``setuptools-rust`` with ``target = "swh.loader.git._gix"``, which
correctly compiles and installs the shared object *directly* next to
this file.  When that path is used, this shim is shadowed by the
compiled ``.so`` and never runs.

The iterative developer workflow uses ``maturin develop`` against
``gix-py/Cargo.toml``.  ``maturin`` installs the extension under the
top-level ``_gix`` package in site-packages, not as
``swh.loader.git._gix``.  Without this shim every rebuild would require
a manual ``cp target/maturin/lib_gix.so
swh/loader/git/_gix.*.so`` — and in practice that copy kept drifting
stale, silently causing rebuilds to "do nothing".

This shim bridges the two: ``import swh.loader.git._gix`` falls through
to the top-level ``_gix`` package (which ``maturin`` keeps current), so
``maturin develop`` is sufficient on its own.  If a setuptools-rust-built
``.so`` is ever placed next to this file, Python's finder picks the
``.so`` first and this file is ignored — so production behaviour is
unchanged.
"""
from _gix import *  # type: ignore  # noqa: F401, F403
from _gix import _gix as _native_module  # type: ignore

# Propagate metadata so callers that introspect the module see the
# native docstring/version, not this shim's.
__doc__ = _native_module.__doc__
if hasattr(_native_module, "__all__"):
    __all__ = list(_native_module.__all__)
