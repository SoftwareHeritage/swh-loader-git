// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Gitoxide-backed git fetch and pack parsing for swh-loader-git.
//!
//! This crate is the pure-Rust library layer. It has no PyO3 dependency.
//! The Python binding lives in the sibling `gix-py` crate.

mod fetch;
mod inflate;
mod objects;
mod traverse;

pub use fetch::{fetch_pack, fetch_pack_to_file, FetchPackFileResult, FetchPackResult};
pub use inflate::{inflate_pack_typed, iter_pack_objects, PackIterator};
pub use objects::{
    BlobResult, InflateResult, PackObject, RawResult, TreeEntry, TreeResult, TypedObject,
};
pub use traverse::{DirectTreeInflater, ParallelInflater};

/// Returns the library version string. Used to verify the binding is loaded correctly.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_non_empty() {
        assert!(!version().is_empty());
    }
}
