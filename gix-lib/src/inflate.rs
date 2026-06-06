// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Single-threaded pack inflation paths.

use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use gix_features::zlib::Inflate;
use gix_object::Kind as ObjectKind;
use gix_pack::data::decode::entry::ResolvedBase;

use crate::objects::{
    commit_hash_will_match, hash_blob, parse_tree, tree_hash_will_match, BlobResult, InflateResult,
    PackObject, RawResult, TreeResult, TypedObject,
};

// ---------------------------------------------------------------------------
// Streaming pack iterator
// ---------------------------------------------------------------------------

/// Streaming iterator over objects in a pack file on disk.
///
/// Memory usage is O(largest single object), not O(total pack).
/// Created via [`PackIterator::open`].
pub struct PackIterator {
    pack_file: gix_pack::data::File,
    inflate: Inflate,
    cache: gix_pack::cache::lru::StaticLinkedList<64>,
    offset: u64,
    remaining: u32,
}

impl PackIterator {
    /// Open a pack file on disk for streaming iteration.
    pub fn open(pack_path: &Path) -> Result<Self> {
        let pack_file = gix_pack::data::File::at(pack_path, gix_hash::Kind::Sha1)
            .with_context(|| format!("failed to open pack file {}", pack_path.display()))?;
        let num_objects = pack_file.num_objects();
        Ok(PackIterator {
            pack_file,
            inflate: Inflate::default(),
            cache: gix_pack::cache::lru::StaticLinkedList::<64>::new(64 * 1024 * 1024),
            offset: 12,
            remaining: num_objects,
        })
    }

    /// Decode and return the next object, or `None` if all objects consumed.
    pub fn next_object(&mut self) -> Result<Option<TypedObject>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;

        let resolve: &dyn Fn(&gix_hash::oid, &mut Vec<u8>) -> Option<ResolvedBase> =
            &|_, _| None;

        let entry = self
            .pack_file
            .entry(self.offset)
            .with_context(|| format!("bad pack entry header at offset {}", self.offset))?;
        let data_offset = entry.data_offset;
        let mut out = Vec::new();
        let outcome = self
            .pack_file
            .decode_entry(
                entry,
                &mut out,
                &mut self.inflate,
                resolve,
                &mut self.cache,
            )
            .with_context(|| format!("failed to decode pack entry at offset {}", self.offset))?;

        let hash = gix_object::compute_hash(gix_hash::Kind::Sha1, outcome.kind, &out)
            .context("failed to compute object SHA-1")?;
        let mut sha1_git = [0u8; 20];
        sha1_git.copy_from_slice(hash.as_slice());

        self.offset = data_offset + outcome.compressed_size as u64;

        let obj = match outcome.kind {
            ObjectKind::Blob => {
                let (sha1, sha256, blake2s256) = hash_blob(&out);
                TypedObject::Blob {
                    sha1_git,
                    sha1,
                    sha256,
                    blake2s256,
                    data: out,
                }
            }
            ObjectKind::Tree => {
                let entries = parse_tree(&out)
                    .context("failed to parse tree entries")?;
                let hash_match = tree_hash_will_match(&out, &entries);
                TypedObject::Tree {
                    sha1_git,
                    raw_data: out,
                    entries,
                    hash_match,
                }
            }
            ObjectKind::Commit => {
                let hash_match = commit_hash_will_match(&out);
                TypedObject::Commit {
                    sha1_git,
                    data: out,
                    hash_match,
                }
            }
            ObjectKind::Tag => {
                let hash_match = commit_hash_will_match(&out);
                TypedObject::Tag {
                    sha1_git,
                    data: out,
                    hash_match,
                }
            }
        };

        Ok(Some(obj))
    }
}

/// Parse and inflate all objects from a raw pack byte stream.
///
/// `pack_bytes` must be a complete, non-thin pack (PACK header + objects + trailer).
/// Returns one [`PackObject`] per object in pack order.
/// Delta chains are fully resolved so every returned object is ready to be hashed and stored.
pub fn iter_pack_objects(pack_bytes: &[u8]) -> Result<Vec<PackObject>> {
    anyhow::ensure!(pack_bytes.len() >= 12, "pack too short for header");

    // 1. Write pack bytes to a temp file so gix can memory-map it for delta resolution.
    let mut tmp = tempfile::NamedTempFile::new().context("failed to create temp file for pack")?;
    tmp.write_all(pack_bytes)
        .context("failed to write pack bytes to temp file")?;
    tmp.flush().context("failed to flush pack temp file")?;

    // 2. Open as gix memory-mapped pack File (random-access for OfsDelta chains).
    let pack_file = gix_pack::data::File::at(tmp.path(), gix_hash::Kind::Sha1)
        .context("failed to open pack file")?;

    // 3. Read pack header for the object count.
    let num_objects = {
        let header: [u8; 12] = pack_bytes[..12]
            .try_into()
            .expect("length checked by the ensure! above");
        let (_version, n) =
            gix_pack::data::header::decode(&header).context("corrupt pack header")?;
        n
    };

    // 4. Single-pass decode: walk entries sequentially using the memory-mapped File.
    //    For each entry, decode_entry returns Outcome.compressed_size which is the
    //    number of compressed bytes consumed for THIS entry, allowing us to compute
    //    the next entry's offset without a separate streaming pass.
    let mut objects = Vec::with_capacity(num_objects as usize);
    let mut inflate = Inflate::default();
    let mut cache = gix_pack::cache::lru::StaticLinkedList::<64>::new(64 * 1024 * 1024);
    let resolve: &dyn Fn(&gix_hash::oid, &mut Vec<u8>) -> Option<ResolvedBase> = &|_, _| None;

    let mut offset: u64 = 12; // skip 12-byte pack header
    for _ in 0..num_objects {
        let entry = pack_file
            .entry(offset)
            .with_context(|| format!("bad pack entry header at offset {offset}"))?;
        let data_offset = entry.data_offset;
        let mut out = Vec::new();
        let outcome = pack_file
            .decode_entry(entry, &mut out, &mut inflate, resolve, &mut cache)
            .with_context(|| format!("failed to decode pack entry at offset {offset}"))?;

        let hash = gix_object::compute_hash(gix_hash::Kind::Sha1, outcome.kind, &out)
            .context("failed to compute object SHA-1")?;
        let mut sha1 = [0u8; 20];
        sha1.copy_from_slice(hash.as_slice());

        let type_num = match outcome.kind {
            ObjectKind::Commit => 1u8,
            ObjectKind::Tree => 2u8,
            ObjectKind::Blob => 3u8,
            ObjectKind::Tag => 4u8,
        };

        objects.push(PackObject {
            type_num,
            sha1,
            data: out,
        });

        offset = data_offset + outcome.compressed_size as u64;
    }

    Ok(objects)
}

// ---------------------------------------------------------------------------
// Typed pack inflation with per-type hashing
// ---------------------------------------------------------------------------

/// Inflate a pack and return results partitioned by object type.
///
/// Returns an [`InflateResult`] containing:
///   - `blobs`:   with sha1_git, sha1, sha256, blake2s256, and raw data
///   - `trees`:   with sha1_git, raw data, and parsed entries
///   - `commits`: with sha1_git and raw data
///   - `tags`:    with sha1_git and raw data
///
/// For blobs, all four hash algorithms are computed in Rust.
/// For trees, the binary format is parsed in Rust.
/// For commits/tags, raw bytes are returned for Python-side parsing.
pub fn inflate_pack_typed(pack_bytes: &[u8]) -> Result<InflateResult> {
    anyhow::ensure!(pack_bytes.len() >= 12, "pack too short for header");

    // 1. Write pack bytes to a temp file so gix can memory-map it for delta resolution.
    let mut tmp = tempfile::NamedTempFile::new().context("failed to create temp file for pack")?;
    tmp.write_all(pack_bytes)
        .context("failed to write pack bytes to temp file")?;
    tmp.flush().context("failed to flush pack temp file")?;

    // 2. Open as gix memory-mapped pack File.
    let pack_file = gix_pack::data::File::at(tmp.path(), gix_hash::Kind::Sha1)
        .context("failed to open pack file")?;

    // 3. Read object count from pack header.
    let num_objects = {
        let header: [u8; 12] = pack_bytes[..12]
            .try_into()
            .expect("length checked by the ensure! above");
        let (_version, n) =
            gix_pack::data::header::decode(&header).context("corrupt pack header")?;
        n
    };

    // 4. Single-pass decode with LRU cache for delta resolution.
    let mut result = InflateResult {
        blobs: Vec::new(),
        trees: Vec::new(),
        commits: Vec::new(),
        tags: Vec::new(),
    };
    let mut inflate = Inflate::default();
    let mut cache = gix_pack::cache::lru::StaticLinkedList::<64>::new(64 * 1024 * 1024);
    let resolve: &dyn Fn(&gix_hash::oid, &mut Vec<u8>) -> Option<ResolvedBase> = &|_, _| None;

    let mut offset: u64 = 12; // skip 12-byte pack header
    for _ in 0..num_objects {
        let entry = pack_file
            .entry(offset)
            .with_context(|| format!("bad pack entry header at offset {offset}"))?;
        let data_offset = entry.data_offset;
        let mut out = Vec::new();
        let outcome = pack_file
            .decode_entry(entry, &mut out, &mut inflate, resolve, &mut cache)
            .with_context(|| format!("failed to decode pack entry at offset {offset}"))?;

        // Compute sha1_git (git-style content-addressed hash with type+length header).
        let hash = gix_object::compute_hash(gix_hash::Kind::Sha1, outcome.kind, &out)
            .context("failed to compute object SHA-1")?;
        let mut sha1_git = [0u8; 20];
        sha1_git.copy_from_slice(hash.as_slice());

        match outcome.kind {
            ObjectKind::Blob => {
                let (sha1, sha256, blake2s256) = hash_blob(&out);
                result.blobs.push(BlobResult {
                    sha1_git,
                    sha1,
                    sha256,
                    blake2s256,
                    data: out,
                });
            }
            ObjectKind::Tree => {
                let entries = parse_tree(&out).with_context(|| {
                    format!(
                        "failed to parse tree {}",
                        sha1_git.iter().map(|b| format!("{b:02x}")).collect::<String>()
                    )
                })?;
                result.trees.push(TreeResult {
                    sha1_git,
                    raw_data: out,
                    entries,
                });
            }
            ObjectKind::Commit => {
                result.commits.push(RawResult {
                    sha1_git,
                    data: out,
                });
            }
            ObjectKind::Tag => {
                result.tags.push(RawResult {
                    sha1_git,
                    data: out,
                });
            }
        }

        offset = data_offset + outcome.compressed_size as u64;
    }

    Ok(result)
}
