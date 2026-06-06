// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Object model, hashing, and parsing: typed objects, tree parsing,
//! blob multi-hashing, and SWH-model round-trip hash checks.

use anyhow::{Context, Result};
use blake2::Blake2s256;
use digest::Digest;
use sha1_smol::Sha1 as Sha1Hash;
use sha2::Sha256;

/// Check whether the SWH model's re-serialization of parsed tree entries
/// produces bytes identical to the original raw git tree data.
///
/// If true, Python can skip `Directory.compute_hash()` entirely because
/// the round-trip is exact and the hash will match sha1_git.
pub(crate) fn tree_hash_will_match(raw_data: &[u8], entries: &[TreeEntry]) -> bool {
    // Apply the same transforms as the Python converter:
    //  1. Replace '/' with '_' in names
    //  2. Determine entry type from mode (dir if mode & 0o040000)
    //  3. Sort by (name + "/" for dirs, name for others) — bytewise
    //  4. Serialize as: <mode_octal><space><name><NUL><20-byte sha>
    //  5. Compare with raw_data

    struct Transformed {
        mode: u32,
        name: Vec<u8>,
        is_dir: bool,
        sha1: [u8; 20],
    }

    let mut entries: Vec<Transformed> = entries
        .iter()
        .map(|e| {
            let is_dir = (e.mode & 0o040000 == 0o040000) && (e.mode & 0o160000 != 0o160000);
            Transformed {
                mode: e.mode,
                name: e.name.iter().map(|&b| if b == b'/' { b'_' } else { b }).collect(),
                is_dir,
                sha1: e.sha1,
            }
        })
        .collect();

    entries.sort_by(|a, b| {
        let mut ka = a.name.clone();
        if a.is_dir {
            ka.push(b'/');
        }
        let mut kb = b.name.clone();
        if b.is_dir {
            kb.push(b'/');
        }
        ka.cmp(&kb)
    });

    let mut buf = Vec::with_capacity(raw_data.len());
    for e in &entries {
        let mode_str = format!("{:o}", e.mode);
        buf.extend_from_slice(mode_str.as_bytes());
        buf.push(b' ');
        buf.extend_from_slice(&e.name);
        buf.push(0);
        buf.extend_from_slice(&e.sha1);
    }

    buf == raw_data
}

/// Check whether the SWH model's re-serialization of a parsed commit
/// produces bytes identical to the original raw git commit data.
///
/// The SWH model parses headers (joining continuation lines) then
/// re-serializes with `escape_newlines` (replacing `\n` with `\n `).
/// If this round-trip is exact, `Revision.compute_hash()` will return
/// sha1_git and Python can skip the verification.
pub(crate) fn commit_hash_will_match(raw_data: &[u8]) -> bool {
    // Parse headers and body, then re-serialize using the SWH format.
    // The SWH format (format_git_object_from_headers):
    //   for each (key, value): key + " " + escape_newlines(value) + "\n"
    //   if body: "\n" + body

    let mut headers: Vec<(&[u8], Vec<u8>)> = Vec::new();
    let mut pos = 0;
    let data = raw_data;

    // Parse headers
    loop {
        if pos >= data.len() {
            break; // EOF without body
        }
        // Check for blank line (end of headers)
        if data[pos] == b'\n' {
            pos += 1; // skip the blank line
            break;
        }
        // Continuation line?
        if data[pos] == b' ' {
            if let Some(last) = headers.last_mut() {
                last.1.push(b'\n');
                // Find end of this line
                let eol = data[pos + 1..]
                    .iter()
                    .position(|&b| b == b'\n')
                    .map(|p| pos + 1 + p)
                    .unwrap_or(data.len());
                last.1.extend_from_slice(&data[pos + 1..eol]);
                pos = if eol < data.len() { eol + 1 } else { eol };
                continue;
            }
        }
        // Normal header line: "key value\n"
        let eol = data[pos..]
            .iter()
            .position(|&b| b == b'\n')
            .map(|p| pos + p)
            .unwrap_or(data.len());
        let line = &data[pos..eol];
        if let Some(space) = line.iter().position(|&b| b == b' ') {
            let key = &line[..space];
            let value = &line[space + 1..];
            headers.push((key, value.to_vec()));
        } else {
            return false; // malformed header
        }
        pos = if eol < data.len() { eol + 1 } else { eol };
    }

    let body = if pos < data.len() {
        Some(&data[pos..])
    } else {
        None
    };

    // Re-serialize using SWH format
    let mut buf = Vec::with_capacity(raw_data.len());
    for (key, value) in &headers {
        buf.extend_from_slice(key);
        buf.push(b' ');
        // escape_newlines: replace \n with \n<space>
        for &b in value.iter() {
            buf.push(b);
            if b == b'\n' {
                buf.push(b' ');
            }
        }
        buf.push(b'\n');
    }
    if let Some(body) = body {
        buf.push(b'\n');
        buf.extend_from_slice(body);
    }

    buf == raw_data
}

/// Object yielded by [`PackIterator`].
pub enum TypedObject {
    Blob {
        sha1_git: [u8; 20],
        sha1: [u8; 20],
        sha256: [u8; 32],
        blake2s256: [u8; 32],
        data: Vec<u8>,
    },
    Tree {
        sha1_git: [u8; 20],
        raw_data: Vec<u8>,
        entries: Vec<TreeEntry>,
        /// True if the SWH model's re-serialization will produce the same
        /// bytes as raw_data, meaning `Directory.compute_hash()` would
        /// return sha1_git.  When true, Python can skip re-verification.
        hash_match: bool,
    },
    Commit {
        sha1_git: [u8; 20],
        data: Vec<u8>,
        hash_match: bool,
    },
    Tag {
        sha1_git: [u8; 20],
        data: Vec<u8>,
        hash_match: bool,
    },
}

/// A fully-inflated git object parsed from a pack file.
pub struct PackObject {
    /// Dulwich-compatible type number: Commit=1, Tree=2, Blob=3, Tag=4.
    pub type_num: u8,
    /// 20-byte SHA-1 of the object.
    pub sha1: [u8; 20],
    /// Raw (uncompressed) object data.
    pub data: Vec<u8>,
}

/// A parsed tree entry (mode + name + 20-byte SHA-1 target).
pub struct TreeEntry {
    pub mode: u32,
    pub name: Vec<u8>,
    pub sha1: [u8; 20],
}

/// A blob with all four hash algorithms computed in Rust.
pub struct BlobResult {
    pub sha1_git: [u8; 20],
    pub sha1: [u8; 20],
    pub sha256: [u8; 32],
    pub blake2s256: [u8; 32],
    pub data: Vec<u8>,
}

/// A tree with its sha1_git, raw data (for raw_manifest fallback), and parsed entries.
pub struct TreeResult {
    pub sha1_git: [u8; 20],
    pub raw_data: Vec<u8>,
    pub entries: Vec<TreeEntry>,
}

/// A commit or tag with its sha1_git and raw data.
pub struct RawResult {
    pub sha1_git: [u8; 20],
    pub data: Vec<u8>,
}

/// Result of [`inflate_pack_typed`]: objects partitioned by type.
pub struct InflateResult {
    pub blobs: Vec<BlobResult>,
    pub trees: Vec<TreeResult>,
    pub commits: Vec<RawResult>,
    pub tags: Vec<RawResult>,
}

/// Compute sha1, sha256, and blake2s256 of raw content bytes.
pub(crate) fn hash_blob(data: &[u8]) -> ([u8; 20], [u8; 32], [u8; 32]) {
    let sha1 = Sha1Hash::digest(data);
    let sha256 = Sha256::digest(data);
    let blake2s = Blake2s256::digest(data);

    let mut s1 = [0u8; 20];
    s1.copy_from_slice(&sha1);
    let mut s256 = [0u8; 32];
    s256.copy_from_slice(&sha256);
    let mut b2s = [0u8; 32];
    b2s.copy_from_slice(&blake2s);

    (s1, s256, b2s)
}

/// Parse the binary git tree format into a list of [`TreeEntry`].
pub(crate) fn parse_tree(data: &[u8]) -> Result<Vec<TreeEntry>> {
    let mut entries = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        // Find space after mode
        let space = data[pos..]
            .iter()
            .position(|&b| b == b' ')
            .ok_or_else(|| anyhow::anyhow!("missing space in tree entry"))?;
        let mode = u32::from_str_radix(
            std::str::from_utf8(&data[pos..pos + space]).context("invalid mode bytes")?,
            8,
        )
        .context("mode is not a valid octal number")?;
        pos += space + 1;

        // Find NUL after name
        let nul = data[pos..]
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| anyhow::anyhow!("missing NUL in tree entry"))?;
        let name = data[pos..pos + nul].to_vec();
        pos += nul + 1;

        // Read 20-byte SHA-1
        if pos + 20 > data.len() {
            anyhow::bail!("truncated tree entry");
        }
        let mut sha1 = [0u8; 20];
        sha1.copy_from_slice(&data[pos..pos + 20]);
        pos += 20;

        entries.push(TreeEntry { mode, name, sha1 });
    }
    Ok(entries)
}
