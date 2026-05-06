// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Gitoxide-backed git fetch and pack parsing for swh-loader-git.
//!
//! This crate is the pure-Rust library layer. It has no PyO3 dependency.
//! The Python binding lives in the sibling `gix-py` crate.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use blake2::Blake2s256;
use bstr::BString;
use digest::Digest;
use gix_features::zlib::Inflate;
use gix_hash::ObjectId;
use gix_object::Kind as ObjectKind;
use gix_pack::data::decode::entry::ResolvedBase;
use gix_protocol::handshake::Ref;
use gix_transport::client::blocking_io::connect::{connect, Options as ConnectOptions};
use gix_transport::client::blocking_io::{ExtendedBufRead, Transport};
use gix_transport::packetline::read::ProgressAction;
use gix_transport::{Protocol, Service};
use sha1_smol::Sha1 as Sha1Hash;
use sha2::Sha256;

/// Result of a successful `fetch_pack` call.
pub struct FetchPackResult {
    /// Maps ref name (e.g. `refs/heads/main`) → SHA-1 hex string (40 chars).
    pub remote_refs: HashMap<BString, String>,
    /// Maps symbolic ref name → target ref name (e.g. `HEAD` → `refs/heads/main`).
    pub symbolic_refs: HashMap<BString, BString>,
    /// Raw pack data bytes (empty if nothing new to fetch).
    pub pack_bytes: Vec<u8>,
}

/// Fetch a pack from a remote git repository over HTTP/HTTPS.
///
/// * `url`        — remote URL (http:// or https://)
/// * `wants`      — list of 20-byte SHA-1 object IDs to request
/// * `haves`      — list of 20-byte SHA-1 object IDs we already have (for delta compression)
/// * `size_limit` — maximum pack size in bytes (0 = unlimited); returns error if exceeded
pub fn fetch_pack(
    url: &str,
    wants: Vec<[u8; 20]>,
    haves: Vec<[u8; 20]>,
    size_limit: u64,
) -> Result<FetchPackResult> {
    // 1. Parse URL
    let url_parsed =
        gix_url::parse(url.into()).with_context(|| format!("invalid git URL: {url}"))?;

    // 2. Connect (blocking HTTP transport via curl).
    let options = ConnectOptions {
        version: Protocol::V1,
        ..Default::default()
    };
    let mut transport =
        connect(url_parsed, options).with_context(|| format!("failed to connect to {url}"))?;

    // 3. Handshake, parse refs, and release the transport borrow — all inside a block
    //    so that `outcome` (which borrows `transport`) is dropped before we reuse it
    //    for the actual fetch.
    let (actual_protocol, capabilities, parsed_refs) = {
        let mut outcome = transport
            .handshake(Service::UploadPack, &[])
            .context("git handshake failed")?;
        let actual_protocol = outcome.actual_protocol;

        // 4. Parse refs from the raw packet-line reader.
        let (parsed_refs, _shallow_updates) = match outcome.refs.take() {
            Some(mut reader) => {
                gix_protocol::handshake::refs::from_v1_refs_received_as_part_of_handshake_and_capabilities(
                    &mut *reader,
                    outcome.capabilities.iter(),
                )
                .context("failed to parse remote refs")?
            }
            None => (vec![], vec![]),
        };

        // Capabilities are valid to use now (refs reader consumed above).
        // `outcome` (and its borrow of `transport`) is dropped at end of block.
        let capabilities = outcome.capabilities;
        (actual_protocol, capabilities, parsed_refs)
    }; // outcome dropped here — transport borrow released

    // 5. Build remote_refs / symbolic_refs maps from parsed_refs.
    let mut remote_refs: HashMap<BString, String> = HashMap::new();
    let mut symbolic_refs: HashMap<BString, BString> = HashMap::new();

    for r in &parsed_refs {
        match r {
            Ref::Direct {
                full_ref_name,
                object,
            } => {
                remote_refs.insert(full_ref_name.clone(), object.to_hex().to_string());
            }
            Ref::Symbolic {
                full_ref_name,
                target,
                object,
                ..
            } => {
                symbolic_refs.insert(full_ref_name.clone(), target.clone());
                // Also record the resolved OID.
                remote_refs.insert(full_ref_name.clone(), object.to_hex().to_string());
            }
            Ref::Peeled {
                full_ref_name,
                object,
                ..
            } => {
                // Peeled tag target — store under `<name>^{}` (standard git convention).
                let mut peeled_name = full_ref_name.clone();
                peeled_name.extend_from_slice(b"^{}");
                remote_refs.insert(peeled_name, object.to_hex().to_string());
            }
            Ref::Unborn { .. } => {
                // Empty repo — no objects to record.
            }
        }
    }

    // Early exit: nothing to fetch.
    if wants.is_empty() {
        return Ok(FetchPackResult {
            remote_refs,
            symbolic_refs,
            pack_bytes: vec![],
        });
    }

    // 6. Build fetch arguments.
    let features =
        gix_protocol::Command::Fetch.default_features(actual_protocol, &capabilities);
    let mut args = gix_protocol::fetch::Arguments::new(actual_protocol, features, false);

    for sha in &wants {
        args.want(ObjectId::from_bytes_or_panic(sha));
    }
    for sha in &haves {
        args.have(ObjectId::from_bytes_or_panic(sha));
    }

    // 7. Send DONE immediately (no multi-round negotiation — loader handles this).
    let mut reader = args
        .send(&mut transport, true)
        .context("failed to send fetch arguments")?;

    // 8. Parse ACK/NAK and check whether the server is sending a pack.
    let response =
        gix_protocol::fetch::Response::from_line_reader(actual_protocol, &mut reader, true, false)
            .context("failed to parse fetch response")?;

    // 9. Read pack data.
    //    Enable side-band demultiplexing so the Read impl strips band indicators
    //    and only returns band-1 (pack data), forwarding band-2/3 (progress/error).
    reader.set_progress_handler(Some(Box::new(
        |_is_error: bool, _text: &[u8]| ProgressAction::Continue(()),
    )));

    let mut pack_bytes: Vec<u8> = Vec::new();
    if response.has_pack() {
        if size_limit > 0 {
            // Read up to size_limit bytes then probe for overflow.
            let mut limited = (&mut reader).take(size_limit);
            limited
                .read_to_end(&mut pack_bytes)
                .context("failed to read pack data")?;
            if pack_bytes.len() as u64 == size_limit {
                let mut probe = [0u8; 1];
                let n = reader.read(&mut probe).unwrap_or(0);
                if n > 0 {
                    anyhow::bail!("pack size exceeds limit of {size_limit} bytes");
                }
            }
        } else {
            reader
                .read_to_end(&mut pack_bytes)
                .context("failed to read pack data")?;
        }
    }

    Ok(FetchPackResult {
        remote_refs,
        symbolic_refs,
        pack_bytes,
    })
}

/// Result of a `fetch_pack_to_file` call (pack written to disk, not returned).
pub struct FetchPackFileResult {
    pub remote_refs: HashMap<BString, String>,
    pub symbolic_refs: HashMap<BString, BString>,
    pub pack_size: u64,
}

/// Like [`fetch_pack`] but writes the pack data to `pack_path` on disk
/// instead of returning it in memory.  For large repositories this avoids
/// holding the entire pack in a `Vec<u8>`.
pub fn fetch_pack_to_file(
    url: &str,
    wants: Vec<[u8; 20]>,
    haves: Vec<[u8; 20]>,
    size_limit: u64,
    pack_path: &Path,
) -> Result<FetchPackFileResult> {
    let url_parsed =
        gix_url::parse(url.into()).with_context(|| format!("invalid git URL: {url}"))?;
    let options = ConnectOptions {
        version: Protocol::V1,
        ..Default::default()
    };
    let mut transport =
        connect(url_parsed, options).with_context(|| format!("failed to connect to {url}"))?;

    let (actual_protocol, capabilities, parsed_refs) = {
        let mut outcome = transport
            .handshake(Service::UploadPack, &[])
            .context("git handshake failed")?;
        let actual_protocol = outcome.actual_protocol;
        let (parsed_refs, _) = match outcome.refs.take() {
            Some(mut reader) => {
                gix_protocol::handshake::refs::from_v1_refs_received_as_part_of_handshake_and_capabilities(
                    &mut *reader,
                    outcome.capabilities.iter(),
                )
                .context("failed to parse remote refs")?
            }
            None => (vec![], vec![]),
        };
        let capabilities = outcome.capabilities;
        (actual_protocol, capabilities, parsed_refs)
    };

    let mut remote_refs: HashMap<BString, String> = HashMap::new();
    let mut symbolic_refs: HashMap<BString, BString> = HashMap::new();
    for r in &parsed_refs {
        match r {
            Ref::Direct {
                full_ref_name,
                object,
            } => {
                remote_refs.insert(full_ref_name.clone(), object.to_hex().to_string());
            }
            Ref::Symbolic {
                full_ref_name,
                target,
                object,
                ..
            } => {
                symbolic_refs.insert(full_ref_name.clone(), target.clone());
                remote_refs.insert(full_ref_name.clone(), object.to_hex().to_string());
            }
            Ref::Peeled {
                full_ref_name,
                object,
                ..
            } => {
                let mut peeled_name = full_ref_name.clone();
                peeled_name.extend_from_slice(b"^{}");
                remote_refs.insert(peeled_name, object.to_hex().to_string());
            }
            Ref::Unborn { .. } => {}
        }
    }

    if wants.is_empty() {
        std::fs::write(pack_path, b"").context("failed to create empty pack file")?;
        return Ok(FetchPackFileResult {
            remote_refs,
            symbolic_refs,
            pack_size: 0,
        });
    }

    let features =
        gix_protocol::Command::Fetch.default_features(actual_protocol, &capabilities);
    let mut args = gix_protocol::fetch::Arguments::new(actual_protocol, features, false);
    for sha in &wants {
        args.want(ObjectId::from_bytes_or_panic(sha));
    }
    for sha in &haves {
        args.have(ObjectId::from_bytes_or_panic(sha));
    }

    let mut reader = args
        .send(&mut transport, true)
        .context("failed to send fetch arguments")?;
    let response =
        gix_protocol::fetch::Response::from_line_reader(actual_protocol, &mut reader, true, false)
            .context("failed to parse fetch response")?;

    reader.set_progress_handler(Some(Box::new(
        |_is_error: bool, _text: &[u8]| ProgressAction::Continue(()),
    )));

    let mut pack_size: u64 = 0;
    if response.has_pack() {
        let mut file = std::fs::File::create(pack_path)
            .with_context(|| format!("failed to create pack file {}", pack_path.display()))?;
        if size_limit > 0 {
            let mut limited = (&mut reader).take(size_limit);
            pack_size = std::io::copy(&mut limited, &mut file)
                .context("failed to write pack data to file")?;
            if pack_size == size_limit {
                let mut probe = [0u8; 1];
                if reader.read(&mut probe).unwrap_or(0) > 0 {
                    drop(file);
                    let _ = std::fs::remove_file(pack_path);
                    anyhow::bail!("pack size exceeds limit of {size_limit} bytes");
                }
            }
        } else {
            pack_size = std::io::copy(&mut reader, &mut file)
                .context("failed to write pack data to file")?;
        }
        file.flush().context("failed to flush pack file")?;
    }

    Ok(FetchPackFileResult {
        remote_refs,
        symbolic_refs,
        pack_size,
    })
}

// ---------------------------------------------------------------------------
// Streaming pack iterator
// ---------------------------------------------------------------------------

/// Check whether the SWH model's re-serialization of parsed tree entries
/// produces bytes identical to the original raw git tree data.
///
/// If true, Python can skip `Directory.compute_hash()` entirely because
/// the round-trip is exact and the hash will match sha1_git.
fn tree_hash_will_match(raw_data: &[u8], entries: &[TreeEntry]) -> bool {
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
fn commit_hash_will_match(raw_data: &[u8]) -> bool {
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

// ---------------------------------------------------------------------------
// Parallel pack inflation via git index-pack + traverse_with_index
// ---------------------------------------------------------------------------

/// Accumulated timing statistics from the parallel traversal.
struct TraverseStats {
    /// Nanoseconds spent computing sha1_git.
    hash_ns: AtomicU64,
    /// Nanoseconds spent computing blob multi-hash (sha1+sha256+blake2s256).
    blob_hash_ns: AtomicU64,
    /// Nanoseconds spent parsing trees + tree_hash_will_match.
    tree_parse_ns: AtomicU64,
    /// Nanoseconds spent on commit_hash_will_match.
    commit_ns: AtomicU64,
    /// Nanoseconds spent on channel send (backpressure wait).
    send_ns: AtomicU64,
    /// Total objects processed.
    count: AtomicU64,
}

impl TraverseStats {
    fn new() -> Self {
        TraverseStats {
            hash_ns: AtomicU64::new(0),
            blob_hash_ns: AtomicU64::new(0),
            tree_parse_ns: AtomicU64::new(0),
            commit_ns: AtomicU64::new(0),
            send_ns: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
}

/// Parallel pack inflater using `git index-pack` for delta resolution
/// and `gix_pack::index::File::traverse_with_index()` for true parallel
/// decompression and object processing.
///
/// Finished `TypedObject`s are sent through a bounded channel so the
/// Python side can consume them one at a time.
pub struct ParallelInflater {
    receiver: std::sync::Mutex<std::sync::mpsc::Receiver<Result<TypedObject>>>,
    _handle: Option<std::thread::JoinHandle<()>>,
}

impl ParallelInflater {
    /// Open a pack file for parallel inflation.
    ///
    /// This first runs `git index-pack` to build an `.idx` file (which
    /// resolves all delta chains), then spawns a background thread that
    /// calls `traverse_with_index` to decompress and process every object
    /// in parallel, sending results through a bounded channel.
    ///
    /// `channel_bound` controls the bounded channel capacity (back-pressure).
    pub fn open(pack_path: &Path, channel_bound: usize) -> Result<Self> {
        // Step 1: generate pack index via git index-pack (skip if .idx already exists).
        let idx_path = pack_path.with_extension("idx");
        if idx_path.exists() {
            eprintln!(
                "[gix-traverse stats] reusing existing index {}",
                idx_path.display()
            );
        } else {
            let index_pack_start = Instant::now();
            let status = std::process::Command::new("git")
                .arg("index-pack")
                .arg("-o")
                .arg(&idx_path)
                .arg(pack_path)
                .status()
                .context("failed to run git index-pack")?;
            eprintln!(
                "[gix-traverse stats] git_index_pack={:.1}s",
                index_pack_start.elapsed().as_secs_f64()
            );
            if !status.success() {
                anyhow::bail!("git index-pack failed with status {status}");
            }
        }

        let (sender, receiver) = std::sync::mpsc::sync_channel(channel_bound);

        let pack_path_owned = pack_path.to_owned();
        let idx_path_owned = idx_path;
        let handle = std::thread::spawn(move || {
            if let Err(e) =
                run_indexed_traverse(&pack_path_owned, &idx_path_owned, sender.clone())
            {
                let _ = sender.send(Err(e));
            }
            // Clean up the .idx file we generated.
            let _ = std::fs::remove_file(&idx_path_owned);
        });

        Ok(ParallelInflater {
            receiver: std::sync::Mutex::new(receiver),
            _handle: Some(handle),
        })
    }

    /// Receive the next inflated object, or `None` when all objects have been processed.
    pub fn next_object(&self) -> Result<Option<TypedObject>> {
        let rx = self.receiver.lock().expect("receiver mutex poisoned");
        match rx.recv() {
            Ok(result) => result.map(Some),
            Err(_) => Ok(None), // channel closed = done
        }
    }
}

/// Error type used inside the `traverse_with_index` processor callback.
///
/// We need a concrete `std::error::Error` type (not `Box<dyn Error>`)
/// because `gix_pack::index::traverse::Error<E>` requires `E: Error`.
#[derive(Debug)]
struct TraverseProcessorError(String);

impl std::fmt::Display for TraverseProcessorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TraverseProcessorError {}

/// Use `traverse_with_index` to decompress every object in the pack in
/// parallel (gitoxide handles the thread pool internally via its delta
/// tree traversal), compute hashes, and send `TypedObject`s through
/// the channel.
fn run_indexed_traverse(
    pack_path: &Path,
    idx_path: &Path,
    output_sender: std::sync::mpsc::SyncSender<Result<TypedObject>>,
) -> Result<()> {
    let index = gix_pack::index::File::at(idx_path, gix_hash::Kind::Sha1)
        .context("failed to open pack index")?;
    let pack = gix_pack::data::File::at(pack_path, gix_hash::Kind::Sha1)
        .context("failed to open pack data")?;

    let should_interrupt = std::sync::atomic::AtomicBool::new(false);

    let sender = output_sender;

    let stats = Arc::new(TraverseStats::new());

    let processor = {
        let sender = sender.clone();
        let stats = Arc::clone(&stats);
        move |kind: ObjectKind,
              decompressed: &[u8],
              _entry: &gix_pack::index::Entry,
              _progress: &dyn gix_features::progress::Progress|
              -> std::result::Result<(), TraverseProcessorError> {
            // Time sha1_git computation
            let t0 = Instant::now();
            let hash = gix_object::compute_hash(gix_hash::Kind::Sha1, kind, decompressed)
                .map_err(|e| TraverseProcessorError(format!("failed to compute SHA-1: {e}")))?;
            let mut sha1_git = [0u8; 20];
            sha1_git.copy_from_slice(hash.as_slice());
            stats.hash_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

            let typed_obj = match kind {
                ObjectKind::Blob => {
                    let t1 = Instant::now();
                    let (sha1, sha256, blake2s256) = hash_blob(decompressed);
                    stats.blob_hash_ns.fetch_add(t1.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    TypedObject::Blob {
                        sha1_git,
                        sha1,
                        sha256,
                        blake2s256,
                        data: decompressed.to_vec(),
                    }
                }
                ObjectKind::Tree => {
                    let t1 = Instant::now();
                    let entries = parse_tree(decompressed)
                        .map_err(|e| TraverseProcessorError(format!("failed to parse tree: {e:#}")))?;
                    let hash_match = tree_hash_will_match(decompressed, &entries);
                    stats.tree_parse_ns.fetch_add(t1.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    TypedObject::Tree {
                        sha1_git,
                        raw_data: decompressed.to_vec(),
                        entries,
                        hash_match,
                    }
                }
                ObjectKind::Commit => {
                    let t1 = Instant::now();
                    let hash_match = commit_hash_will_match(decompressed);
                    stats.commit_ns.fetch_add(t1.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    TypedObject::Commit {
                        sha1_git,
                        data: decompressed.to_vec(),
                        hash_match,
                    }
                }
                ObjectKind::Tag => {
                    let t1 = Instant::now();
                    let hash_match = commit_hash_will_match(decompressed);
                    stats.commit_ns.fetch_add(t1.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    TypedObject::Tag {
                        sha1_git,
                        data: decompressed.to_vec(),
                        hash_match,
                    }
                }
            };

            let t_send = Instant::now();
            sender
                .send(Ok(typed_obj))
                .map_err(|_| TraverseProcessorError("output channel closed".into()))?;
            stats.send_ns.fetch_add(t_send.elapsed().as_nanos() as u64, Ordering::Relaxed);

            stats.count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    };

    let mut progress = gix_features::progress::Discard;

    let traverse_start = Instant::now();
    index
        .traverse_with_index(
            &pack,
            processor,
            &mut progress,
            &should_interrupt,
            gix_pack::index::traverse::with_index::Options {
                thread_limit: None, // use all available cores
                check: gix_pack::index::traverse::SafetyCheck::SkipFileAndObjectChecksumVerification,
            },
        )
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let traverse_elapsed = traverse_start.elapsed();

    eprintln!(
        "[gix-traverse stats] objects={} total_traverse={:.1}s sha1_git={:.1}s blob_hash={:.1}s tree_parse={:.1}s commit={:.1}s send_wait={:.1}s",
        stats.count.load(Ordering::Relaxed),
        traverse_elapsed.as_secs_f64(),
        stats.hash_ns.load(Ordering::Relaxed) as f64 / 1e9,
        stats.blob_hash_ns.load(Ordering::Relaxed) as f64 / 1e9,
        stats.tree_parse_ns.load(Ordering::Relaxed) as f64 / 1e9,
        stats.commit_ns.load(Ordering::Relaxed) as f64 / 1e9,
        stats.send_ns.load(Ordering::Relaxed) as f64 / 1e9,
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Direct delta-tree traversal (no git index-pack, no .idx file)
// ---------------------------------------------------------------------------

/// Parallel pack inflater that builds the delta dependency tree directly
/// from a streaming header scan, then traverses in parallel.
///
/// Compared to [`ParallelInflater`], this eliminates:
/// - The `git index-pack` subprocess (~5 min for Linux kernel)
/// - The `.idx` file generation and cleanup
/// - The second full pack read to rebuild the tree from the index
///
/// The trade-off: a sequential inflate-to-null pass to discover entry
/// boundaries (~30-60s for 7.9 GB), then `Tree::traverse()` decompresses
/// in parallel as before.
pub struct DirectTreeInflater {
    receiver: std::sync::Mutex<std::sync::mpsc::Receiver<Result<TypedObject>>>,
    _handle: Option<std::thread::JoinHandle<()>>,
}

impl DirectTreeInflater {
    /// Open a pack file for parallel inflation without `git index-pack`.
    ///
    /// Scans pack entry headers to build a delta tree, then traverses
    /// in parallel.  Only supports packs with OFS_DELTA references
    /// (standard for non-thin packs from `git upload-pack`).  If a
    /// REF_DELTA is encountered, returns an error.
    ///
    /// `channel_bound` controls the bounded channel capacity (back-pressure).
    pub fn open(pack_path: &Path, channel_bound: usize) -> Result<Self> {
        let (sender, receiver) = std::sync::mpsc::sync_channel(channel_bound);

        let pack_path_owned = pack_path.to_owned();
        let handle = std::thread::spawn(move || {
            if let Err(e) = run_direct_tree_traverse(&pack_path_owned, sender.clone()) {
                let _ = sender.send(Err(e));
            }
        });

        Ok(DirectTreeInflater {
            receiver: std::sync::Mutex::new(receiver),
            _handle: Some(handle),
        })
    }

    /// Receive the next inflated object, or `None` when all objects have been processed.
    pub fn next_object(&self) -> Result<Option<TypedObject>> {
        let rx = self.receiver.lock().expect("receiver mutex poisoned");
        match rx.recv() {
            Ok(result) => result.map(Some),
            Err(_) => Ok(None), // channel closed = done
        }
    }
}

/// Build a delta tree from a streaming header scan and traverse it in parallel.
///
/// Phase 1 (sequential): Iterate pack entries with `BytesToEntriesIter` using
/// `Mode::AsIs` + `EntryDataMode::Ignore` — this inflates compressed data to
/// /dev/null just to find entry boundaries.  For each entry, we read the header
/// (type + base distance for deltas) and build the tree directly via
/// `Tree::add_root()` / `Tree::add_child()`.
///
/// Phase 2 (parallel): `tree.traverse()` decompresses every object using
/// work-stealing threads, calling our processor to hash, parse, and send
/// `TypedObject`s through the channel.
fn run_direct_tree_traverse(
    pack_path: &Path,
    output_sender: std::sync::mpsc::SyncSender<Result<TypedObject>>,
) -> Result<()> {
    // Open the memory-mapped pack (used by traverse for random-access reads).
    let pack = gix_pack::data::File::at(pack_path, gix_hash::Kind::Sha1)
        .context("failed to open pack data file")?;

    // Phase 1: streaming scan to build delta tree.
    let scan_start = Instant::now();
    let reader = std::io::BufReader::with_capacity(
        64 * 1024,
        std::fs::File::open(pack_path)
            .with_context(|| format!("failed to open pack file {}", pack_path.display()))?,
    );
    let iter = gix_pack::data::input::BytesToEntriesIter::new_from_header(
        reader,
        gix_pack::data::input::Mode::AsIs,
        gix_pack::data::input::EntryDataMode::Ignore,
        gix_hash::Kind::Sha1,
    )
    .context("failed to create pack entry iterator")?;

    let num_objects = iter.size_hint().1.unwrap_or(0);
    let mut tree = gix_pack::cache::delta::Tree::with_capacity(num_objects)
        .context("failed to allocate delta tree")?;

    for entry_result in iter {
        let entry = entry_result.map_err(|e| anyhow::anyhow!("pack scan: {e}"))?;
        let offset = entry.pack_offset;

        use gix_pack::data::entry::Header::*;
        match entry.header {
            Tree | Blob | Commit | Tag => {
                tree.add_root(offset, ())?;
            }
            OfsDelta { base_distance } => {
                let base_offset = offset
                    .checked_sub(base_distance)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "ofs-delta at offset {offset}: base_distance {base_distance} exceeds offset"
                        )
                    })?;
                tree.add_child(base_offset, offset, ())?;
            }
            RefDelta { base_id } => {
                anyhow::bail!(
                    "ref-delta at offset {offset} (base {base_id}): direct tree traversal \
                     requires ofs-delta only packs. Use ParallelInflater for thin packs."
                );
            }
        }
    }
    let scan_elapsed = scan_start.elapsed();
    eprintln!(
        "[direct-tree stats] scan={:.1}s objects={}",
        scan_elapsed.as_secs_f64(),
        num_objects,
    );

    // Phase 2: parallel traversal.
    let should_interrupt = AtomicBool::new(false);
    let stats = Arc::new(TraverseStats::new());
    let sender = output_sender;

    let processor = {
        let sender = sender.clone();
        let stats = Arc::clone(&stats);
        move |_data: &mut (),
              _progress: &dyn gix_features::progress::Progress,
              ctx: gix_pack::cache::delta::traverse::Context<'_>|
              -> std::result::Result<(), TraverseProcessorError> {
            let kind = ctx
                .entry
                .header
                .as_kind()
                .expect("non-delta object after tree resolution");

            // Compute sha1_git.
            let t0 = Instant::now();
            let hash = gix_object::compute_hash(gix_hash::Kind::Sha1, kind, ctx.decompressed)
                .map_err(|e| TraverseProcessorError(format!("failed to compute SHA-1: {e}")))?;
            let mut sha1_git = [0u8; 20];
            sha1_git.copy_from_slice(hash.as_slice());
            stats
                .hash_ns
                .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

            let typed_obj = match kind {
                ObjectKind::Blob => {
                    let t1 = Instant::now();
                    let (sha1, sha256, blake2s256) = hash_blob(ctx.decompressed);
                    stats
                        .blob_hash_ns
                        .fetch_add(t1.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    TypedObject::Blob {
                        sha1_git,
                        sha1,
                        sha256,
                        blake2s256,
                        data: ctx.decompressed.to_vec(),
                    }
                }
                ObjectKind::Tree => {
                    let t1 = Instant::now();
                    let entries = parse_tree(ctx.decompressed)
                        .map_err(|e| TraverseProcessorError(format!("failed to parse tree: {e:#}")))?;
                    let hash_match = tree_hash_will_match(ctx.decompressed, &entries);
                    stats
                        .tree_parse_ns
                        .fetch_add(t1.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    TypedObject::Tree {
                        sha1_git,
                        raw_data: ctx.decompressed.to_vec(),
                        entries,
                        hash_match,
                    }
                }
                ObjectKind::Commit => {
                    let t1 = Instant::now();
                    let hash_match = commit_hash_will_match(ctx.decompressed);
                    stats
                        .commit_ns
                        .fetch_add(t1.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    TypedObject::Commit {
                        sha1_git,
                        data: ctx.decompressed.to_vec(),
                        hash_match,
                    }
                }
                ObjectKind::Tag => {
                    let t1 = Instant::now();
                    let hash_match = commit_hash_will_match(ctx.decompressed);
                    stats
                        .commit_ns
                        .fetch_add(t1.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    TypedObject::Tag {
                        sha1_git,
                        data: ctx.decompressed.to_vec(),
                        hash_match,
                    }
                }
            };

            let t_send = Instant::now();
            sender
                .send(Ok(typed_obj))
                .map_err(|_| TraverseProcessorError("output channel closed".into()))?;
            stats
                .send_ns
                .fetch_add(t_send.elapsed().as_nanos() as u64, Ordering::Relaxed);
            stats.count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    };

    let traverse_start = Instant::now();
    tree.traverse(
        |slice, data: &gix_pack::data::File| data.entry_slice(slice),
        &pack,
        pack.pack_end() as u64,
        processor,
        gix_pack::cache::delta::traverse::Options {
            object_progress: Box::new(gix_features::progress::Discard),
            size_progress: &mut gix_features::progress::Discard,
            thread_limit: None, // use all available cores
            should_interrupt: &should_interrupt,
            object_hash: gix_hash::Kind::Sha1,
        },
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    let traverse_elapsed = traverse_start.elapsed();

    eprintln!(
        "[direct-tree stats] objects={} traverse={:.1}s sha1_git={:.1}s blob_hash={:.1}s tree_parse={:.1}s commit={:.1}s send_wait={:.1}s",
        stats.count.load(Ordering::Relaxed),
        traverse_elapsed.as_secs_f64(),
        stats.hash_ns.load(Ordering::Relaxed) as f64 / 1e9,
        stats.blob_hash_ns.load(Ordering::Relaxed) as f64 / 1e9,
        stats.tree_parse_ns.load(Ordering::Relaxed) as f64 / 1e9,
        stats.commit_ns.load(Ordering::Relaxed) as f64 / 1e9,
        stats.send_ns.load(Ordering::Relaxed) as f64 / 1e9,
    );

    Ok(())
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
        let header: [u8; 12] = pack_bytes[..12].try_into().unwrap();
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
fn hash_blob(data: &[u8]) -> ([u8; 20], [u8; 32], [u8; 32]) {
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
fn parse_tree(data: &[u8]) -> Result<Vec<TreeEntry>> {
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
        let header: [u8; 12] = pack_bytes[..12].try_into().unwrap();
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
