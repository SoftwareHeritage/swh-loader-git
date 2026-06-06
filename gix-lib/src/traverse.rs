// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Multi-threaded pack traversal: parallel inflaters built on gitoxide's
//! delta-tree traversal.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use gix_object::Kind as ObjectKind;

use crate::objects::{
    commit_hash_will_match, hash_blob, parse_tree, tree_hash_will_match, TypedObject,
};

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
            // catch_unwind: a panic in the traversal must reach the consumer
            // as an Err.  Without it the sender is silently dropped, recv()
            // reports end-of-channel, and the consumer would mistake a
            // half-processed pack for a complete one.
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_indexed_traverse(&pack_path_owned, &idx_path_owned, sender.clone())
            }));
            match outcome {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    let _ = sender.send(Err(e));
                }
                Err(panic) => {
                    let _ = sender.send(Err(anyhow::anyhow!(
                        "pack traversal thread panicked: {}",
                        panic_message(&panic)
                    )));
                }
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
        // A poisoned lock only means a previous caller panicked mid-recv();
        // the mpsc receiver itself stays consistent, so recover the guard
        // instead of propagating the panic into the (Python) caller.
        let rx = self
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match rx.recv() {
            Ok(result) => result.map(Some),
            Err(_) => Ok(None), // channel closed = done
        }
    }
}

/// Render a panic payload from `std::panic::catch_unwind` as a message string.
fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
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
            // Same catch_unwind rationale as ParallelInflater::open: a panic
            // must surface as an Err, not as a silently-truncated stream.
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_direct_tree_traverse(&pack_path_owned, sender.clone())
            }));
            match outcome {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    let _ = sender.send(Err(e));
                }
                Err(panic) => {
                    let _ = sender.send(Err(anyhow::anyhow!(
                        "direct-tree traversal thread panicked: {}",
                        panic_message(&panic)
                    )));
                }
            }
        });

        Ok(DirectTreeInflater {
            receiver: std::sync::Mutex::new(receiver),
            _handle: Some(handle),
        })
    }

    /// Receive the next inflated object, or `None` when all objects have been processed.
    pub fn next_object(&self) -> Result<Option<TypedObject>> {
        // See ParallelInflater::next_object for the poisoned-lock rationale.
        let rx = self
            .receiver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
            let kind = ctx.entry.header.as_kind().ok_or_else(|| {
                TraverseProcessorError(
                    "delta header survived tree resolution (corrupt pack or gix-pack bug)"
                        .to_string(),
                )
            })?;

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
