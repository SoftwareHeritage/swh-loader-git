# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Benchmark: isolate the channel/GIL bottleneck in the parallel pipeline.

Runs three experiments on DirectTreePackReader to separate:
  1. Pure Rust throughput (no Python processing, just iterate)
  2. Channel buffer size effect (4096 vs 65536 vs 262144)
  3. Blob data transfer cost (full pipeline vs metadata-only)

Usage:
    python bench_channel_bottleneck.py /path/to/pack.pack [exp1|exp2|exp3|all]

Experiment 1 — "iterate-only vs full-pipeline"
  Compares:
    (a) iterate-only: Python calls __next__() but only reads the type tag
    (b) full-pipeline: Python runs all converters (blob_to_content, tree_to_directory, etc.)
  If (a) >> (b), the Python converters are the bottleneck.
  If (a) ≈ (b), the bottleneck is in crossing the channel, not in Python processing.

Experiment 2 — "channel buffer size"
  Runs iterate-only with channel_bound = 4096, 65536, 262144.
  If throughput increases with buffer size, backpressure stalls are the issue.
  If throughput is flat, sustained consumer rate is the bottleneck (buffer doesn't help).

Experiment 3 — "what's in the channel"
  Requires the Rust side to support a `metadata_only=True` mode that sends
  hashes+size for blobs instead of full data.  If not available, this test
  is skipped — but we can still estimate the impact by comparing blob-heavy
  vs blob-light throughput windows from experiment 1 logs.
"""

import os
import sys
import time

import swh.loader.git._gix as _gix


def progress(label, t_start, count, last_report_time):
    """Print a 10s progress line. Returns updated last_report_time."""
    now = time.perf_counter()
    if now - last_report_time > 10:
        elapsed = now - t_start
        print(
            f"  [{label}] {count:,} objects in {elapsed:.0f}s ({count / elapsed:.0f} obj/s)"
        )
        return now
    return last_report_time


# ---------------------------------------------------------------------------
# Experiment 1: iterate-only vs full-pipeline
# ---------------------------------------------------------------------------


def exp1_iterate_only(pack_path, channel_bound=4096):
    """Pull every object from the channel but do minimal work in Python.

    This measures the maximum rate at which Python can drain the channel,
    including GIL acquisition on each __next__() call but excluding any
    model conversion work.
    """
    print(f"\n  [exp1a] iterate-only (channel_bound={channel_bound})")
    reader = _gix.DirectTreePackReader(pack_path, channel_bound=channel_bound)
    t_start = time.perf_counter()
    count = 0
    n_blob = n_tree = n_commit = n_tag = 0
    total_blob_bytes = 0
    last_report = t_start

    for obj_tuple in reader:
        t = obj_tuple[0]
        if t == 3:
            n_blob += 1
            total_blob_bytes += len(obj_tuple[5])
        elif t == 2:
            n_tree += 1
        elif t == 1:
            n_commit += 1
        elif t == 4:
            n_tag += 1
        count += 1
        last_report = progress("exp1a", t_start, count, last_report)

    elapsed = time.perf_counter() - t_start
    print(
        f"  [exp1a] DONE: {count:,} objects in {elapsed:.1f}s ({count / elapsed:.0f} obj/s)"
    )
    print(
        f"          blobs={n_blob:,} trees={n_tree:,} commits={n_commit:,} tags={n_tag:,}"
    )
    print(f"          blob data: {total_blob_bytes / 1024 / 1024 / 1024:.2f} GB")
    return elapsed, count


def exp1_full_pipeline(pack_path, channel_bound=4096):
    """Pull every object AND run the full Python conversion pipeline.

    Same as bench_full_pipeline.py's run_full_pipeline but with fixed
    channel_bound.
    """
    import swh.loader.git.converters as conv
    from swh.model.model import Directory

    print(f"\n  [exp1b] full-pipeline (channel_bound={channel_bound})")
    reader = _gix.DirectTreePackReader(pack_path, channel_bound=channel_bound)
    t_start = time.perf_counter()
    count = 0
    t_python = 0.0
    last_report = t_start

    for obj_tuple in reader:
        t = obj_tuple[0]
        t2 = time.perf_counter()
        if t == 3:  # blob
            conv.blob_to_content_precomputed(
                obj_tuple[1], obj_tuple[2], obj_tuple[3], obj_tuple[4], obj_tuple[5]
            )
        elif t == 2:  # tree
            s = obj_tuple[1]
            if isinstance(s, Directory):
                pass  # already built
            else:
                conv.tree_to_directory_preparsed(
                    s, obj_tuple[2], obj_tuple[3], obj_tuple[4]
                )
        elif t == 1:  # commit
            conv.commit_to_revision(obj_tuple[1], obj_tuple[2], obj_tuple[3])
        elif t == 4:  # tag
            conv.tag_to_release(obj_tuple[1], obj_tuple[2], obj_tuple[3])
        t_python += time.perf_counter() - t2
        count += 1
        last_report = progress("exp1b", t_start, count, last_report)

    elapsed = time.perf_counter() - t_start
    print(
        f"  [exp1b] DONE: {count:,} objects in {elapsed:.1f}s ({count / elapsed:.0f} obj/s)"
    )
    print(
        f"          Python conversion time: {t_python:.1f}s ({t_python / elapsed * 100:.1f}%)"
    )
    return elapsed, count


# ---------------------------------------------------------------------------
# Experiment 2: channel buffer size sweep
# ---------------------------------------------------------------------------


def exp2_buffer_sweep(pack_path):
    """Run iterate-only at different channel_bound values."""
    bounds = [1024, 4096, 65536, 262144]
    results = {}

    for bound in bounds:
        print(f"\n  [exp2] channel_bound={bound}")
        reader = _gix.DirectTreePackReader(pack_path, channel_bound=bound)
        t_start = time.perf_counter()
        count = 0
        last_report = t_start

        for obj_tuple in reader:
            count += 1
            last_report = progress(f"exp2-{bound}", t_start, count, last_report)

        elapsed = time.perf_counter() - t_start
        rate = count / elapsed
        results[bound] = (elapsed, rate)
        print(
            f"  [exp2] bound={bound}: {count:,} objects in {elapsed:.1f}s ({rate:.0f} obj/s)"
        )

    print("\n  [exp2] SUMMARY:")
    print(f"  {'bound':>10s}  {'time':>8s}  {'obj / s':>8s}  {'vs 4096':>8s}")
    base_rate = results[4096][1]
    for bound in bounds:
        elapsed, rate = results[bound]
        ratio = rate / base_rate
        print(f"  {bound:>10d}  {elapsed:>7.1f}s  {rate:>7.0f}  {ratio:>7.2f}x")

    return results


# ---------------------------------------------------------------------------
# Experiment 3: estimate blob data transfer cost
# ---------------------------------------------------------------------------


def exp3_blob_transfer_estimate(pack_path, channel_bound=4096):
    """Measure per-object overhead by timing channel recv + tuple access.

    For each object, time how long __next__() takes (includes GIL acquire +
    channel recv + tuple construction + data copy from Rust).  Compare blob
    objects (large data) vs commit/tree objects (small data) to estimate the
    per-byte overhead of crossing the FFI boundary.
    """
    print(f"\n  [exp3] Per-object channel timing (channel_bound={channel_bound})")
    reader = _gix.DirectTreePackReader(pack_path, channel_bound=channel_bound)

    # We'll sample timing of __next__() calls.
    # Can't time every call (too much overhead), so we batch.
    t_start = time.perf_counter()
    count = 0
    n_blob = n_tree = n_commit = 0
    blob_bytes_total = 0

    # Accumulate wall time spent in __next__ per type
    blob_recv_ns = 0
    tree_recv_ns = 0
    commit_recv_ns = 0

    last_report = t_start
    it = iter(reader)

    while True:
        t0 = time.perf_counter_ns()
        try:
            obj_tuple = next(it)
        except StopIteration:
            break
        t1 = time.perf_counter_ns()
        recv_ns = t1 - t0

        t = obj_tuple[0]
        if t == 3:  # blob
            n_blob += 1
            blob_bytes_total += len(obj_tuple[5])
            blob_recv_ns += recv_ns
        elif t == 2:  # tree
            n_tree += 1
            # raw tree data is in different positions depending on variant
            tree_recv_ns += recv_ns
        elif t == 1:  # commit
            n_commit += 1
            commit_recv_ns += recv_ns
        elif t == 4:
            commit_recv_ns += recv_ns

        count += 1
        last_report = progress("exp3", t_start, count, last_report)

    elapsed = time.perf_counter() - t_start

    # Compute average recv time per object per type
    blob_avg_us = (blob_recv_ns / n_blob / 1000) if n_blob else 0
    tree_avg_us = (tree_recv_ns / n_tree / 1000) if n_tree else 0
    commit_avg_us = (commit_recv_ns / n_commit / 1000) if n_commit else 0

    blob_avg_bytes = (blob_bytes_total / n_blob) if n_blob else 0

    print(
        f"  [exp3] DONE: {count:,} objects in {elapsed:.1f}s ({count / elapsed:.0f} obj/s)"
    )
    print("")
    print("  Per-type __next__() timing (includes GIL + channel recv + data copy):")
    blob_gb = blob_bytes_total / 1e9
    print(
        f"    blob:   {blob_avg_us:>8.1f} µs/obj  "
        f"(n={n_blob:,}, avg {blob_avg_bytes:.0f} bytes/blob, {blob_gb:.1f} GB total)"
    )
    print(f"    tree:   {tree_avg_us:>8.1f} µs/obj  (n={n_tree:,})")
    print(f"    commit: {commit_avg_us:>8.1f} µs/obj  (n={n_commit:,})")
    print("")
    print("  Total __next__() time by type:")
    print(
        f"    blob:   {blob_recv_ns / 1e9:>7.1f}s  ({blob_recv_ns / 1e9 / elapsed * 100:.1f}%)"
    )
    print(
        f"    tree:   {tree_recv_ns / 1e9:>7.1f}s  ({tree_recv_ns / 1e9 / elapsed * 100:.1f}%)"
    )
    commit_pct = commit_recv_ns / 1e9 / elapsed * 100
    print(f"    commit: {commit_recv_ns / 1e9:>7.1f}s  ({commit_pct:.1f}%)")
    print("")

    # Estimate: if blobs took the same time as commits (i.e., data size didn't matter),
    # how much faster would we be?
    if commit_avg_us > 0 and blob_avg_us > commit_avg_us:
        saved_ns = (blob_avg_us - commit_avg_us) * 1000 * n_blob
        print("  Estimated blob data transfer overhead:")
        print(
            f"    Extra time per blob vs commit: {blob_avg_us - commit_avg_us:.1f} µs"
        )
        print(f"    Total overhead for {n_blob:,} blobs: {saved_ns / 1e9:.1f}s")
        print(f"    That's {saved_ns / 1e9 / elapsed * 100:.1f}% of wall time")

    return elapsed, count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <pack_file> [exp1|exp2|exp3|all]")
        sys.exit(1)

    pack_path = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else "all"
    pack_size = os.path.getsize(pack_path)
    print(f"Pack: {pack_path}")
    print(f"Size: {pack_size:,} bytes ({pack_size / 1024 / 1024 / 1024:.2f} GB)")

    if mode in ("exp1", "all"):
        print(f"\n{'=' * 70}")
        print("  EXPERIMENT 1: iterate-only vs full-pipeline")
        print("  (isolates Python converter cost)")
        print(f"{'=' * 70}")
        t_iterate, n = exp1_iterate_only(pack_path)
        t_full, _ = exp1_full_pipeline(pack_path)
        overhead = t_full - t_iterate
        print("\n  [exp1] COMPARISON:")
        print(f"    iterate-only:  {t_iterate:.1f}s ({n / t_iterate:.0f} obj/s)")
        print(f"    full-pipeline: {t_full:.1f}s ({n / t_full:.0f} obj/s)")
        print(
            f"    Python converter overhead: {overhead:.1f}s ({overhead / t_full * 100:.1f}%)"
        )
        if t_iterate > 0 and t_full > 0:
            print(
                "    -> If converters are <5% overhead, "
                "the channel crossing is the bottleneck"
            )

    if mode in ("exp2", "all"):
        print(f"\n{'=' * 70}")
        print("  EXPERIMENT 2: channel buffer size sweep")
        print("  (isolates backpressure vs sustained rate)")
        print(f"{'=' * 70}")
        exp2_buffer_sweep(pack_path)

    if mode in ("exp3", "all"):
        print(f"\n{'=' * 70}")
        print("  EXPERIMENT 3: per-object channel timing by type")
        print("  (isolates blob data transfer cost)")
        print(f"{'=' * 70}")
        exp3_blob_transfer_estimate(pack_path)


if __name__ == "__main__":
    main()
