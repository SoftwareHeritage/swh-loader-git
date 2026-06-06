# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Benchmark: full pipeline (Rust inflate + Python conversion) on a large pack.

Compares three modes:
  sequential  - PackReader (single-threaded, Phase 3+4 baseline = 99 min)
  indexed     - ParallelPackReader (git index-pack + parallel traverse)
  direct      - DirectTreePackReader (no index-pack, direct tree traverse)

Usage:
    python bench_full_pipeline.py /path/to/pack.pack [sequential|indexed|direct|all]

Reports per-type object counts, timing breakdown, and throughput.
"""

import os
import sys
import time

import swh.loader.git._gix as _gix
import swh.loader.git.converters as conv
from swh.model.model import Directory


def run_full_pipeline(reader_iter, label):
    sep = "=" * 70
    print(f"\n{sep}")
    print(f"  {label}")
    print(sep)

    t_start = time.perf_counter()
    t_blob = t_tree = t_commit = t_tag = 0.0
    n_blob = n_tree = n_commit = n_tag = 0
    n_tree_v = n_commit_v = n_tag_v = 0
    total_blob_bytes = 0
    last_report = t_start

    for obj_tuple in reader_iter:
        t = obj_tuple[0]

        if t == 3:  # blob
            n_blob += 1
            total_blob_bytes += len(obj_tuple[5])
            t2 = time.perf_counter()
            conv.blob_to_content_precomputed(
                obj_tuple[1], obj_tuple[2], obj_tuple[3], obj_tuple[4], obj_tuple[5]
            )
            t_blob += time.perf_counter() - t2
        elif t == 2:  # tree
            n_tree += 1
            s = obj_tuple[1]
            if isinstance(s, Directory):
                n_tree_v += 1
                t2 = time.perf_counter()
                _ = s  # Directory already built by Rust
                t_tree += time.perf_counter() - t2
            else:
                t2 = time.perf_counter()
                conv.tree_to_directory_preparsed(
                    s, obj_tuple[2], obj_tuple[3], obj_tuple[4]
                )
                t_tree += time.perf_counter() - t2
        elif t == 1:  # commit
            n_commit += 1
            if obj_tuple[3]:
                n_commit_v += 1
            t2 = time.perf_counter()
            conv.commit_to_revision(obj_tuple[1], obj_tuple[2], obj_tuple[3])
            t_commit += time.perf_counter() - t2
        elif t == 4:  # tag
            n_tag += 1
            if obj_tuple[3]:
                n_tag_v += 1
            t2 = time.perf_counter()
            conv.tag_to_release(obj_tuple[1], obj_tuple[2], obj_tuple[3])
            t_tag += time.perf_counter() - t2

        now = time.perf_counter()
        if now - last_report > 10:
            total = n_blob + n_tree + n_commit + n_tag
            elapsed = now - t_start
            print(
                f"  ... {total:,} objects in {elapsed:.0f}s "
                f"({total / elapsed:.0f} obj/s)"
            )
            last_report = now

    t_total = time.perf_counter() - t_start
    total_objects = n_blob + n_tree + n_commit + n_tag

    t_rust = t_total - t_blob - t_tree - t_commit - t_tag

    print()
    print(f"  Objects: {total_objects:,}")
    print(f"    blobs:   {n_blob:,}")
    print(f"    trees:   {n_tree:,}  (Rust-built: {n_tree_v:,})")
    print(f"    commits: {n_commit:,}  (verified: {n_commit_v:,})")
    print(f"    tags:    {n_tag:,}  (verified: {n_tag_v:,})")
    print(f"  Total blob data: {total_blob_bytes / 1024 / 1024 / 1024:.2f} GB")
    print()
    print("  Timing breakdown:")
    print(f"    Rust inflate+hash+verify: {t_rust:.1f}s ({t_rust / 60:.1f} min)")
    print(f"    Python blob convert:      {t_blob:.1f}s")
    print(f"    Python tree convert:      {t_tree:.1f}s")
    print(f"    Python commit convert:    {t_commit:.1f}s")
    print(f"    Python tag convert:       {t_tag:.1f}s")
    print(f"    TOTAL:                    {t_total:.1f}s ({t_total / 60:.1f} min)")
    print(f"    Throughput:               {total_objects / t_total:.0f} obj/s")

    return t_total, total_objects


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <pack_file> [sequential|indexed|direct|all]")
        sys.exit(1)

    pack_path = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else "all"
    pack_size = os.path.getsize(pack_path)
    print(f"Pack: {pack_path}")
    print(f"Size: {pack_size:,} bytes ({pack_size / 1024 / 1024 / 1024:.2f} GB)")

    results = {}

    if mode in ("sequential", "all"):
        reader = _gix.PackReader(pack_path)
        t, n = run_full_pipeline(reader, "Sequential PackReader (Phase 3+4 baseline)")
        results["sequential"] = t

    if mode in ("indexed", "all"):
        reader = _gix.ParallelPackReader(pack_path)
        t, n = run_full_pipeline(
            reader, "ParallelPackReader (git index-pack + parallel)"
        )
        results["indexed"] = t

    if mode in ("direct", "all"):
        reader = _gix.DirectTreePackReader(pack_path)
        t, n = run_full_pipeline(
            reader, "DirectTreePackReader (no index-pack, direct tree)"
        )
        results["direct"] = t

    if len(results) > 1:
        sep = "=" * 70
        print(f"\n{sep}")
        print("  COMPARISON")
        print(sep)
        for name, t in sorted(results.items(), key=lambda x: x[1], reverse=True):
            print(f"  {name:15s}: {t:.1f}s ({t / 60:.1f} min)")
        if "sequential" in results:
            base = results["sequential"]
            for name, t in sorted(results.items(), key=lambda x: x[1]):
                if name != "sequential":
                    saved = base - t
                    print(
                        f"  {name} vs sequential: {base / t:.2f}x speedup, "
                        f"saves {saved:.0f}s ({saved / 60:.1f} min)"
                    )


if __name__ == "__main__":
    main()
