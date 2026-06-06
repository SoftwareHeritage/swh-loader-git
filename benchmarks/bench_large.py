# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Benchmark: streaming PackReader on a large pack file.

Usage:
    python benchmarks/bench_large.py /path/to/pack.pack

Reports per-type object counts, timing breakdown, and memory usage.
"""

import os
import sys
import time
import tracemalloc

import swh.loader.git._gix as _gix  # noqa: E402
import swh.loader.git.converters as conv  # noqa: E402
from swh.model.model import Directory  # noqa: E402


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <pack_file>")
        sys.exit(1)

    pack_path = sys.argv[1]
    pack_size = os.path.getsize(pack_path)
    print(f"Pack file: {pack_path}")
    print(f"Pack size: {pack_size:,} bytes ({pack_size / 1024 / 1024 / 1024:.2f} GB)")
    print()

    # Phase 1: count objects and measure streaming inflation + conversion
    print("=== Streaming inflate + convert (PackReader, single pass) ===")

    tracemalloc.start()
    tracemalloc.reset_peak()

    t_start = time.perf_counter()
    t_blob = t_tree = t_commit = t_tag = 0.0
    n_blob = n_tree = n_commit = n_tag = 0
    n_tree_v = n_commit_v = n_tag_v = 0
    total_blob_bytes = 0
    last_report = t_start

    for obj_tuple in _gix.PackReader(pack_path):
        t = obj_tuple[0]
        s = obj_tuple[1]

        if t == 3:  # blob
            n_blob += 1
            total_blob_bytes += len(obj_tuple[5])
            t2 = time.perf_counter()
            conv.blob_to_content_precomputed(
                s, obj_tuple[2], obj_tuple[3], obj_tuple[4], obj_tuple[5]
            )
            t_blob += time.perf_counter() - t2
        elif t == 2:  # tree
            n_tree += 1
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
            conv.commit_to_revision(s, obj_tuple[2], obj_tuple[3])
            t_commit += time.perf_counter() - t2
        elif t == 4:  # tag
            n_tag += 1
            if obj_tuple[3]:
                n_tag_v += 1
            t2 = time.perf_counter()
            conv.tag_to_release(s, obj_tuple[2], obj_tuple[3])
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
    _, peak_mem = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    t_rust = t_total - t_blob - t_tree - t_commit - t_tag
    total_objects = n_blob + n_tree + n_commit + n_tag

    print()
    print(f"Objects: {total_objects:,}")
    print(f"  blobs:   {n_blob:,}")
    print(f"  trees:   {n_tree:,}  (verified: {n_tree_v})")
    print(f"  commits: {n_commit:,}  (verified: {n_commit_v})")
    print(f"  tags:    {n_tag:,}  (verified: {n_tag_v})")
    print(f"Total blob data: {total_blob_bytes / 1024 / 1024 / 1024:.2f} GB")
    print()
    print("Timing breakdown:")
    print(f"  Rust inflate+hash+verify: {t_rust:.1f}s")
    print(f"  Python blob convert:      {t_blob:.1f}s")
    print(f"  Python tree convert:      {t_tree:.1f}s")
    print(f"  Python commit convert:    {t_commit:.1f}s")
    print(f"  Python tag convert:       {t_tag:.1f}s")
    print(f"  Total:                    {t_total:.1f}s ({t_total / 60:.1f} min)")
    print(f"  Throughput:               {total_objects / t_total:.0f} objects/s")
    print()
    print(f"Peak Python memory: {peak_mem / 1024 / 1024:.1f} MB")
    print()


if __name__ == "__main__":
    main()
