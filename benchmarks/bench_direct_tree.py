# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Benchmark: DirectTreePackReader vs ParallelPackReader on a large pack.

Usage:
    python benchmarks/bench_direct_tree.py /path/to/pack.pack [mode]

Where mode is:
    direct   - DirectTreePackReader only (default)
    indexed  - ParallelPackReader only (git index-pack based)
    both     - run both and compare
"""

import os
import sys
import time


def run_reader(reader_cls, pack_path, label):
    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print(f"{'=' * 60}")

    t_start = time.perf_counter()
    n_blob = n_tree = n_commit = n_tag = 0
    last_report = t_start

    reader = reader_cls(pack_path)
    t_init = time.perf_counter() - t_start
    print(f"  init: {t_init:.1f}s")

    for obj_tuple in reader:
        t = obj_tuple[0]
        if t == 3:
            n_blob += 1
        elif t == 2:
            n_tree += 1
        elif t == 1:
            n_commit += 1
        elif t == 4:
            n_tag += 1

        now = time.perf_counter()
        if now - last_report > 10:
            total = n_blob + n_tree + n_commit + n_tag
            elapsed = now - t_start
            print(
                f"  ... {total:,} objects in {elapsed:.0f}s ({total / elapsed:.0f} obj/s)"
            )
            last_report = now

    t_total = time.perf_counter() - t_start
    total = n_blob + n_tree + n_commit + n_tag

    print(f"\n  Objects: {total:,}")
    print(
        f"    blobs={n_blob:,}  trees={n_tree:,}  commits={n_commit:,}  tags={n_tag:,}"
    )
    print(f"  Total: {t_total:.1f}s ({t_total / 60:.1f} min)")
    print(f"  Throughput: {total / t_total:.0f} obj/s")
    return t_total, total


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <pack_file> [direct|indexed|both]")
        sys.exit(1)

    pack_path = sys.argv[1]
    mode = sys.argv[2] if len(sys.argv) > 2 else "direct"
    pack_size = os.path.getsize(pack_path)
    print(f"Pack: {pack_path}")
    print(f"Size: {pack_size:,} bytes ({pack_size / 1024 / 1024 / 1024:.2f} GB)")

    import swh.loader.git._gix as _gix

    results = {}

    if mode in ("direct", "both"):
        t, n = run_reader(
            _gix.DirectTreePackReader, pack_path, "DirectTreePackReader (no index-pack)"
        )
        results["direct"] = t

    if mode in ("indexed", "both"):
        t, n = run_reader(
            _gix.ParallelPackReader, pack_path, "ParallelPackReader (git index-pack)"
        )
        results["indexed"] = t

    if "direct" in results and "indexed" in results:
        speedup = results["indexed"] / results["direct"]
        saved = results["indexed"] - results["direct"]
        sep = "=" * 60
        print(f"\n{sep}")
        print("  COMPARISON")
        print(sep)
        print(f"  Indexed:  {results['indexed']:.1f}s")
        print(f"  Direct:   {results['direct']:.1f}s")
        print(f"  Speedup:  {speedup:.2f}x")
        print(f"  Saved:    {saved:.0f}s ({saved / 60:.1f} min)")


if __name__ == "__main__":
    main()
