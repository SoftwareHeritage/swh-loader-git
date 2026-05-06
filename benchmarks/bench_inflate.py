# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Benchmark: dulwich PackInflater vs gix iter_pack_objects.

Measures the wall time to inflate all objects from a pack file, comparing
dulwich (Python/C zlib) against gix (Rust gix-pack).

Usage:
    python benchmarks/bench_inflate.py [URL] [--runs N]

Requires real network access to fetch a pack.  Clears proxy env vars.
"""

import argparse
import io
import os
import statistics
import time

for _var in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
    os.environ.pop(_var, None)

from dulwich.object_format import SHA1  # noqa: E402
from dulwich.pack import PackData, PackInflater  # noqa: E402

import swh.loader.git._gix as _gix  # noqa: E402

# ── helpers ──────────────────────────────────────────────────────────────────


def fetch_pack_bytes(url: str) -> bytes:
    """Fetch a full pack from the remote using gix (fastest available)."""
    remote_refs, _, _ = _gix.fetch_pack(url, [], [])
    if not remote_refs:
        raise RuntimeError("no refs found")
    wants = [bytes.fromhex(sha) for sha in set(remote_refs.values())]
    _, _, pack_bytes = _gix.fetch_pack(url, wants, [], 0)
    return pack_bytes


def dulwich_inflate(pack_bytes: bytes) -> list:
    """Inflate all objects from pack_bytes using dulwich PackInflater."""
    buf = io.BytesIO(pack_bytes)
    pack_data = PackData.from_file(file=buf, size=len(pack_bytes), object_format=SHA1)
    objects = []
    for obj in PackInflater.for_pack_data(pack_data):
        objects.append((obj.type_num, obj.id, obj.as_raw_string()))
    return objects


def gix_inflate(pack_bytes: bytes) -> list:
    """Inflate all objects from pack_bytes using gix iter_pack_objects."""
    return _gix.iter_pack_objects(pack_bytes)


# ── timing ───────────────────────────────────────────────────────────────────


def timeit(fn, runs: int) -> tuple:
    """Return (min, median, max) wall times in seconds over `runs` calls."""
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return min(times), statistics.median(times), max(times)


def fmt(t: float) -> str:
    return f"{t * 1000:.1f} ms"


def report(label: str, runs: int, mn: float, med: float, mx: float) -> None:
    print(
        f"  {label:<30s}  min={fmt(mn)}  median={fmt(med)}  max={fmt(mx)}"
        f"  ({runs} run{'s' if runs > 1 else ''})"
    )


# ── main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "url",
        nargs="?",
        default="https://gitlab.softwareheritage.org/swh/devel/swh-py-template.git",
        help="git repository URL to benchmark against",
    )
    parser.add_argument(
        "--runs", type=int, default=5, help="number of timed repetitions (default: 5)"
    )
    args = parser.parse_args()

    url = args.url
    runs = args.runs

    print(f"\nRepository : {url}")
    print(f"Repetitions: {runs}")

    # Fetch once; benchmark inflation only.
    print("\n  Fetching pack from remote...")
    pack_bytes = fetch_pack_bytes(url)
    print(f"  Pack size: {len(pack_bytes):,} bytes\n")

    # ── Warmup + object count ────────────────────────────────────────────
    gix_objects = gix_inflate(pack_bytes)
    dul_objects = dulwich_inflate(pack_bytes)
    print(f"  Objects: gix={len(gix_objects)}  dulwich={len(dul_objects)}")

    # Cross-validate: same SHA-1 set (gix returns 20-byte raw, dulwich returns hex)
    gix_shas = {sha.hex() for (_, sha, _) in gix_objects}
    dul_shas = {
        sha.decode() if isinstance(sha, bytes) else sha for (_, sha, _) in dul_objects
    }
    if gix_shas == dul_shas:
        print("  SHA-1 cross-validation: PASS (all objects match)\n")
    else:
        only_gix = gix_shas - dul_shas
        only_dul = dul_shas - gix_shas
        print("  SHA-1 cross-validation: MISMATCH!")
        print(f"    only in gix: {len(only_gix)}  only in dulwich: {len(only_dul)}\n")

    # ── Benchmark ────────────────────────────────────────────────────────
    print("=== Pack inflation benchmark ===")

    mn, med, mx = timeit(lambda: gix_inflate(pack_bytes), runs)
    report("gix (Rust)", runs, mn, med, mx)

    mn_d, med_d, mx_d = timeit(lambda: dulwich_inflate(pack_bytes), runs)
    report("dulwich (Python)", runs, mn_d, med_d, mx_d)

    speedup = med_d / med if med > 0 else float("nan")
    print(
        f"\n  → gix is {speedup:.2f}× {'faster' if speedup > 1 else 'slower'} "
        f"(median)\n"
    )


if __name__ == "__main__":
    main()
