# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Benchmark: dulwich vs gitoxide network fetch.

Measures the wall time to:
  1. List remote refs (no pack transferred)
  2. Fetch a full pack (all objects)

for the same repository using both backends.

Usage:
    python benchmarks/bench_fetch.py [URL] [--runs N]

Requires real network access; clears http_proxy/https_proxy if set.
"""

import argparse
import io
import os
import statistics
import time

# Remove any proxy that would block network access (e.g. the swh_proxy pytest
# fixture which sets http_proxy=http://localhost:999 for test isolation).
for _var in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
    os.environ.pop(_var, None)

import dulwich.client  # noqa: E402  (must come after proxy clearing)
from dulwich.object_store import ObjectStoreGraphWalker  # noqa: E402

import swh.loader.git._gix as _gix  # noqa: E402

# ── helpers ──────────────────────────────────────────────────────────────────


def dulwich_list_refs(url: str) -> dict:
    """Return remote_refs dict using dulwich (no pack)."""
    client, path = dulwich.client.get_transport_and_path(
        location=url,
        config=None,
        operation="pull",
        thin_packs=False,
    )
    pack_buffer = io.BytesIO()
    walker = ObjectStoreGraphWalker([], get_parents=lambda c: [])
    result = client.fetch_pack(
        path.encode(),
        lambda refs, **kw: [],  # wants nothing → no pack
        walker,
        pack_buffer.write,
    )
    return result.refs or {}


def dulwich_fetch_pack(url: str, wants_hex: list) -> bytes:
    """Fetch a pack for the given wants (hex bytes) using dulwich."""
    client, path = dulwich.client.get_transport_and_path(
        location=url,
        config=None,
        operation="pull",
        thin_packs=False,
    )
    pack_buffer = io.BytesIO()
    walker = ObjectStoreGraphWalker([], get_parents=lambda c: [])
    client.fetch_pack(
        path.encode(),
        lambda refs, **kw: wants_hex,
        walker,
        pack_buffer.write,
    )
    return pack_buffer.getvalue()


def gix_list_refs(url: str) -> dict:
    """Return remote_refs dict using gix (no pack)."""
    remote_refs, _, _ = _gix.fetch_pack(url, [], [])
    return remote_refs


def gix_fetch_pack(url: str, wants_hex: list) -> bytes:
    """Fetch a pack for the given wants (hex bytes) using gix."""
    wants_bin = [
        bytes.fromhex(sha.decode() if isinstance(sha, bytes) else sha)
        for sha in wants_hex
    ]
    _, _, pack_bytes = _gix.fetch_pack(url, wants_bin, [], 0)
    return pack_bytes


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
        "--runs", type=int, default=3, help="number of timed repetitions (default: 3)"
    )
    parser.add_argument(
        "--no-full-fetch",
        action="store_true",
        help="skip the full pack fetch (list-refs only)",
    )
    args = parser.parse_args()

    url = args.url
    runs = args.runs

    print(f"\nRepository : {url}")
    print(f"Repetitions: {runs}\n")

    # ── Phase A: list refs ────────────────────────────────────────────────
    print("=== Phase A: list remote refs (no pack transfer) ===")

    mn, med, mx = timeit(lambda: gix_list_refs(url), runs)
    report("gix (gitoxide)", runs, mn, med, mx)

    mn_d, med_d, mx_d = timeit(lambda: dulwich_list_refs(url), runs)
    report("dulwich", runs, mn_d, med_d, mx_d)

    speedup_a = med_d / med if med > 0 else float("nan")
    print(
        f"\n  → gix is {speedup_a:.2f}× {'faster' if speedup_a > 1 else 'slower'} "
        f"(median)\n"
    )

    if args.no_full_fetch:
        return

    # ── Phase B: full pack fetch ──────────────────────────────────────────
    print("=== Phase B: full pack fetch (all objects) ===")

    gix_refs = gix_list_refs(url)
    if not gix_refs:
        print("  No refs found — skipping full fetch benchmark.")
        return

    wants_hex = list({sha for sha in gix_refs.values()})
    print(f"  Fetching {len(wants_hex)} unique tip SHA(s) from {len(gix_refs)} refs")

    mn, med, mx = timeit(lambda: gix_fetch_pack(url, wants_hex), runs)
    report("gix (gitoxide)", runs, mn, med, mx)
    gix_pack = gix_fetch_pack(url, wants_hex)

    wants_hex_bytes = [s.encode() for s in wants_hex]
    mn_d, med_d, mx_d = timeit(lambda: dulwich_fetch_pack(url, wants_hex_bytes), runs)
    report("dulwich", runs, mn_d, med_d, mx_d)
    dul_pack = dulwich_fetch_pack(url, wants_hex_bytes)

    speedup_b = med_d / med if med > 0 else float("nan")
    print(
        f"\n  → gix is {speedup_b:.2f}× {'faster' if speedup_b > 1 else 'slower'} "
        f"(median)"
    )

    print(
        f"\n  Pack sizes — gix: {len(gix_pack):,} bytes  "
        f"dulwich: {len(dul_pack):,} bytes"
    )
    if len(gix_pack) != len(dul_pack):
        print("  (sizes differ — both are valid server-generated packs)")

    print()


if __name__ == "__main__":
    main()
