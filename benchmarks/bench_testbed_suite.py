#!/usr/bin/env python3
# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Multi-size benchmark harness for the SWH git loader.

Runs the full-pipeline benchmark (Rust inflate + Python conversion) across the
testbed packs at multiple size classes and in multiple modes (direct, indexed,
both). Captures rich metrics (throughput, per-type counts, timing breakdown,
memory, CPU, stats-line fields) and appends each run to a JSONL result file.

Designed to accumulate results across runs and code revisions.

Usage:
    python bench_testbed_suite.py --mode both --sizes small
    python bench_testbed_suite.py --mode direct --sizes manual --repos flask,django
    python bench_testbed_suite.py --mode both --sizes all
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import resource
import subprocess
import sys
import time
import traceback
from typing import Any

# ---------------------------------------------------------------------------
# Testbed layout (paths are evaluated on the machine where this script runs)
# ---------------------------------------------------------------------------

TESTBED_ROOT = Path(os.environ.get("SWH_TESTBED_ROOT", str(Path.home() / "testbed")))
LINUX_PACK = Path(
    os.environ.get("SWH_LINUX_PACK", str(Path.home() / "linux-kernel-test.pack"))
)
RESULTS_FILE = Path(
    os.environ.get("SWH_TESTBED_RESULTS", str(Path.home() / "testbed-results.jsonl"))
)

# Repo -> (dir_name_or_none, metadata). dir_name None means use the bare pack path (LINUX_PACK).
REPOS: dict[str, dict[str, Any]] = {
    "swh-py-template": {
        "dir": "swh-py-template.git",
        "pack_size": 115 * 1024,
        "obj_count": 611,
        "commit_count": 143,
    },
    "flask": {
        "dir": "flask.git",
        "pack_size": 7 * 1024 * 1024,
        "obj_count": 26_000,
        "commit_count": 5_500,
    },
    "requests": {
        "dir": "requests.git",
        "pack_size": 11 * 1024 * 1024,
        "obj_count": 27_000,
        "commit_count": 6_700,
    },
    "django": {
        "dir": "django.git",
        "pack_size": 157 * 1024 * 1024,
        "obj_count": 558_000,
        "commit_count": 52_000,
    },
    "kubernetes": {
        "dir": "kubernetes.git",
        "pack_size": 581 * 1024 * 1024,
        "obj_count": 1_700_000,
        "commit_count": 157_000,
    },
    "llvm-project": {
        "dir": "llvm-project.git",
        "pack_size": 1_900 * 1024 * 1024,
        "obj_count": 7_100_000,
        "commit_count": 590_000,
    },
    "libreoffice": {
        "dir": "libreoffice.git",
        "pack_size": 2_100 * 1024 * 1024,
        "obj_count": 6_600_000,
        "commit_count": 629_000,
    },
    "gcc": {
        "dir": "gcc.git",
        "pack_size": 12 * 1024 * 1024 * 1024,
        "obj_count": 3_300_000,
        "commit_count": 315_000,
    },
    "linux": {
        "dir": None,
        "pack_size": 7_900 * 1024 * 1024,
        "obj_count": 13_500_000,
        "commit_count": 1_100_000,
    },
    "chromium": {
        "dir": "chromium.git",
        "pack_size": 30 * 1024 * 1024 * 1024,
        "obj_count": 27_900_000,
        "commit_count": 2_000_000,
    },
}

SIZE_CLASSES: dict[str, list[str]] = {
    "small": ["swh-py-template", "flask", "requests"],
    "medium": ["django", "kubernetes"],
    "large": ["llvm-project", "libreoffice", "gcc"],
    "xl": ["linux"],
    "extreme": ["chromium"],
    "all": [
        "swh-py-template",
        "flask",
        "requests",
        "django",
        "kubernetes",
        "llvm-project",
        "libreoffice",
        "gcc",
        "linux",
        "chromium",
    ],
}


# ---------------------------------------------------------------------------
# Pack path resolution
# ---------------------------------------------------------------------------


def find_pack_path(repo: str) -> Path:
    """Return the .pack file path for a given testbed repo."""
    meta = REPOS[repo]
    if meta["dir"] is None:
        return LINUX_PACK
    pack_dir = TESTBED_ROOT / meta["dir"] / "objects" / "pack"
    candidates = sorted(pack_dir.glob("pack-*.pack"))
    if not candidates:
        raise FileNotFoundError(f"No pack file found in {pack_dir}")
    # Pick largest (the main pack)
    candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
    return candidates[0]


# ---------------------------------------------------------------------------
# Stats-line parser
#
# Looks for lines like:
#   [direct-tree stats] scan=0.05s traverse=1.23s sha1_git=0.7s blob_hash=0.5s \
#       tree_parse=0.1s commit=0.1s send_wait=0.4s objects=611
#   [gix-traverse stats] git_index_pack=3.4s traverse=5.6s sha1_git=... send_wait=...
# ---------------------------------------------------------------------------

STATS_RE = re.compile(r"\[(direct-tree|gix-traverse) stats\]\s+(.*)")
KV_RE = re.compile(r"(\w+)=([\d.]+)(s?)")


def parse_stats_line(text: str) -> dict[str, Any]:
    """Parse the last [*-stats] line found in captured stderr/stdout."""
    out: dict[str, Any] = {}
    last = None
    for line in text.splitlines():
        m = STATS_RE.search(line)
        if m:
            last = (m.group(1), m.group(2))
    if not last:
        return out
    out["stats_source"] = last[0]
    for km in KV_RE.finditer(last[1]):
        key = km.group(1)
        try:
            val = float(km.group(2))
        except ValueError:
            continue
        out[f"stats_{key}"] = val
    return out


# ---------------------------------------------------------------------------
# Captured-stderr helper: redirect C-level FDs while the Rust side prints
# ---------------------------------------------------------------------------


class CapturedStderr:
    """Capture stderr from both Python and C extensions (FD-level dup)."""

    def __enter__(self) -> "CapturedStderr":
        self._saved_fd = os.dup(2)
        self._read_fd, self._write_fd = os.pipe()
        os.dup2(self._write_fd, 2)
        os.close(self._write_fd)
        self._chunks: list[bytes] = []
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # Restore stderr FIRST so prints below work
        sys.stderr.flush()
        os.dup2(self._saved_fd, 2)
        os.close(self._saved_fd)
        # Drain pipe (non-blocking-ish: set O_NONBLOCK)
        import fcntl

        flags = fcntl.fcntl(self._read_fd, fcntl.F_GETFL)
        fcntl.fcntl(self._read_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
        try:
            while True:
                try:
                    data = os.read(self._read_fd, 65536)
                except BlockingIOError:
                    break
                if not data:
                    break
                self._chunks.append(data)
        finally:
            os.close(self._read_fd)

    def text(self) -> str:
        return b"".join(self._chunks).decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Per-run benchmark
# ---------------------------------------------------------------------------


def _run_one_mode(pack_path: Path, mode: str) -> dict[str, Any]:
    """Run the full-pipeline benchmark for a given pack + mode (direct/indexed).

    Returns a metrics dict. Captures stderr of the Rust side so we can parse
    its stats line.
    """
    import swh.loader.git._gix as _gix
    import swh.loader.git.converters as conv
    from swh.model.model import Directory

    if mode == "direct":
        reader_cls = _gix.DirectTreePackReader
    elif mode == "indexed":
        reader_cls = _gix.ParallelPackReader
    else:
        raise ValueError(f"unknown mode {mode!r}")

    # Baseline RSS before we start
    rss_before_kb = _read_vmrss_kb()

    n_blob = n_tree = n_commit = n_tag = 0
    n_tree_v = n_commit_v = n_tag_v = 0
    t_blob = t_tree = t_commit = t_tag = 0.0
    total_blob_bytes = 0

    cpu_t0 = time.process_time()
    wall_t0 = time.perf_counter()

    cap = CapturedStderr()
    with cap:
        reader = reader_cls(str(pack_path))
        for obj_tuple in reader:
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
                    # Directory already built in Rust; no-op conversion.
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

    wall = time.perf_counter() - wall_t0
    cpu = time.process_time() - cpu_t0
    stderr_text = cap.text()

    # Peak RSS via resource (ru_maxrss on Linux is in KB)
    ru = resource.getrusage(resource.RUSAGE_SELF)
    peak_rss_kb = ru.ru_maxrss
    rss_after_kb = _read_vmrss_kb()

    total_objects = n_blob + n_tree + n_commit + n_tag
    t_python = t_blob + t_tree + t_commit + t_tag
    t_rust = max(0.0, wall - t_python)

    metrics: dict[str, Any] = {
        "mode": mode,
        "wall_time_s": wall,
        "cpu_time_s": cpu,
        "throughput_obj_per_s": total_objects / wall if wall > 0 else 0.0,
        "objects": total_objects,
        "n_blob": n_blob,
        "n_tree": n_tree,
        "n_tree_rust_built": n_tree_v,
        "n_commit": n_commit,
        "n_commit_verified": n_commit_v,
        "n_tag": n_tag,
        "n_tag_verified": n_tag_v,
        "blob_bytes": total_blob_bytes,
        "py_blob_s": t_blob,
        "py_tree_s": t_tree,
        "py_commit_s": t_commit,
        "py_tag_s": t_tag,
        "py_total_s": t_python,
        "rust_s": t_rust,
        "peak_rss_kb": peak_rss_kb,
        "rss_before_kb": rss_before_kb,
        "rss_after_kb": rss_after_kb,
        "cpu_wall_ratio": cpu / wall if wall > 0 else 0.0,
        "stderr_tail": stderr_text[-4000:],
    }

    # Merge Rust stats-line fields
    stats = parse_stats_line(stderr_text)
    metrics.update(stats)

    # Derived: effective cores and channel utilization (best-effort).
    thread_seconds = sum(
        metrics.get(k, 0.0) or 0.0
        for k in (
            "stats_sha1_git",
            "stats_blob_hash",
            "stats_tree_parse",
            "stats_commit",
        )
    )
    send_wait = metrics.get("stats_send_wait", 0.0) or 0.0
    metrics["effective_cores_est"] = (thread_seconds / wall) if wall > 0 else 0.0
    # We don't know thread_count from stats; approximate with nproc when available.
    thread_count = os.cpu_count() or 1
    metrics["channel_util_est"] = send_wait / (wall * thread_count) if wall > 0 else 0.0

    return metrics


def _read_vmrss_kb() -> int:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except Exception:
        pass
    return -1


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def git_commit_hash() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parent,
        )
        return out.stdout.strip()
    except Exception:
        return "unknown"


def git_dirty() -> bool:
    try:
        out = subprocess.run(
            ["git", "status", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parent,
        )
        return bool(out.stdout.strip())
    except Exception:
        return False


def run_repo(repo: str, mode: str) -> dict[str, Any]:
    meta = REPOS[repo]
    pack_path = find_pack_path(repo)
    actual_pack_size = pack_path.stat().st_size

    record: dict[str, Any] = {
        "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
        "host": os.uname().nodename,
        "repo": repo,
        "mode": mode,
        "pack_path": str(pack_path),
        "pack_size_expected": meta["pack_size"],
        "pack_size_actual": actual_pack_size,
        "obj_count_expected": meta["obj_count"],
        "commit_count_expected": meta["commit_count"],
        "git_commit": git_commit_hash(),
        "git_dirty": git_dirty(),
        "cpu_count": os.cpu_count(),
        "python": sys.version.split()[0],
    }

    sep = "=" * 70
    print(
        f"\n{sep}\n  {repo}  [mode={mode}]  "
        f"pack={actual_pack_size / 1e6:.1f} MB\n{sep}"
    )
    sys.stdout.flush()

    try:
        metrics = _run_one_mode(pack_path, mode)
        record.update(metrics)
        record["status"] = "ok"
    except Exception as e:
        record["status"] = "error"
        record["error"] = str(e)
        record["traceback"] = traceback.format_exc()
        print(f"  FAILED: {e}", file=sys.stderr)

    return record


def append_jsonl(record: dict[str, Any]) -> None:
    # Avoid dumping the giant stderr_tail more than necessary, but keep it for debugging
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_FILE, "a") as f:
        f.write(json.dumps(record, default=str) + "\n")


def load_all_results() -> list[dict[str, Any]]:
    if not RESULTS_FILE.exists():
        return []
    out = []
    with open(RESULTS_FILE) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def print_summary_table(records: list[dict[str, Any]]) -> None:
    if not records:
        print("  (no results yet)")
        return
    sep120 = "=" * 120
    print(f"\n{sep120}")
    print("  Accumulated results summary")
    print(sep120)
    header = (
        f"{'ts':<20} {'repo':<18} {'mode':<8} {'status':<6} "
        f"{'wall_s':>8} {'obj / s':>10} {'objects':>12} "
        f"{'peak_MB':>8} {'eff_cores':>9} {'git':<10}"
    )
    print(header)
    print("-" * len(header))
    for r in records:
        ts = (r.get("ts", "")[:19]).replace("T", " ")
        wall = r.get("wall_time_s", 0) or 0
        thr = r.get("throughput_obj_per_s", 0) or 0
        objs = r.get("objects", 0) or 0
        peak = (r.get("peak_rss_kb", 0) or 0) / 1024.0
        eff = r.get("effective_cores_est", 0) or 0
        gh = (r.get("git_commit", "") or "")[:8]
        print(
            f"{ts:<20} {str(r.get('repo',''))[:18]:<18} "
            f"{str(r.get('mode',''))[:8]:<8} {str(r.get('status',''))[:6]:<6} "
            f"{wall:>8.1f} {thr:>10.0f} {objs:>12,} "
            f"{peak:>8.0f} {eff:>9.2f} {gh:<10}"
        )


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=("direct", "indexed", "both"), default="both")
    p.add_argument(
        "--sizes", choices=tuple(SIZE_CLASSES.keys()) + ("manual",), default="small"
    )
    p.add_argument(
        "--repos", default="", help="comma-separated (used with --sizes manual)"
    )
    p.add_argument("--dry-run", action="store_true", help="print plan, do not run")
    args = p.parse_args()

    if args.sizes == "manual":
        repos = [r.strip() for r in args.repos.split(",") if r.strip()]
    else:
        repos = SIZE_CLASSES[args.sizes]

    unknown = [r for r in repos if r not in REPOS]
    if unknown:
        print(f"ERROR: unknown repos: {unknown}", file=sys.stderr)
        return 2

    modes = ["direct", "indexed"] if args.mode == "both" else [args.mode]

    print(f"Plan: repos={repos} modes={modes} results_file={RESULTS_FILE}")
    if args.dry_run:
        return 0

    for repo in repos:
        for mode in modes:
            rec = run_repo(repo, mode)
            append_jsonl(rec)
            # Print a one-line summary for this run
            if rec.get("status") == "ok":
                print(
                    f"  -> {rec['wall_time_s']:.1f}s  "
                    f"{rec['throughput_obj_per_s']:.0f} obj/s  "
                    f"objects={rec['objects']:,}  "
                    f"peak_rss={rec.get('peak_rss_kb', 0) / 1024:.0f} MB"
                )
            else:
                print(f"  -> FAILED: {rec.get('error')}")
            sys.stdout.flush()

    # Final accumulated summary
    print_summary_table(load_all_results())
    return 0


if __name__ == "__main__":
    sys.exit(main())
