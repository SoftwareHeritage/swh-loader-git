# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Dulwich-baseline benchmark for the staging-rollout proposal.

Runs the legacy ``swh.loader.git.from_disk.GitLoaderFromDisk`` against
each repo in the testbed.  ``GitLoaderFromDisk`` still uses dulwich
end-to-end (``swh-loader-git/swh/loader/git/from_disk.py:14-17,230,239,261``);
the only meaningful difference from the pre-rehaul production
``GitLoader`` is that it reads from a local bare repo rather than
fetching a pack over HTTP.  By using identical local repos for both
the dulwich runs and the gix runs we eliminate network variability
and produce strictly comparable numbers.

Storage backend: in-memory (``swh.storage.in_memory``).  This keeps
storage cost constant across both pipelines so the comparison
isolates the loader.

Output format: append-only JSON-line file, the same shape as
``bench_testbed_suite.py`` produces, with ``mode='dulwich'`` and
``loader='from_disk'``.  Default file:
``~/testbed-dulwich-results.jsonl`` (override with
``SWH_DULWICH_RESULTS`` env var).

Usage::

    python bench_dulwich_baseline.py --repos flask,django,kubernetes,llvm-project,linux

Repos must already be cloned bare under ``~/testbed/<name>.git/``.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
from pathlib import Path
import resource
import subprocess
import sys
import time
import traceback
from typing import Any, Dict

# ---------------------------------------------------------------------------
# Repo testbed metadata (mirrors bench_testbed_suite.py)
# ---------------------------------------------------------------------------

TESTBED_ROOT = Path(os.environ.get("SWH_TESTBED_ROOT", str(Path.home() / "testbed")))
RESULTS_FILE = Path(
    os.environ.get(
        "SWH_DULWICH_RESULTS",
        str(Path.home() / "testbed-dulwich-results.jsonl"),
    )
)

REPOS: Dict[str, Dict[str, Any]] = {
    # name → bare repo dir name, expected size (bytes), object count, commits
    "swh-py-template": {
        "dir": "swh-py-template.git",
        "obj_count": 611,
        "commit_count": 143,
    },
    "flask": {"dir": "flask.git", "obj_count": 26_179, "commit_count": 5_572},
    "requests": {"dir": "requests.git", "obj_count": 26_575, "commit_count": 6_679},
    "django": {"dir": "django.git", "obj_count": 558_423, "commit_count": 52_018},
    "kubernetes": {
        "dir": "kubernetes.git",
        "obj_count": 1_734_359,
        "commit_count": 157_176,
    },
    "llvm-project": {
        "dir": "llvm-project.git",
        "obj_count": 7_086_259,
        "commit_count": 589_525,
    },
    "libreoffice": {
        "dir": "libreoffice.git",
        "obj_count": 6_573_747,
        "commit_count": 629_148,
    },
    "gcc": {"dir": "gcc.git", "obj_count": 3_345_101, "commit_count": 315_302},
    "linux": {"dir": "linux.git", "obj_count": 13_546_420, "commit_count": 1_100_000},
    "chromium": {
        "dir": "chromium.git",
        "obj_count": 27_916_948,
        "commit_count": 2_034_503,
    },
}


# ---------------------------------------------------------------------------
# Metric capture helpers
# ---------------------------------------------------------------------------


def _read_vmrss_kb() -> int:
    """Current process VmRSS in KB, -1 if not readable."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except Exception:
        pass
    return -1


def _peak_vmpeak_kb() -> int:
    """Lifetime peak VmPeak (also KB)."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmPeak:"):
                    return int(line.split()[1])
    except Exception:
        pass
    return -1


def _git_commit_hash() -> str:
    """HEAD of the swh-loader-git checkout running this benchmark."""
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


def _git_dirty() -> bool:
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


def _pack_size_bytes(repo_dir: Path) -> int:
    """Sum of all .pack files in objects/pack/."""
    total = 0
    pack_dir = repo_dir / "objects" / "pack"
    if pack_dir.is_dir():
        for p in pack_dir.iterdir():
            if p.suffix == ".pack":
                total += p.stat().st_size
    return total


# ---------------------------------------------------------------------------
# The actual benchmark
# ---------------------------------------------------------------------------


def run_one(repo: str) -> Dict[str, Any]:
    """Run the dulwich-based GitLoaderFromDisk against ``repo`` and
    return a metrics dict.  Storage is in-memory to isolate loader cost."""
    meta = REPOS[repo]
    repo_dir = TESTBED_ROOT / meta["dir"]
    if not repo_dir.is_dir():
        raise FileNotFoundError(f"repo not found: {repo_dir}")

    pack_size = _pack_size_bytes(repo_dir)

    record: Dict[str, Any] = {
        "ts": dt.datetime.now(tz=dt.timezone.utc).isoformat(timespec="seconds"),
        "host": os.uname().nodename,
        "repo": repo,
        "loader": "from_disk",
        "mode": "dulwich",
        "directory": str(repo_dir),
        "pack_size_actual": pack_size,
        "obj_count_expected": meta["obj_count"],
        "commit_count_expected": meta["commit_count"],
        "git_commit": _git_commit_hash(),
        "git_dirty": _git_dirty(),
        "cpu_count": os.cpu_count(),
        "python": sys.version.split()[0],
        "dulwich_version": _dulwich_version(),
    }

    # Lazy imports so the script can introspect even if swh.* isn't
    # installed.
    from swh.loader.git.from_disk import GitLoaderFromDisk
    from swh.storage import get_storage

    storage = get_storage("memory")
    rss_before_kb = _read_vmrss_kb()

    cpu_t0 = time.process_time()
    wall_t0 = time.perf_counter()

    try:
        loader = GitLoaderFromDisk(
            storage=storage,
            url=f"https://example.org/{repo}",  # synthetic origin URL
            directory=str(repo_dir),
        )
        result = loader.load()
        wall_t1 = time.perf_counter()
        cpu_t1 = time.process_time()
        record["status"] = "ok"
        record["load_status"] = result
    except Exception as exc:
        wall_t1 = time.perf_counter()
        cpu_t1 = time.process_time()
        record["status"] = "error"
        record["error"] = repr(exc)
        record["traceback"] = traceback.format_exc()

    record["wall_time_s"] = wall_t1 - wall_t0
    record["cpu_time_s"] = cpu_t1 - cpu_t0

    # Try to get content/directory/revision/release counts from the
    # in-memory storage (handy for sanity checking).
    try:
        # in-memory storage exposes its internal state via direct attrs.
        # Counts work for sanity check only — not authoritative.
        record["objects_in_storage"] = {
            "content": len(getattr(storage, "_contents", {}))
            + len(getattr(storage, "_skipped_contents", {})),
            "directory": len(getattr(storage, "_directories", {})),
            "revision": len(getattr(storage, "_revisions", {})),
            "release": len(getattr(storage, "_releases", {})),
            "snapshot": len(getattr(storage, "_snapshots", {})),
        }
    except Exception:
        pass

    rss_after_kb = _read_vmrss_kb()
    peak_kb = _peak_vmpeak_kb()
    if peak_kb < 0:
        # fall back to ru_maxrss (KB on Linux)
        try:
            peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        except Exception:
            peak_kb = -1
    record["rss_before_kb"] = rss_before_kb
    record["rss_after_kb"] = rss_after_kb
    record["peak_rss_kb"] = peak_kb

    # Throughput preference order:
    #   1. real count from storage (if accessible)
    #   2. expected count from REPOS metadata (always available)
    # in-memory storage's internal attrs are not always exposed; fall back
    # to expected counts for the throughput number, which is what we
    # actually want for a fair comparison vs gix runs (those use the
    # same expected count internally).
    real_count = sum(record.get("objects_in_storage", {}).values()) or 0
    expected = meta.get("obj_count", 0) or 0
    objs = real_count if real_count > 0 else expected
    record["objects_processed"] = objs
    record["objects_count_source"] = "storage" if real_count > 0 else "expected"
    if record["wall_time_s"] > 0 and objs > 0:
        record["throughput_obj_per_s"] = objs / record["wall_time_s"]

    return record


def _dulwich_version() -> str:
    try:
        import dulwich

        return getattr(dulwich, "__version__", "unknown")
    except Exception:
        return "missing"


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def append_record(record: Dict[str, Any]) -> None:
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")


def print_summary() -> None:
    if not RESULTS_FILE.exists():
        return
    rows = []
    with open(RESULTS_FILE) as f:
        for line in f:
            line = line.strip()
            if not line or not line.startswith("{"):
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    if not rows:
        return

    sep = "=" * 110
    print(f"\n{sep}")
    print("  Dulwich baseline accumulated results")
    print(sep)
    print(
        f"{'ts':<19}  {'repo':<17} {'status':<6} "
        f"{'wall_s':>10} {'obj/s':>9} {'peak_GB':>8} {'cpu_ratio':>10}"
    )
    print("-" * 110)
    for r in rows:
        ts = (r.get("ts", "")[:19]).replace("T", " ")
        wall = r.get("wall_time_s", 0) or 0
        thr = r.get("throughput_obj_per_s", 0) or 0
        peak_gb = (r.get("peak_rss_kb", 0) or 0) / 1024 / 1024
        cpu_ratio = (r.get("cpu_time_s", 0) or 0) / wall if wall > 0 else 0
        print(
            f"{ts:<19}  {r.get('repo', '?'):<17} {r.get('status', '?'):<6} "
            f"{wall:>10.1f} {thr:>9.0f} {peak_gb:>8.1f} {cpu_ratio:>10.2f}"
        )


def main() -> int:
    p = argparse.ArgumentParser(
        description="Dulwich-baseline benchmark using GitLoaderFromDisk + memory storage"
    )
    p.add_argument(
        "--repos",
        type=str,
        required=True,
        help="Comma-separated repo names (e.g. flask,django,kubernetes)",
    )
    p.add_argument(
        "--summary-only",
        action="store_true",
        help="Print accumulated summary and exit (no benchmark runs)",
    )
    args = p.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.summary_only:
        print_summary()
        return 0

    repos = [r.strip() for r in args.repos.split(",") if r.strip()]
    unknown = [r for r in repos if r not in REPOS]
    if unknown:
        print(
            f"ERROR: unknown repo(s): {', '.join(unknown)}.  "
            f"Known: {', '.join(sorted(REPOS))}",
            file=sys.stderr,
        )
        return 1

    print(f"Plan: dulwich baseline runs against {repos}; " f"results → {RESULTS_FILE}")

    for repo in repos:
        meta = REPOS[repo]
        repo_dir = TESTBED_ROOT / meta["dir"]
        sep = "=" * 70
        print(f"\n{sep}")
        print(f"  {repo}  [loader=from_disk, mode=dulwich]")
        print(f"  directory: {repo_dir}")
        print(f"  expected objects: {meta['obj_count']:,}")
        print(sep)
        sys.stdout.flush()

        try:
            record = run_one(repo)
        except Exception as exc:
            record = {
                "ts": dt.datetime.now(tz=dt.timezone.utc).isoformat(timespec="seconds"),
                "host": os.uname().nodename,
                "repo": repo,
                "loader": "from_disk",
                "mode": "dulwich",
                "status": "error",
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }

        append_record(record)
        if record["status"] == "ok":
            print(
                f"  -> {record.get('wall_time_s', 0):.1f} s  "
                f"{record.get('throughput_obj_per_s', 0):.0f} obj/s  "
                f"peak {record.get('peak_rss_kb', 0) / 1024 / 1024:.1f} GB  "
                f"cpu/wall = {record['cpu_time_s'] / max(record['wall_time_s'], 1):.2f}"
            )
        else:
            print(f"  -> FAILED: {record.get('error', '?')}")

    print_summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
