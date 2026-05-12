# Benchmarks for the gix-based git loader

This directory contains the benchmark scripts used to evaluate the
gitoxide-based pack pipeline against the legacy dulwich path. They are
self-contained Python scripts that exercise either component-level
primitives (fetch, inflate, channel I/O) or the full loader pipeline,
and emit timings + memory numbers as JSON-lines to a results file.

## Quick start (single benchmark, ~minutes)

After building the `_gix` extension once via `maturin develop --release`
(see the top-level `README.rst` "Build" section for prerequisites):

```bash
# Full-pipeline bench against a small repo, both gix and dulwich:
python benchmarks/bench_full_pipeline.py --url https://github.com/pallets/flask.git

# Or invoke the multi-size testbed runner (driver for bench_testbed_suite.py):
./benchmarks/run_testbed_suite.sh small
```

Results land in `~/testbed-results.jsonl` (or per-script-defined output paths).

## Multi-size testbed runner

`run_testbed_suite.sh` is the recommended entry point. It runs the
full-pipeline benchmark across the canonical testbed repos at each size
class:

| Class | Approx pack size | Repos | Wall-time per run |
|---|---|---|---|
| `small` | ≤ 50 MB | a small set of curated repos | minutes |
| `medium` | 50–500 MB | typical PyPI projects | 30–60 min |
| `large` | 500 MB–5 GB | Postgres, Django, etc. | hours (run in background) |
| `xl` | 5–30 GB | Linux kernel, Chromium | many hours (background) |
| `extreme` | > 30 GB | mega-monorepos | very long (background) |

Suite invocations:

```bash
./benchmarks/run_testbed_suite.sh small           # foreground, quick
./benchmarks/run_testbed_suite.sh medium          # foreground, 30-60 min
./benchmarks/run_testbed_suite.sh large-bg        # nohup background
./benchmarks/run_testbed_suite.sh xl-bg           # nohup background
./benchmarks/run_testbed_suite.sh all-staged      # small foreground; medium foreground; large + xl background
```

Override the swh-loader-git checkout path with `SWH_LOADER_GIT_DIR=...`
if running from a non-default location.

## Per-script bench catalogue

Each script is a CLI tool with `--help`. Brief summaries:

| Script | What it measures | Granularity |
|---|---|---|
| `bench_fetch.py` | Pack download time only (gix-protocol vs dulwich.client). | per-repo, network-bound |
| `bench_inflate.py` | Pack inflation time, given a pre-downloaded pack on disk. | per-repo, CPU-bound |
| `bench_direct_tree.py` | DirectTreeInflater path — pack scan without `git index-pack`. | per-repo, CPU + memory |
| `bench_channel_bottleneck.py` | Channel-bound iteration latency; isolates the PyO3 boundary. | microsecond-scale |
| `bench_full_pipeline.py` | End-to-end loader run (fetch → inflate → hash → emit). | per-repo wall time |
| `bench_large.py` | Large-pack stress test, peak RSS + ru_maxrss capture. | per-repo, multi-GB |
| `bench_dulwich_baseline.py` | Same shape as `bench_full_pipeline.py`, dulwich path only. | per-repo wall time |
| `bench_testbed_suite.py` | Driver that runs the full pipeline across the size-classed testbed. | per-class, multi-repo |
| `run_testbed_suite.sh` | Shell wrapper for `bench_testbed_suite.py`. | — |

## Prerequisites

1. **`_gix.*.so` built into the working tree.**  Run `maturin develop
   --release` once after a fresh clone; rebuild whenever a `gix-lib/` or
   `gix-py/` source file changes.

2. **Sibling SWH packages editable-installed.**  Same as the test suite
   — see the top-level `README.rst` "Test" section.

3. **Disk space.**  Large/xl runs check out the testbed repos to a
   temporary directory; expect 10–60 GB transient usage.

4. **Network egress** to the upstream forges (GitHub, GitLab) unless you
   pre-stage the testbed packs locally.

## Reading results

`~/testbed-results.jsonl` (or the script-specific output) is a
JSON-lines file with one object per benchmark run. Common keys:

| Key | Type | Notes |
|---|---|---|
| `repo` | str | URL or path of the input repo. |
| `mode` | str | `gix` or `dulwich`. |
| `wall_seconds` | float | End-to-end wall time. |
| `peak_rss_mb` | int | Peak RSS as measured by `psutil` or `ru_maxrss`. |
| `objects` | int | Total objects parsed. |
| `pack_bytes` | int | Pack size on disk. |
| `bench_version` | str | Git SHA of the bench script. |

Pair each gix run with its dulwich counterpart on the same repo for a
fair ratio. The `bench_testbed_suite.py` driver does this automatically
when invoked with `--mode both`.

## Reproducibility on `maxxi`

The audit notes' Docker test rig at
`swh-environment/notes/git-loader-rehaul/test-rig/` provides a
self-contained reproduction harness (bookworm + PG-17 + Rust + maturin
+ pytest stack, ~600 MB image, ~15–20 min wall-time per run cycle).
Useful for verifying bench results against a known-clean substrate
when investigating regressions.
