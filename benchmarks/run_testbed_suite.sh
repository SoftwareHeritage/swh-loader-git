#!/bin/bash
# Driver script for the multi-size testbed benchmark suite.
# Run this on a bench host (e.g., maxxi) after a fresh ``_gix.*.so`` has
# been built via ``maturin develop`` (see benchmarks/README.md for the
# full setup).
#
# Usage:
#   ./run_testbed_suite.sh small           # ~minutes
#   ./run_testbed_suite.sh medium          # ~30-60 min
#   ./run_testbed_suite.sh large-bg        # launches large class in background
#   ./run_testbed_suite.sh xl-bg           # linux, background
#   ./run_testbed_suite.sh all-staged      # small foreground, medium foreground, large+xl background
#
# Working directory: defaults to the swh-loader-git checkout containing
# this script.  Override with the SWH_LOADER_GIT_DIR environment variable
# if you need a different checkout (e.g., a stale clone with a pre-built
# .so).
#
# Results are appended to ~/testbed-results.jsonl

set -euo pipefail

cd "${SWH_LOADER_GIT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
source ~/.cargo/env 2>/dev/null || true
unset CONDA_PREFIX || true
export PYTHONPATH="$PWD"

CLASS="${1:-small}"
HARNESS="benchmarks/bench_testbed_suite.py"

case "$CLASS" in
  small)
    python -u "$HARNESS" --mode both --sizes small
    ;;
  medium)
    python -u "$HARNESS" --mode both --sizes medium
    ;;
  large)
    python -u "$HARNESS" --mode both --sizes large
    ;;
  xl)
    python -u "$HARNESS" --mode both --sizes xl
    ;;
  extreme)
    python -u "$HARNESS" --mode both --sizes extreme
    ;;
  large-bg)
    nohup python -u "$HARNESS" --mode both --sizes large \
        > ~/testbed-large.log 2>&1 &
    echo "Started large class PID=$!; log ~/testbed-large.log"
    ;;
  xl-bg)
    nohup python -u "$HARNESS" --mode both --sizes xl \
        > ~/testbed-xl.log 2>&1 &
    echo "Started xl class PID=$!; log ~/testbed-xl.log"
    ;;
  extreme-bg)
    nohup python -u "$HARNESS" --mode both --sizes extreme \
        > ~/testbed-extreme.log 2>&1 &
    echo "Started extreme class PID=$!; log ~/testbed-extreme.log"
    ;;
  all-staged)
    echo "=== small (foreground) ==="
    python -u "$HARNESS" --mode both --sizes small
    echo "=== medium (foreground) ==="
    python -u "$HARNESS" --mode both --sizes medium
    echo "=== large (background) ==="
    nohup python -u "$HARNESS" --mode both --sizes large \
        > ~/testbed-large.log 2>&1 &
    echo "large PID=$!; log ~/testbed-large.log"
    echo "=== xl (background, after large starts) ==="
    nohup python -u "$HARNESS" --mode both --sizes xl \
        > ~/testbed-xl.log 2>&1 &
    echo "xl PID=$!; log ~/testbed-xl.log"
    ;;
  *)
    echo "Unknown class: $CLASS" >&2
    echo "Usage: $0 {small|medium|large|xl|extreme|large-bg|xl-bg|extreme-bg|all-staged}" >&2
    exit 2
    ;;
esac
