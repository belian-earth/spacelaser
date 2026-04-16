#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# End-to-end benchmark orchestrator
# ---------------------------------------------------------------------------
#
# Runs the workload from benchmarks/workload.json through all three
# pipelines, the equivalence check, renders BENCHMARK-RESULT.md, and
# archives the timing parquets to a timestamped directory.
#
# Pipeline order:
#   0. search.R          one CMR call -> granules.parquet (shared input)
#   1. bench-spacelaser  Rust, partial HTTP range reads
#   2. bench-hdf5r       Status quo: curl::multi_download + hdf5r
#   3. bench-h5coro      Python via uv, partial HTTP range reads
#   4. equivalence       cross-pipeline comparison
#   5. render            BENCHMARK-RESULT.Rmd -> .md
#   6. archive           timing parquets -> archive/<timestamp>/
#
# Usage (from package root):
#   benchmarks/run.sh
#
# Optional env vars:
#   SPACELASER_BENCH_DIR  persistent download dir for the status-quo
#                         pipeline (defaults to a cold-cache tempdir)
#   EARTHDATA_TOKEN       EDL user token for h5coro (skips if unset)

set -euo pipefail

# Work from package root regardless of where run.sh is invoked
cd "$(dirname "$0")/.."

stamp="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
echo "=== spacelaser benchmark: ${stamp} ==="
echo

# 0. CMR search (shared input for all pipelines)
Rscript benchmarks/search.R
echo

# 1. Spacelaser
Rscript benchmarks/bench-spacelaser.R
echo

# 2. Status quo
Rscript benchmarks/bench-hdf5r.R
echo

# 3. h5coro (Python via uv). Soft-skip if uv is missing or
# EARTHDATA_TOKEN is unset — the Python script aborts early in either
# case, but we don't want to fail the whole benchmark over it.
if command -v uv >/dev/null 2>&1; then
  uv run --project benchmarks/python --quiet \
    benchmarks/python/bench_h5coro.py || \
    echo "(h5coro pipeline failed — see stderr above; benchmark continues)"
else
  echo "(uv not found on PATH — skipping h5coro pipeline)"
fi
echo

# 4. Equivalence (handles whichever subset of pipelines produced data)
Rscript benchmarks/equivalence.R
echo

# 5. Render results document
Rscript -e 'rmarkdown::render("benchmarks/BENCHMARK-RESULT.Rmd", quiet = TRUE)'
echo "rendered benchmarks/BENCHMARK-RESULT.md"

# 6. Archive timing parquets (data parquets are gitignored, not archived)
archive_dir="benchmarks/results/archive/${stamp}"
mkdir -p "${archive_dir}"
for f in benchmarks/results/latest/*-timing.parquet \
         benchmarks/results/latest/equivalence.parquet \
         benchmarks/workload.json; do
  if [[ -f "${f}" ]]; then cp "${f}" "${archive_dir}/"; fi
done
echo "archived to ${archive_dir}/"
