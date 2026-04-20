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
#   0. search.R          one CMR call -> granules.parquet (shared input
#                        for pipelines 1-3; gedidb does its own query)
#   1. bench-spacelaser  Rust, partial HTTP range reads
#   2. bench-hdf5r       Status quo: curl::multi_download + hdf5r
#   3. bench-h5coro      Python via uv, partial HTTP range reads
#   4. bench-gedidb      Python via uv, query pre-indexed GFZ TileDB
#   5. equivalence       cross-pipeline comparison
#   6. render            BENCHMARK-RESULT.Rmd -> .md
#   7. archive           timing parquets -> archive/<timestamp>/
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

# 4. gedidb (Python via uv). Queries the public GFZ TileDB; no NASA
# Earthdata credentials required. Soft-skip on failure so one pipeline
# going down doesn't block the others.
if command -v uv >/dev/null 2>&1; then
  uv run --project benchmarks/python-gedidb --quiet \
    benchmarks/python-gedidb/bench_gedidb.py || \
    echo "(gedidb pipeline failed — see stderr above; benchmark continues)"
else
  echo "(uv not found on PATH — skipping gedidb pipeline)"
fi
echo

# 5. Equivalence (handles whichever subset of pipelines produced data)
Rscript benchmarks/equivalence.R
echo

# 6. Render results document
Rscript -e 'rmarkdown::render("benchmarks/BENCHMARK-RESULT.Rmd", quiet = TRUE)'
echo "rendered benchmarks/BENCHMARK-RESULT.md"

# 7. Archive timing parquets (data parquets are gitignored, not archived)
archive_dir="benchmarks/results/archive/${stamp}"
mkdir -p "${archive_dir}"
for f in benchmarks/results/latest/*-timing.parquet \
         benchmarks/results/latest/equivalence.parquet \
         benchmarks/workload.json; do
  if [[ -f "${f}" ]]; then cp "${f}" "${archive_dir}/"; fi
done
echo "archived to ${archive_dir}/"
