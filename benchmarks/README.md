# Benchmarks

End-to-end comparison of spacelaser against the competent-R status-quo
pipeline for a GEDI L2A spatial subset query.

## What we compare

Two pipelines given the same CMR search result:

1. **Status quo** — `curl::multi_download()` of the full HDF5 granules
   (concurrent, netrc-authenticated, following the URS OAuth redirect
   chain), then `hdf5r` with targeted row reads: full-read lat/lon,
   compute spatial filter indices, then read each requested column at
   those rows only. `data.table::rbindlist()` for assembly.
2. **Spacelaser** — a single `sl_read(granules, bbox, columns)` call.
   HTTP Range reads, Rust-side spatial filter per beam, concurrent
   column fetches, no local storage.

CMR search is excluded from per-pipeline timings; both hit the same
unauthenticated endpoint via `httr2`.

## Running

Requires Earthdata credentials in `~/.netrc` (used by both pipelines).

```sh
# Cold-cache run (deletes downloads after)
Rscript benchmarks/compare.R

# Dev iteration: reuse cached granules across runs
SPACELASER_BENCH_DIR=/tmp/spacelaser-bench-cache Rscript benchmarks/compare.R
```

Results archive to `benchmarks/results/<timestamp>.csv`.

## Equivalence check

Before timings are reported the script canonicalises both outputs (sort
by `beam`, `lat_lowestmode`, `lon_lowestmode`), drops pipeline-specific
columns (`geometry`, `time` — see note below), and asserts every
shared column matches to `all.equal()` tolerance `1e-6`. If the two
pipelines disagree, the benchmark refuses to report speedups.

The test column set excludes `shot_number` (uint64) because R has no
native u64 type: spacelaser widens via `hi*2^32 + lo`, hdf5r uses
`bit64::integer64` via a tangled option chain, and the two don't
produce comparable doubles. The bulk-data pipeline comparison doesn't
need it — `(beam, lat, lon)` aligns rows uniquely within the bbox.

## Workload

See `setup.R` for the definitions. Kept small to stay friendly to
modest disk budgets:

- **Region**: Mondah forest, Gabon (real GEDI calibration site, dense
  tropical canopy).
- **Bbox size**: on the order of the package's other test bboxes
  (0.03° square), occasionally scaled up for scaling experiments.
- **Date range**: 2020-2021 (2 years).
- **Columns**: ecologist-realistic mix of scalars, a 2D expansion
  (`rh` → `rh0..rh100`), and nested-subgroup columns
  (`land_cover_data/*`).
