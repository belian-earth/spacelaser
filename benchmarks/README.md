# Benchmarks

End-to-end comparison of spacelaser against the competent-R status-quo
pipeline for a GEDI L2A spatial subset query. Same CMR search results,
same column set, same equivalence tolerance, very different
architectural approach.

## Headline result

Mondah forest, Gabon (0.03° × 0.03° bbox), GEDI L2A, 2020-2021,
11 granules, ecologist-realistic column set
(quality / canopy heights / land cover, including 2D `rh` expansion).

| | spacelaser | status quo (curl + hdf5r) |
|---|---|---|
| **Total wall time** | **69.6 s** | 1,568.3 s |
| Phase split | one phase | download 1,567 s + read 1.3 s |
| Bytes transferred | (small) | 27.2 GiB downloaded |
| Disk used | 0 | 27.2 GiB |
| Rows returned | 1,246 | 1,246 |
| Equivalence (112 shared cols) | bit-perfect, `all.equal` 1e-6 | — |

**Speedup: 22.5×.**

The most striking number isn't actually the speedup — it's the phase
split for status quo: **1,567 seconds spent downloading, 1.3 seconds
spent reading**. The bottleneck isn't HDF5 parsing or any other CPU
work; it's that the standard pattern requires moving every byte to
disk before you can filter to the few thousand shots you actually
care about. Spacelaser sidesteps the move entirely by issuing
targeted HTTP Range requests against the remote files.

For repeat queries against different bboxes, this gap compounds: you
re-pay 27 GiB of download per query in the status-quo pattern, while
spacelaser stays in the ~70 s ballpark.

## Variance

Across multiple cold-cache runs we have observed the status-quo
download phase fluctuate from ~1,100 s to ~1,600 s purely on NASA
network conditions, while spacelaser sits steadily in the 70-80 s
range. The honest framing of the speedup is therefore:

> spacelaser ~70 s consistently; status quo 1,100-1,600 s depending on
> NASA's mood; speedup robustly in the **15-22×** band.

The worst-case status-quo number is bounded by network throughput
into your machine; the best-case is bounded by NASA's serving rate.
Spacelaser's variance is much smaller because it transfers far fewer
bytes — fewer bytes means less exposure to transient throttling.

## What we compare

Two pipelines given the same CMR search result:

1. **Status quo** — `curl::multi_download()` of the full HDF5 granules
   (concurrent, netrc-authenticated, following the URS OAuth redirect
   chain), then `hdf5r` with targeted row reads: full-read lat/lon,
   compute spatial filter indices, then `ds[, idx]` on each requested
   column at those rows only. `data.table::rbindlist()` for assembly.
   No straw man — best practice for the pattern.
2. **Spacelaser** — a single `sl_read(granules, bbox, columns)` call.
   HTTP Range reads against remote granules, Rust-side spatial filter
   per beam, concurrent column fetches, no local storage.

CMR search itself is excluded from per-pipeline timings; both hit the
same unauthenticated endpoint via `httr2`.

## Equivalence check

Before timings are reported the script canonicalises both outputs (sort
by `beam`, `lat_lowestmode`, `lon_lowestmode`), drops pipeline-specific
columns (`geometry` because `wk_xy` doesn't compare element-wise;
`time` because spacelaser converts to POSIXct while raw `delta_time`
is what comes back from hdf5r), and asserts every shared column
matches to `all.equal()` tolerance `1e-6`. **If the two pipelines
disagree, the benchmark refuses to report any speedup.**

The test column set excludes `shot_number` (uint64). R has no native
u64 type; spacelaser widens via `hi*2^32 + lo`, hdf5r uses
`bit64::integer64` via a tangled option chain, and the two don't
produce comparable doubles. The bulk-data pipeline comparison doesn't
need it — `(beam, lat, lon)` aligns rows uniquely within the bbox.

## Workload

See `setup.R` for the definitions:

- **Region**: Mondah forest, Gabon (real GEDI calibration site, dense
  tropical canopy)
- **Bbox**: 0.03° × 0.03° (matches the package's other test bboxes,
  realistic plot-scale spatial query)
- **Date range**: 2020-2021 (2 years)
- **Product**: GEDI L2A
- **Columns**: ecologist-realistic mix —
  `quality_flag, degrade_flag, sensitivity, elev_lowestmode,
  elev_highestreturn, solar_elevation, rh (→ rh0..rh100),
  landsat_treecover, modis_treecover`

## Reproducing

Requires a NASA Earthdata account and credentials in `~/.netrc` (used
by both pipelines).

The Rust crate **must be compiled with `--release`** for the spacelaser
side to be measured fairly. `rextendr::document()` (the standard
dev-iteration command) writes a debug-profile Makevars by default,
which produces a binary somewhat slower than release. The benchmark
script will refuse to run on a debug build via `rust_is_debug()`.

```sh
# One-time: ensure release build
unset DEBUG
Rscript -e 'source("tools/config.R")'              # rewrites src/Makevars
Rscript -e 'devtools::install(quick = TRUE)'        # release rebuild + install

# Cold-cache run (deletes downloads after — the honest comparison)
Rscript benchmarks/compare.R

# Dev iteration: reuse cached granules across runs
SPACELASER_BENCH_DIR=/tmp/spacelaser-bench-cache Rscript benchmarks/compare.R
```

Each cold-cache run takes ~25-30 minutes (status quo dominated by
download), needs ~30 GB of free disk, and uses ~0.3 GB after the
status-quo pipeline cleans up.

Results archive to `benchmarks/results/<timestamp>.csv` for historical
reference.

## Caveats

- **One region, one product, one bbox size.** A 1° × 1° bbox or
  ATL03 photon-level reads would shift the absolute numbers; the
  ratio shouldn't change much because the structural advantage
  (no full file download) is independent of workload size.
- **Network-bound throughout.** Both pipelines are bottlenecked on
  HTTP. Variance from NASA-side serving rate dominates the noise.
- **Status-quo includes only the file-fetch + read phases.** Real
  user workflows often add cataloguing, caching, or polygon
  filtering on top; those are downstream and would apply equally
  to either pipeline's output.
- **No CDN / browser cache poisoning.** Cold-cache runs use a fresh
  tempdir; there's no cross-run leakage.
