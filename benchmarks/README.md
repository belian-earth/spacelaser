# Benchmarks

End-to-end comparison of spacelaser alongside several alternative
approaches for a GEDI L2A spatial subset query. All pipelines given
the same CMR search result, the same column set, the same equivalence
tolerance.

**For results, see [`BENCHMARK-RESULT.md`](BENCHMARK-RESULT.md)** —
auto-generated from the latest run.

## Layout

```
benchmarks/
├── README.md              this file (structure + how to run)
├── BENCHMARK-RESULT.Rmd   template — reads timing parquets, renders headline
├── BENCHMARK-RESULT.md    rendered output, tracked
├── run.sh                 orchestrator: runs all three pipelines + renders
├── setup.R                shared workload constants (bbox, dates, columns)
├── bench-hdf5r.R          download + read: curl::multi_download + hdf5r
├── bench-spacelaser.R     spacelaser (Rust) pipeline
├── bench-h5coro.R         h5coro (Python via uv) pipeline
├── equivalence.R          cross-pipeline equivalence check
├── python/                uv-managed Python env for h5coro
├── python-gedidb/         uv-managed Python env for gedidb (TileDB query)
└── results/
    ├── latest/            most recent run's outputs
    │   ├── *-timing.parquet   1-row records per pipeline  (tracked)
    │   ├── equivalence.parquet  pairwise match results     (tracked)
    │   └── *-data.parquet     full per-pipeline outputs   (gitignored)
    └── archive/<timestamp>/   historical timings           (tracked)
```

Each `bench-*.R` script is standalone: sources `setup.R`, runs one
pipeline, writes its timing + data parquets. Add a new pipeline by
dropping in another `bench-<name>.R` beside the existing ones, writing
the same two artefacts.

## Run

```sh
# Full four-pipeline run + render + archive
benchmarks/run.sh

# Or each step individually
Rscript benchmarks/bench-hdf5r.R
Rscript benchmarks/bench-spacelaser.R
uv run --project benchmarks/python         benchmarks/python/bench_h5coro.py
uv run --project benchmarks/python-gedidb  benchmarks/python-gedidb/bench_gedidb.py
Rscript benchmarks/equivalence.R
Rscript -e 'rmarkdown::render("benchmarks/BENCHMARK-RESULT.Rmd")'

# Dev iteration: reuse cached downloads across runs
SPACELASER_BENCH_DIR=/tmp/spacelaser-bench-cache benchmarks/run.sh
```

## Prerequisites

- **NASA Earthdata account** with `~/.netrc` entry for
  `urs.earthdata.nasa.gov`. Used by status-quo and spacelaser pipelines.
- **Release-build spacelaser.** `rextendr::document()` generates a
  debug-profile Makevars by default. Rebuild with:
  ```sh
  unset DEBUG
  Rscript -e 'source("tools/config.R")'
  Rscript -e 'devtools::install(quick = TRUE)'
  ```
- For the h5coro pipeline (optional — skips cleanly if missing):
  - [`uv`](https://docs.astral.sh/uv/) on PATH
  - `EARTHDATA_TOKEN` env var set to an EDL user token from
    <https://urs.earthdata.nasa.gov/user_tokens>

## Adding a pipeline

1. Create `bench-<name>.R` that sources `setup.R` and writes
   `results/latest/<name>-timing.parquet` +
   `results/latest/<name>-data.parquet` via the `bench_write_*` helpers.
2. Add a line to `run.sh` that runs it.
3. Add it to the pipeline list in `BENCHMARK-RESULT.Rmd`'s headline chunk.
4. `equivalence.R` picks it up automatically.
