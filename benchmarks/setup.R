# ---------------------------------------------------------------------------
# Shared benchmark setup (R bench scripts only)
# ---------------------------------------------------------------------------
#
# Loads the workload spec from workload.json and exposes it as
# constants every R bench script can use, plus the path conventions
# and timing/data writers. CMR search lives in search.R (one-shot,
# called once per benchmark run); each bench-*.R reads the resulting
# granules.parquet rather than re-querying CMR.
#
# Sourced by bench-hdf5r.R, bench-spacelaser.R, equivalence.R.

# Always benchmark against an installed --release build. devtools::load_all()
# triggers rextendr::document() which sets DEBUG=true and produces a debug
# binary (~6-15% slower for our workload, but anywhere from 2-10x slower for
# byte-heavy work in general). The Rust side exposes rust_is_debug() so we
# can fail fast if someone is about to time the wrong binary.
#
# TODO: once the Makevars/build flow no longer silently produces debug
# binaries during dev iteration, drop rust_is_debug() + this guard and
# the corresponding Rust FFI export.
if (!"spacelaser" %in% loadedNamespaces()) {
  if (!requireNamespace("spacelaser", quietly = TRUE)) {
    if (requireNamespace("devtools", quietly = TRUE) && file.exists("DESCRIPTION")) {
      message("spacelaser not installed — installing from source (release build)…")
      Sys.unsetenv("DEBUG")  # rextendr::document() sets this; clear before install
      devtools::install(quick = TRUE, quiet = TRUE, upgrade = "never")
    } else {
      stop(
        "Install spacelaser first:\n",
        "  Sys.unsetenv('DEBUG'); devtools::install(quick = TRUE)\n",
        "load_all() is deliberately not used here — it produces debug builds."
      )
    }
  }
  library(spacelaser)
}
if (isTRUE(spacelaser:::rust_is_debug())) {
  stop(
    "Loaded spacelaser was built with debug assertions on. Benchmark would\n",
    "underestimate spacelaser's true performance. Rebuild with:\n",
    "  Sys.unsetenv('DEBUG')\n",
    "  Rscript -e 'source(\"tools/config.R\")'   # rewrites src/Makevars\n",
    "  Rscript -e 'devtools::install(quick = TRUE)'"
  )
}
suppressPackageStartupMessages({
  library(data.table)
})

# ---------------------------------------------------------------------------
# Workload spec — loaded from benchmarks/workload.json
# ---------------------------------------------------------------------------

bench_workload <- function() {
  jsonlite::fromJSON("benchmarks/workload.json", simplifyVector = FALSE)
}

.workload <- bench_workload()
BENCH_BBOX  <- sl_bbox(
  .workload$bbox$xmin, .workload$bbox$ymin,
  .workload$bbox$xmax, .workload$bbox$ymax
)
BENCH_START   <- .workload$date_start
BENCH_END     <- .workload$date_end
BENCH_PRODUCT <- .workload$product
BENCH_COLUMNS <- unlist(.workload$columns)
rm(.workload)

# ---------------------------------------------------------------------------
# Paths (single source of truth for the directory layout)
# ---------------------------------------------------------------------------

bench_paths <- function() {
  root   <- "benchmarks"
  latest <- file.path(root, "results", "latest")
  dir.create(latest, recursive = TRUE, showWarnings = FALSE)
  list(
    root       = root,
    python_dir = file.path(root, "python"),
    workload   = file.path(root, "workload.json"),
    granules   = file.path(latest, "granules.parquet"),
    latest     = latest,
    archive    = file.path(root, "results", "archive"),
    equiv      = file.path(latest, "equivalence.parquet"),
    timing     = function(pipeline) file.path(latest, sprintf("%s-timing.parquet", pipeline)),
    data       = function(pipeline) file.path(latest, sprintf("%s-data.parquet",   pipeline))
  )
}

# Read the granule list written by search.R. Rebuilds the sl_gedi_search
# class + bbox/product attrs so spacelaser's S3 dispatch still works.
bench_load_granules <- function() {
  paths <- bench_paths()
  if (!file.exists(paths$granules)) {
    stop(
      "Run benchmarks/search.R first (or the full benchmarks/run.sh).\n",
      "Missing: ", paths$granules
    )
  }
  df <- as.data.frame(arrow::read_parquet(paths$granules))
  # Re-attach class + attrs so sl_read.sl_gedi_search dispatches.
  # spacelaser:::new_sl_search() does this; granules.parquet drops the
  # geometry column on write (wk_xy doesn't round-trip), but spacelaser
  # only needs `url` from the frame anyway.
  spacelaser:::new_sl_search(
    df, product = BENCH_PRODUCT, bbox = BENCH_BBOX, sensor = "gedi"
  )
}

# ---------------------------------------------------------------------------
# Per-run output writers
# ---------------------------------------------------------------------------

# One-row timing record. Tracked in git so changes show up in the diff.
bench_write_timing <- function(pipeline,
                               seconds_total,
                               seconds_download = NA_real_,
                               seconds_read     = NA_real_,
                               bytes_downloaded = NA_real_,
                               n_rows           = NA_integer_,
                               n_granules       = NA_integer_,
                               notes            = NA_character_) {
  paths <- bench_paths()
  row <- data.frame(
    pipeline         = pipeline,
    timestamp        = format(Sys.time(), "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC"),
    bbox             = paste(unclass(BENCH_BBOX), collapse = ","),
    date_start       = BENCH_START,
    date_end         = BENCH_END,
    product          = BENCH_PRODUCT,
    n_granules       = as.integer(n_granules),
    n_rows           = as.integer(n_rows),
    seconds_total    = as.numeric(seconds_total),
    seconds_download = as.numeric(seconds_download),
    seconds_read     = as.numeric(seconds_read),
    bytes_downloaded = as.numeric(bytes_downloaded),
    notes            = notes,
    stringsAsFactors = FALSE
  )
  arrow::write_parquet(row, paths$timing(pipeline))
}

# Full per-pipeline output. Gitignored — large, regenerated every run.
bench_write_data <- function(pipeline, data) {
  paths <- bench_paths()
  df <- as.data.frame(data)
  # Drop columns that don't round-trip through parquet cleanly:
  # - geometry (wk_xy): arrow doesn't know this S3 class
  # - time (POSIXct): keeps fine, but equivalence check doesn't need it
  df$geometry <- NULL
  df$time <- NULL
  arrow::write_parquet(df, paths$data(pipeline))
}

# Canonicalise a per-pipeline result so cross-pipeline comparison ignores
# row order (per-beam parallelism is non-deterministic). Used by
# equivalence.R; ordering uses (beam, lat, lon) which is unique per shot
# within the bbox.
bench_canonicalise <- function(df) {
  if (nrow(df) == 0L) return(df)
  stopifnot(all(c("beam", "lat_lowestmode", "lon_lowestmode") %in% names(df)))
  df <- as.data.frame(df)
  df <- df[order(df$beam, df$lat_lowestmode, df$lon_lowestmode), , drop = FALSE]
  df$geometry <- NULL
  df$time <- NULL
  rownames(df) <- NULL
  df
}
