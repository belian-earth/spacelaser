# ---------------------------------------------------------------------------
# Shared benchmark setup
# ---------------------------------------------------------------------------
#
# Defines the workload that both pipelines (spacelaser vs download +
# hdf5r) are benchmarked against, and runs the CMR search once so both
# operate on an identical granule list.
#
# Sourced by `compare.R`. Not a standalone script.

# Always benchmark against an installed --release build. devtools::load_all()
# triggers rextendr::document() which sets DEBUG=true and produces a debug
# binary (~6-15% slower for our workload, but anywhere from 2-10x slower for
# byte-heavy work in general). The Rust side exposes rust_is_debug() so we
# can fail fast if someone is about to time the wrong binary.
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

# Mondah forest, Gabon — well-known GEDI calibration site, dense tropical
# canopy. 0.03° × 0.03° at this location and date range yields ~11 granules,
# small enough for ~30 GB cold-cache downloads on a typical dev machine
# while still producing a meaningful spatial subset workload (~1200 shots).
# This is the canonical "headline" benchmark workload. For scaling
# experiments, bump bbox or extend BENCH_END (see results/ for past runs).
BENCH_BBOX  <- sl_bbox(9.32, 0.55, 9.35, 0.58)
BENCH_START <- "2020-01-01"
BENCH_END   <- "2021-12-31"
BENCH_PRODUCT <- "L2A"

# Ecologist-realistic column set — mixes 1D scalars, 2D (`rh` → rh0..rh100),
# and nested-subgroup columns (land_cover_data/*).
# Note: GEDI's `shot_number` is uint64 which R has no native type for.
# spacelaser widens via hi*2^32+lo arithmetic; hdf5r uses bit64 via a
# tangled option chain. To keep the benchmark focused on the bulk-data
# pipeline (which is where the speedup story lives), we skip shot_number
# here and sort-align rows via (beam, lat, lon).
BENCH_COLUMNS <- c(
  "quality_flag", "degrade_flag", "sensitivity",
  "elev_lowestmode", "elev_highestreturn", "solar_elevation",
  "rh",
  "landsat_treecover", "modis_treecover"
)

# Shared granule list — produced by one CMR search, used by both pipelines.
# Search time is excluded from per-pipeline timings (it's the same
# unauthenticated httr2 call either way).
bench_search <- function() {
  cli::cli_inform("Searching CMR ({BENCH_PRODUCT}, {BENCH_START}..{BENCH_END})")
  t0 <- Sys.time()
  granules <- sl_search(
    BENCH_BBOX,
    product = BENCH_PRODUCT,
    date_start = BENCH_START,
    date_end = BENCH_END
  )
  elapsed <- as.numeric(Sys.time() - t0, units = "secs")
  cli::cli_inform("  → {nrow(granules)} granules in {round(elapsed, 2)}s")
  list(granules = granules, search_seconds = elapsed)
}

# Canonicalise a read result so we can compare across pipelines without
# caring about row order (per-beam parallelism produces different orderings).
bench_canonicalise <- function(df) {
  if (nrow(df) == 0L) return(df)
  stopifnot(all(c("beam", "lat_lowestmode", "lon_lowestmode") %in% names(df)))
  df <- as.data.frame(df)
  # Sort-align rows across pipelines. (beam, lat, lon) is unique per
  # shot within our test bbox — beam identifies which of 8 orbital
  # tracks, lat/lon pin the shot deterministically.
  df <- df[order(df$beam, df$lat_lowestmode, df$lon_lowestmode), , drop = FALSE]
  # Drop geometry (wk_xy doesn't compare cleanly element-wise) and time
  # (POSIXct-vs-numeric differs by pipeline but the content is in the
  # raw HDF5 columns). shot_number is u64 and isn't in our test set.
  df$geometry <- NULL
  df$time <- NULL
  rownames(df) <- NULL
  df
}
