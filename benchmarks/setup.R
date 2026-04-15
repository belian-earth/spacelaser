# ---------------------------------------------------------------------------
# Shared benchmark setup
# ---------------------------------------------------------------------------
#
# Defines the workload that both pipelines (spacelaser vs download +
# hdf5r) are benchmarked against, and runs the CMR search once so both
# operate on an identical granule list.
#
# Sourced by `compare.R`. Not a standalone script.

# Spacelaser itself should already be loaded before sourcing this file
# (either via `devtools::load_all(".")` during dev, or `library(spacelaser)`
# when installed). That lets the same benchmark scripts work from a dev
# checkout and from an installed package.
if (!"spacelaser" %in% loadedNamespaces()) {
  if (requireNamespace("spacelaser", quietly = TRUE)) {
    library(spacelaser)
  } else if (requireNamespace("devtools", quietly = TRUE) && file.exists("DESCRIPTION")) {
    devtools::load_all(".", quiet = TRUE)
  } else {
    stop("Install spacelaser or run from a dev checkout with devtools available.")
  }
}
suppressPackageStartupMessages({
  library(data.table)
})

# Mondah forest, Gabon — well-known GEDI calibration site, dense tropical
# canopy. 0.03° × 0.03° matches the PNW test bbox, same order of magnitude
# as a typical plot-scale spatial query. If granule counts come in too
# low/high, tune these.
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
