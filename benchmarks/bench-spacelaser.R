# ---------------------------------------------------------------------------
# Spacelaser pipeline
# ---------------------------------------------------------------------------
#
# Single call to sl_read() on the CMR search result. Partial HTTP
# reads, per-beam spatial filter in Rust, concurrent column fetches,
# no local storage.
#
# Invoke standalone:
#   Rscript benchmarks/bench-spacelaser.R
#
# Writes:
#   results/latest/spacelaser-timing.parquet  (1 row, tracked)
#   results/latest/spacelaser-data.parquet    (full data, gitignored)

if (!"spacelaser" %in% loadedNamespaces()) library(spacelaser)
source("benchmarks/setup.R")

run_spacelaser_pipeline <- function(granules, bbox, columns) {
  t_start <- Sys.time()
  result <- sl_read(granules, bbox = bbox, columns = columns)
  t_end <- Sys.time()

  list(
    result           = result,
    seconds_total    = as.numeric(t_end - t_start, units = "secs"),
    # spacelaser has no separate download phase; reads are interleaved
    # with extraction across HTTP range requests.
    seconds_download = NA_real_,
    seconds_read     = NA_real_,
    bytes_downloaded = NA_real_,
    n_granules       = nrow(granules)
  )
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if (sys.nframe() == 0L || !exists(".bench_sourced", inherits = FALSE)) {
  paths <- bench_paths()
  cli::cli_h2("Spacelaser: partial HTTP range reads (Rust)")

  granules <- bench_load_granules()

  run <- run_spacelaser_pipeline(granules, BENCH_BBOX, BENCH_COLUMNS)

  cli::cli_inform(c(
    "total: {round(run$seconds_total, 1)}s",
    "rows: {nrow(run$result)}"
  ))

  bench_write_timing(
    pipeline      = "spacelaser",
    seconds_total = run$seconds_total,
    n_rows        = nrow(run$result),
    n_granules    = run$n_granules
  )
  bench_write_data("spacelaser", run$result)
  cli::cli_alert_success("wrote {.path {paths$timing('spacelaser')}}")
}
