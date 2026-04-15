# ---------------------------------------------------------------------------
# Orchestrator: run both pipelines, verify equivalence, report results
# ---------------------------------------------------------------------------
#
# Usage: Rscript benchmarks/compare.R
#
# Produces:
#   - Stdout: a human-readable summary table
#   - benchmarks/results/<date>.csv: raw phase-level timings for archiving

here <- function(...) file.path("benchmarks", ...)
source(here("setup.R"))
source(here("bench-hdf5r.R"))
source(here("bench-spacelaser.R"))

# ---------------------------------------------------------------------------
# 1. CMR search (shared)
# ---------------------------------------------------------------------------

search <- bench_search()
granules <- search$granules
if (nrow(granules) == 0L) {
  cli::cli_abort("No granules found; adjust BENCH_BBOX or BENCH_START/END.")
}
cli::cli_inform("")

# ---------------------------------------------------------------------------
# 2. Status-quo pipeline
# ---------------------------------------------------------------------------

cli::cli_h2("Status quo: curl::multi_download + hdf5r")
# Dest dir is configurable via SPACELASER_BENCH_DIR. Default is a tempdir
# which makes cold-cache honest. Set to a persistent path during dev
# iteration to avoid re-downloading 20+ GB every time.
dest_dir <- Sys.getenv("SPACELASER_BENCH_DIR", unset = "")
if (!nzchar(dest_dir)) {
  dest_dir <- tempfile("bench-hdf5r-")
  on.exit(unlink(dest_dir, recursive = TRUE, force = TRUE), add = TRUE)
  cli::cli_inform("download dir: {.path {dest_dir}} (cold cache — deleted after run)")
} else {
  dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
  cli::cli_inform("download dir: {.path {dest_dir}} (persistent — reuses existing files)")
}

hdf5r_run <- run_hdf5r_pipeline(
  granules, BENCH_BBOX, BENCH_COLUMNS, dest_dir
)
cli::cli_inform(c(
  "download: {round(hdf5r_run$seconds_download, 1)}s, \\
   read: {round(hdf5r_run$seconds_read, 1)}s, \\
   total: {round(hdf5r_run$seconds_total, 1)}s",
  "bytes on disk: {format(structure(hdf5r_run$bytes_downloaded, \\
    class = \"object_size\"), units = \"auto\")}",
  "rows: {nrow(hdf5r_run$result)}"
))
cli::cli_inform("")

# ---------------------------------------------------------------------------
# 3. Spacelaser pipeline
# ---------------------------------------------------------------------------

cli::cli_h2("Spacelaser: partial HTTP range reads")
sl_run <- run_spacelaser_pipeline(granules, BENCH_BBOX, BENCH_COLUMNS)
cli::cli_inform(c(
  "total: {round(sl_run$seconds_total, 1)}s",
  "rows: {nrow(sl_run$result)}"
))
cli::cli_inform("")

# ---------------------------------------------------------------------------
# 4. Equivalence check
# ---------------------------------------------------------------------------
#
# Both pipelines should produce the same shots, the same lat/lon, and the
# same science columns within floating-point tolerance. Order differs
# (per-beam parallelism) so we canonicalise before comparing.

cli::cli_h2("Equivalence check")
a <- bench_canonicalise(hdf5r_run$result)
b <- bench_canonicalise(sl_run$result)

row_match <- nrow(a) == nrow(b)
cli::cli_inform("rows:           {nrow(a)} (hdf5r) vs {nrow(b)} (spacelaser)")

common_cols <- intersect(names(a), names(b))
only_hdf5r  <- setdiff(names(a), names(b))
only_sl     <- setdiff(names(b), names(a))
cli::cli_inform("columns shared: {length(common_cols)}")
if (length(only_hdf5r) > 0L)
  cli::cli_inform("only in hdf5r:  {paste(only_hdf5r, collapse = ', ')}")
if (length(only_sl) > 0L)
  cli::cli_inform("only in spacelaser: {paste(only_sl, collapse = ', ')}")

# Per-column numeric check
if (row_match) {
  divergent <- character(0)
  for (col in common_cols) {
    ea <- a[[col]]; eb <- b[[col]]
    ok <- isTRUE(all.equal(ea, eb, tolerance = 1e-6))
    if (!ok) divergent <- c(divergent, col)
  }
  if (length(divergent) == 0L) {
    cli::cli_alert_success("All {length(common_cols)} shared columns match numerically.")
  } else {
    cli::cli_alert_danger("Divergent columns: {paste(divergent, collapse = ', ')}")
  }
}
cli::cli_inform("")

# ---------------------------------------------------------------------------
# 5. Summary table + archive
# ---------------------------------------------------------------------------

speedup <- round(hdf5r_run$seconds_total / sl_run$seconds_total, 1)
cli::cli_h2("Summary")
cli::cli_inform(c(
  "granules searched: {nrow(granules)}",
  "spacelaser total:  {round(sl_run$seconds_total, 1)}s",
  "status quo total:  {round(hdf5r_run$seconds_total, 1)}s",
  "speedup:           {speedup}x",
  "bytes not downloaded: {format(structure(hdf5r_run$bytes_downloaded, \\
    class = \"object_size\"), units = \"auto\")}"
))

# Archive
stamp <- format(Sys.time(), "%Y-%m-%d-%H%M%S")
results <- data.frame(
  timestamp        = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
  bbox             = paste(unclass(BENCH_BBOX), collapse = ","),
  date_start       = BENCH_START,
  date_end         = BENCH_END,
  product          = BENCH_PRODUCT,
  n_granules       = nrow(granules),
  hdf5r_dl_s       = round(hdf5r_run$seconds_download, 2),
  hdf5r_read_s     = round(hdf5r_run$seconds_read, 2),
  hdf5r_total_s    = round(hdf5r_run$seconds_total, 2),
  hdf5r_bytes      = hdf5r_run$bytes_downloaded,
  hdf5r_rows       = nrow(hdf5r_run$result),
  sl_total_s       = round(sl_run$seconds_total, 2),
  sl_rows          = nrow(sl_run$result),
  speedup          = speedup,
  stringsAsFactors = FALSE
)
results_path <- here("results", sprintf("%s.csv", stamp))
write.csv(results, results_path, row.names = FALSE)
cli::cli_inform("archived: {.file {results_path}}")
