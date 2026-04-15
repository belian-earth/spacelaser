# ---------------------------------------------------------------------------
# Step 0 of the benchmark: CMR granule search
# ---------------------------------------------------------------------------
#
# Reads benchmarks/workload.json, calls sl_search() once, writes the
# resulting granule list to results/latest/granules.parquet. Each
# bench-* script (R or Python) reads that parquet so all three
# pipelines operate on exactly the same input — no risk of CMR drift
# between pipelines mid-run, and no triple-querying.
#
# Invoke standalone:
#   Rscript benchmarks/search.R
# Or via run.sh as the first step.

source("benchmarks/setup.R")

paths <- bench_paths()

cli::cli_h2("CMR search")
cli::cli_inform(
  "{BENCH_PRODUCT}, {BENCH_START}..{BENCH_END}, bbox = {format(BENCH_BBOX)}"
)

t0 <- Sys.time()
granules <- sl_search(
  BENCH_BBOX,
  product    = BENCH_PRODUCT,
  date_start = BENCH_START,
  date_end   = BENCH_END
)
elapsed <- as.numeric(Sys.time() - t0, units = "secs")

if (nrow(granules) == 0L) {
  cli::cli_abort("No granules found for the workload — check workload.json.")
}

# Drop geometry (wk_wkt doesn't round-trip cleanly through parquet —
# arrow doesn't know the wk classes; the bench scripts only need url +
# id + times anyway).
df <- as.data.frame(granules)
df$geometry <- NULL

arrow::write_parquet(df, paths$granules)
cli::cli_alert_success(
  "{nrow(df)} granule{?s} in {round(elapsed, 1)}s -> {.path {paths$granules}}"
)
