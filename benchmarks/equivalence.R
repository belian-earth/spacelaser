# ---------------------------------------------------------------------------
# Cross-pipeline equivalence check
# ---------------------------------------------------------------------------
#
# Reads the *-data.parquet files produced by each bench-* script,
# canonicalises each frame, then does pairwise all.equal() checks on
# all shared columns. Writes a small parquet of pair-wise match
# results to results/latest/equivalence.parquet.
#
# Tolerates any subset of pipelines having run. If only hdf5r and
# spacelaser are present, only that pair is checked. If none overlap,
# this exits cleanly with a warning.

if (!"spacelaser" %in% loadedNamespaces()) library(spacelaser)
source("benchmarks/setup.R")

paths <- bench_paths()

# Which pipelines produced data this run?
pipelines <- c("hdf5r", "spacelaser", "h5coro")
data_files <- setNames(
  vapply(pipelines, paths$data, character(1)),
  pipelines
)
present <- pipelines[file.exists(data_files)]
cli::cli_h2("Equivalence check")
cli::cli_inform("pipelines with data: {.val {present}}")

if (length(present) < 2L) {
  cli::cli_alert_warning(
    "Need at least two pipelines' data to compare — skipping equivalence."
  )
  # Still emit an empty parquet so BENCHMARK-RESULT.Rmd can assume it's there
  arrow::write_parquet(
    data.frame(
      left = character(0), right = character(0),
      n_common_cols = integer(0), row_count_match = logical(0),
      all_match = logical(0), divergent_cols = character(0),
      stringsAsFactors = FALSE
    ),
    paths$equiv
  )
  quit(save = "no")
}

# Load + canonicalise
loaded <- lapply(present, function(p) {
  df <- as.data.frame(arrow::read_parquet(data_files[[p]]))
  bench_canonicalise(df)
})
names(loaded) <- present

# Pairwise checks
pair_check <- function(x, y) {
  common <- intersect(names(x), names(y))
  if (length(common) == 0L)
    return(list(n_common = 0L, row_match = FALSE,
                all_match = FALSE, divergent = "no shared columns"))
  if (nrow(x) != nrow(y))
    return(list(n_common = length(common), row_match = FALSE,
                all_match = FALSE,
                divergent = sprintf("row-count mismatch: %d vs %d", nrow(x), nrow(y))))
  divergent <- character(0)
  for (col in common) {
    if (!isTRUE(all.equal(x[[col]], y[[col]], tolerance = 1e-6))) {
      divergent <- c(divergent, col)
    }
  }
  list(n_common = length(common), row_match = TRUE,
       all_match = length(divergent) == 0L,
       divergent = if (length(divergent) == 0L) "" else paste(divergent, collapse = ","))
}

pairs <- combn(present, 2, simplify = FALSE)
rows <- lapply(pairs, function(pr) {
  r <- pair_check(loaded[[pr[1]]], loaded[[pr[2]]])
  if (r$all_match) {
    cli::cli_alert_success(
      "{pr[1]} vs {pr[2]}: {r$n_common} shared cols, all match"
    )
  } else {
    cli::cli_alert_danger(
      "{pr[1]} vs {pr[2]}: {r$divergent}"
    )
  }
  data.frame(
    left = pr[1], right = pr[2],
    n_common_cols = r$n_common,
    row_count_match = r$row_match,
    all_match = r$all_match,
    divergent_cols = r$divergent,
    stringsAsFactors = FALSE
  )
})
out <- do.call(rbind, rows)
arrow::write_parquet(out, paths$equiv)
cli::cli_alert_success("wrote {.path {paths$equiv}}")
