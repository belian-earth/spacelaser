# ---------------------------------------------------------------------------
# Spacelaser pipeline
# ---------------------------------------------------------------------------
#
# Single call to sl_read() on the search result. Partial HTTP reads,
# per-beam spatial filter in Rust, concurrent column fetches, no local
# storage.

run_spacelaser_pipeline <- function(granules, bbox, columns) {
  t_start <- Sys.time()
  result <- sl_read(granules, bbox = bbox, columns = columns)
  t_end <- Sys.time()

  list(
    result = result,
    seconds_total = as.numeric(t_end - t_start, units = "secs"),
    # There's no separate "download" phase for spacelaser — HTTP Range
    # requests and extraction interleave. We report total only.
    seconds_download = NA_real_,
    seconds_read     = NA_real_,
    # Bytes on the wire: not instrumented. It's clearly much less than
    # the status-quo full-download byte count, which is the story.
    bytes_downloaded = NA_real_,
    n_granules       = nrow(granules)
  )
}
