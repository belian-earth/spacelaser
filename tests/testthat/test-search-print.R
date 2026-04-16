# ---------------------------------------------------------------------------
# Snapshot tests for print.sl_gedi_search / print.sl_icesat2_search
# ---------------------------------------------------------------------------
#
# Pins the one-line header (class | product | count | bbox) plus the
# truncated body and the "... with N more" suffix. Uses synthetic
# search results so the snapshot is independent of live CMR output.

make_gedi_search <- function(n = 3L, product = "L2A",
                             bbox = sl_bbox(-124.04, 41.39, -124.01, 41.42)) {
  df <- tibble::tibble(
    id         = sprintf("G-%02d", seq_len(n)),
    time_start = as.POSIXct("2022-01-01", tz = "UTC") + seq_len(n) * 3600,
    time_end   = as.POSIXct("2022-01-01", tz = "UTC") + seq_len(n) * 3600 + 60,
    url        = sprintf("https://example.test/GEDI02_A_%02d.h5", seq_len(n)),
    geometry   = wk::wkt(rep("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", n),
                         crs = wk::wk_crs_longlat())
  )
  attr(df, "product") <- product
  attr(df, "bbox") <- bbox
  class(df) <- c("sl_gedi_search", class(df))
  df
}

make_icesat2_search <- function(n = 3L, product = "ATL08",
                                bbox = sl_bbox(-124.04, 41.39, -124.01, 41.42)) {
  df <- make_gedi_search(n, product, bbox)
  df$url <- sprintf("https://example.test/ATL08_%02d.h5", seq_len(n))
  attr(df, "product") <- product
  class(df) <- c("sl_icesat2_search", class(df)[-1L])
  df
}

test_that("print.sl_gedi_search renders header and rows", {
  withr::local_options(cli.width = 80, cli.num_colors = 1L)
  expect_snapshot(print(make_gedi_search(3L)))
})

test_that("print.sl_gedi_search truncates after n = 10 with a tail line", {
  withr::local_options(cli.width = 80, cli.num_colors = 1L)
  expect_snapshot(print(make_gedi_search(12L)))
})

test_that("print.sl_gedi_search reports no granules for an empty search", {
  withr::local_options(cli.width = 80, cli.num_colors = 1L)
  expect_snapshot(print(make_gedi_search(0L)))
})

test_that("print.sl_icesat2_search renders header and rows", {
  withr::local_options(cli.width = 80, cli.num_colors = 1L)
  expect_snapshot(print(make_icesat2_search(3L)))
})

test_that("print.sl_icesat2_search reports no granules for an empty search", {
  withr::local_options(cli.width = 80, cli.num_colors = 1L)
  expect_snapshot(print(make_icesat2_search(0L)))
})
