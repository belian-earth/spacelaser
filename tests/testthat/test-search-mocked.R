# ---------------------------------------------------------------------------
# sl_search() — full pipeline tests with mocked CMR responses
# ---------------------------------------------------------------------------
#
# These tests exercise sl_search() end-to-end against recorded CMR responses
# using httptest2. They cover URL construction, pagination, JSON parsing,
# result-class construction, and attribute propagation without hitting the
# network.
#
# To re-record fixtures: delete the relevant subdirectory under
# `tests/testthat/_mocks/` and re-run the tests with a network connection.
# CMR is unauthenticated, so no Earthdata credentials are needed.

test_bbox_search <- function() {
  sl_bbox(-124.04, 41.39, -124.01, 41.42)
}
test_dates_search <- function() {
  list(start = "2020-06-01", end = "2020-09-01")
}

# Per-product: every product-sensor combination resolves to a valid
# search result with the expected class and attributes. One mock dir
# per product. The bbox and date range are identical across products
# so fixture sizes stay small.
#
# Keeping these as 12 discrete tests (rather than a loop) so a failure
# points at a specific product in the testthat output.

expect_search_result <- function(granules, product, sensor, bbox) {
  cls <- paste0("sl_", sensor, "_search")
  testthat::expect_s3_class(granules, cls)
  testthat::expect_identical(attr(granules, "product"), product)
  testthat::expect_identical(attr(granules, "bbox"), bbox)
  testthat::expect_true(all(
    c("id", "time_start", "time_end", "url", "geometry") %in% names(granules)
  ))
  if (nrow(granules) > 0L) {
    testthat::expect_true(all(grepl("\\.h5$", granules$url, ignore.case = TRUE)))
  }
}

# ---- GEDI ----------------------------------------------------------------

test_that("sl_search L1B returns sl_gedi_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("search-l1b", {
    g <- sl_search(bb, product = "L1B", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "L1B", "gedi", bb)
  })
})

test_that("sl_search L2A returns sl_gedi_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("search-l2a", {
    g <- sl_search(bb, product = "L2A", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "L2A", "gedi", bb)
  })
})

test_that("sl_search L2B returns sl_gedi_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("search-l2b", {
    g <- sl_search(bb, product = "L2B", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "L2B", "gedi", bb)
  })
})

test_that("sl_search L4A returns sl_gedi_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("search-l4a", {
    g <- sl_search(bb, product = "L4A", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "L4A", "gedi", bb)
  })
})

test_that("sl_search L4C returns sl_gedi_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("search-l4c", {
    g <- sl_search(bb, product = "L4C", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "L4C", "gedi", bb)
  })
})

# ---- ICESat-2 ------------------------------------------------------------

test_that("sl_search ATL03 returns sl_icesat2_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("search-atl03", {
    g <- sl_search(bb, product = "ATL03", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "ATL03", "icesat2", bb)
  })
})

test_that("sl_search ATL06 returns sl_icesat2_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("search-atl06", {
    g <- sl_search(bb, product = "ATL06", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "ATL06", "icesat2", bb)
  })
})

test_that("sl_search ATL07 returns sl_icesat2_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("search-atl07", {
    g <- sl_search(bb, product = "ATL07", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "ATL07", "icesat2", bb)
  })
})

test_that("sl_search ATL08 returns sl_icesat2_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("search-atl08", {
    g <- sl_search(bb, product = "ATL08", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "ATL08", "icesat2", bb)
  })
})

test_that("sl_search ATL10 returns sl_icesat2_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("search-atl10", {
    g <- sl_search(bb, product = "ATL10", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "ATL10", "icesat2", bb)
  })
})

test_that("sl_search ATL13 returns sl_icesat2_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("search-atl13", {
    g <- sl_search(bb, product = "ATL13", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "ATL13", "icesat2", bb)
  })
})

test_that("sl_search ATL24 returns sl_icesat2_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("search-atl24", {
    g <- sl_search(bb, product = "ATL24", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "ATL24", "icesat2", bb)
  })
})

# ---- Cross-cutting -------------------------------------------------------

test_that("sl_search returns an empty result for a bbox with no coverage", {
  # A polar bbox that GEDI (lat band ±51.6°) will never cover.
  skip_unless_httptest2()
  bb <- sl_bbox(0, 85, 1, 86)
  with_cmr_mock("search-empty-gedi", {
    g <- sl_search(bb, product = "L2A",
                   date_start = "2020-06-01", date_end = "2020-07-01")
    expect_s3_class(g, "sl_gedi_search")
    expect_equal(nrow(g), 0L)
    expect_identical(attr(g, "product"), "L2A")
  })
})

test_that("sl_search result survives [ subsetting with attributes intact", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("search-l2a", {
    g <- sl_search(bb, product = "L2A", date_start = dr$start, date_end = dr$end)
    if (nrow(g) == 0L) {
      skip("No granules in fixture to subset")
    }
    sub <- g[1, ]
    expect_s3_class(sub, "sl_gedi_search")
    expect_identical(attr(sub, "product"), "L2A")
    expect_identical(attr(sub, "bbox"), bb)
    expect_equal(nrow(sub), 1L)
  })
})
