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
  with_cmr_mock("l1b", {
    g <- sl_search(bb, product = "L1B", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "L1B", "gedi", bb)
  })
})

test_that("sl_search L2A returns sl_gedi_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("l2a", {
    g <- sl_search(bb, product = "L2A", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "L2A", "gedi", bb)
  })
})

test_that("sl_search L2B returns sl_gedi_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("l2b", {
    g <- sl_search(bb, product = "L2B", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "L2B", "gedi", bb)
  })
})

test_that("sl_search L4A returns sl_gedi_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("l4a", {
    g <- sl_search(bb, product = "L4A", date_start = dr$start, date_end = dr$end, version = "2.1")
    expect_search_result(g, "L4A", "gedi", bb)
    expect_identical(attr(g, "version"), "2.1")
    expect_true(all(grepl("_V002", g$url)))
  })
})

test_that("sl_search L4A defaults to version 3", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("l4a-v3", {
    g <- sl_search(bb, product = "L4A", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "L4A", "gedi", bb)
    expect_identical(attr(g, "version"), "3")
    expect_gt(nrow(g), 0L)
    expect_true(all(grepl("_V003", g$url)))
    expect_identical(attr(g[1, ], "version"), "3")
  })
})

test_that("sl_search rejects a version for single-version products and unknown versions", {
  bb <- test_bbox_search()
  expect_error(sl_search(bb, product = "L2A", version = "3"), "applies only to")
  expect_error(sl_search(bb, product = "L4A", version = "2"), "must be one of")
})

test_that("sl_search L4C returns sl_gedi_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("l4c", {
    g <- sl_search(bb, product = "L4C", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "L4C", "gedi", bb)
  })
})

# ---- ICESat-2 ------------------------------------------------------------

test_that("sl_search ATL03 returns sl_icesat2_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("atl03", {
    g <- sl_search(bb, product = "ATL03", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "ATL03", "icesat2", bb)
  })
})

test_that("sl_search ATL06 returns sl_icesat2_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("atl06", {
    g <- sl_search(bb, product = "ATL06", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "ATL06", "icesat2", bb)
  })
})

test_that("sl_search ATL07 returns sl_icesat2_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("atl07", {
    g <- sl_search(bb, product = "ATL07", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "ATL07", "icesat2", bb)
  })
})

test_that("sl_search ATL08 returns sl_icesat2_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("atl08", {
    g <- sl_search(bb, product = "ATL08", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "ATL08", "icesat2", bb)
  })
})

test_that("sl_search ATL10 returns sl_icesat2_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("atl10", {
    g <- sl_search(bb, product = "ATL10", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "ATL10", "icesat2", bb)
  })
})

test_that("sl_search ATL13 returns sl_icesat2_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("atl13", {
    g <- sl_search(bb, product = "ATL13", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "ATL13", "icesat2", bb)
  })
})

test_that("sl_search ATL24 returns sl_icesat2_search", {
  skip_unless_httptest2()
  bb <- test_bbox_search(); dr <- test_dates_search()
  with_cmr_mock("atl24", {
    g <- sl_search(bb, product = "ATL24", date_start = dr$start, date_end = dr$end)
    expect_search_result(g, "ATL24", "icesat2", bb)
  })
})

# ---- Cross-cutting -------------------------------------------------------

test_that("sl_search returns an empty result for a bbox with no coverage", {
  # A polar bbox that GEDI (lat band ±51.6°) will never cover.
  skip_unless_httptest2()
  bb <- sl_bbox(0, 85, 1, 86)
  with_cmr_mock("empty", {
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
  with_cmr_mock("l2a", {
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

# ---------------------------------------------------------------------------
# Closed collections
# ---------------------------------------------------------------------------

test_that("selectable versions resolve to their own collections", {
  expect_identical(product_search_spec("L1B")$version, "2")
  expect_identical(product_search_spec("L1B", "3")$concept_id, "C3974616058-LPCLOUD")
  expect_identical(product_search_spec("ATL24")$concept_id, "C3433822507-NSIDC_CPRD")
  expect_identical(product_search_spec("ATL24", 3)$concept_id, "C4312123631-NSIDC_CPRD")
  expect_identical(product_search_spec("L4A")$version, "3")
  expect_error(product_search_spec("L1B", "4"), "must be one of")
  expect_error(product_search_spec("L2A", "3"), "applies only to")
})

test_that("a search past the end of a closed collection warns", {
  ended <- function(product, version = NULL, date_end) {
    spec <- product_search_spec(product, version)
    rlang::reset_warning_verbosity(
      paste0("spacelaser_collection_ended_", product, spec$version)
    )
    warn_collection_ended(spec, product, date_end)
  }
  # Closed, with a newer version that can be searched.
  expect_warning(ended("L1B", date_end = "2026-01-01"), "holds no granules after 2025-07-10")
  w <- tryCatch(ended("L1B", date_end = "2026-01-01"), warning = conditionMessage)
  expect_match(w, 'version = "3"', fixed = TRUE)
  # Closed, and the newer version is not read yet.
  w <- tryCatch(ended("L2A", date_end = "2026-01-01"), warning = conditionMessage)
  expect_match(w, "does not read the newer version of L2A")
  # Inside the collection, or an open collection: silent.
  expect_no_warning(ended("L2A", date_end = "2025-07-10"))
  expect_no_warning(ended("L1B", "3", date_end = "2026-01-01"))
  expect_no_warning(ended("L4A", date_end = "2026-01-01"))
  expect_no_warning(ended("ATL08", date_end = "2026-01-01"))
  # date_end defaults to today.
  expect_warning(ended("ATL24", date_end = NULL), "holds no granules after 2024-11-07")
})
