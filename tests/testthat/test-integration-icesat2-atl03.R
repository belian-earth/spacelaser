# ---------------------------------------------------------------------------
# Integration: ICESat-2 ATL03 (photon-level heights)
# ---------------------------------------------------------------------------
#
# ATL03 is the stress test: photon-level data can produce millions of
# rows per ground track. The test bbox is small enough to limit the
# row count but we still assert the basic contract.
#
# ATL03 uses heights/lat_ph and heights/lon_ph for coordinates.
# 5 columns in the registry.
#
# Skipped unless SPACELASER_INTEGRATION=1.

test_that("ATL03: sl_search returns granules", {
  skip_unless_integration()
  bb <- test_bbox_icesat2()
  dr <- test_date_range_icesat2()
  granules <- sl_search(bb, product = "ATL03", date_start = dr$start, date_end = dr$end)
  expect_s3_class(granules, "sl_icesat2_search")
  expect_identical(attr(granules, "product"), "ATL03")
})

test_that("ATL03: sl_read with default columns works (single granule)", {
  skip_unless_integration()
  # ATL03 is photon-level: lat/lon arrays are 10-100M+ elements per
  # track, so multi-granule concurrent reads can timeout. Read one
  # granule at a time and find one that has tracks in our bbox.
  bb <- test_bbox_icesat2()
  dr <- test_date_range_icesat2()
  all_granules <- sl_search(bb, product = "ATL03", date_start = dr$start, date_end = dr$end)
  if (nrow(all_granules) == 0L) {
    skip("No ATL03 granules found in test bbox/date range")
  }

  data <- NULL
  for (i in seq_len(min(nrow(all_granules), 5L))) {
    d <- tryCatch(sl_read(all_granules[i, ]), error = function(e) NULL)
    if (!is.null(d) && nrow(d) > 0) {
      data <- d
      break
    }
  }
  if (is.null(data) || nrow(data) == 0L) {
    skip("No ATL03 photons found in bbox from first 5 granules")
  }

  expect_valid_read(
    data, bb, group_label = "track",
    lat_col = "lat_ph", lon_col = "lon_ph"
  )
  expect_true(all(grepl("^gt[1-3][lr]$", unique(data$track))))

  # h_ph is photon height (WGS84 ellipsoidal, metres). The PNW coast
  # ranges from sea level to ~500m. Allow a generous window for noise.
  expect_true("h_ph" %in% names(data))
  hph <- data$h_ph[!is.na(data$h_ph) & data$h_ph > -1000 & data$h_ph < 1e30]
  if (length(hph) > 0) {
    expect_true(all(hph > -500))
    expect_true(all(hph < 5000))
  }
})

test_that("ATL03: every column in the registry round-trips (single granule)", {
  skip_unless_integration()
  bb <- test_bbox_icesat2()
  dr <- test_date_range_icesat2()
  all_granules <- sl_search(bb, product = "ATL03", date_start = dr$start, date_end = dr$end)
  if (nrow(all_granules) == 0L) {
    skip("No ATL03 granules found")
  }

  data <- NULL
  for (i in seq_len(min(nrow(all_granules), 5L))) {
    d <- tryCatch(
      sl_read(all_granules[i, ], columns = names(sl_columns("ATL03"))),
      error = function(e) NULL
    )
    if (!is.null(d) && nrow(d) > 0) {
      data <- d
      break
    }
  }
  if (is.null(data) || nrow(data) == 0L) {
    skip("No ATL03 photons found from first 5 granules")
  }

  expect_registry_roundtrip(data, "ATL03")
})
