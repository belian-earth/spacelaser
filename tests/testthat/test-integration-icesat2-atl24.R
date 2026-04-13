# ---------------------------------------------------------------------------
# Integration: ICESat-2 ATL24 (Coastal/Nearshore Bathymetry)
# ---------------------------------------------------------------------------
#
# ATL24 measures seafloor and sea surface heights in shallow coastal
# waters. Photon-level data (like ATL03). Flat structure: all datasets
# directly under /gtx/, no subgroups. Uses sequential single-granule
# reads because not every granule has tracks in the bbox.
#
# Skipped unless SPACELASER_INTEGRATION=1.

test_that("ATL24: sl_search returns granules", {
  skip_unless_integration()
  bb <- test_bbox_icesat2()
  dr <- test_date_range_icesat2()
  granules <- sl_search(bb, product = "ATL24", date_start = dr$start, date_end = dr$end)
  expect_s3_class(granules, "sl_icesat2_search")
  expect_identical(attr(granules, "product"), "ATL24")
})

test_that("ATL24: sl_read with default columns works", {
  skip_unless_integration()
  bb <- test_bbox_icesat2()
  dr <- test_date_range_icesat2()
  all_granules <- sl_search(bb, product = "ATL24", date_start = dr$start, date_end = dr$end)
  if (nrow(all_granules) == 0L) skip("No ATL24 granules found")

  data <- NULL
  for (i in seq_len(min(nrow(all_granules), 7L))) {
    d <- tryCatch(sl_read(all_granules[i, ]), error = function(e) NULL)
    if (!is.null(d) && nrow(d) > 0) { data <- d; break }
  }
  if (is.null(data) || nrow(data) == 0L) {
    skip("No ATL24 photons found in bbox from first 7 granules")
  }

  bb_obj <- attr(all_granules, "bbox")
  expect_valid_read(
    data, bb_obj, group_label = "track",
    lat_col = "lat_ph", lon_col = "lon_ph"
  )
  expect_true("ortho_h" %in% names(data))
  expect_true("class_ph" %in% names(data))
})

test_that("ATL24: every column in the registry round-trips", {
  skip_unless_integration()
  bb <- test_bbox_icesat2()
  dr <- test_date_range_icesat2()
  all_granules <- sl_search(bb, product = "ATL24", date_start = dr$start, date_end = dr$end)
  if (nrow(all_granules) == 0L) skip("No ATL24 granules found")

  data <- NULL
  for (i in seq_len(min(nrow(all_granules), 7L))) {
    d <- tryCatch(
      sl_read(all_granules[i, ], columns = names(sl_columns("ATL24"))),
      error = function(e) NULL
    )
    if (!is.null(d) && nrow(d) > 0) { data <- d; break }
  }
  if (is.null(data) || nrow(data) == 0L) {
    skip("No ATL24 photons found from first 7 granules")
  }

  expect_registry_roundtrip(data, "ATL24")
})
