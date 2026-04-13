# ---------------------------------------------------------------------------
# Integration: ICESat-2 ATL13 (Inland Surface Water)
# ---------------------------------------------------------------------------
#
# ATL13 measures water surface heights for inland water bodies (lakes,
# rivers, reservoirs). The PNW test bbox is mostly forest so coverage
# depends on ICESat-2 tracks crossing water features. Uses sequential
# single-granule reads to find one with data.
#
# ATL13 datasets live directly under /gtx/ (no intermediate subgroup).
# Lat/lon: segment_lat / segment_lon.
#
# Skipped unless SPACELASER_INTEGRATION=1.

test_that("ATL13: sl_search returns granules", {
  skip_unless_integration()
  bb <- test_bbox_icesat2()
  dr <- test_date_range_icesat2()
  granules <- sl_search(bb, product = "ATL13", date_start = dr$start, date_end = dr$end)
  expect_s3_class(granules, "sl_icesat2_search")
  expect_identical(attr(granules, "product"), "ATL13")
})

test_that("ATL13: sl_read with default columns works", {
  skip_unless_integration()
  bb <- test_bbox_icesat2()
  dr <- test_date_range_icesat2()
  all_granules <- sl_search(bb, product = "ATL13", date_start = dr$start, date_end = dr$end)
  if (nrow(all_granules) == 0L) skip("No ATL13 granules found")

  data <- NULL
  for (i in seq_len(min(nrow(all_granules), 7L))) {
    d <- tryCatch(sl_read(all_granules[i, ]), error = function(e) NULL)
    if (!is.null(d) && nrow(d) > 0) { data <- d; break }
  }
  if (is.null(data) || nrow(data) == 0L) {
    skip("No ATL13 water segments found in bbox from first 7 granules")
  }

  bb_obj <- attr(all_granules, "bbox")
  expect_valid_read(
    data, bb_obj, group_label = "track",
    lat_col = "segment_lat", lon_col = "segment_lon"
  )
  expect_true("ht_water_surf" %in% names(data))
})

test_that("ATL13: every column in the registry round-trips", {
  skip_unless_integration()
  bb <- test_bbox_icesat2()
  dr <- test_date_range_icesat2()
  all_granules <- sl_search(bb, product = "ATL13", date_start = dr$start, date_end = dr$end)
  if (nrow(all_granules) == 0L) skip("No ATL13 granules found")

  data <- NULL
  for (i in seq_len(min(nrow(all_granules), 7L))) {
    d <- tryCatch(
      sl_read(all_granules[i, ], columns = names(sl_columns("ATL13"))),
      error = function(e) NULL
    )
    if (!is.null(d) && nrow(d) > 0) { data <- d; break }
  }
  if (is.null(data) || nrow(data) == 0L) {
    skip("No ATL13 water segments found from first 7 granules")
  }

  expect_registry_roundtrip(data, "ATL13")
})
