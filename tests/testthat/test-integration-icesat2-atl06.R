# ---------------------------------------------------------------------------
# Integration: ICESat-2 ATL06 (land ice elevation segments)
# ---------------------------------------------------------------------------
#
# ATL06 uses land_ice_segments/latitude and land_ice_segments/longitude.
# 7 columns in the registry. The PNW test site is not glaciated, so
# data coverage depends on ICESat-2 processing: ATL06 segments are
# generated for all terrain, not just ice. Some granules may have no
# segments in the bbox.
#
# Skipped unless SPACELASER_INTEGRATION=1.

test_that("ATL06: sl_search returns granules", {
  skip_unless_integration()
  bb <- test_bbox_icesat2()
  dr <- test_date_range_icesat2()
  granules <- sl_search(bb, product = "ATL06", date_start = dr$start, date_end = dr$end)
  expect_s3_class(granules, "sl_icesat2_search")
  expect_identical(attr(granules, "product"), "ATL06")
})

test_that("ATL06: sl_read with default columns works", {
  skip_unless_integration()
  granules <- search_or_skip(
    "ATL06", max_granules = 7L,
    date_range = test_date_range_icesat2(),
    bbox = test_bbox_icesat2()
  )
  bb <- attr(granules, "bbox")
  data <- sl_read(granules)

  expect_valid_read(
    data, bb, group_label = "track",
    lat_col = "latitude", lon_col = "longitude"
  )
  expect_true(all(grepl("^gt[1-3][lr]$", unique(data$track))))
  expect_true("h_li" %in% names(data))
})

test_that("ATL06: every column in the registry round-trips", {
  skip_unless_integration()
  granules <- search_or_skip(
    "ATL06", max_granules = 7L,
    date_range = test_date_range_icesat2(),
    bbox = test_bbox_icesat2()
  )
  data <- sl_read(granules, columns = names(sl_columns("ATL06")))
  expect_registry_roundtrip(data, "ATL06")
})
