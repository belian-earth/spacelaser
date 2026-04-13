# ---------------------------------------------------------------------------
# Integration: ICESat-2 ATL08 (land/vegetation segments, 100m resolution)
# ---------------------------------------------------------------------------
#
# ATL08 is the first ICESat-2 product validated. It exercises:
#   - sl_search with an ATL* product (different DAAC: NSIDC vs LPCLOUD)
#   - sl_icesat2_search class and its dispatch in sl_read
#   - land_segments/latitude and land_segments/longitude lat/lon paths
#   - Nested subgroup columns: land_segments/canopy/h_canopy,
#     land_segments/terrain/h_te_best_fit
#   - `track` group label (not `beam`)
#   - Ground track IDs (gt1l, gt1r, ..., gt3r)
#
# Skipped unless SPACELASER_INTEGRATION=1.

test_that("ATL08: sl_search returns granules with attributes", {
  skip_unless_integration()
  bb <- test_bbox_icesat2()
  dr <- test_date_range_icesat2()
  granules <- sl_search(bb, product = "ATL08", date_start = dr$start, date_end = dr$end)

  expect_s3_class(granules, "sl_icesat2_search")
  expect_identical(attr(granules, "product"), "ATL08")
  expect_identical(attr(granules, "bbox"), bb)
  if (nrow(granules) > 0) {
    expect_true(all(grepl("\\.h5$", granules$url, ignore.case = TRUE)))
  }
})

test_that("ATL08: sl_read with default columns produces a valid canopy tibble", {
  skip_unless_integration()
  granules <- search_or_skip("ATL08", max_granules = 7L, date_range = test_date_range_icesat2(), bbox = test_bbox_icesat2())
  bb <- attr(granules, "bbox")

  data <- sl_read(granules)

  # ATL08 uses land_segments/latitude → stripped to `latitude` in output.
  expect_valid_read(
    data,
    bb,
    group_label = "track",
    lat_col = "latitude",
    lon_col = "longitude"
  )

  # Ground track IDs follow the gt{1-3}{l,r} pattern
  expect_true(all(grepl("^gt[1-3][lr]$", unique(data$track))))

  # Marquee ATL08 columns
  expect_true(all(c("h_canopy", "h_te_best_fit", "night_flag") %in% names(data)))

  # h_canopy: canopy height in metres. Fill values are large negatives or
  # 3.4e38. After excluding fills, values should be physically plausible.
  # ATL08 can return noisy estimates above 120m in dense canopy, so we
  # use a generous 200m ceiling and allow negative values (ATL08 reports
  # negative h_canopy when the canopy surface is detected below the
  # terrain reference).
  hc <- data$h_canopy[
    !is.na(data$h_canopy) & data$h_canopy > -1000 & data$h_canopy < 1e30
  ]
  if (length(hc) > 0) {
    expect_true(all(hc > -100))
    expect_true(all(hc < 200))
  }

  # h_te_best_fit: terrain elevation in metres (WGS84 ellipsoidal height).
  # PNW coast is roughly 0-500m elevation. Allow a generous range.
  hte <- data$h_te_best_fit[
    !is.na(data$h_te_best_fit) & data$h_te_best_fit > -1000 & data$h_te_best_fit < 1e30
  ]
  if (length(hte) > 0) {
    expect_true(all(hte > -500))
    expect_true(all(hte < 5000))
  }
})

test_that("ATL08: column subset returns requested plus required columns", {
  skip_unless_integration()
  granules <- search_or_skip("ATL08", max_granules = 7L, date_range = test_date_range_icesat2(), bbox = test_bbox_icesat2())

  data <- sl_read(
    granules,
    columns = c("h_canopy", "canopy_openness", "night_flag")
  )

  must_have <- c(
    "h_canopy", "canopy_openness", "night_flag",
    "latitude", "longitude", "track", "geometry"
  )
  expect_true(all(must_have %in% names(data)))
})

test_that("ATL08: every column in the registry round-trips", {
  skip_unless_integration()
  granules <- search_or_skip("ATL08", max_granules = 7L, date_range = test_date_range_icesat2(), bbox = test_bbox_icesat2())

  data <- sl_read(granules, columns = names(sl_columns("ATL08")))
  expect_registry_roundtrip(data, "ATL08")
})
