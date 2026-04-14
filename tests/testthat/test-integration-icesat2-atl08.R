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

test_that("ATL08: land-surface flags round-trip with sensible geography", {
  # ATL08's closest equivalent to L1B `surface_type` is a set of
  # per-segment classification scalars: `segment_landcover` (MODIS IGBP
  # class code), `urban_flag` (0/1), and `segment_watermask` (0/1).
  # This test exercises the read path for these together and checks
  # their values are physically meaningful for the PNW forest bbox.
  skip_unless_integration()
  granules <- search_or_skip(
    "ATL08",
    max_granules = 7L,
    date_range = test_date_range_icesat2(),
    bbox = test_bbox_icesat2()
  )

  data <- sl_read(
    granules,
    columns = c("segment_landcover", "urban_flag", "segment_watermask")
  )
  expect_gt(nrow(data), 0)
  expect_true(all(
    c("segment_landcover", "urban_flag", "segment_watermask") %in% names(data)
  ))

  # `segment_landcover` uses Copernicus Global Land Cover codes: 0
  # (unknown), 20, 30, 40, 50, 60, 70, 80, 90, 100, 111-116 (closed
  # forest types), 121-126 (open forest types), 200 (ocean). Valid
  # values are within 0-200, and for a PNW coast bbox we expect a mix
  # of forest codes (111-116) and ocean (200).
  lc <- data$segment_landcover[!is.na(data$segment_landcover)]
  if (length(lc) > 0) {
    expect_true(all(lc >= 0L & lc <= 200L))
    expect_true(any(lc %in% 111:116))  # forest segments present
  }

  # PNW coast forest: expect no urban segments. Allow a small tolerance
  # because ICESat-2 segments are 100m and the 10 km bbox brushes the
  # coast, but urban_flag should be overwhelmingly zero.
  uf <- data$urban_flag[!is.na(data$urban_flag)]
  if (length(uf) > 0) {
    expect_true(all(uf %in% c(0L, 1L)))
    expect_lt(mean(uf == 1L), 0.05)
  }

  # segment_watermask is 0/1. This bbox spans coast and inland forest,
  # so both values are plausible.
  wm <- data$segment_watermask[!is.na(data$segment_watermask)]
  if (length(wm) > 0) {
    expect_true(all(wm %in% c(0L, 1L)))
  }
})

test_that("ATL08: every column in the registry round-trips", {
  skip_unless_integration()
  granules <- search_or_skip("ATL08", max_granules = 7L, date_range = test_date_range_icesat2(), bbox = test_bbox_icesat2())

  data <- sl_read(granules, columns = names(sl_columns("ATL08")))
  expect_registry_roundtrip(data, "ATL08")
})
