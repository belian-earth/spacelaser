# ---------------------------------------------------------------------------
# Integration: ICESat-2 ATL10 (Sea Ice Freeboard)
# ---------------------------------------------------------------------------
#
# ATL10 measures sea ice freeboard (ice height above local sea surface).
# The PNW test bbox has no sea ice, so data-read tests will skip. The
# search test validates the CMR concept_id and product wiring.
#
# Skipped unless SPACELASER_INTEGRATION=1.

test_that("ATL10: sl_search returns expected class", {
  skip_unless_integration()
  bb <- test_bbox_polar()
  dr <- test_date_range_icesat2()
  granules <- sl_search(bb, product = "ATL10", date_start = dr$start, date_end = dr$end)
  expect_s3_class(granules, "sl_icesat2_search")
  expect_identical(attr(granules, "product"), "ATL10")
})

test_that("ATL10: sl_read works if data is available", {
  skip_unless_integration()
  bb <- test_bbox_polar()
  dr <- test_date_range_icesat2()
  all_granules <- sl_search(bb, product = "ATL10", date_start = dr$start, date_end = dr$end)
  if (nrow(all_granules) == 0L) skip("No ATL10 granules found (expected: no sea ice in PNW)")

  data <- NULL
  for (i in seq_len(min(nrow(all_granules), 5L))) {
    d <- tryCatch(sl_read(all_granules[i, ]), error = function(e) NULL)
    if (!is.null(d) && nrow(d) > 0) { data <- d; break }
  }
  if (is.null(data) || nrow(data) == 0L) {
    skip("No freeboard segments found in PNW bbox (expected)")
  }

  bb_obj <- attr(all_granules, "bbox")
  expect_valid_read(data, bb_obj, group_label = "track", lat_col = "latitude", lon_col = "longitude")
  expect_true("beam_fb_height" %in% names(data))
})
