# ---------------------------------------------------------------------------
# Integration: GEDI L4A (above-ground biomass density)
# ---------------------------------------------------------------------------
#
# L4A is the first product validated against real data because:
#   - Smallest registry (37 columns, all scalar — no 2D expansion)
#   - Same lat/lon paths as L2A (already known to work in dev.R)
#   - AGBD values have a clear physical range (0 to ~2000 Mg/ha) so
#     "did the bytes round-trip correctly?" is easy to assert
#   - Different DAAC (ORNL_CLOUD vs LPCLOUD) — exercises a second host
#
# Skipped unless SPACELASER_INTEGRATION=1.

test_that("L4A: sl_search returns granules with attributes", {
  skip_unless_integration()
  bb <- test_bbox()
  dr <- test_date_range()
  granules <- sl_search(bb, product = "L4A", date_start = dr$start, date_end = dr$end)

  expect_s3_class(granules, "sl_gedi_search")
  expect_identical(attr(granules, "product"), "L4A")
  expect_identical(attr(granules, "bbox"), bb)
  expect_true(all(c("id", "url", "time_start", "geometry") %in% names(granules)))
  if (nrow(granules) > 0) {
    expect_true(all(grepl("\\.h5$", granules$url, ignore.case = TRUE)))
  }
})

test_that("L4A: sl_read with default columns produces a valid biomass tibble", {
  skip_unless_integration()
  granules <- search_or_skip("L4A", max_granules = 2L)
  bb <- attr(granules, "bbox")

  data <- sl_read(granules)

  expect_valid_read(data, bb, group_label = "beam")

  # GEDI beam IDs follow the BEAM#### pattern
  expect_true(all(grepl("^BEAM\\d{4}$", unique(data$beam))))

  # The marquee L4A column must be present
  expect_true("agbd" %in% names(data))

  # AGBD physical range for quality-filtered estimates. Fill value is
  # -9999 ("no prediction made"). Upper bound of 3000 Mg/ha is a
  # generous global ceiling: the Pacific NW test site is old-growth
  # Douglas fir / coastal redwood, which routinely hits ~2200 Mg/ha
  # (highest-biomass forest type on Earth), so anything tighter would
  # false-flag real data.
  agbd_q <- data$agbd[
    !is.na(data$l4_quality_flag) &
      data$l4_quality_flag == 1 &
      !is.na(data$agbd) &
      data$agbd > -9990
  ]
  if (length(agbd_q) > 0) {
    expect_true(all(agbd_q >= 0))
    expect_true(all(agbd_q < 3000))
  }

  # AGBD prediction interval: lower <= upper where both are real
  pi_real <-
    !is.na(data$agbd_pi_lower) & data$agbd_pi_lower > -9990 &
    !is.na(data$agbd_pi_upper) & data$agbd_pi_upper > -9990
  if (any(pi_real)) {
    expect_true(all(
      data$agbd_pi_lower[pi_real] <= data$agbd_pi_upper[pi_real]
    ))
  }
})

test_that("L4A: requesting a column subset returns only those columns plus required ones", {
  skip_unless_integration()
  granules <- search_or_skip("L4A", max_granules = 1L)

  data <- sl_read(
    granules,
    columns = c("agbd", "agbd_se", "l4_quality_flag", "sensitivity")
  )

  # Requested columns plus auto-added: lat/lon, beam, geometry
  must_have <- c(
    "agbd", "agbd_se", "l4_quality_flag", "sensitivity",
    "lat_lowestmode", "lon_lowestmode", "beam", "geometry"
  )
  expect_true(all(must_have %in% names(data)))
})

test_that("L4A: every column in the registry round-trips through a real read", {
  skip_unless_integration()
  granules <- search_or_skip("L4A", max_granules = 1L)

  data <- sl_read(granules, columns = names(sl_columns("L4A")))
  expect_registry_roundtrip(data, "L4A")
})
