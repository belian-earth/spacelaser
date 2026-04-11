# ---------------------------------------------------------------------------
# Integration: GEDI L2B (canopy cover and vertical profile metrics)
# ---------------------------------------------------------------------------
#
# L2B is the second product validated and exercises several code paths
# that L4A did not:
#
#   - `geolocation/lat_lowestmode` lat/lon (same prefix that L1B uses).
#     If L2B reads correctly, the L1B metadata path is also proven.
#   - 2D profile columns: cover_z, pai_z, pavd_z, pgap_theta_z. These
#     de-interleave Rust-side into {name}0..{name}N-1.
#   - `rh100` as a scalar (unlike L2A's 2D `rh[N,101]`).
#   - 77-column registry, the largest GEDI default set validated so far.
#
# Skipped unless SPACELASER_INTEGRATION=1.

test_that("L2B: sl_search returns granules with attributes", {
  skip_unless_integration()
  bb <- test_bbox()
  dr <- test_date_range()
  granules <- sl_search(bb, product = "L2B", date_start = dr$start, date_end = dr$end)

  expect_s3_class(granules, "sl_gedi_search")
  expect_identical(attr(granules, "product"), "L2B")
  expect_identical(attr(granules, "bbox"), bb)
  if (nrow(granules) > 0) {
    expect_true(all(grepl("\\.h5$", granules$url, ignore.case = TRUE)))
  }
})

test_that("L2B: sl_read with default columns produces a valid canopy tibble", {
  skip_unless_integration()
  granules <- search_or_skip("L2B", max_granules = 2L)
  bb <- attr(granules, "bbox")

  data <- sl_read(granules)

  # Cross-product contract. L2B uses `geolocation/lat_lowestmode` in the
  # registry, which strips to `lat_lowestmode` in the output, so the
  # default lat/lon column names in expect_valid_read still apply.
  expect_valid_read(data, bb, group_label = "beam")

  expect_true(all(grepl("^BEAM\\d{4}$", unique(data$beam))))

  # Marquee canopy columns present
  expect_true(all(c("cover", "pai", "fhd_normal", "rh100") %in% names(data)))

  # Physical ranges for quality-filtered rows. L2B fill value is -9999.
  # Use l2b_quality_flag == 1 as the "trust this row" gate.
  good <- !is.na(data$l2b_quality_flag) & data$l2b_quality_flag == 1

  cover_q <- data$cover[good & data$cover > -9990]
  if (length(cover_q) > 0) {
    # Canopy cover is a fraction in [0, 1]
    expect_true(all(cover_q >= 0))
    expect_true(all(cover_q <= 1))
  }

  pai_q <- data$pai[good & data$pai > -9990]
  if (length(pai_q) > 0) {
    # Plant area index is non-negative. Temperate forests rarely exceed
    # 15; use 20 as a generous global ceiling.
    expect_true(all(pai_q >= 0))
    expect_true(all(pai_q < 20))
  }

  fhd_q <- data$fhd_normal[good & data$fhd_normal > -9990]
  if (length(fhd_q) > 0) {
    # Foliage height diversity (Shannon-style): non-negative, typically
    # under 4 for even the most structurally complex canopies.
    expect_true(all(fhd_q >= 0))
    expect_true(all(fhd_q < 5))
  }

  # rh100 is a scalar in L2B (height of highest return above ground,
  # in cm). Should not have been expanded to rh100_0..N.
  expect_true("rh100" %in% names(data))
  expect_false(any(grepl("^rh100\\d", names(data))))
})

test_that("L2B: 2D profile columns expand Rust-side into {name}N", {
  skip_unless_integration()
  granules <- search_or_skip("L2B", max_granules = 1L)

  data <- sl_read(
    granules,
    columns = c("cover_z", "pai_z", "pavd_z")
  )

  # Each *_z column should produce multiple expanded sub-columns.
  # Vertical profile bin count is product-defined (typically ~30 for
  # 5 m bins up to 150 m); we assert "more than one" rather than a
  # specific count so the test survives future product updates.
  cover_z_cols <- grep("^cover_z\\d+$", names(data), value = TRUE)
  pai_z_cols   <- grep("^pai_z\\d+$",   names(data), value = TRUE)
  pavd_z_cols  <- grep("^pavd_z\\d+$",  names(data), value = TRUE)

  expect_gt(length(cover_z_cols), 1)
  expect_gt(length(pai_z_cols),   1)
  expect_gt(length(pavd_z_cols),  1)

  # All three profiles should share the same bin count (they are
  # defined on the same height grid per granule).
  expect_equal(length(cover_z_cols), length(pai_z_cols))
  expect_equal(length(cover_z_cols), length(pavd_z_cols))
})

test_that("L2B: every column in the registry round-trips through a real read", {
  skip_unless_integration()
  granules <- search_or_skip("L2B", max_granules = 1L)

  data <- sl_read(granules, columns = names(sl_columns("L2B")))
  expect_registry_roundtrip(data, "L2B")
})
