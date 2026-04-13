# ---------------------------------------------------------------------------
# Integration: GEDI L4C (Waveform Structural Complexity Index)
# ---------------------------------------------------------------------------
#
# L4C is a newer GEDI product providing the WSCI metric. Same beam
# structure as L4A, lat/lon at root, ORNL_CLOUD DAAC.
#
# Skipped unless SPACELASER_INTEGRATION=1.

test_that("L4C: sl_search returns granules with attributes", {
  skip_unless_integration()
  bb <- test_bbox()
  dr <- test_date_range()
  granules <- sl_search(bb, product = "L4C", date_start = dr$start, date_end = dr$end)

  expect_s3_class(granules, "sl_gedi_search")
  expect_identical(attr(granules, "product"), "L4C")
})

test_that("L4C: sl_read with default columns produces valid WSCI data", {
  skip_unless_integration()
  granules <- search_or_skip("L4C", max_granules = 1L)
  bb <- attr(granules, "bbox")

  data <- sl_read(granules)

  expect_valid_read(data, bb, group_label = "beam")
  expect_true(all(grepl("^BEAM\\d{4}$", unique(data$beam))))
  expect_true("wsci" %in% names(data))

  wsci_q <- data$wsci[
    !is.na(data$wsci_quality_flag) &
      data$wsci_quality_flag == 1 &
      !is.na(data$wsci)
  ]
  if (length(wsci_q) > 0) {
    expect_true(all(wsci_q >= 0))
    expect_true(all(wsci_q < 100))
  }
})

test_that("L4C: every column in the registry round-trips", {
  skip_unless_integration()
  granules <- search_or_skip("L4C", max_granules = 1L)

  data <- sl_read(granules, columns = names(sl_columns("L4C")))
  expect_registry_roundtrip(data, "L4C")
})
