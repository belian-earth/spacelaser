# ---------------------------------------------------------------------------
# Integration: GEDI L1B (waveforms + geolocation metadata)
# ---------------------------------------------------------------------------
#
# L1B is the most structurally complex GEDI product because it contains the
# received and transmitted waveforms as variable-length-per-shot datasets.
# The tests here cover both the metadata path (same shape as L2B) and the
# opt-in waveform path which exercises the pool-column machinery.
#
# Key L1B-specific paths validated:
#   - geolocation/latitude_bin0 / longitude_bin0 (not lat_lowestmode)
#   - 77 scalar columns default set
#   - rxwaveform / txwaveform as opt-in list columns
#
# Skipped unless SPACELASER_INTEGRATION=1.

test_that("L1B: sl_search returns granules with attributes", {
  skip_unless_integration()
  bb <- test_bbox()
  dr <- test_date_range()
  granules <- sl_search(bb, product = "L1B", date_start = dr$start, date_end = dr$end)

  expect_s3_class(granules, "sl_gedi_search")
  expect_identical(attr(granules, "product"), "L1B")
  expect_identical(attr(granules, "bbox"), bb)
  if (nrow(granules) > 0) {
    expect_true(all(grepl("\\.h5$", granules$url, ignore.case = TRUE)))
  }
})

test_that("L1B: sl_read default columns produces a valid tibble", {
  skip_unless_integration()
  granules <- search_or_skip("L1B", max_granules = 1L)
  bb <- attr(granules, "bbox")

  data <- sl_read(granules)

  # L1B uses geolocation/latitude_bin0 and longitude_bin0 (not
  # lat_lowestmode). After subgroup prefix stripping, the output
  # column names are `latitude_bin0` and `longitude_bin0`.
  expect_valid_read(
    data,
    bb,
    group_label = "beam",
    lat_col = "latitude_bin0",
    lon_col = "longitude_bin0"
  )

  expect_true(all(grepl("^BEAM\\d{4}$", unique(data$beam))))

  # rxwaveform is in the L1B default set (it's the core L1B data).
  # txwaveform is opt-in only.
  expect_true("rxwaveform" %in% names(data))
  expect_true(is.list(data$rxwaveform))
  expect_false("txwaveform" %in% names(data))

  # Marquee metadata columns we'd want in any L1B read
  expect_true(all(c(
    "shot_number", "delta_time", "rx_energy",
    "elevation_bin0", "solar_elevation"
  ) %in% names(data)))

  # solar_elevation is a real angle in [-90, 90] degrees
  se_real <- data$solar_elevation[
    !is.na(data$solar_elevation) & data$solar_elevation > -1000
  ]
  if (length(se_real) > 0) {
    expect_true(all(se_real >= -90 & se_real <= 90))
  }
})

test_that("L1B: every scalar column in the registry round-trips", {
  skip_unless_integration()
  granules <- search_or_skip("L1B", max_granules = 1L)

  # Exclude pool columns from this sweep; waveforms are covered separately.
  scalar_only <- setdiff(
    names(sl_columns("L1B")),
    c("rxwaveform", "txwaveform")
  )
  data <- sl_read(granules, columns = scalar_only)

  registry_names <- scalar_only
  out_names <- names(data)
  missing <- character(0)
  for (nm in registry_names) {
    if (nm %in% out_names) next
    # 2D expansion: rh -> rh0, rh1, ...
    if (any(grepl(paste0("^", nm, "\\d+$"), out_names))) next
    # Transposed 2D expansion: surface_type -> surface_type_land, ...
    if (any(grepl(paste0("^", nm, "_[a-z_]+$"), out_names))) next
    missing <- c(missing, nm)
  }
  expect_equal(missing, character(0))
})

test_that("L1B: surface_type expands into 5 boolean columns with sensible geography", {
  # surface_type is stored as a transposed `[5, N]` dataset in L1B (the
  # only such layout across all supported products). The Rust reader
  # reads the full matrix and emits one column per category. This test
  # exercises that pipeline end-to-end and sanity-checks the values:
  #   - column names match .gedi_l1b_transposed_columns labels
  #   - each column is integer 0/1
  #   - for the PNW forest bbox, `surface_type_land` should be set for
  #     essentially every shot, and `sea_ice` / `land_ice` should be
  #     zero everywhere (coastal CA has no ice).
  skip_unless_integration()
  granules <- search_or_skip("L1B", max_granules = 1L)

  data <- sl_read(granules, columns = c("shot_number", "surface_type"))
  expect_gt(nrow(data), 0)

  expected <- c(
    "surface_type_land", "surface_type_ocean",
    "surface_type_sea_ice", "surface_type_land_ice",
    "surface_type_inland_water"
  )
  expect_true(all(expected %in% names(data)))

  # All five columns should be integer 0/1
  for (nm in expected) {
    vals <- data[[nm]]
    expect_true(is.integer(vals) || is.numeric(vals))
    expect_true(all(vals %in% c(0L, 1L)))
  }

  # Geography: PNW forest bbox should be ~100% land, and contain
  # zero sea_ice / land_ice shots.
  expect_gt(mean(data$surface_type_land == 1L), 0.95)
  expect_equal(sum(data$surface_type_sea_ice), 0L)
  expect_equal(sum(data$surface_type_land_ice), 0L)
})

test_that("L1B: rxwaveform and txwaveform are returned as list columns", {
  skip_unless_integration()
  granules <- search_or_skip("L1B", max_granules = 1L)

  data <- sl_read(
    granules,
    columns = c(
      "shot_number", "rx_energy",
      "rxwaveform", "txwaveform"
    )
  )

  expect_gt(nrow(data), 0)

  # Both waveforms should be present as list columns
  expect_true("rxwaveform" %in% names(data))
  expect_true("txwaveform" %in% names(data))
  expect_type(data$rxwaveform, "list")
  expect_type(data$txwaveform, "list")

  # The required sample-index columns should have been auto-added
  expect_true(all(c(
    "rx_sample_start_index", "rx_sample_count",
    "tx_sample_start_index", "tx_sample_count"
  ) %in% names(data)))

  # Each list element should be a numeric vector
  expect_true(all(vapply(data$rxwaveform, is.numeric, logical(1))))
  expect_true(all(vapply(data$txwaveform, is.numeric, logical(1))))

  # The length of each rx waveform must match its rx_sample_count.
  # This is the critical correctness check: if the slicing offsets
  # are wrong, lengths will mismatch. Filter to non-NA counts (fill
  # values are replaced with NA by the reader).
  rx_counts <- as.integer(data$rx_sample_count)
  rx_lengths <- vapply(data$rxwaveform, length, integer(1))
  valid_rx <- !is.na(rx_counts)
  expect_equal(rx_lengths[valid_rx], rx_counts[valid_rx])

  # Same for tx
  tx_counts <- as.integer(data$tx_sample_count)
  tx_lengths <- vapply(data$txwaveform, length, integer(1))
  valid_tx <- !is.na(tx_counts)
  expect_equal(tx_lengths[valid_tx], tx_counts[valid_tx])

  # A well-formed waveform has at least some non-zero values (the laser
  # return). If slicing landed in the wrong region of the pool we'd see
  # all zeros. Assert on the first shot as a sanity check.
  first_rx <- data$rxwaveform[[1]]
  expect_true(any(first_rx != 0))
  expect_true(all(is.finite(first_rx)))
})
