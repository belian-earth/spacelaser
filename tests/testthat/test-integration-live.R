# ---------------------------------------------------------------------------
# Live end-to-end tests against real NASA Earthdata
# ---------------------------------------------------------------------------
#
# Three deliberately-small tests that catch the things synthetic
# fixtures can't:
#   - NASA auth chain (netrc → Basic auth → URS OAuth redirect → cookie)
#   - HTTPS / byte-range request availability per DAAC
#   - CMR concept-ID validity and response-schema drift
#   - Production HDF5 file schemas still matching reader assumptions
#
# Parser correctness and per-product registry round-tripping is
# covered offline via tests/testthat/fixtures/*.h5 (see
# test-read-fixtures.R), so this file stays focused on network-facing
# regressions.
#
# Skipped on CRAN. Also skipped locally when no Earthdata credentials
# are configured, so running `devtools::test()` without an account
# still succeeds.

test_that("GEDI L1B: live read end-to-end (auth + HTTPS + pool columns)", {
  # GEDI L1B exercises the most complex parts of the stack: LPCLOUD
  # DAAC auth, pool-column (rxwaveform) indexing, and the full Rust
  # HTTP pipeline against a ~1 GB production file.
  skip_if_no_earthdata()

  bb <- test_bbox()
  dr <- test_date_range()
  granules <- sl_search(bb, product = "L1B",
                        date_start = dr$start, date_end = dr$end)
  if (nrow(granules) == 0L) {
    skip("No live L1B granules in test bbox/date range")
  }
  granules <- granules[1L, ]  # one granule is enough for smoke

  data <- sl_read(granules)

  expect_valid_read(
    data, bb,
    group_label = "beam",
    lat_col = "latitude_bin0",
    lon_col = "longitude_bin0"
  )
  expect_true(all(grepl("^BEAM\\d{4}$", unique(data$beam))))

  # rxwaveform is the main reason L1B exists. If pool-column indexing
  # regressed (1→0-based offsets, span read), list lengths won't match
  # rx_sample_count or waveforms will be all zeros.
  expect_true("rxwaveform" %in% names(data))
  expect_true(is.list(data$rxwaveform))
  rx_counts <- as.integer(data$rx_sample_count)
  rx_lengths <- vapply(data$rxwaveform, length, integer(1))
  valid <- !is.na(rx_counts)
  expect_equal(rx_lengths[valid], rx_counts[valid])

  first_rx <- data$rxwaveform[[1]]
  expect_true(any(first_rx != 0))
  expect_true(all(is.finite(first_rx)))
})

test_that("ICESat-2 ATL08: live read end-to-end (NSIDC auth + nested subgroups)", {
  # ATL08 exercises a different DAAC (NSIDC_CPRD) than GEDI and the
  # ICESat-2 branch of the reader (gt* track names, nested
  # land_segments/canopy/* paths).
  skip_if_no_earthdata()

  bb <- test_bbox_icesat2()
  dr <- test_date_range_icesat2()
  granules <- sl_search(bb, product = "ATL08",
                        date_start = dr$start, date_end = dr$end)
  if (nrow(granules) == 0L) {
    skip("No live ATL08 granules in test bbox/date range")
  }
  # Limit to a handful so the read stays quick
  if (nrow(granules) > 5L) granules <- granules[1:5, ]

  data <- sl_read(granules)

  expect_valid_read(
    data, bb,
    group_label = "track",
    lat_col = "latitude",
    lon_col = "longitude"
  )
  expect_true(all(grepl("^gt[1-3][lr]$", unique(data$track))))

  # Marquee nested-subgroup column resolution (land_segments/canopy/h_canopy)
  expect_true("h_canopy" %in% names(data))
  hc <- data$h_canopy[!is.na(data$h_canopy) &
                        data$h_canopy > -1000 & data$h_canopy < 1e30]
  if (length(hc) > 0) {
    # ATL08 canopy heights are physically bounded; generous range to
    # allow for the noisy detections the algorithm can emit.
    expect_true(all(hc > -100 & hc < 200))
  }
})

test_that("Live CMR probe: concept IDs still valid across DAACs", {
  # No auth needed — CMR search is unauthenticated. This test catches
  # concept-ID retirements and CMR response-schema drift. Running it
  # against one product per DAAC keeps it fast (~1 s) while covering
  # the three distinct catalogs we query.
  skip_on_cran()

  bb <- test_bbox()
  dr <- test_date_range()

  for (spec in list(
    list(product = "L2A",   daac = "LPCLOUD"),
    list(product = "ATL08", daac = "NSIDC_CPRD"),
    list(product = "L4A",   daac = "ORNL_CLOUD")
  )) {
    granules <- sl_search(bb, product = spec$product,
                          date_start = dr$start, date_end = dr$end)
    # Empty results are acceptable (coverage gap); what we're checking
    # is that the call completes without error and returns the right
    # search-result class.
    expected_cls <- if (startsWith(spec$product, "ATL")) {
      "sl_icesat2_search"
    } else {
      "sl_gedi_search"
    }
    expect_true(
      inherits(granules, expected_cls),
      info = sprintf("%s (%s) should return %s", spec$product, spec$daac, expected_cls)
    )
  }
})
