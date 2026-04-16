# ---------------------------------------------------------------------------
# sl_extract_waveforms()
# ---------------------------------------------------------------------------
#
# Exercises the L1B waveform long-form expansion against the synthetic
# L1B fixture. The fixture is generated with known sample counts per
# shot (500 samples rxwaveform, 128 samples txwaveform), so we can
# assert exact row totals after extraction.

fixture_path <- function(name) testthat::test_path("fixtures", name)

fixture_bbox <- function() sl_bbox(-124.04, 41.39, -124.01, 41.42)

l1b_data <- function(columns = NULL) {
  sl_read(fixture_path("gedi-l1b.h5"),
          product = "L1B", bbox = fixture_bbox(),
          columns = columns)
}

test_that("sl_extract_waveforms expands rxwaveform to one row per sample", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  d <- l1b_data()
  wf <- sl_extract_waveforms(d)

  expected_rows <- sum(as.integer(d$rx_sample_count))
  expect_equal(nrow(wf), expected_rows)
  expect_true(all(c("shot_number", "elevation", "amplitude") %in% names(wf)))
  expect_type(wf$elevation, "double")
  expect_type(wf$amplitude, "double")
})

test_that("sl_extract_waveforms interpolates elevation between bin0 and lastbin", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  # Fixture reuses shot_number across beams; pin to one beam so
  # (beam, shot_number) is unique.
  d <- l1b_data()
  d <- d[d$beam == d$beam[1L], ]
  wf <- sl_extract_waveforms(d)

  s1 <- d[1L, ]
  n <- as.integer(s1$rx_sample_count)
  step <- (s1$elevation_bin0 - s1$elevation_lastbin) / n

  first <- wf[wf$shot_number == s1$shot_number, ]
  expect_equal(nrow(first), n)
  expect_equal(first$elevation[1L], s1$elevation_bin0 - step)
  expect_equal(first$elevation[n], s1$elevation_lastbin)
})

test_that("sl_extract_waveforms carries beam through when present", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  d <- l1b_data()
  wf <- sl_extract_waveforms(d)

  expect_true("beam" %in% names(wf))
  expect_setequal(unique(wf$beam), unique(d$beam))
})

test_that("sl_extract_waveforms errors on missing required columns", {
  d <- tibble::tibble(shot_number = 1, elevation_bin0 = 100)
  expect_error(sl_extract_waveforms(d), "Missing")
})

test_that("sl_extract_waveforms returns an empty tibble when no rows are valid", {
  d <- tibble::tibble(
    shot_number       = 1:2,
    elevation_bin0    = c(NA_real_, 100),
    elevation_lastbin = c(50, NA_real_),
    rx_sample_count   = c(10L, 10L),
    rxwaveform        = list(numeric(10), numeric(10))
  )
  wf <- sl_extract_waveforms(d)

  expect_s3_class(wf, "data.frame")
  expect_equal(nrow(wf), 0L)
})

test_that("sl_extract_waveforms drops shots with zero sample count", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  d <- l1b_data()
  d <- d[d$beam == d$beam[1L], ]
  dropped_shot <- d$shot_number[1L]
  d$rx_sample_count[1L] <- 0L
  d$rxwaveform[[1L]] <- numeric(0)

  wf <- sl_extract_waveforms(d)
  expect_false(dropped_shot %in% wf$shot_number)
  expect_equal(nrow(wf), sum(as.integer(d$rx_sample_count)))
})
