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

# ---------------------------------------------------------------------------
# height_ref modes
# ---------------------------------------------------------------------------

test_that("height_ref = 'geoid' subtracts geoid per shot", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  d <- l1b_data()
  d <- d[d$beam == d$beam[1L], ]

  wf_ell   <- sl_extract_waveforms(d)
  wf_geoid <- sl_extract_waveforms(d, height_ref = "geoid")

  # For each shot, the per-sample elevation should differ from the
  # ellipsoidal reading by exactly the geoid value for that shot.
  geoid_by_shot <- d$geoid[match(wf_ell$shot_number, d$shot_number)]
  expect_equal(wf_ell$elevation - wf_geoid$elevation, geoid_by_shot)
})

test_that("height_ref = 'tandemx' subtracts digital_elevation_model per shot", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  d <- l1b_data()
  d <- d[d$beam == d$beam[1L], ]

  wf_ell <- sl_extract_waveforms(d)
  wf_dem <- sl_extract_waveforms(d, height_ref = "tandemx")

  dem_by_shot <- d$digital_elevation_model[
    match(wf_ell$shot_number, d$shot_number)
  ]
  expect_equal(wf_ell$elevation - wf_dem$elevation, dem_by_shot)
})

test_that("height_ref = 'srtm' subtracts digital_elevation_model_srtm per shot", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  d <- l1b_data()
  d <- d[d$beam == d$beam[1L], ]

  wf_ell  <- sl_extract_waveforms(d)
  wf_srtm <- sl_extract_waveforms(d, height_ref = "srtm")

  srtm_by_shot <- d$digital_elevation_model_srtm[
    match(wf_ell$shot_number, d$shot_number)
  ]
  expect_equal(wf_ell$elevation - wf_srtm$elevation, srtm_by_shot)
})

test_that("height_ref errors when the referenced column is absent", {
  d <- tibble::tibble(
    shot_number       = 1:2,
    elevation_bin0    = c(100, 110),
    elevation_lastbin = c(50, 60),
    rx_sample_count   = c(10L, 10L),
    rxwaveform        = list(numeric(10), numeric(10))
  )

  expect_error(
    sl_extract_waveforms(d, height_ref = "geoid"),
    "Missing.*geoid"
  )
  expect_error(
    sl_extract_waveforms(d, height_ref = "tandemx"),
    "digital_elevation_model"
  )
  expect_error(
    sl_extract_waveforms(d, height_ref = "srtm"),
    "digital_elevation_model_srtm"
  )
})

test_that("height_ref rejects unknown values", {
  d <- tibble::tibble(
    shot_number       = 1L,
    elevation_bin0    = 100,
    elevation_lastbin = 50,
    rx_sample_count   = 10L,
    rxwaveform        = list(numeric(10))
  )
  expect_error(sl_extract_waveforms(d, height_ref = "above_ground"))
})

# ---------------------------------------------------------------------------
# normalise_amplitude modes
# ---------------------------------------------------------------------------

test_that("normalise_amplitude = 'noise' subtracts noise_mean_corrected per shot", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  d <- l1b_data()
  d <- d[d$beam == d$beam[1L], ]

  wf_raw   <- sl_extract_waveforms(d)
  wf_noise <- sl_extract_waveforms(d, normalise_amplitude = "noise")

  noise_by_shot <- d$noise_mean_corrected[
    match(wf_raw$shot_number, d$shot_number)
  ]
  expect_equal(wf_raw$amplitude - wf_noise$amplitude, noise_by_shot)
})

test_that("normalise_amplitude = 'snr' applies (amp - mean) / sd per shot", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  d <- l1b_data()
  d <- d[d$beam == d$beam[1L], ]

  wf_raw <- sl_extract_waveforms(d)
  wf_snr <- sl_extract_waveforms(d, normalise_amplitude = "snr")

  mu <- d$noise_mean_corrected[match(wf_raw$shot_number, d$shot_number)]
  sd <- d$noise_stddev_corrected[match(wf_raw$shot_number, d$shot_number)]
  expect_equal(wf_snr$amplitude, (wf_raw$amplitude - mu) / sd)
})

test_that("normalise_amplitude errors when noise columns are absent", {
  d <- tibble::tibble(
    shot_number       = 1:2,
    elevation_bin0    = c(100, 110),
    elevation_lastbin = c(50, 60),
    rx_sample_count   = c(10L, 10L),
    rxwaveform        = list(numeric(10), numeric(10))
  )

  expect_error(
    sl_extract_waveforms(d, normalise_amplitude = "noise"),
    "noise_mean_corrected"
  )
  expect_error(
    sl_extract_waveforms(d, normalise_amplitude = "snr"),
    "noise_mean_corrected|noise_stddev_corrected"
  )
})

test_that("normalise_amplitude rejects unknown values", {
  d <- tibble::tibble(
    shot_number       = 1L,
    elevation_bin0    = 100,
    elevation_lastbin = 50,
    rx_sample_count   = 10L,
    rxwaveform        = list(numeric(10))
  )
  expect_error(sl_extract_waveforms(d, normalise_amplitude = "zscore"))
})

test_that("height_ref and normalise_amplitude compose independently", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  d <- l1b_data()
  d <- d[d$beam == d$beam[1L], ]

  wf <- sl_extract_waveforms(d,
                             height_ref          = "srtm",
                             normalise_amplitude = "noise")

  # Elevation path matches the srtm-only extraction
  wf_ref_only <- sl_extract_waveforms(d, height_ref = "srtm")
  expect_equal(wf$elevation, wf_ref_only$elevation)

  # Amplitude path matches the noise-only extraction
  wf_amp_only <- sl_extract_waveforms(d, normalise_amplitude = "noise")
  expect_equal(wf$amplitude, wf_amp_only$amplitude)
})
