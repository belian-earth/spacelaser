# ---------------------------------------------------------------------------
# sl_hdf5_groups() + sl_hdf5_read() against local fixtures
# ---------------------------------------------------------------------------
#
# Both helpers require credentials resolution to succeed even when the
# URL is file:// (the Rust side ignores creds for local files but the
# R wrapper still calls sl_earthdata_creds() before dispatch). We set
# fake env creds per test and reset the session cache so the resolution
# is deterministic.

fixture_path <- function(name) testthat::test_path("fixtures", name)

fixture_url <- function(name) {
  paste0("file://", normalizePath(fixture_path(name)))
}

local_fake_creds <- function(.local_envir = parent.frame()) {
  withr::local_envvar(
    EARTHDATA_USERNAME = "dummy",
    EARTHDATA_PASSWORD = "dummy",
    .local_envir = .local_envir
  )
  if (exists("earthdata_creds", envir = spacelaser:::.sl_env, inherits = FALSE)) {
    rm("earthdata_creds", envir = spacelaser:::.sl_env)
  }
  withr::defer(
    {
      if (exists("earthdata_creds", envir = spacelaser:::.sl_env, inherits = FALSE)) {
        rm("earthdata_creds", envir = spacelaser:::.sl_env)
      }
    },
    envir = .local_envir
  )
}

test_that("sl_hdf5_groups lists beams at the root of an L2A fixture", {
  skip_if_not(file.exists(fixture_path("gedi-l2a.h5")))
  local_fake_creds()

  groups <- sl_hdf5_groups(fixture_url("gedi-l2a.h5"))
  expect_type(groups, "character")
  expect_true(all(c("BEAM0000", "BEAM0101") %in% groups))
})

test_that("sl_hdf5_groups descends into a beam path", {
  skip_if_not(file.exists(fixture_path("gedi-l2a.h5")))
  local_fake_creds()

  members <- sl_hdf5_groups(fixture_url("gedi-l2a.h5"), path = "BEAM0000")
  expect_true("lat_lowestmode" %in% members)
  expect_true("lon_lowestmode" %in% members)
})

test_that("sl_hdf5_read returns a typed vector for a 1D numeric dataset", {
  skip_if_not(file.exists(fixture_path("gedi-l2a.h5")))
  local_fake_creds()

  lat <- sl_hdf5_read(fixture_url("gedi-l2a.h5"), "BEAM0000/lat_lowestmode")
  expect_type(lat, "double")
  expect_true(length(lat) > 0L)
  expect_true(all(is.finite(lat)))
})

test_that("sl_hdf5_groups requires a url", {
  local_fake_creds()
  expect_error(sl_hdf5_groups(), "url")
})

test_that("sl_hdf5_read requires both url and dataset", {
  local_fake_creds()
  expect_error(sl_hdf5_read(), "url")
  expect_error(sl_hdf5_read(fixture_url("gedi-l2a.h5")), "dataset")
})
