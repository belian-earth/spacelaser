# ---------------------------------------------------------------------------
# Live integration test helpers
# ---------------------------------------------------------------------------
#
# These helpers support the small end-to-end suite in
# test-integration-live.R, which hits real NASA CMR + DAAC HTTPS to
# catch anything synthetic fixtures can't: auth chain regressions,
# CMR schema drift, NASA HTTP/range-request availability, and real
# production file schemas matching our reader's assumptions.
#
# Tests skip on CRAN (per policy) and skip silently if no Earthdata
# credentials are available, so `devtools::test()` works out of the
# box whether you have credentials set up or not.

#' Skip a test unless live-integration mode is opted in.
#'
#' Integration tests are opt-in via `SPACELASER_INTEGRATION`. Default
#' `devtools::test()` runs stay deterministic and offline, and the
#' five-runner R-CMD-check matrix does not hammer NASA with parallel
#' authenticated reads. The test-coverage workflow exports the flag
#' so the live suite still runs there (single ubuntu runner).
#'
#' Local use: `Sys.setenv(SPACELASER_INTEGRATION = 1); devtools::test()`.
#' @noRd
skip_unless_integration <- function() {
  testthat::skip_on_cran()
  if (!nzchar(Sys.getenv("SPACELASER_INTEGRATION", unset = ""))) {
    testthat::skip("SPACELASER_INTEGRATION not set (integration tests are opt-in)")
  }
}

#' Same as `skip_unless_integration()`, but also requires Earthdata
#' credentials. Use for tests that authenticate against URS/DAAC
#' endpoints; pure CMR probes should call the base helper instead.
#' @noRd
skip_if_no_earthdata <- function() {
  skip_unless_integration()
  creds <- tryCatch(
    spacelaser:::sl_earthdata_creds(),
    error = function(e) NULL
  )
  testthat::skip_if(
    is.null(creds),
    "No Earthdata credentials (set EARTHDATA_USERNAME/PASSWORD or ~/.netrc)"
  )
}

#' Small PNW forest bbox with confirmed GEDI + ICESat-2 coverage.
#' Roughly 3 km × 3 km — GEDI lands enough footprints here for a
#' read, and ICESat-2 ground tracks cross it through the year.
#' @noRd
test_bbox <- function() {
  sl_bbox(-124.04, 41.39, -124.01, 41.42)
}

#' Wider bbox for ICESat-2 tests. ICESat-2 ground tracks are ~3.3 km
#' apart at mid-latitudes; the GEDI bbox (3 km) can miss all tracks
#' for a full year. 10 km × 10 km guarantees several crossings.
#' @noRd
test_bbox_icesat2 <- function() {
  sl_bbox(-124.10, 41.36, -124.00, 41.45)
}

#' Default test date windows. Kept short so granule counts stay small.
#' @noRd
test_date_range <- function() {
  list(start = "2020-06-01", end = "2020-09-01")
}
test_date_range_icesat2 <- function() {
  list(start = "2020-01-01", end = "2021-01-01")
}

#' Common post-read assertions for the cross-product contract.
#' @noRd
expect_valid_read <- function(
  data,
  bbox,
  group_label,
  lat_col = "lat_lowestmode",
  lon_col = "lon_lowestmode"
) {
  testthat::expect_s3_class(data, "data.frame")
  testthat::expect_gt(nrow(data), 0)
  testthat::expect_true(all(c(lat_col, lon_col, group_label, "geometry") %in%
                              names(data)))

  b <- unclass(bbox)
  lon <- data[[lon_col]]
  lat <- data[[lat_col]]
  testthat::expect_true(all(lon >= b[["xmin"]] & lon <= b[["xmax"]]))
  testthat::expect_true(all(lat >= b[["ymin"]] & lat <= b[["ymax"]]))
}
