# ---------------------------------------------------------------------------
# Integration test helpers
# ---------------------------------------------------------------------------
#
# These tests hit real NASA Earthdata services (CMR + DAAC HTTPS) and
# require valid Earthdata credentials. They are skipped by default so that
# `devtools::test()` stays fast and offline-friendly.
#
# To run them:
#   SPACELASER_INTEGRATION=1 Rscript -e 'devtools::test()'
#
# To run a single product's suite:
#   SPACELASER_INTEGRATION=1 Rscript -e 'devtools::test(filter = "integration-gedi-l4a")'
#
# Credentials are resolved by `sl_earthdata_creds()` from environment
# variables or `~/.netrc` (see ?sl_search and the README authentication
# section).

#' Skip a test unless integration mode is enabled and credentials resolve.
#' @noRd
skip_unless_integration <- function() {
  testthat::skip_if(
    Sys.getenv("SPACELASER_INTEGRATION") != "1",
    "Integration tests disabled (set SPACELASER_INTEGRATION=1 to run)"
  )
  creds <- tryCatch(
    spacelaser:::sl_earthdata_creds(),
    error = function(e) NULL
  )
  testthat::skip_if(
    is.null(creds),
    "No Earthdata credentials found (set EARTHDATA_USERNAME/PASSWORD or ~/.netrc)"
  )
}

#' Small Pacific Northwest forest bbox known to have GEDI and ICESat-2
#' coverage. Roughly 3 km x 3 km, sized to keep spatial reads fast
#' (dominated by per-granule HTTP navigation, not row count).
#' @noRd
test_bbox <- function() {
  sl_bbox(-124.04, 41.39, -124.01, 41.42)
}

#' Default test date range. Tight enough to keep granule counts manageable
#' (typically a handful of granules across GEDI and ICESat-2). Picked
#' inside the GEDI mission window so all GEDI products have data.
#' @noRd
test_date_range <- function() {
  list(start = "2020-06-01", end = "2020-09-01")
}

#' Search and skip the test gracefully if no granules are returned.
#'
#' Wraps `sl_search()` so that an empty CMR result (network hiccup,
#' coverage gap, transient outage) skips rather than fails the test.
#' Also caps the number of granules at `max_granules` to bound test
#' duration; the cap goes through `[.sl_*_search` so the bbox/product
#' attributes are preserved.
#'
#' @noRd
search_or_skip <- function(product, max_granules = 2L) {
  bb <- test_bbox()
  dr <- test_date_range()
  granules <- sl_search(
    bb,
    product = product,
    date_start = dr$start,
    date_end = dr$end
  )
  if (nrow(granules) == 0L) {
    testthat::skip(sprintf(
      "No %s granules found in test bbox/date range; skipping",
      product
    ))
  }
  if (nrow(granules) > max_granules) {
    granules <- granules[seq_len(max_granules), ]
  }
  granules
}

#' Assertions every product's read result must satisfy.
#'
#' Validates the cross-product contract: result is a data frame with
#' rows, has lat/lon/geometry plus the group identifier (`beam` or
#' `track`), and every footprint falls within the search bbox.
#' Product-specific assertions live in the per-product test files.
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
  testthat::expect_true(all(c(lat_col, lon_col, group_label, "geometry") %in% names(data)))

  b <- unclass(bbox)
  lon <- data[[lon_col]]
  lat <- data[[lat_col]]
  testthat::expect_true(all(lon >= b[["xmin"]] & lon <= b[["xmax"]]))
  testthat::expect_true(all(lat >= b[["ymin"]] & lat <= b[["ymax"]]))
}
