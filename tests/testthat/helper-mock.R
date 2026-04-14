# ---------------------------------------------------------------------------
# HTTP mocking helpers (httptest2)
# ---------------------------------------------------------------------------
#
# `sl_search()` hits NASA CMR via httr2. We use httptest2 to record those
# responses once, then replay from fixtures for fast, deterministic, offline
# tests of the full search path (URL construction, pagination, JSON parsing,
# result-class construction, attribute propagation).
#
# Fixtures live in `tests/testthat/_mocks/<hostname>/...` and are committed
# to the repo. To re-record (e.g. after a CMR change), delete the relevant
# subdirectory and re-run the test suite with a network connection.
#
# CMR search is unauthenticated, so no Earthdata credentials are needed to
# record fixtures.

skip_unless_httptest2 <- function() {
  testthat::skip_if_not_installed("httptest2")
}

with_cmr_mock <- function(name, expr) {
  # `with_mock_dir()` records on first run (if dir absent), replays on
  # subsequent runs. Fixture paths are resolved relative to the test file.
  httptest2::with_mock_dir(file.path("_mocks", name), expr)
}
