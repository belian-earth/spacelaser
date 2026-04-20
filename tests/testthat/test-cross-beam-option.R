# ---------------------------------------------------------------------------
# Cross-beam opt-in: option resolution + parity
# ---------------------------------------------------------------------------
#
# User-facing control is the `spacelaser.cross_beam_scan` option. The
# env var `SPACELASER_CROSS_BEAM_SCAN` is an internal transport — the
# R wrapper sets it from the option before calling Rust and restores
# afterwards. These tests cover:
#
#   1. `.onLoad` registers the option at FALSE by default
#   2. `sl_cross_beam_enabled()` resolves the option correctly
#   3. Enabling the option propagates through to Rust and produces
#      output identical to the default path (correctness-preserving)
#   4. The internal env var transport doesn't leak past one call

fixture_path <- function(name) testthat::test_path("fixtures", name)
fixture_bbox <- function() sl_bbox(-124.04, 41.39, -124.01, 41.42)

# ---------------------------------------------------------------------------
# Option registration via .onLoad
# ---------------------------------------------------------------------------

test_that(".onLoad registers spacelaser.cross_beam_scan at FALSE by default", {
  current <- getOption("spacelaser.cross_beam_scan")
  expect_false(is.null(current))
  expect_false(isTRUE(current))
})

# ---------------------------------------------------------------------------
# Helper resolution
# ---------------------------------------------------------------------------

test_that("sl_cross_beam_enabled is FALSE by default", {
  withr::local_options(spacelaser.cross_beam_scan = FALSE)
  expect_false(spacelaser:::sl_cross_beam_enabled())
})

test_that("sl_cross_beam_enabled is TRUE when option is TRUE", {
  withr::local_options(spacelaser.cross_beam_scan = TRUE)
  expect_true(spacelaser:::sl_cross_beam_enabled())
})

test_that("sl_cross_beam_enabled is FALSE for non-TRUE option values", {
  withr::local_options(spacelaser.cross_beam_scan = NA)
  expect_false(spacelaser:::sl_cross_beam_enabled())
  withr::local_options(spacelaser.cross_beam_scan = "yes")
  expect_false(spacelaser:::sl_cross_beam_enabled())
  withr::local_options(spacelaser.cross_beam_scan = 1L)
  expect_false(spacelaser:::sl_cross_beam_enabled())
})

# ---------------------------------------------------------------------------
# End-to-end parity + internal transport scoping
# ---------------------------------------------------------------------------

test_that("option enables cross-beam with identical output to default", {
  skip_if_not(file.exists(fixture_path("gedi-l2a.h5")))

  baseline <- {
    withr::local_options(spacelaser.cross_beam_scan = FALSE)
    sl_read(fixture_path("gedi-l2a.h5"),
            product = "L2A", bbox = fixture_bbox())
  }
  via_option <- {
    withr::local_options(spacelaser.cross_beam_scan = TRUE)
    sl_read(fixture_path("gedi-l2a.h5"),
            product = "L2A", bbox = fixture_bbox())
  }

  expect_equal(nrow(baseline), nrow(via_option))
  expect_setequal(names(baseline), names(via_option))
  for (col in names(baseline)) {
    if (inherits(baseline[[col]], "wk_xy")) next
    if (is.list(baseline[[col]]))            next
    expect_identical(baseline[[col]], via_option[[col]],
                     info = paste("disagreement in", col))
  }
})

test_that("internal env var transport does not leak past one sl_read call", {
  skip_if_not(file.exists(fixture_path("gedi-l2a.h5")))

  # Starting with the env var unset, enable via option, make a call,
  # and confirm the env var is NOT left set afterwards. Guards against
  # the internal transport leaking into the session or other tests.
  withr::local_options(spacelaser.cross_beam_scan = TRUE)
  withr::local_envvar(SPACELASER_CROSS_BEAM_SCAN = "")

  .unused <- sl_read(fixture_path("gedi-l2a.h5"),
                     product = "L2A", bbox = fixture_bbox())

  expect_equal(Sys.getenv("SPACELASER_CROSS_BEAM_SCAN", unset = ""), "")
})
