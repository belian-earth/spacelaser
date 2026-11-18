# ---------------------------------------------------------------------------
# Granules that fail in a multi-granule read
# ---------------------------------------------------------------------------
#
# A granule that still fails after the Rust side's retries is reported, not
# silently dropped: a warning and the `failed_granules` attribute when some
# granules were read, an error when none were.

bbox <- sl_bbox(-124.04, 41.39, -124.01, 41.42)
l2a <- testthat::test_path("fixtures", "gedi-l2a.h5")

read_multi <- function(urls) {
  ll <- gedi_lat_lon("L2A")
  read_product_multi(
    urls = urls, product = "L2A", bbox = bbox, columns = NULL,
    rust_multi_fn = rust_read_gedi_multi, lat_col = ll$lat, lon_col = ll$lon,
    group_label = "beam", element_label = "footprint"
  )
}

test_that("a read with no failed granules carries an empty failed_granules", {
  skip_if_not(file.exists(l2a))
  data <- read_multi(l2a)
  expect_gt(nrow(data), 0L)
  expect_equal(nrow(attr(data, "failed_granules")), 0L)
})

test_that("a granule that fails is reported with its error", {
  skip_if_not(file.exists(l2a))
  missing <- file.path(tempdir(), "no-such-granule.h5")
  expect_warning(data <- read_multi(c(l2a, missing)), "1 of 2 granules failed")
  expect_equal(nrow(data), nrow(read_multi(l2a)))
  failed <- attr(data, "failed_granules")
  expect_equal(failed$url, missing)
  expect_match(failed$error, "Local file I/O")
})

test_that("a read where every granule fails is an error", {
  missing <- file.path(tempdir(), c("a.h5", "b.h5"))
  expect_error(read_multi(missing), "All 2 granules failed")
})

test_that("R's SIGPIPE handler is restored after a read", {
  skip_on_os("windows")
  skip_if_not(file.exists(l2a))
  read_multi(l2a)
  # R's handler turns SIGPIPE into an R error; if the reader had left the
  # signal ignored, this would not raise
  expect_error(tools::pskill(Sys.getpid(), tools::SIGPIPE), "SIGPIPE")
})

# A column whose chunks cannot be read fails the granule. It must never
# come back as NA for the affected beam, which reads as missing data.
corrupt_copy <- function(src, at) {
  path <- tempfile(fileext = ".h5")
  bytes <- readBin(src, "raw", file.size(src))
  start <- floor(length(bytes) * at)
  bytes[start + seq_len(3000L)] <- as.raw(0xA5)
  writeBin(bytes, path)
  path
}

test_that("a granule with an unreadable column is an error, not NA", {
  skip_if_not(file.exists(l2a))
  clean <- suppressMessages(sl_read(l2a, bbox = bbox, product = "L2A"))
  for (at in c(0.3, 0.6, 0.75)) {
    bad <- corrupt_copy(l2a, at)
    out <- tryCatch(suppressMessages(sl_read(bad, bbox = bbox, product = "L2A")), error = function(e) e)
    if (inherits(out, "error")) {
      expect_match(conditionMessage(out), "All 1 granule failed")
    } else {
      # Corruption that lands in bytes this read never touches.
      expect_equal(colSums(is.na(out[names(clean) != "geometry"])),
                   colSums(is.na(clean[names(clean) != "geometry"])))
    }
  }
})

test_that("a character-vector read reports failed granules like a search read", {
  skip_if_not(file.exists(l2a))
  missing <- file.path(tempdir(), "GEDI02_A_missing.h5")
  expect_warning(
    data <- suppressMessages(sl_read(c(l2a, missing), bbox = bbox, product = "L2A")),
    "1 of 2 granules failed"
  )
  expect_equal(attr(data, "failed_granules")$url, missing)
  expect_error(
    suppressMessages(sl_read(missing, bbox = bbox, product = "L2A")),
    "All 1 granule failed"
  )
  expect_error(sl_read(l2a, bbox = bbox, product = "L2A", colums = "rh"), "must be empty")
})
