# ---------------------------------------------------------------------------
# convert_time: delta_time (numeric seconds) -> time (POSIXct)
# ---------------------------------------------------------------------------
#
# By default, sl_read() converts the raw delta_time column to a POSIXct
# column named `time`, using 2018-01-01 00:00:00 UTC as the reference
# epoch for both GEDI and ICESat-2. Setting `convert_time = FALSE`
# preserves the raw numeric seconds-since-epoch column under its
# original name.

l2a <- testthat::test_path("fixtures", "gedi-l2a.h5")
bbox <- sl_bbox(-124.04, 41.39, -124.01, 41.42)

test_that("convert_time = TRUE replaces delta_time with POSIXct `time`", {
  skip_if_not(file.exists(l2a), "L2A fixture not built")

  data <- sl_read(l2a, product = "L2A", bbox = bbox,
                  columns = c("shot_number", "delta_time"))

  expect_true("time" %in% names(data))
  expect_false("delta_time" %in% names(data))
  expect_s3_class(data$time, "POSIXct")
  expect_equal(attr(data$time, "tzone"), "UTC")
})

test_that("convert_time = FALSE keeps raw numeric delta_time column", {
  skip_if_not(file.exists(l2a))

  data <- sl_read(l2a, product = "L2A", bbox = bbox,
                  columns = c("shot_number", "delta_time"),
                  convert_time = FALSE)

  expect_true("delta_time" %in% names(data))
  expect_false("time" %in% names(data))
  expect_type(data$delta_time, "double")
  expect_false(inherits(data$delta_time, "POSIXct"))
})

test_that("`time` values are the epoch plus delta_time seconds", {
  skip_if_not(file.exists(l2a))

  # Read both forms; time should equal epoch + delta_time elementwise.
  raw <- sl_read(l2a, product = "L2A", bbox = bbox,
                 columns = c("shot_number", "delta_time"),
                 convert_time = FALSE)
  converted <- sl_read(l2a, product = "L2A", bbox = bbox,
                       columns = c("shot_number", "delta_time"))

  epoch <- as.POSIXct("2018-01-01 00:00:00", tz = "UTC")
  expected <- epoch + raw$delta_time
  expect_equal(as.numeric(converted$time), as.numeric(expected))
})

test_that("convert_delta_time is a no-op when delta_time absent", {
  df <- tibble::tibble(a = 1:3, b = 4:6)
  expect_identical(spacelaser:::convert_delta_time(df), df)
})

test_that("convert_delta_time preserves column position", {
  df <- tibble::tibble(
    shot_number = 1:3,
    delta_time = c(0, 60, 3600),  # 0, 1 min, 1 hour past epoch
    quality_flag = c(1L, 1L, 0L)
  )
  out <- spacelaser:::convert_delta_time(df)

  # Column at index 2 should be renamed, stay at index 2, become POSIXct
  expect_equal(names(out), c("shot_number", "time", "quality_flag"))
  expect_s3_class(out$time, "POSIXct")
  expect_equal(
    out$time,
    as.POSIXct(c("2018-01-01 00:00:00", "2018-01-01 00:01:00",
                 "2018-01-01 01:00:00"), tz = "UTC")
  )
})
