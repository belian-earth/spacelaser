# ---------------------------------------------------------------------------
# sl_search() date argument validation
# ---------------------------------------------------------------------------
#
# date_start / date_end are deliberately strict: YYYY-MM-DD character or a
# Date object. Other formats are rejected at the call site with clear
# errors — no silent "returns zero granules" surprises from malformed
# input reaching CMR as garbage.

parse <- function(x) spacelaser:::parse_search_date(x, arg = "date_start")

test_that("parse_search_date accepts YYYY-MM-DD character", {
  expect_equal(parse("2020-06-01"), as.Date("2020-06-01"))
  expect_equal(parse("2024-12-31"), as.Date("2024-12-31"))
})

test_that("parse_search_date accepts Date objects", {
  expect_equal(parse(as.Date("2020-06-01")), as.Date("2020-06-01"))
})

test_that("parse_search_date rejects non-YYYY-MM-DD character forms", {
  expect_error(parse("01/06/2020"),   "YYYY-MM-DD")
  expect_error(parse("2020-06"),      "YYYY-MM-DD")
  expect_error(parse("June 1 2020"),  "YYYY-MM-DD")
  expect_error(parse("20200601"),     "YYYY-MM-DD")
  expect_error(parse("2020-6-1"),     "YYYY-MM-DD")
  expect_error(parse(""),             "YYYY-MM-DD")
})

test_that("parse_search_date rejects strings that pass the regex but aren't real dates", {
  # The regex allows any \d{4}-\d{2}-\d{2}; as.Date catches the impossible
  # calendar values.
  expect_error(parse("2020-13-01"), "real calendar date")
  expect_error(parse("2020-02-30"), "real calendar date")
})

test_that("parse_search_date rejects POSIXct with format hint", {
  expect_error(
    parse(as.POSIXct("2020-06-01", tz = "UTC")),
    'format\\(x, "%Y-%m-%d"\\)'
  )
})

test_that("parse_search_date rejects other types", {
  expect_error(parse(20200601),       "Date")
  expect_error(parse(NULL),           "Date")  # caller should have %||%-ed
  expect_error(parse(list("2020-06-01")), "Date")
})

test_that("parse_search_date rejects length != 1", {
  expect_error(parse(c("2020-06-01", "2020-06-02")), "length 1")
  expect_error(parse(character(0)),                  "length 1")
})

test_that("parse_search_date rejects NA Date", {
  expect_error(parse(as.Date(NA)), "non-NA")
})

# ---------------------------------------------------------------------------
# End >= start check
# ---------------------------------------------------------------------------
#
# Covered in the mocked sl_search tests implicitly (ranges are well-formed
# there), so here we just drive the error path directly.

test_that("sl_search aborts if date_end < date_start", {
  bb <- sl_bbox(-124.04, 41.39, -124.01, 41.42)
  expect_error(
    sl_search(bb, product = "L2A",
              date_start = "2020-09-01",
              date_end = "2020-06-01"),
    "must be on or after"
  )
})
