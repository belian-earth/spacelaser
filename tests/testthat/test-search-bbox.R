# ---------------------------------------------------------------------------
# bbox is carried on sl_*_search objects and validated by sl_read()
# ---------------------------------------------------------------------------

# Build a synthetic search result without hitting the network. Mirrors the
# shape of `build_find_result()` so the S3 dispatch path is exercised
# end-to-end up to the point where Rust would be called.
fake_search <- function(sensor = "gedi", product = "L2A", bbox = NULL) {
  bbox <- bbox %||% sl_bbox(-124.07, 41.36, -124.00, 41.44)
  df <- vctrs::new_data_frame(list(
    id = "fake-id",
    time_start = as.POSIXct("2024-01-01", tz = "UTC"),
    time_end = as.POSIXct("2024-01-01", tz = "UTC"),
    url = NA_character_,
    geometry = wk::wkt(NA_character_, crs = wk::wk_crs_longlat())
  ))
  # Use the unexported constructor directly via :::
  spacelaser:::new_sl_search(
    df,
    product = product,
    bbox = bbox,
    sensor = sensor
  )
}

test_that("find_*() result carries product and bbox attributes", {
  bb <- sl_bbox(-124.07, 41.36, -124.00, 41.44)
  s <- fake_search(bbox = bb)
  expect_s3_class(s, "sl_gedi_search")
  expect_identical(attr(s, "product"), "L2A")
  expect_identical(attr(s, "bbox"), bb)
})

test_that("numeric bbox passed to find_*() is coerced to sl_bbox on attr", {
  # Mirrors what sl_search() does internally: validate then attach.
  bb_num <- c(-124.07, 41.36, -124.00, 41.44)
  bb <- spacelaser:::validate_bbox(bb_num)
  s <- fake_search(bbox = bb)
  expect_s3_class(attr(s, "bbox"), "sl_bbox")
})

test_that("check_bbox_within() accepts identical and contained bboxes", {
  outer <- sl_bbox(-124.07, 41.36, -124.00, 41.44)
  expect_silent(spacelaser:::check_bbox_within(outer, outer))
  inner <- sl_bbox(-124.05, 41.38, -124.02, 41.42)
  expect_silent(spacelaser:::check_bbox_within(inner, outer))
})

test_that("check_bbox_within() rejects bboxes that exceed any edge", {
  outer <- sl_bbox(-124.07, 41.36, -124.00, 41.44)

  # Each cardinal direction nudged outside.
  west  <- sl_bbox(-124.10, 41.36, -124.00, 41.44)
  south <- sl_bbox(-124.07, 41.30, -124.00, 41.44)
  east  <- sl_bbox(-124.07, 41.36,  -123.99, 41.44)
  north <- sl_bbox(-124.07, 41.36, -124.00, 41.50)

  for (b in list(west, south, east, north)) {
    expect_error(
      spacelaser:::check_bbox_within(b, outer),
      "extends outside the search bbox"
    )
  }
})

test_that("check_bbox_within() accepts a numeric vector and validates it", {
  outer <- sl_bbox(-124.07, 41.36, -124.00, 41.44)
  expect_silent(
    spacelaser:::check_bbox_within(
      c(-124.05, 41.38, -124.02, 41.42),
      outer
    )
  )
})

test_that("sl_read() on a search object errors when explicit bbox exceeds search", {
  s <- fake_search()
  wider <- sl_bbox(-125.00, 40.00, -123.00, 42.00)
  expect_error(
    sl_read(s, bbox = wider),
    "extends outside the search bbox"
  )
})

# ---------------------------------------------------------------------------
# Subset preserves class + attributes
# ---------------------------------------------------------------------------

test_that("[.sl_gedi_search preserves product, bbox, and class on row subset", {
  bb <- sl_bbox(-124.07, 41.36, -124.00, 41.44)
  df <- vctrs::new_data_frame(list(
    id = c("G-1", "G-2", "G-3"),
    time_start = as.POSIXct(rep("2024-01-01", 3), tz = "UTC"),
    time_end = as.POSIXct(rep("2024-01-01", 3), tz = "UTC"),
    url = c("a.h5", "b.h5", "c.h5"),
    geometry = wk::wkt(rep(NA_character_, 3), crs = wk::wk_crs_longlat())
  ))
  s <- spacelaser:::new_sl_search(df, product = "L4A", bbox = bb, sensor = "gedi")

  sub <- s[1:2, ]
  expect_s3_class(sub, "sl_gedi_search")
  expect_identical(attr(sub, "product"), "L4A")
  expect_identical(attr(sub, "bbox"), bb)
  expect_equal(nrow(sub), 2L)
})

test_that("[.sl_icesat2_search preserves product, bbox, and class on row subset", {
  bb <- sl_bbox(-124.07, 41.36, -124.00, 41.44)
  df <- vctrs::new_data_frame(list(
    id = c("G-1", "G-2"),
    time_start = as.POSIXct(rep("2024-01-01", 2), tz = "UTC"),
    time_end = as.POSIXct(rep("2024-01-01", 2), tz = "UTC"),
    url = c("a.h5", "b.h5"),
    geometry = wk::wkt(rep(NA_character_, 2), crs = wk::wk_crs_longlat())
  ))
  s <- spacelaser:::new_sl_search(df, product = "ATL08", bbox = bb, sensor = "icesat2")

  sub <- s[1, ]
  expect_s3_class(sub, "sl_icesat2_search")
  expect_identical(attr(sub, "product"), "ATL08")
  expect_identical(attr(sub, "bbox"), bb)
  expect_equal(nrow(sub), 1L)
})

test_that("subsetting a search object still dispatches to sl_read.sl_*_search", {
  s <- fake_search()
  sub <- s[1, ]
  # Should still use the search-object dispatch (which checks bbox containment).
  # Passing a wider bbox should still error rather than fall through to .default.
  wider <- sl_bbox(-125, 40, -123, 42)
  expect_error(sl_read(sub, bbox = wider), "extends outside the search bbox")
})
