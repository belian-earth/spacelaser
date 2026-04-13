# ---------------------------------------------------------------------------
# sl_columns()
# ---------------------------------------------------------------------------

test_that("sl_columns() returns correct counts for GEDI products", {
  expect_length(sl_columns("L1B"), 81)
  expect_length(sl_columns("L2A"), 45)
  expect_length(sl_columns("L2B"), 76)
  expect_length(sl_columns("L4A"), 39)
  expect_length(sl_columns("L4C"), 40)
})

test_that("sl_columns() returns correct counts for ICESat-2 products", {
  expect_length(sl_columns("ATL03"), 14)
  expect_length(sl_columns("ATL06"), 43)
  expect_length(sl_columns("ATL08"), 69)
})

test_that("sl_columns() returns named character vector for GEDI", {
  cols <- sl_columns("L2A")
  expect_type(cols, "character")
  expect_true(!is.null(names(cols)))
  expect_true("rh" %in% names(cols))
  expect_equal(unname(cols[["landsat_treecover"]]), "land_cover_data/landsat_treecover")
})

test_that("sl_columns() returns named character vector for ICESat-2", {
  cols <- sl_columns("ATL08")
  expect_type(cols, "character")
  expect_true("h_canopy" %in% names(cols))
  expect_equal(unname(cols[["h_canopy"]]), "land_segments/canopy/h_canopy")
})

test_that("sl_columns() errors on invalid product", {
  expect_error(sl_columns("L3A"))
  expect_error(sl_columns("ATL99"))
})

# ---------------------------------------------------------------------------
# validate_columns()
# ---------------------------------------------------------------------------

test_that("validate_columns(NULL) returns all defaults", {
  result <- validate_columns(NULL, "L2A")
  expect_type(result, "character")
  expect_true(length(result) > 0)
  # Should include all default HDF5 paths
  expect_true("rh" %in% result)
  expect_true("quality_flag" %in% result)
  expect_true("land_cover_data/landsat_treecover" %in% result)

  result_atl08 <- validate_columns(NULL, "ATL08")
  expect_true("land_segments/canopy/h_canopy" %in% result_atl08)
})

test_that("validate_columns() resolves short names to HDF5 paths", {
  result <- validate_columns(c("rh", "quality_flag"), "L2A")
  expect_equal(result, c("rh", "quality_flag"))
})

test_that("validate_columns() resolves prefixed columns", {
  result <- validate_columns(c("landsat_treecover"), "L2A")
  expect_equal(result, "land_cover_data/landsat_treecover")
})

test_that("validate_columns() passes through raw HDF5 paths", {
  result <- validate_columns(
    c("rh", "some/custom/path"),
    "L2A"
  )
  expect_true("some/custom/path" %in% result)
  expect_true("rh" %in% result)
})

test_that("validate_columns() errors on invalid column with helpful message", {
  expect_error(
    validate_columns(c("bogus"), "L2A"),
    "bogus"
  )
})

test_that("validate_columns() works for ICESat-2 products", {
  result <- validate_columns(c("h_canopy", "night_flag"), "ATL08")
  expect_equal(result, c("land_segments/canopy/h_canopy", "land_segments/night_flag"))
})

test_that("validate_columns() errors on unknown product", {
  expect_error(
    validate_columns(c("foo"), "UNKNOWN"),
    "Unknown product"
  )
})

# ---------------------------------------------------------------------------
# 2D column expansion (e.g., rh90 -> rh)
# ---------------------------------------------------------------------------

test_that("validate_columns() resolves expanded 2D names like rh90", {
  result <- validate_columns(c("rh90"), "L2A")
  expect_equal(result, "rh")
})

test_that("validate_columns() deduplicates when multiple rh variants requested", {
  result <- validate_columns(c("rh0", "rh50", "rh100"), "L2A")
  # All resolve to "rh", deduplicated to one entry

  expect_equal(result, "rh")
})

test_that("validate_columns() handles mix of rh and rh90", {
  result <- validate_columns(c("rh", "rh90", "quality_flag"), "L2A")
  expect_equal(result, c("rh", "quality_flag"))
})

test_that("rh100 in L2B is an exact match (scalar), not 2D expansion", {
  result <- validate_columns(c("rh100"), "L2B")
  expect_equal(result, "rh100")
})

# ---------------------------------------------------------------------------
# ensure_lat_lon()
# ---------------------------------------------------------------------------

test_that("ensure_lat_lon() adds missing lat/lon", {
  result <- ensure_lat_lon(
    c("rh", "quality_flag"),
    "lat_lowestmode",
    "lon_lowestmode"
  )
  expect_true("lat_lowestmode" %in% result)
  expect_true("lon_lowestmode" %in% result)
  expect_true("rh" %in% result)
  expect_length(result, 4)
})

test_that("ensure_lat_lon() does not duplicate existing lat/lon", {
  result <- ensure_lat_lon(
    c("lat_lowestmode", "lon_lowestmode", "rh"),
    "lat_lowestmode",
    "lon_lowestmode"
  )
  expect_equal(sum(result == "lat_lowestmode"), 1)
  expect_equal(sum(result == "lon_lowestmode"), 1)
  expect_length(result, 3)
})

test_that("ensure_lat_lon() handles prefixed lat/lon paths", {
  result <- ensure_lat_lon(
    c("land_segments/canopy/h_canopy"),
    "land_segments/latitude",
    "land_segments/longitude"
  )
  expect_true("land_segments/latitude" %in% result)
  expect_true("land_segments/longitude" %in% result)
  expect_length(result, 3)
})
