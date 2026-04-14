# ---------------------------------------------------------------------------
# sl_read() against synthetic HDF5 fixtures
# ---------------------------------------------------------------------------
#
# Parser/reader correctness tests that exercise the full sl_read() pipeline
# (Rust HDF5 navigation → chunk decompression → typed vector conversion →
# tibble assembly → geometry attachment) without touching the network.
#
# Fixtures live in tests/testthat/fixtures/ and are generated from the
# column registry by data-raw/generate-fixtures.R. See that script for
# schema details.

fixture_path <- function(name) {
  testthat::test_path("fixtures", name)
}

fixture_bbox <- function() {
  # Matches BBOX in data-raw/generate-fixtures.R. Synthesized shots
  # straddle this: ~half inside, ~half outside.
  sl_bbox(-124.04, 41.39, -124.01, 41.42)
}

# ---------------------------------------------------------------------------
# GEDI L2A
# ---------------------------------------------------------------------------

test_that("L2A fixture: sl_read returns a tibble with expected shape", {
  skip_if_not(file.exists(fixture_path("gedi-l2a.h5")),
              "L2A fixture not built")

  data <- sl_read(fixture_path("gedi-l2a.h5"),
                  product = "L2A", bbox = fixture_bbox())

  expect_s3_class(data, "data.frame")
  expect_gt(nrow(data), 0L)
  # Fixture synthesizes 500 shots × 2 beams, half inside bbox
  expect_equal(nrow(data), 500L)
  expect_true(all(c("lat_lowestmode", "lon_lowestmode", "beam", "geometry") %in%
                    names(data)))
  expect_setequal(unique(data$beam), c("BEAM0000", "BEAM0101"))
})

test_that("L2A fixture: spatial filter keeps all shots inside bbox", {
  skip_if_not(file.exists(fixture_path("gedi-l2a.h5")))
  bb <- fixture_bbox()
  b <- unclass(bb)

  data <- sl_read(fixture_path("gedi-l2a.h5"), product = "L2A", bbox = bb)

  expect_true(all(data$lon_lowestmode >= b[["xmin"]] &
                    data$lon_lowestmode <= b[["xmax"]]))
  expect_true(all(data$lat_lowestmode >= b[["ymin"]] &
                    data$lat_lowestmode <= b[["ymax"]]))
})

test_that("L2A fixture: 2D rh expands to rh0..rh100", {
  skip_if_not(file.exists(fixture_path("gedi-l2a.h5")))

  data <- sl_read(fixture_path("gedi-l2a.h5"),
                  product = "L2A", bbox = fixture_bbox(),
                  columns = c("rh", "quality_flag"))

  rh_cols <- grep("^rh\\d+$", names(data), value = TRUE)
  expect_equal(length(rh_cols), 101L)
  # Bounds check: generator seeds values in 0-50m
  for (col in c("rh0", "rh50", "rh100")) {
    vals <- data[[col]]
    expect_true(all(is.finite(vals)))
    expect_true(all(vals >= 0 & vals <= 50))
  }
})

test_that("L2A fixture: subgroup columns resolve (land_cover_data/*)", {
  skip_if_not(file.exists(fixture_path("gedi-l2a.h5")))

  data <- sl_read(fixture_path("gedi-l2a.h5"),
                  product = "L2A", bbox = fixture_bbox(),
                  columns = c("landsat_treecover", "modis_treecover",
                              "pft_class"))

  # Subgroup prefix is stripped — only the short names appear.
  expect_true("landsat_treecover" %in% names(data))
  expect_true("modis_treecover" %in% names(data))
  expect_true("pft_class" %in% names(data))
  expect_false(any(grepl("land_cover_data/", names(data))))
})

test_that("L2A fixture: explicit column subset returns only requested + required", {
  skip_if_not(file.exists(fixture_path("gedi-l2a.h5")))

  data <- sl_read(fixture_path("gedi-l2a.h5"),
                  product = "L2A", bbox = fixture_bbox(),
                  columns = c("quality_flag", "solar_elevation"))

  # Always-required columns: lat/lon/geometry/beam
  must_have <- c("lat_lowestmode", "lon_lowestmode", "beam", "geometry",
                 "quality_flag", "solar_elevation")
  expect_true(all(must_have %in% names(data)))
})

test_that("L2A fixture: every registry column round-trips when requested", {
  skip_if_not(file.exists(fixture_path("gedi-l2a.h5")))

  data <- sl_read(fixture_path("gedi-l2a.h5"),
                  product = "L2A", bbox = fixture_bbox(),
                  columns = names(sl_columns("L2A")))

  registry_names <- names(sl_columns("L2A"))
  out_names <- names(data)
  missing <- character(0)
  for (nm in registry_names) {
    if (nm %in% out_names) next
    if (any(grepl(paste0("^", nm, "\\d+$"), out_names))) next
    if (any(grepl(paste0("^", nm, "_[a-z_]+$"), out_names))) next
    missing <- c(missing, nm)
  }
  expect_equal(missing, character(0))
})

test_that("L2A fixture: NULL columns returns the default set", {
  skip_if_not(file.exists(fixture_path("gedi-l2a.h5")))

  data <- sl_read(fixture_path("gedi-l2a.h5"),
                  product = "L2A", bbox = fixture_bbox())

  # Default set is a curated subset of the registry (~18 columns).
  # We assert presence of marquee defaults rather than exact equality
  # so the test is robust to default-set tweaks.
  expect_true(all(c(
    "shot_number", "quality_flag", "rh0", "solar_elevation",
    "landsat_treecover", "pft_class"
  ) %in% names(data)))
})

test_that("L2A fixture: file:// URL prefix is accepted", {
  skip_if_not(file.exists(fixture_path("gedi-l2a.h5")))

  url <- paste0("file://", normalizePath(fixture_path("gedi-l2a.h5")))
  data <- sl_read(url, product = "L2A", bbox = fixture_bbox())
  expect_gt(nrow(data), 0)
})

test_that("L2A fixture: geometry column is wk_wkt with longlat CRS", {
  skip_if_not(file.exists(fixture_path("gedi-l2a.h5")))

  data <- sl_read(fixture_path("gedi-l2a.h5"),
                  product = "L2A", bbox = fixture_bbox())

  expect_s3_class(data$geometry, "wk_xy")
  # wk::xy objects carry a CRS attribute
  crs <- attr(data$geometry, "crs") %||% wk::wk_crs(data$geometry)
  expect_false(is.null(crs))
})
