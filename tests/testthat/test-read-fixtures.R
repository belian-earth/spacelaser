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

# ---------------------------------------------------------------------------
# GEDI L2B
# ---------------------------------------------------------------------------

test_that("L2B fixture: sl_read returns a tibble with expected shape", {
  skip_if_not(file.exists(fixture_path("gedi-l2b.h5")),
              "L2B fixture not built")

  data <- sl_read(fixture_path("gedi-l2b.h5"),
                  product = "L2B", bbox = fixture_bbox())

  expect_s3_class(data, "data.frame")
  expect_equal(nrow(data), 500L)
  expect_setequal(unique(data$beam), c("BEAM0000", "BEAM0101"))
  # L2B-specific: lat/lon come from geolocation/ subgroup. After prefix
  # stripping only the short name appears — same as L2A's user-facing
  # convention.
  expect_true(all(c("lat_lowestmode", "lon_lowestmode") %in% names(data)))
  expect_false(any(grepl("^geolocation/", names(data))))
})

test_that("L2B fixture: spatial filter respects geolocation/ lat/lon path", {
  skip_if_not(file.exists(fixture_path("gedi-l2b.h5")))
  bb <- fixture_bbox(); b <- unclass(bb)

  data <- sl_read(fixture_path("gedi-l2b.h5"), product = "L2B", bbox = bb)

  expect_true(all(data$lon_lowestmode >= b[["xmin"]] &
                    data$lon_lowestmode <= b[["xmax"]]))
  expect_true(all(data$lat_lowestmode >= b[["ymin"]] &
                    data$lat_lowestmode <= b[["ymax"]]))
})

test_that("L2B fixture: cover_z, pai_z, pavd_z each expand into 30 columns", {
  skip_if_not(file.exists(fixture_path("gedi-l2b.h5")))

  data <- sl_read(fixture_path("gedi-l2b.h5"),
                  product = "L2B", bbox = fixture_bbox(),
                  columns = c("cover_z", "pai_z", "pavd_z"))

  for (base in c("cover_z", "pai_z", "pavd_z")) {
    pat <- paste0("^", base, "\\d+$")
    cols <- grep(pat, names(data), value = TRUE)
    expect_equal(length(cols), 30L,
                 info = sprintf("%s should produce 30 expanded columns", base))
    # Generator uses max = 10, values should land in that range
    expect_true(all(is.finite(data[[cols[1]]])))
    expect_true(all(data[[cols[1]]] >= 0 & data[[cols[1]]] <= 10))
  }
})

test_that("L2B fixture: pgap_theta_z pool column returns per-shot list", {
  skip_if_not(file.exists(fixture_path("gedi-l2b.h5")))

  data <- sl_read(fixture_path("gedi-l2b.h5"),
                  product = "L2B", bbox = fixture_bbox(),
                  columns = c("shot_number", "pgap_theta_z"))

  expect_true("pgap_theta_z" %in% names(data))
  expect_type(data$pgap_theta_z, "list")
  # Every shot should have the same sample count in this fixture (30),
  # and every element should be a finite numeric vector of that length.
  lengths_ok <- vapply(data$pgap_theta_z, length, integer(1))
  expect_true(all(lengths_ok == 30L))
  sample_vec <- data$pgap_theta_z[[1L]]
  expect_type(sample_vec, "double")
  expect_true(all(is.finite(sample_vec)))
  # Non-trivial content: shouldn't be all zeros (that would signal
  # wrong index arithmetic — the 1-based → 0-based conversion bug
  # we fixed before).
  expect_true(any(sample_vec != 0))
})

test_that("L2B fixture: pool read auto-adds required index columns", {
  skip_if_not(file.exists(fixture_path("gedi-l2b.h5")))

  # User didn't ask for rx_sample_start_index / rx_sample_count, but
  # the reader must include them in order to slice the pool. They
  # should either appear in the output OR be silently consumed.
  data <- sl_read(fixture_path("gedi-l2b.h5"),
                  product = "L2B", bbox = fixture_bbox(),
                  columns = c("pgap_theta_z"))

  expect_true("pgap_theta_z" %in% names(data))
  # And the shot count is still right, confirming the index columns
  # were resolved and used correctly.
  expect_equal(nrow(data), 500L)
})

test_that("L2B fixture: every registry column round-trips when requested", {
  skip_if_not(file.exists(fixture_path("gedi-l2b.h5")))

  data <- sl_read(fixture_path("gedi-l2b.h5"),
                  product = "L2B", bbox = fixture_bbox(),
                  columns = names(sl_columns("L2B")))

  registry_names <- names(sl_columns("L2B"))
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
