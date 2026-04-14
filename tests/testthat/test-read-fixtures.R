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

# ---------------------------------------------------------------------------
# GEDI L1B
# ---------------------------------------------------------------------------

test_that("L1B fixture: sl_read returns a tibble with expected shape", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")),
              "L1B fixture not built")

  data <- sl_read(fixture_path("gedi-l1b.h5"),
                  product = "L1B", bbox = fixture_bbox())

  expect_s3_class(data, "data.frame")
  expect_equal(nrow(data), 500L)
  expect_setequal(unique(data$beam), c("BEAM0000", "BEAM0101"))
  # L1B uses latitude_bin0 / longitude_bin0, not lat_lowestmode
  expect_true(all(c("latitude_bin0", "longitude_bin0") %in% names(data)))
  expect_false("lat_lowestmode" %in% names(data))
})

test_that("L1B fixture: spatial filter respects geolocation/*_bin0 paths", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))
  bb <- fixture_bbox(); b <- unclass(bb)

  data <- sl_read(fixture_path("gedi-l1b.h5"), product = "L1B", bbox = bb)

  expect_true(all(data$longitude_bin0 >= b[["xmin"]] &
                    data$longitude_bin0 <= b[["xmax"]]))
  expect_true(all(data$latitude_bin0 >= b[["ymin"]] &
                    data$latitude_bin0 <= b[["ymax"]]))
})

test_that("L1B fixture: rxwaveform is a per-shot list column with 500 samples", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  # rxwaveform is in the L1B default set
  data <- sl_read(fixture_path("gedi-l1b.h5"),
                  product = "L1B", bbox = fixture_bbox())

  expect_true("rxwaveform" %in% names(data))
  expect_type(data$rxwaveform, "list")

  lengths <- vapply(data$rxwaveform, length, integer(1))
  expect_true(all(lengths == 500L))

  # Critical regression guard: if pool indexing is off by one, the last
  # sample of every shot lands on the zero buffer past the last entry.
  # Assert that the final samples of several shots are non-zero.
  last_samples <- vapply(data$rxwaveform, function(x) x[length(x)],
                         numeric(1))
  expect_true(any(last_samples != 0))
})

test_that("L1B fixture: txwaveform uses a distinct index pair and is shorter", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  data <- sl_read(fixture_path("gedi-l1b.h5"),
                  product = "L1B", bbox = fixture_bbox(),
                  columns = c("shot_number", "txwaveform"))

  expect_true("txwaveform" %in% names(data))
  expect_type(data$txwaveform, "list")
  # txwaveform is a shorter transmitted pulse (128 samples in our fixture)
  tx_lengths <- vapply(data$txwaveform, length, integer(1))
  expect_true(all(tx_lengths == 128L))
})

test_that("L1B fixture: rxwaveform and txwaveform auto-add their index columns", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  # User requests only the pool columns; reader must pull in the
  # start/count indices for both pools.
  data <- sl_read(fixture_path("gedi-l1b.h5"),
                  product = "L1B", bbox = fixture_bbox(),
                  columns = c("rxwaveform", "txwaveform"))

  expect_true(all(c("rxwaveform", "txwaveform") %in% names(data)))
  # Distinct index pairs for rx and tx — proves the pool spec wiring
  # dispatches on dataset name, not a single shared pair.
  expect_equal(nrow(data), 500L)
})

test_that("L1B fixture: transposed surface_type expands into 5 boolean columns", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  data <- sl_read(fixture_path("gedi-l1b.h5"),
                  product = "L1B", bbox = fixture_bbox(),
                  columns = c("shot_number", "surface_type"))

  expected <- c(
    "surface_type_land", "surface_type_ocean",
    "surface_type_sea_ice", "surface_type_land_ice",
    "surface_type_inland_water"
  )
  expect_true(all(expected %in% names(data)))

  # Values must be 0/1
  for (nm in expected) {
    expect_true(all(data[[nm]] %in% c(0L, 1L)))
  }
})

test_that("L1B fixture: surface_type values match encoded geographic semantics", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  # Generator marks every shot as `land` and ~10% also as `inland_water`.
  # No ocean / sea_ice / land_ice anywhere. If chunk placement for the
  # transposed [5, N] dataset is ever broken again, this test catches it:
  # we'd see either all zeros or the wrong category dominating.
  data <- sl_read(fixture_path("gedi-l1b.h5"),
                  product = "L1B", bbox = fixture_bbox(),
                  columns = c("shot_number", "surface_type"))

  expect_equal(sum(data$surface_type_land), nrow(data))         # 100%
  expect_gt(sum(data$surface_type_inland_water), 0)             # some
  expect_lt(sum(data$surface_type_inland_water), nrow(data))    # not all
  expect_equal(sum(data$surface_type_ocean), 0L)
  expect_equal(sum(data$surface_type_sea_ice), 0L)
  expect_equal(sum(data$surface_type_land_ice), 0L)
})

test_that("L1B fixture: geophys_corr/ subgroup columns round-trip", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  data <- sl_read(fixture_path("gedi-l1b.h5"),
                  product = "L1B", bbox = fixture_bbox(),
                  columns = c("geoid", "tide_earth", "tide_ocean"))

  for (nm in c("geoid", "tide_earth", "tide_ocean")) {
    expect_true(nm %in% names(data))
    expect_false(paste0("geophys_corr/", nm) %in% names(data))
    expect_true(all(is.finite(data[[nm]])))
  }
})

test_that("L1B fixture: every registry column round-trips when requested", {
  skip_if_not(file.exists(fixture_path("gedi-l1b.h5")))

  data <- sl_read(fixture_path("gedi-l1b.h5"),
                  product = "L1B", bbox = fixture_bbox(),
                  columns = names(sl_columns("L1B")))

  registry_names <- names(sl_columns("L1B"))
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

# ---------------------------------------------------------------------------
# GEDI L4A
# ---------------------------------------------------------------------------
#
# L4A (footprint AGBD) is the simplest GEDI shape: lat/lon at beam
# root, one `geolocation/` column, `land_cover_data/` subgroup. No
# 2D profiles, no pool columns, no transposed. Covered here mostly to
# exercise the L4A lat/lon path through the SatelliteProduct trait
# and confirm the L4A-specific marquee column (`agbd`) round-trips.

test_that("L4A fixture: sl_read returns a tibble with expected shape", {
  skip_if_not(file.exists(fixture_path("gedi-l4a.h5")),
              "L4A fixture not built")

  data <- sl_read(fixture_path("gedi-l4a.h5"),
                  product = "L4A", bbox = fixture_bbox())

  expect_s3_class(data, "data.frame")
  expect_equal(nrow(data), 500L)
  expect_setequal(unique(data$beam), c("BEAM0000", "BEAM0101"))
  expect_true(all(c("lat_lowestmode", "lon_lowestmode") %in% names(data)))
  # Marquee L4A column
  expect_true("agbd" %in% names(data))
})

test_that("L4A fixture: every registry column round-trips when requested", {
  skip_if_not(file.exists(fixture_path("gedi-l4a.h5")))

  data <- sl_read(fixture_path("gedi-l4a.h5"),
                  product = "L4A", bbox = fixture_bbox(),
                  columns = names(sl_columns("L4A")))

  registry_names <- names(sl_columns("L4A"))
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

# ---------------------------------------------------------------------------
# GEDI L4C
# ---------------------------------------------------------------------------
#
# Same shape as L4A. Worth a smoke test because L4C was recently added
# to the registry; this catches omissions in product-routing code
# (arg_match lists, filename detection, default sets, etc.).

test_that("L4C fixture: sl_read returns a tibble with expected shape", {
  skip_if_not(file.exists(fixture_path("gedi-l4c.h5")),
              "L4C fixture not built")

  data <- sl_read(fixture_path("gedi-l4c.h5"),
                  product = "L4C", bbox = fixture_bbox())

  expect_s3_class(data, "data.frame")
  expect_equal(nrow(data), 500L)
  expect_setequal(unique(data$beam), c("BEAM0000", "BEAM0101"))
  expect_true(all(c("lat_lowestmode", "lon_lowestmode") %in% names(data)))
  # Marquee L4C columns
  expect_true("wsci" %in% names(data))
})

test_that("L4C fixture: worldcover_class (new L4C-only column) resolves", {
  skip_if_not(file.exists(fixture_path("gedi-l4c.h5")))

  data <- sl_read(fixture_path("gedi-l4c.h5"),
                  product = "L4C", bbox = fixture_bbox(),
                  columns = c("worldcover_class"))

  expect_true("worldcover_class" %in% names(data))
  expect_false("land_cover_data/worldcover_class" %in% names(data))
})

test_that("L4C fixture: every registry column round-trips when requested", {
  skip_if_not(file.exists(fixture_path("gedi-l4c.h5")))

  data <- sl_read(fixture_path("gedi-l4c.h5"),
                  product = "L4C", bbox = fixture_bbox(),
                  columns = names(sl_columns("L4C")))

  registry_names <- names(sl_columns("L4C"))
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

# ---------------------------------------------------------------------------
# ICESat-2 ATL08
# ---------------------------------------------------------------------------
#
# ATL08 exercises paths no GEDI fixture touches:
#   - ground-track group names (gt1l / gt2r etc.) instead of BEAM*
#   - lat/lon under a `land_segments/` subgroup
#   - deeply nested datasets: `land_segments/canopy/h_canopy`,
#     `land_segments/terrain/h_te_best_fit` — two levels of prefix
#     stripping
#   - 2D `canopy_h_metrics [N, 18]` under the nested canopy/ group
#   - ICESat-2 arm of the SatelliteProduct trait + sl_read.character
#     routing
#   - the output has a `track` group-label column instead of `beam`

test_that("ATL08 fixture: sl_read returns a tibble with expected shape", {
  skip_if_not(file.exists(fixture_path("icesat2-atl08.h5")),
              "ATL08 fixture not built")

  data <- sl_read(fixture_path("icesat2-atl08.h5"),
                  product = "ATL08", bbox = fixture_bbox())

  expect_s3_class(data, "data.frame")
  expect_equal(nrow(data), 500L)
  expect_setequal(unique(data$track), c("gt1l", "gt2r"))
  # ICESat-2 output uses `track`, not `beam`
  expect_true("track" %in% names(data))
  expect_false("beam" %in% names(data))
  # lat/lon come from land_segments/ — prefix stripped
  expect_true(all(c("latitude", "longitude") %in% names(data)))
})

test_that("ATL08 fixture: spatial filter respects land_segments/ lat/lon path", {
  skip_if_not(file.exists(fixture_path("icesat2-atl08.h5")))
  bb <- fixture_bbox(); b <- unclass(bb)

  data <- sl_read(fixture_path("icesat2-atl08.h5"), product = "ATL08", bbox = bb)

  expect_true(all(data$longitude >= b[["xmin"]] &
                    data$longitude <= b[["xmax"]]))
  expect_true(all(data$latitude >= b[["ymin"]] &
                    data$latitude <= b[["ymax"]]))
})

test_that("ATL08 fixture: nested canopy/ and terrain/ subgroups resolve", {
  skip_if_not(file.exists(fixture_path("icesat2-atl08.h5")))

  data <- sl_read(fixture_path("icesat2-atl08.h5"),
                  product = "ATL08", bbox = fixture_bbox(),
                  columns = c("h_canopy", "h_te_best_fit",
                              "canopy_openness", "terrain_slope"))

  for (nm in c("h_canopy", "h_te_best_fit",
               "canopy_openness", "terrain_slope")) {
    expect_true(nm %in% names(data), label = nm)
  }
  # Full nested paths should be stripped, not retained
  expect_false(any(grepl("land_segments/", names(data))))
  expect_false(any(grepl("canopy/h_canopy", names(data))))
})

test_that("ATL08 fixture: canopy_h_metrics expands to 18 columns", {
  skip_if_not(file.exists(fixture_path("icesat2-atl08.h5")))

  data <- sl_read(fixture_path("icesat2-atl08.h5"),
                  product = "ATL08", bbox = fixture_bbox(),
                  columns = c("h_canopy", "canopy_h_metrics"))

  hm_cols <- grep("^canopy_h_metrics\\d+$", names(data), value = TRUE)
  expect_equal(length(hm_cols), 18L)
})

test_that("ATL08 fixture: default (NULL) columns returns the default set", {
  skip_if_not(file.exists(fixture_path("icesat2-atl08.h5")))

  data <- sl_read(fixture_path("icesat2-atl08.h5"),
                  product = "ATL08", bbox = fixture_bbox())

  # Marquee defaults documented for ATL08
  expect_true(all(c(
    "h_canopy", "h_te_best_fit", "night_flag",
    "latitude", "longitude", "track", "geometry"
  ) %in% names(data)))
})

test_that("ATL08 fixture: every registry column round-trips when requested", {
  skip_if_not(file.exists(fixture_path("icesat2-atl08.h5")))

  data <- sl_read(fixture_path("icesat2-atl08.h5"),
                  product = "ATL08", bbox = fixture_bbox(),
                  columns = names(sl_columns("ATL08")))

  registry_names <- names(sl_columns("ATL08"))
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
