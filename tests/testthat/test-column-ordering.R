# ---------------------------------------------------------------------------
# Output column ordering
# ---------------------------------------------------------------------------
#
# The Rust reader uses a HashMap internally so its raw output column
# order is unstable across runs. assemble_read_result() applies a
# canonical layout; these tests pin that layout so it doesn't drift.

bbox <- sl_bbox(-124.04, 41.39, -124.01, 41.42)
l2a <- testthat::test_path("fixtures", "gedi-l2a.h5")
l1b <- testthat::test_path("fixtures", "gedi-l1b.h5")

# ---- Marquee front matter --------------------------------------------------

test_that("output starts with group / shot_number / time / lat / lon", {
  skip_if_not(file.exists(l2a))

  data <- sl_read(l2a, product = "L2A", bbox = bbox)
  nm <- names(data)

  expect_equal(nm[1], "beam")
  expect_equal(nm[2], "shot_number")
  expect_equal(nm[3], "time")
  expect_equal(nm[4], "lat_lowestmode")
  expect_equal(nm[5], "lon_lowestmode")
})

test_that("track replaces beam in the marquee for ICESat-2 reads", {
  fx <- testthat::test_path("fixtures", "icesat2-atl08.h5")
  skip_if_not(file.exists(fx))

  data <- sl_read(fx, product = "ATL08", bbox = bbox)
  expect_equal(names(data)[1], "track")
})

test_that("convert_time = FALSE keeps `delta_time` in the marquee slot", {
  skip_if_not(file.exists(l2a))

  data <- sl_read(l2a, product = "L2A", bbox = bbox, convert_time = FALSE)
  expect_equal(names(data)[3], "delta_time")
  expect_false("time" %in% names(data))
})

# ---- User-requested order preserved ---------------------------------------

test_that("user column order preserved in the science middle", {
  skip_if_not(file.exists(l2a))

  data <- sl_read(l2a, product = "L2A", bbox = bbox,
                  columns = c("modis_treecover", "quality_flag",
                              "landsat_treecover"))
  nm <- names(data)
  pos <- function(x) match(x, nm)
  # The three requested columns appear in the order requested,
  # after the marquee (beam / lat / lon — no time / shot_number here
  # because they weren't requested).
  expect_lt(pos("modis_treecover"), pos("quality_flag"))
  expect_lt(pos("quality_flag"),    pos("landsat_treecover"))
})

# ---- 2D expansion ordering ------------------------------------------------

test_that("2D expansions are sorted numerically (rh0..rh100, not rh0,rh1,rh10)", {
  skip_if_not(file.exists(l2a))

  data <- sl_read(l2a, product = "L2A", bbox = bbox,
                  columns = c("quality_flag", "rh"))
  nm <- names(data)
  rh_cols <- nm[grepl("^rh\\d+$", nm)]
  suffixes <- as.integer(sub("^rh", "", rh_cols))
  expect_identical(suffixes, 0:100)
})

test_that("2D expansion sits adjacent to its base column position", {
  skip_if_not(file.exists(l2a))

  data <- sl_read(l2a, product = "L2A", bbox = bbox,
                  columns = c("quality_flag", "rh", "landsat_treecover"))
  nm <- names(data)
  rh100_pos <- match("rh100", nm)
  ltc_pos   <- match("landsat_treecover", nm)
  qf_pos    <- match("quality_flag", nm)
  rh0_pos   <- match("rh0", nm)
  expect_equal(rh100_pos - rh0_pos, 100L)        # contiguous block
  expect_lt(qf_pos, rh0_pos)                     # before the rh block
  expect_lt(rh100_pos, ltc_pos)                  # after the rh block
})

# ---- Transposed expansion ordering ----------------------------------------

test_that("transposed surface_type expands in registry label order", {
  skip_if_not(file.exists(l1b))

  data <- sl_read(l1b, product = "L1B", bbox = bbox,
                  columns = c("rxwaveform", "surface_type"))
  nm <- names(data)
  surf <- nm[grepl("^surface_type_", nm)]
  expect_identical(surf, c(
    "surface_type_land", "surface_type_ocean", "surface_type_sea_ice",
    "surface_type_land_ice", "surface_type_inland_water"
  ))
})

# ---- Auto-added trailing block --------------------------------------------

test_that("auto-added pool deps land before geometry, pool indices are stripped", {
  skip_if_not(file.exists(l1b))

  data <- sl_read(l1b, product = "L1B", bbox = bbox,
                  columns = c("rxwaveform", "surface_type"))
  nm <- names(data)
  geom_pos <- match("geometry", nm)
  expect_equal(geom_pos, length(nm))  # geometry last

  # Pool-index columns (rx_sample_start_index) are consumed by the Rust
  # pool slicer and stripped from the output. Only the rxwaveform deps
  # that the user might need (elevation_bin0, elevation_lastbin) appear
  # before geometry.
  expect_false("rx_sample_start_index" %in% nm)
  expect_true("elevation_bin0" %in% nm)
  expect_true("elevation_lastbin" %in% nm)
})

test_that("user-requested pool index stays in the middle (not stripped)", {
  skip_if_not(file.exists(l1b))

  data <- sl_read(l1b, product = "L1B", bbox = bbox,
                  columns = c("rx_sample_start_index", "rxwaveform",
                              "rx_sample_count"))
  nm <- names(data)
  # rx_sample_start_index was explicitly requested → kept in middle,
  # NOT stripped (stripping only applies to auto-added indices).
  expect_true("rx_sample_start_index" %in% nm)
  expect_true("rx_sample_count" %in% nm)

  pos_start <- match("rx_sample_start_index", nm)
  pos_rx    <- match("rxwaveform", nm)
  pos_count <- match("rx_sample_count", nm)
  expect_lt(pos_start, pos_rx)
  expect_lt(pos_rx,    pos_count)
})

# ---- Stability ------------------------------------------------------------

test_that("column order is stable across repeated reads", {
  skip_if_not(file.exists(l2a))
  a <- sl_read(l2a, product = "L2A", bbox = bbox)
  b <- sl_read(l2a, product = "L2A", bbox = bbox)
  expect_identical(names(a), names(b))
})

# ---- geometry last --------------------------------------------------------

test_that("geometry is always the last column", {
  skip_if_not(file.exists(l2a))
  for (cols in list(NULL,
                    c("rh"),
                    c("quality_flag", "modis_treecover"),
                    c("rh", "delta_time"))) {
    d <- sl_read(l2a, product = "L2A", bbox = bbox, columns = cols)
    expect_equal(names(d)[length(names(d))], "geometry",
                 info = paste("columns =", paste(cols, collapse = ", ")))
  }
})
