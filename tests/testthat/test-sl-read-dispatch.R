# ---------------------------------------------------------------------------
# sl_read() dispatch + detect_sensor() filename-based routing
# ---------------------------------------------------------------------------
#
# Exercises the three arms of sl_read() generic dispatch and every
# branch of detect_sensor(): the GEDI / ICESat-2 filename patterns,
# the product-only override path, and every failure mode.

# ---------------------------------------------------------------------------
# sl_read.default
# ---------------------------------------------------------------------------

test_that("sl_read.default aborts with a helpful message for unknown classes", {
  expect_error(
    sl_read(42L, bbox = sl_bbox(-1, -1, 1, 1)),
    "does not know how to handle"
  )
  expect_error(
    sl_read(list(a = 1), bbox = sl_bbox(-1, -1, 1, 1)),
    "does not know how to handle"
  )
})

# ---------------------------------------------------------------------------
# detect_sensor — GEDI filename patterns
# ---------------------------------------------------------------------------

test_that("detect_sensor recognises every GEDI product filename", {
  spec <- list(
    "GEDI01_B_2022001000000_O00000_00_T00000_02_005_02_V002.h5" = "L1B",
    "GEDI02_A_2022001000000_O00000_00_T00000_02_003_02_V002.h5" = "L2A",
    "GEDI02_B_2022001000000_O00000_00_T00000_02_003_02_V002.h5" = "L2B",
    "GEDI04_A_2022001000000_O00000_00_T00000_02_003_02_V002.h5" = "L4A",
    "GEDI_L4A_AGB_Density_V2_1_2056.h5"                         = "L4A",
    "GEDI04_C_2022001000000_O00000_00_T00000_02_003_02_V002.h5" = "L4C",
    "GEDI_L4C_WSCI_V2_1_2056.h5"                                = "L4C"
  )
  for (fname in names(spec)) {
    info <- spacelaser:::detect_sensor(fname)
    expect_equal(info$product, spec[[fname]], label = fname)
    expect_identical(info$read_fn, spacelaser:::read_gedi, label = fname)
  }
})

test_that("detect_sensor is case-insensitive on the GEDI prefix (sensor routing)", {
  # Sensor prefix match is case-insensitive; the product suffix match
  # is not, so an all-lowercase filename routes to the GEDI arm but
  # needs an explicit product.
  info <- spacelaser:::detect_sensor("gedi02_a_2022001.h5", product = "L2A")
  expect_identical(info$read_fn, spacelaser:::read_gedi)
})

test_that("detect_sensor aborts on unknown GEDI suffix", {
  expect_error(
    spacelaser:::detect_sensor("GEDI99_X_foo.h5"),
    "Cannot detect GEDI product"
  )
})

# ---------------------------------------------------------------------------
# detect_sensor — ICESat-2 filename patterns
# ---------------------------------------------------------------------------

test_that("detect_sensor recognises every ICESat-2 product filename", {
  spec <- list(
    "ATL03_20220101000000_00000000_005_01.h5" = "ATL03",
    "ATL06_20220101000000_00000000_005_01.h5" = "ATL06",
    "ATL07_20220101000000_00000000_005_01.h5" = "ATL07",
    "ATL08_20220101000000_00000000_005_01.h5" = "ATL08",
    "ATL10_20220101000000_00000000_005_01.h5" = "ATL10",
    "ATL13_20220101000000_00000000_005_01.h5" = "ATL13",
    "ATL24_20220101000000_00000000_005_01.h5" = "ATL24"
  )
  for (fname in names(spec)) {
    info <- spacelaser:::detect_sensor(fname)
    expect_equal(info$product, spec[[fname]], label = fname)
    expect_identical(info$read_fn, spacelaser:::read_icesat2, label = fname)
  }
})

test_that("detect_sensor aborts on unknown ATL suffix", {
  expect_error(
    spacelaser:::detect_sensor("ATL99_foo.h5"),
    "Cannot detect ICESat-2 product"
  )
})

# ---------------------------------------------------------------------------
# detect_sensor — explicit product override
# ---------------------------------------------------------------------------

test_that("explicit product argument overrides filename inference", {
  # GEDI filename, ICESat-2 product argument — product wins only for the
  # returned product string, but read_fn is still chosen from the GEDI
  # arm because the filename matched ^GEDI. That is the documented
  # precedence: filename → sensor, product arg → product string.
  info <- spacelaser:::detect_sensor("GEDI02_A_foo.h5", product = "L2B")
  expect_equal(info$product, "L2B")
  expect_identical(info$read_fn, spacelaser:::read_gedi)
})

test_that("detect_sensor routes by product when the filename is unrecognisable", {
  info_gedi <- spacelaser:::detect_sensor("mystery.h5", product = "L2A")
  expect_identical(info_gedi$read_fn, spacelaser:::read_gedi)

  info_is2 <- spacelaser:::detect_sensor("mystery.h5", product = "ATL08")
  expect_identical(info_is2$read_fn, spacelaser:::read_icesat2)
})

test_that("detect_sensor aborts when neither filename nor product identifies the sensor", {
  expect_error(
    spacelaser:::detect_sensor("mystery.h5"),
    "Cannot detect sensor"
  )
  expect_error(
    spacelaser:::detect_sensor("mystery.h5", product = "NOT_A_PRODUCT"),
    "Unknown product"
  )
})
