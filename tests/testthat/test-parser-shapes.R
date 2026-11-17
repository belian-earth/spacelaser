# ---------------------------------------------------------------------------
# HDF5 shapes and datatypes the per-product fixtures do not contain
# ---------------------------------------------------------------------------
#
# parser-v1.h5 and parser-v2.h5 are written by
# data-raw/fixtures/parser-shapes.py; see that script for what each group
# holds and why. The test columns live in a subgroup `t`, so they are
# requested as raw paths.

shape_bbox <- function() sl_bbox(-124.04, 41.39, -124.01, 41.42)

read_shape <- function(file, product, columns, ...) {
  suppressMessages(sl_read(
    test_path("fixtures", file), bbox = shape_bbox(),
    product = product, columns = paste0("t/", columns), ...
  ))
}

# 25 of the 100 rows of BEAM0001 and gt1l hold the sentinel.
fill_rows <- (seq_len(100) - 1L) %% 4L == 0L

test_that("a declared _FillValue becomes NA in every numeric type", {
  cols <- c("f32_declared", "f64_declared", "i8_declared", "i16_declared",
            "i32_declared", "i64_declared", "u8_declared", "u16_declared",
            "u32_declared", "i8_minus_one")
  d <- read_shape("parser-v1.h5", "L2A", cols)
  d <- d[d$beam == "BEAM0001", ]
  expect_equal(nrow(d), 100L)
  for (col in cols) {
    expect_identical(is.na(d[[col]]), fill_rows, info = col)
    expect_equal(as.numeric(d[[col]][!fill_rows]),
                 ((seq_len(100) - 1L) %% 50L)[!fill_rows], info = col)
  }
  expect_s3_class(d$i64_declared, "integer64")
  expect_type(d$i8_declared, "integer")
  expect_type(d$u32_declared, "double")
})

test_that("a type maximum that no attribute declares stays data", {
  d <- read_shape("parser-v1.h5", "L2A", c("i8_undeclared", "u8_undeclared"))
  d <- d[d$beam == "BEAM0001", ]
  expect_false(anyNA(d$i8_undeclared))
  expect_false(anyNA(d$u8_undeclared))
  expect_true(all(d$i8_undeclared[fill_rows] == 127L))
  expect_true(all(d$u8_undeclared[fill_rows] == 255L))
})

test_that("GEDI's undeclared -9999 and -999999 become NA", {
  d <- read_shape("parser-v1.h5", "L2A", c("f32_gedi", "f64_gedi", "i16_gedi"))
  d <- d[d$beam == "BEAM0001", ]
  for (col in c("f32_gedi", "f64_gedi", "i16_gedi")) {
    expect_identical(is.na(d[[col]]), fill_rows, info = col)
  }
})

test_that("the ATLAS float and int32 sentinels become NA without an attribute", {
  d <- read_shape("parser-v1.h5", "ATL13",
                  c("f32_atlas", "f64_atlas", "i32_atlas", "i8_atlas"))
  expect_equal(nrow(d), 100L)
  for (col in c("f32_atlas", "f64_atlas", "i32_atlas")) {
    expect_identical(is.na(d[[col]]), fill_rows, info = col)
  }
  # No ICESat-2 float survives as the float32 maximum.
  expect_false(any(d$f32_atlas > 1e38, na.rm = TRUE))
  # 127 is a legitimate int8 value unless the dataset declares it.
  expect_false(anyNA(d$i8_atlas))
})

test_that("a declared fill applies to each column of a 2-D dataset", {
  d <- read_shape("parser-v1.h5", "L2A", "matrix")
  d <- d[d$beam == "BEAM0001", ]
  expect_false(anyNA(d$matrix0))
  expect_identical(is.na(d$matrix1), fill_rows)
  expect_equal(unique(d$matrix2), 2)
})

test_that("fixed-length strings are character and enums are integer", {
  d <- read_shape("parser-v1.h5", "L2A", c("stratum", "enum_flag"))
  d <- d[d$beam == "BEAM0001", ]
  expect_type(d$stratum, "character")
  expect_equal(d$stratum[1:4], c("ENT_NAm", "DBT_Eu", "GSW", "ENT_NAm"))
  expect_type(d$enum_flag, "integer")
  expect_equal(d$enum_flag[1:4], c(0L, 1L, 0L, 1L))
})

test_that("a column that cannot join the selection is left out with a warning", {
  expect_warning(
    d <- read_shape("parser-v1.h5", "L2A", c("f32_declared", "other_rate", "cube")),
    "2 requested columns could not be read"
  )
  expect_false(any(c("other_rate", "cube") %in% names(d)))
  expect_true("f32_declared" %in% names(d))

  w <- tryCatch(
    read_shape("parser-v1.h5", "L2A", "other_rate"),
    warning = function(w) conditionMessage(w)
  )
  expect_match(w, "different rate")
})

test_that("row pruning keeps the last chunk of a two-level chunk B-tree", {
  d <- read_shape("parser-v1.h5", "L2A", c("row", "row_wide"))
  d <- d[d$beam == "BEAM0000", ]
  expect_equal(d$row, 1001993:1001999)
  expect_equal(d$row_wide, 1001993:1001999)
})

test_that("version 2 object headers, attributes and filter pipelines are read", {
  d <- read_shape("parser-v2.h5", "L2A", c("value", "row"))
  expect_equal(nrow(d), 150L)
  expect_equal(d$row, 0:149)
  expect_identical(is.na(d$value), (0:149) %% 5L == 0L)
  expect_equal(d$value[!is.na(d$value)], (0:149)[(0:149) %% 5L != 0L])
})

test_that("dense link storage lists every link", {
  links <- sl_hdf5_groups(test_path("fixtures", "parser-v2.h5"), "/dense")
  expect_length(links, 600L)
  expect_setequal(substr(links, 1, 4), sprintf("d%03d", 0:599))
})
