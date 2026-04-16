# ---------------------------------------------------------------------------
# parse_column() dtype dispatch (utils.R)
# ---------------------------------------------------------------------------
#
# parse_column converts raw little-endian bytes from the Rust reader
# into a typed R vector based on the dtype descriptor. The dispatch
# has one arm for FloatingPoint and four for FixedPoint (signed ≤ 4
# bytes, u8, u16, u32 fall-through, and 64-bit hi/lo split). Each
# branch gets a direct test with hand-built bytes and info-JSON so we
# don't need to round-trip through the Rust FFI to exercise them.

info_json <- function(dtype, element_size, num_elements) {
  # Matches the JSON shape rust_hdf5_dataset emits (ffi.rs line ~453).
  sprintf(
    '{"dtype":"%s","shape":[%d],"element_size":%d,"num_elements":%d}',
    dtype, num_elements, element_size, num_elements
  )
}

# ---------------------------------------------------------------------------
# FloatingPoint
# ---------------------------------------------------------------------------

test_that("parse_column decodes 8-byte FloatingPoint (double)", {
  vals <- c(-1.5, 0, 3.14159, .Machine$double.xmin, .Machine$double.xmax)
  bytes <- writeBin(vals, raw(), size = 8L, endian = "little")

  out <- spacelaser:::parse_column(
    bytes,
    info_json("FloatingPoint { size: 8, byte_order: LittleEndian }",
              element_size = 8L, num_elements = length(vals))
  )

  expect_type(out, "double")
  expect_equal(out, vals)
})

test_that("parse_column decodes 4-byte FloatingPoint (float)", {
  vals <- c(-1.5, 0, 3.14)
  bytes <- writeBin(as.numeric(vals), raw(), size = 4L, endian = "little")

  out <- spacelaser:::parse_column(
    bytes,
    info_json("FloatingPoint { size: 4, byte_order: LittleEndian }",
              element_size = 4L, num_elements = length(vals))
  )

  expect_type(out, "double")
  expect_equal(out, vals, tolerance = 1e-6)
})

# ---------------------------------------------------------------------------
# FixedPoint — signed ≤ 4 bytes
# ---------------------------------------------------------------------------

test_that("parse_column decodes 4-byte signed FixedPoint (int32)", {
  # -.Machine$integer.max is -2^31 + 1, which is the largest negative
  # value representable as a literal R integer.
  vals <- c(-.Machine$integer.max, -1L, 0L, 1L, .Machine$integer.max)
  bytes <- writeBin(vals, raw(), size = 4L, endian = "little")

  out <- spacelaser:::parse_column(
    bytes,
    info_json(
      "FixedPoint { size: 4, signed: true, byte_order: LittleEndian }",
      element_size = 4L, num_elements = length(vals)
    )
  )

  expect_type(out, "integer")
  expect_equal(out, vals)
})

test_that("parse_column decodes 2-byte signed FixedPoint (int16)", {
  vals <- c(-32768L, -1L, 0L, 32767L)
  bytes <- writeBin(vals, raw(), size = 2L, endian = "little")

  out <- spacelaser:::parse_column(
    bytes,
    info_json(
      "FixedPoint { size: 2, signed: true, byte_order: LittleEndian }",
      element_size = 2L, num_elements = length(vals)
    )
  )

  expect_type(out, "integer")
  expect_equal(out, vals)
})

# ---------------------------------------------------------------------------
# FixedPoint — u8 (size == 1)
# ---------------------------------------------------------------------------

test_that("parse_column decodes 1-byte unsigned FixedPoint via raw-to-integer", {
  vals <- c(0L, 1L, 127L, 200L, 255L)
  bytes <- as.raw(vals)

  out <- spacelaser:::parse_column(
    bytes,
    info_json(
      "FixedPoint { size: 1, signed: false, byte_order: LittleEndian }",
      element_size = 1L, num_elements = length(vals)
    )
  )

  expect_type(out, "integer")
  expect_equal(out, vals)
})

# ---------------------------------------------------------------------------
# FixedPoint — u16
# ---------------------------------------------------------------------------

test_that("parse_column decodes 2-byte unsigned FixedPoint past the signed range", {
  # 60000 would overflow an int16; verifies the unsigned path is taken.
  vals <- c(0L, 1L, 32767L, 60000L, 65535L)
  bytes <- writeBin(as.integer(vals), raw(), size = 2L, endian = "little")

  out <- spacelaser:::parse_column(
    bytes,
    info_json(
      "FixedPoint { size: 2, signed: false, byte_order: LittleEndian }",
      element_size = 2L, num_elements = length(vals)
    )
  )

  expect_type(out, "integer")
  expect_equal(out, vals)
})

# ---------------------------------------------------------------------------
# FixedPoint — u32 fall-through (signed-read of small values)
# ---------------------------------------------------------------------------

test_that("parse_column reads u32 via the signed-fallback path for small values", {
  # The u32 path reads as signed int32; values below 2^31 round-trip
  # exactly, which is what the current implementation promises.
  vals <- c(0L, 1L, 1000000L, 2147483647L)
  bytes <- writeBin(vals, raw(), size = 4L, endian = "little")

  out <- spacelaser:::parse_column(
    bytes,
    info_json(
      "FixedPoint { size: 4, signed: false, byte_order: LittleEndian }",
      element_size = 4L, num_elements = length(vals)
    )
  )

  expect_type(out, "integer")
  expect_equal(out, vals)
})

# ---------------------------------------------------------------------------
# FixedPoint — 64-bit hi/lo split
# ---------------------------------------------------------------------------

test_that("parse_column decodes 8-byte unsigned FixedPoint via lo/hi split", {
  # GEDI shot_number-shaped values: well under 2^53 so exact.
  vals <- c(0, 1, 4294967295, 4294967296, 123456789012345)
  bytes <- raw(0)
  for (v in vals) {
    lo <- v %% 2^32                         # unsigned low word as double
    hi <- floor(v / 2^32)
    # writeBin writes signed int32; map lo > 2^31 - 1 to two's complement.
    lo_i <- as.integer(if (lo > 2147483647) lo - 2^32 else lo)
    hi_i <- as.integer(hi)
    bytes <- c(
      bytes,
      writeBin(lo_i, raw(), size = 4L, endian = "little"),
      writeBin(hi_i, raw(), size = 4L, endian = "little")
    )
  }

  out <- spacelaser:::parse_column(
    bytes,
    info_json(
      "FixedPoint { size: 8, signed: false, byte_order: LittleEndian }",
      element_size = 8L, num_elements = length(vals)
    )
  )

  expect_type(out, "double")
  expect_equal(out, vals)
})

# ---------------------------------------------------------------------------
# Non-numeric fallback (e.g. raw bytes for string / variable-length)
# ---------------------------------------------------------------------------

test_that("parse_column returns bytes unchanged for unrecognised dtypes", {
  bytes <- as.raw(c(0x01, 0x02, 0x03, 0x04))

  out <- spacelaser:::parse_column(
    bytes,
    info_json("String { size: 4 }", element_size = 4L, num_elements = 1L)
  )

  expect_identical(out, bytes)
})
