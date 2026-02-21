#' Create a bounding box for spatial queries
#'
#' @param xmin Minimum longitude (western boundary).
#' @param ymin Minimum latitude (southern boundary).
#' @param xmax Maximum longitude (eastern boundary).
#' @param ymax Maximum latitude (northern boundary).
#' @returns A named double vector of class `sl_bbox`.
#' @export
sl_bbox <- function(xmin, ymin, xmax, ymax) {
  rlang::check_required(xmin)
  rlang::check_required(ymin)
  rlang::check_required(xmax)
  rlang::check_required(ymax)

  if (xmin >= xmax) {
    cli::cli_abort("{.arg xmin} must be less than {.arg xmax}.")
  }
  if (ymin >= ymax) {
    cli::cli_abort("{.arg ymin} must be less than {.arg ymax}.")
  }
  if (ymin < -90 || ymax > 90) {
    cli::cli_abort("Latitude values must be between -90 and 90.")
  }
  if (xmin < -180 || xmax > 180) {
    cli::cli_abort("Longitude values must be between -180 and 180.")
  }

  bbox <- vctrs::new_vctr(
    c(xmin = xmin, ymin = ymin, xmax = xmax, ymax = ymax),
    class = "sl_bbox"
  )
  bbox
}

#' @export
format.sl_bbox <- function(x, ...) {
  sprintf("(%.4f, %.4f) - (%.4f, %.4f)", x[["xmin"]], x[["ymin"]], x[["xmax"]], x[["ymax"]])
}

#' @export
print.sl_bbox <- function(x, ...) {
  cli::cli_text("{.cls sl_bbox}: {format(x)}")
  invisible(x)
}

#' Convert raw bytes from Rust to an R vector based on HDF5 dtype.
#'
#' @param raw_bytes Raw vector of bytes from the Rust reader.
#' @param info_json JSON string with element_size, num_elements, dtype fields.
#' @returns An appropriate R vector (double, integer, raw, etc.).
#' @noRd
parse_column <- function(raw_bytes, info_json) {
  info <- jsonlite_parse(info_json)
  elem_size <- info$element_size
  n <- info$num_elements
  dtype <- info$dtype

  if (grepl("FloatingPoint", dtype, fixed = TRUE)) {
    if (elem_size == 8) {
      readBin(raw_bytes, what = "double", n = n, size = 8, endian = "little")
    } else if (elem_size == 4) {
      readBin(raw_bytes, what = "double", n = n, size = 4, endian = "little")
    } else {
      raw_bytes
    }
  } else if (grepl("FixedPoint", dtype, fixed = TRUE)) {
    if (grepl("signed: true", dtype, fixed = TRUE)) {
      if (elem_size <= 4) {
        readBin(raw_bytes, what = "integer", n = n, size = elem_size, endian = "little")
      } else {
        # 64-bit integers: read as double to avoid overflow
        readBin(raw_bytes, what = "double", n = n, size = 8, endian = "little")
      }
    } else {
      if (elem_size == 1) {
        as.integer(raw_bytes)
      } else if (elem_size <= 4) {
        readBin(raw_bytes, what = "integer", n = n, size = elem_size,
                endian = "little", signed = FALSE)
      } else {
        readBin(raw_bytes, what = "double", n = n, size = 8, endian = "little")
      }
    }
  } else {
    # String or unknown: return raw
    raw_bytes
  }
}

#' Minimal JSON parser (avoid jsonlite dependency for internal use).
#' @noRd
jsonlite_parse <- function(json_str) {
  # Simple JSON parsing for our known structure: {key: value, ...}
  # We only need element_size, num_elements, dtype
  env <- new.env(parent = emptyenv())
  env$element_size <- as.integer(
    regmatches(json_str, regexpr('"element_size":\\s*(\\d+)', json_str))
  )
  env$element_size <- as.integer(gsub('"element_size":\\s*', "", env$element_size))

  env$num_elements <- as.integer(
    regmatches(json_str, regexpr('"num_elements":\\s*(\\d+)', json_str))
  )
  env$num_elements <- as.integer(gsub('"num_elements":\\s*', "", env$num_elements))

  m <- regmatches(json_str, regexpr('"dtype":"([^"]*)"', json_str))
  env$dtype <- gsub('"dtype":"([^"]*)"', "\\1", m)

  as.list(env)
}

#' Build a tibble from Rust beam/track data.
#'
#' Converts raw bytes to proper R types and attaches geometry via wk.
#' @noRd
build_tibble <- function(beam_data, lat_col, lon_col) {
  col_names <- names(beam_data$columns)
  col_info <- beam_data$col_info

  cols <- purrr::map(col_names, function(nm) {
    parse_column(beam_data$columns[[nm]], col_info[[nm]])
  })
  names(cols) <- col_names

  # Determine number of rows from the first non-empty column
  n <- beam_data$n_footprints %||% beam_data$n_elements %||% 0L
  if (n == 0L && length(cols) > 0L) {
    n <- length(cols[[1]])
  }

  tbl <- vctrs::new_data_frame(cols, n = n)

  # Add geometry column from lat/lon via wk
  if (lat_col %in% col_names && lon_col %in% col_names) {
    tbl[["geometry"]] <- wk::xy(
      x = tbl[[lon_col]],
      y = tbl[[lat_col]],
      crs = wk::wk_crs_lonlat()
    )
  }

  tbl
}

`%||%` <- function(x, y) if (is.null(x)) y else x
