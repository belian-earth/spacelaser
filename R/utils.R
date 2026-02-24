# ---------------------------------------------------------------------------
# S3 generic: sl_grab()
# ---------------------------------------------------------------------------

#' Grab satellite lidar data
#'
#' `sl_grab()` is an S3 generic that reads GEDI or ICESat-2 data from remote
#' HDF5 files. It dispatches on the type of its first argument:
#'
#' * An `sl_gedi_search` or `sl_icesat2_search` object (from [find_gedi()] /
#'   [find_icesat2()]) — reads all granules in the search result, combining
#'   rows into a single data frame.
#' * A character vector of URLs — auto-detects the sensor and product from the
#'   file name, or uses the explicit `product` argument.
#'
#' @param x An `sl_gedi_search`, `sl_icesat2_search`, or character vector of
#'   HDF5 URLs.
#' @param bbox An `sl_bbox` or numeric `c(xmin, ymin, xmax, ymax)`. Required.
#' @param ... Additional arguments passed to the underlying reader
#'   ([grab_gedi()] or [grab_icesat2()]).
#'
#' @returns A data frame with one row per footprint/segment.
#'
#' @seealso [find_gedi()], [find_icesat2()], [grab_gedi()], [grab_icesat2()]
#' @export
sl_grab <- function(x, bbox, ...) {
  UseMethod("sl_grab")
}

#' @rdname sl_grab
#' @export
sl_grab.sl_gedi_search <- function(
  x,
  bbox,
  columns = NULL,
  beams = NULL,
  ...
) {
  rlang::check_required(bbox)
  product <- attr(x, "product")
  lat_lon <- gedi_lat_lon(product)

  grab_product_multi(
    urls = x$url,
    product = product,
    bbox = bbox,
    columns = columns,
    groups = beams,
    rust_multi_fn = rust_read_gedi_multi,
    lat_col = lat_lon$lat,
    lon_col = lat_lon$lon,
    group_label = "beam",
    element_label = "footprint"
  )
}

#' @rdname sl_grab
#' @export
sl_grab.sl_icesat2_search <- function(
  x,
  bbox,
  columns = NULL,
  tracks = NULL,
  ...
) {
  rlang::check_required(bbox)
  product <- attr(x, "product")
  geo_cols <- icesat2_lat_lon(product)

  grab_product_multi(
    urls = x$url,
    product = product,
    bbox = bbox,
    columns = columns,
    groups = tracks,
    rust_multi_fn = rust_read_icesat2_multi,
    lat_col = geo_cols$lat,
    lon_col = geo_cols$lon,
    group_label = "track",
    element_label = "element"
  )
}

#' @rdname sl_grab
#' @param product Character. Product level (e.g., `"L2A"`, `"ATL08"`).
#'   Required when `x` is a character vector and the product cannot be
#'   inferred from the file name.
#' @export
sl_grab.character <- function(x, bbox, ..., product = NULL) {
  rlang::check_required(bbox)
  info <- detect_sensor(x[[1L]], product)
  grab_urls(
    x,
    bbox = bbox,
    grab_fn = info$grab_fn,
    product = info$product,
    ...
  )
}

#' @export
sl_grab.default <- function(x, bbox, ...) {
  cls <- paste(class(x), collapse = "/")
  cli::cli_abort(c(
    "{.fun sl_grab} does not know how to handle an object of class {.cls {cls}}.",
    "i" = "Expected an {.cls sl_gedi_search}, {.cls sl_icesat2_search}, or {.cls character} URL vector."
  ))
}

#' Read multiple URLs and row-bind results.
#' @noRd
grab_urls <- function(urls, bbox, grab_fn, product, ...) {
  urls <- urls[!is.na(urls)]
  if (length(urls) == 0L) {
    cli::cli_inform("No URLs to read.")
    return(vctrs::new_data_frame(list(), n = 0L))
  }

  results <- purrr::map(urls, function(u) {
    tryCatch(
      grab_fn(url = u, product = product, bbox = bbox, ...),
      error = function(e) {
        cli::cli_warn(c(
          "!" = "Failed to read {.file {basename(u)}}.",
          "i" = conditionMessage(e)
        ))
        NULL
      }
    )
  })

  results <- purrr::compact(results)
  if (length(results) == 0L) {
    return(vctrs::new_data_frame(list(), n = 0L))
  }

  vctrs::vec_rbind(!!!results)
}

#' Detect sensor and product from a URL filename.
#' @noRd
detect_sensor <- function(url, product = NULL) {
  bn <- basename(url)

  if (grepl("^GEDI", bn, ignore.case = TRUE)) {
    grab_fn <- grab_gedi
    if (is.null(product)) {
      product <- if (grepl("GEDI01_B", bn)) {
        "L1B"
      } else if (grepl("GEDI02_A", bn)) {
        "L2A"
      } else if (grepl("GEDI02_B", bn)) {
        "L2B"
      } else if (grepl("GEDI_L4A|GEDI04_A", bn)) {
        "L4A"
      } else {
        rlang::abort(c(
          "Cannot detect GEDI product from filename.",
          "i" = "Pass {.arg product} explicitly."
        ))
      }
    }
  } else if (grepl("^ATL", bn, ignore.case = TRUE)) {
    grab_fn <- grab_icesat2
    if (is.null(product)) {
      product <- if (grepl("ATL03", bn)) {
        "ATL03"
      } else if (grepl("ATL06", bn)) {
        "ATL06"
      } else if (grepl("ATL08", bn)) {
        "ATL08"
      } else {
        rlang::abort(c(
          "Cannot detect ICESat-2 product from filename.",
          "i" = "Pass {.arg product} explicitly."
        ))
      }
    }
  } else {
    if (is.null(product)) {
      rlang::abort(c(
        "Cannot detect sensor from URL filename {.val {bn}}.",
        "i" = "Pass {.arg product} explicitly."
      ))
    }
    # Infer sensor from the product string
    if (product %in% c("L1B", "L2A", "L2B", "L4A")) {
      grab_fn <- grab_gedi
    } else if (product %in% c("ATL03", "ATL06", "ATL08")) {
      grab_fn <- grab_icesat2
    } else {
      rlang::abort("Unknown product {.val {product}}.")
    }
  }

  list(grab_fn = grab_fn, product = product)
}

# ---------------------------------------------------------------------------
# sl_bbox
# ---------------------------------------------------------------------------

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

  vctrs::new_vctr(
    c(xmin = xmin, ymin = ymin, xmax = xmax, ymax = ymax),
    class = "sl_bbox"
  )
}

#' @export
format.sl_bbox <- function(x, ...) {
  sprintf(
    "(%.4f, %.4f) - (%.4f, %.4f)",
    x[["xmin"]],
    x[["ymin"]],
    x[["xmax"]],
    x[["ymax"]]
  )
}

#' @export
print.sl_bbox <- function(x, ...) {
  cli::cli_text("{.cls sl_bbox}: {format(x)}")
  invisible(x)
}

# ---------------------------------------------------------------------------
# Internal: shared product reader
# ---------------------------------------------------------------------------

#' Common implementation behind grab_gedi() and grab_icesat2().
#'
#' Both functions follow the same workflow:
#'   1. Validate bbox, resolve credentials
#'   2. Call the appropriate Rust reader
#'   3. Convert each group's raw byte columns into an R data frame
#'   4. Attach geometry and group label, combine rows
#'
#' @param url,product,bbox,columns  Forwarded from the public wrapper.
#' @param groups  Beam names (GEDI) or track names (ICESat-2), or `NULL`.
#' @param rust_fn  The Rust FFI function to call (`rust_read_gedi` or
#'   `rust_read_icesat2`).
#' @param lat_col,lon_col  Column names for latitude / longitude (used to
#'   build the `geometry` column via `wk::xy()`).
#' @param group_label  Name for the group identifier column (`"beam"` or
#'   `"track"`).
#' @param element_label  Human-readable name for a row, used in the log
#'   message (`"footprint"` or `"element"`).
#' @noRd
grab_product <- function(
  url,
  product,
  bbox,
  columns,
  groups,
  rust_fn,
  lat_col,
  lon_col,
  group_label,
  element_label
) {
  bbox <- validate_bbox(bbox)
  columns <- validate_columns(columns, product)
  columns <- ensure_lat_lon(columns, lat_col, lon_col)
  b <- unclass(bbox)
  creds <- sl_earthdata_creds()

  cli::cli_progress_step(
    "Reading {product} from {.url {basename(url)}}"
  )

  # Both rust_read_gedi and rust_read_icesat2 accept the group filter
  # (beams / tracks) as the 8th positional argument, followed by
  # username and password for Earthdata OAuth.
  raw_result <- rust_fn(
    url,
    product,
    b[["xmin"]],
    b[["ymin"]],
    b[["xmax"]],
    b[["ymax"]],
    columns,
    groups,
    creds$username,
    creds$password
  )

  if (length(raw_result) == 0L) {
    cli::cli_inform("No {element_label}s found within the bounding box.")
    return(vctrs::new_data_frame(list(), n = 0L))
  }

  group_tbls <- purrr::map(raw_result, function(gd) {
    tbl <- build_tibble(gd, lat_col = lat_col, lon_col = lon_col)
    tbl[[group_label]] <- gd$group_name
    # Strip subgroup prefixes (e.g. "geolocation/lat_lowestmode" ->
    # "lat_lowestmode") for cleaner column names, matching chewie output.
    names(tbl) <- sub(".*/", "", names(tbl))
    tbl
  })

  result <- vctrs::vec_rbind(!!!group_tbls)

  n <- nrow(result)
  cli::cli_progress_done()
  cli::cli_inform(c(
    "v" = "Read {n} {element_label}{?s} from {length(group_tbls)} {group_label}{?s}."
  ))

  result
}

# ---------------------------------------------------------------------------
# Internal: multi-URL product reader (concurrent via Rust)
# ---------------------------------------------------------------------------

#' Read multiple granules concurrently via Rust.
#'
#' All URLs are passed to a single Rust function that processes them in
#' parallel within one async runtime.  This gives much higher throughput
#' than sequential per-file reads.
#'
#' @noRd
grab_product_multi <- function(
  urls,
  product,
  bbox,
  columns,
  groups,
  rust_multi_fn,
  lat_col,
  lon_col,
  group_label,
  element_label
) {
  urls <- urls[!is.na(urls)]
  if (length(urls) == 0L) {
    cli::cli_inform("No URLs to read.")
    return(vctrs::new_data_frame(list(), n = 0L))
  }

  bbox <- validate_bbox(bbox)
  columns <- validate_columns(columns, product)
  columns <- ensure_lat_lon(columns, lat_col, lon_col)
  b <- unclass(bbox)
  creds <- sl_earthdata_creds()

  cli::cli_progress_step(
    "Reading {product} from {length(urls)} granule{?s}"
  )

  raw_result <- rust_multi_fn(
    urls,
    product,
    b[["xmin"]],
    b[["ymin"]],
    b[["xmax"]],
    b[["ymax"]],
    columns,
    groups,
    creds$username,
    creds$password
  )

  if (length(raw_result) == 0L) {
    cli::cli_inform("No {element_label}s found within the bounding box.")
    return(vctrs::new_data_frame(list(), n = 0L))
  }

  group_tbls <- purrr::map(raw_result, function(gd) {
    tbl <- build_tibble(gd, lat_col = lat_col, lon_col = lon_col)
    tbl[[group_label]] <- gd$group_name
    names(tbl) <- sub(".*/", "", names(tbl))
    tbl
  })

  result <- vctrs::vec_rbind(!!!group_tbls)

  n <- nrow(result)
  cli::cli_progress_done()
  cli::cli_inform(c(
    "v" = "Read {n} {element_label}{?s} from {length(group_tbls)} {group_label}{?s}."
  ))

  result
}

# ---------------------------------------------------------------------------
# Internal: lat/lon column helpers (shared between grab_* and sl_grab)
# ---------------------------------------------------------------------------

#' @noRd
gedi_lat_lon <- function(product) {
  switch(
    product,
    "L2A" = list(lat = "lat_lowestmode", lon = "lon_lowestmode"),
    "L2B" = list(
      lat = "geolocation/lat_lowestmode",
      lon = "geolocation/lon_lowestmode"
    ),
    "L4A" = list(lat = "lat_lowestmode", lon = "lon_lowestmode"),
    "L1B" = list(
      lat = "geolocation/latitude_bin0",
      lon = "geolocation/longitude_bin0"
    )
  )
}

#' @noRd
icesat2_lat_lon <- function(product) {
  switch(
    product,
    ATL03 = list(lat = "heights/lat_ph", lon = "heights/lon_ph"),
    ATL06 = list(
      lat = "land_ice_segments/latitude",
      lon = "land_ice_segments/longitude"
    ),
    ATL08 = list(
      lat = "land_segments/latitude",
      lon = "land_segments/longitude"
    )
  )
}

# ---------------------------------------------------------------------------
# Internal: column parsing and tibble construction
# ---------------------------------------------------------------------------

#' Convert raw bytes from Rust to an R vector based on HDF5 dtype.
#'
#' The Rust side sends each column as a raw byte vector plus a JSON string
#' describing the HDF5 datatype (e.g. `FixedPoint { size: 4, signed: true }`)
#' and element count. We use `readBin()` to reinterpret the bytes.
#'
#' @param raw_bytes Raw vector of bytes from the Rust reader.
#' @param info_json JSON string with element_size, num_elements, dtype fields.
#' @returns An appropriate R vector (double, integer, or raw).
#' @noRd
parse_column <- function(raw_bytes, info_json) {
  info <- parse_column_info(info_json)
  n <- info$num_elements
  dtype <- info$dtype
  elem_size <- info$element_size

  if (grepl("FloatingPoint", dtype, fixed = TRUE)) {
    readBin(
      raw_bytes,
      what = "double",
      n = n,
      size = elem_size,
      endian = "little"
    )
  } else if (grepl("FixedPoint", dtype, fixed = TRUE)) {
    signed <- grepl("signed: true", dtype, fixed = TRUE)
    if (signed) {
      if (elem_size <= 4) {
        readBin(
          raw_bytes,
          what = "integer",
          n = n,
          size = elem_size,
          endian = "little"
        )
      } else {
        readBin(raw_bytes, what = "double", n = n, size = 8, endian = "little")
      }
    } else {
      if (elem_size == 1) {
        as.integer(raw_bytes)
      } else if (elem_size == 2) {
        readBin(
          raw_bytes,
          what = "integer",
          n = n,
          size = elem_size,
          endian = "little",
          signed = FALSE
        )
      } else if (elem_size <= 4) {
        # R only supports signed = FALSE for sizes 1 and 2; for uint32
        # read as signed (values > 2^31 - 1 are rare in practice).
        readBin(
          raw_bytes,
          what = "integer",
          n = n,
          size = elem_size,
          endian = "little"
        )
      } else {
        readBin(raw_bytes, what = "double", n = n, size = 8, endian = "little")
      }
    }
  } else {
    raw_bytes
  }
}

#' Extract element_size, num_elements, dtype from column info JSON.
#'
#' Uses simple regex extraction instead of a full JSON parser to avoid
#' pulling in jsonlite as a dependency for this single internal use.
#'
#' @noRd
parse_column_info <- function(json_str) {
  extract_int <- function(key) {
    m <- regmatches(
      json_str,
      regexpr(paste0('"', key, '":\\s*(\\d+)'), json_str)
    )
    as.integer(sub(paste0('"', key, '":\\s*'), "", m))
  }
  m <- regmatches(json_str, regexpr('"dtype":"([^"]*)"', json_str))
  dtype <- sub('"dtype":"([^"]*)"', "\\1", m)

  list(
    element_size = extract_int("element_size"),
    num_elements = extract_int("num_elements"),
    dtype = dtype
  )
}

#' Build a data frame from a Rust group data list.
#'
#' Converts raw byte columns to proper R types and attaches geometry via wk.
#' @noRd
build_tibble <- function(group_data, lat_col, lon_col) {
  col_names <- names(group_data$columns)
  col_info <- group_data$col_info

  cols <- purrr::map(col_names, function(nm) {
    parse_column(group_data$columns[[nm]], col_info[[nm]])
  })
  names(cols) <- col_names

  n <- group_data$n_elements %||% 0L
  if (n == 0L && length(cols) > 0L) {
    n <- length(cols[[1]])
  }

  tbl <- vctrs::new_data_frame(cols, n = n)

  if (lat_col %in% col_names && lon_col %in% col_names) {
    tbl[["geometry"]] <- wk::xy(
      x = tbl[[lon_col]],
      y = tbl[[lat_col]],
      crs = wk::wk_crs_longlat()
    )
  }

  tbl
}

`%||%` <- function(x, y) if (is.null(x)) y else x
