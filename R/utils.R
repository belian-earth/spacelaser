# ---------------------------------------------------------------------------
# S3 generic: sl_read()
# ---------------------------------------------------------------------------

# TODO(vignette): post-hoc beam/track filtering (power beams, strong/weak gt)

#' Read satellite lidar data
#'
#' `sl_read()` is an S3 generic that reads GEDI or ICESat-2 data from remote
#' HDF5 files using HTTP range requests. Only the chunks intersecting the
#' bounding box are fetched; no full-file download. It dispatches on the type
#' of its first argument:
#'
#' * An `sl_gedi_search` or `sl_icesat2_search` object (from [sl_search()]):
#'   reads all granules in the search result, combining rows into a single
#'   data frame. The search bbox is used by default; an explicit `bbox` may
#'   be supplied to subset further but must be contained within it.
#' * A character vector of URLs: auto-detects the sensor and product from the
#'   file name, or uses the explicit `product` argument.
#'
#' All beams (GEDI) or ground tracks (ICESat-2) are always read; the returned
#' data frame includes a `beam` (GEDI) or `track` (ICESat-2) identifier
#' column for post-hoc filtering with [dplyr::filter()] or base subsetting.
#'
#' @param x An `sl_gedi_search`, `sl_icesat2_search`, or character vector of
#'   HDF5 URLs.
#' @param bbox An `sl_bbox` or numeric `c(xmin, ymin, xmax, ymax)`. Required
#'   when `x` is a character vector. Optional when `x` is a search result: it
#'   defaults to the bbox the search was performed with. If supplied, it
#'   must be fully contained within the search bbox; supplying a wider bbox
#'   is an error to avoid silently missing data outside the original search.
#' @param columns Character vector of column names to read (short names from
#'   [sl_columns()]). Latitude and longitude are always included
#'   automatically. `NULL` (default) reads the curated default column set
#'   for the product (see [sl_columns()] with `set = "default"`). Pass
#'   `names(sl_columns(product))` to read all available columns.
#' @param ... Reserved for method-specific arguments and forwarding.
#'
#' @returns A data frame with one row per footprint (GEDI) or
#'   segment/photon (ICESat-2). Columns depend on the product and the
#'   `columns` argument. Fill-value sentinels (-9999, 3.4e38, etc.) are
#'   automatically replaced with `NA`.
#'
#' @details
#' ## Default columns
#'
#' When `columns = NULL`, a curated default set is returned for each
#' product. Use `sl_columns(product, set = "default")` to see which
#' columns are included. The defaults are designed to cover the primary
#' science variables, key quality flags, and basic context without
#' surprises. Use `sl_columns(product, set = "all")` to discover
#' everything available.
#'
#' ## Product-specific notes
#'
#' **GEDI L1B**: The default set includes `rxwaveform`, which is a
#' **list column** (one numeric vector per shot containing the received
#' waveform). Use [sl_extract_waveforms()] to expand this into a
#' long-form data frame with per-sample elevations. The transmitted
#' waveform (`txwaveform`) is available via explicit request but not
#' included in defaults. L1B reads are slower than other products
#' because waveform data requires targeted chunk reads into the pool
#' dataset.
#'
#' **GEDI L2A**: The `rh` column is a 2D dataset \[N, 101\] that
#' expands into 101 columns (`rh0` through `rh100`), representing
#' relative height percentiles in metres. This is included in the
#' default set.
#'
#' **GEDI L2B**: The default set includes `cover_z`, `pai_z`, and
#' `pavd_z`, which are 2D datasets \[N, 30\] representing canopy
#' vertical profiles at 5 m height bins. Each expands to 30 columns
#' (e.g. `cover_z0` through `cover_z29`), adding 90 columns to the
#' output. `pgap_theta_z` is a variable-length list column (similar to
#' L1B waveforms) and is not included in defaults; request it
#' explicitly when needed.
#'
#' **GEDI L2B `rh100`**: Stored in centimetres in the HDF5 file;
#' automatically converted to metres for consistency with L2A.
#'
#' **GEDI L4A**: The `agbd` column is above-ground biomass density in
#' Mg/ha. Prediction intervals (`agbd_pi_lower`, `agbd_pi_upper`) and
#' standard error (`agbd_se`) are included in defaults.
#'
#' **ICESat-2 ATL03**: Photon-level data. A single granule can produce
#' millions of rows. Multi-granule reads may be slow or timeout due to
#' the large lat/lon arrays that must be downloaded for spatial
#' filtering. Use a small bounding box and few granules.
#' `signal_conf_ph` is a 2D column \[N, 5\] (5 surface types: land,
#' ocean, sea ice, land ice, inland water) that expands to 5 columns.
#'
#' **ICESat-2 ATL06**: Land ice elevation segments. The default set
#' includes fit statistics (`n_fit_photons`, `h_robust_sprd`, `snr`)
#' and reference DEM height (`dem_h`). Tidal and geophysical
#' corrections are available via `sl_columns("ATL06")` but not in
#' defaults.
#'
#' **ICESat-2 ATL08**: The default set includes `canopy_h_metrics`, a
#' 2D dataset \[N, 18\] of canopy height percentiles (P10 through P95)
#' that expands to 18 columns. Terrain slope, photon counts, and land
#' cover are also included. The `*_20m` sub-segment columns and
#' `*_abs` (absolute height) variants are available but not in
#' defaults.
#'
#' @seealso [sl_search()], [sl_columns()], [sl_extract_waveforms()]
#' @export
sl_read <- function(x, bbox, ...) {
  UseMethod("sl_read")
}

#' @rdname sl_read
#' @export
sl_read.sl_gedi_search <- function(x, bbox = NULL, columns = NULL, ...) {
  search_bbox <- attr(x, "bbox")
  bbox <- bbox %||% search_bbox
  check_bbox_within(bbox, search_bbox)
  product <- attr(x, "product")
  lat_lon <- gedi_lat_lon(product)

  read_product_multi(
    urls = x$url,
    product = product,
    bbox = bbox,
    columns = columns,
    rust_multi_fn = rust_read_gedi_multi,
    lat_col = lat_lon$lat,
    lon_col = lat_lon$lon,
    group_label = "beam",
    element_label = "footprint"
  )
}

#' @rdname sl_read
#' @export
sl_read.sl_icesat2_search <- function(x, bbox = NULL, columns = NULL, ...) {
  search_bbox <- attr(x, "bbox")
  bbox <- bbox %||% search_bbox
  check_bbox_within(bbox, search_bbox)
  product <- attr(x, "product")
  geo_cols <- icesat2_lat_lon(product)

  read_product_multi(
    urls = x$url,
    product = product,
    bbox = bbox,
    columns = columns,
    rust_multi_fn = rust_read_icesat2_multi,
    lat_col = geo_cols$lat,
    lon_col = geo_cols$lon,
    group_label = "track",
    element_label = "element"
  )
}

#' @rdname sl_read
#' @param product Character. Product level (e.g., `"L2A"`, `"ATL08"`).
#'   Required when `x` is a character vector and the product cannot be
#'   inferred from the file name.
#' @export
sl_read.character <- function(x, bbox, ..., product = NULL) {
  rlang::check_required(bbox)
  info <- detect_sensor(x[[1L]], product)
  read_urls(
    x,
    bbox = bbox,
    read_fn = info$read_fn,
    product = info$product,
    ...
  )
}

#' @export
sl_read.default <- function(x, bbox, ...) {
  cls <- paste(class(x), collapse = "/")
  cli::cli_abort(c(
    "{.fun sl_read} does not know how to handle an object of class {.cls {cls}}.",
    "i" = "Expected an {.cls sl_gedi_search}, {.cls sl_icesat2_search}, or {.cls character} URL vector."
  ))
}

#' Read multiple URLs and row-bind results.
#' @noRd
read_urls <- function(urls, bbox, read_fn, product, ...) {
  urls <- urls[!is.na(urls)]
  if (length(urls) == 0L) {
    cli::cli_inform("No URLs to read.")
    return(tibble::tibble())
  }

  results <- lapply(urls, function(u) {
    tryCatch(
      read_fn(url = u, product = product, bbox = bbox, ...),
      error = function(e) {
        cli::cli_warn(c(
          "!" = "Failed to read {.file {basename(u)}}.",
          "i" = conditionMessage(e)
        ))
        NULL
      }
    )
  })

  results <- Filter(Negate(is.null), results)
  if (length(results) == 0L) {
    return(tibble::tibble())
  }

  vctrs::vec_rbind(!!!results)
}

#' Detect sensor and product from a URL filename.
#' @noRd
detect_sensor <- function(url, product = NULL) {
  bn <- basename(url)

  if (grepl("^GEDI", bn, ignore.case = TRUE)) {
    read_fn <- read_gedi
    if (is.null(product)) {
      product <- if (grepl("GEDI01_B", bn)) {
        "L1B"
      } else if (grepl("GEDI02_A", bn)) {
        "L2A"
      } else if (grepl("GEDI02_B", bn)) {
        "L2B"
      } else if (grepl("GEDI_L4A|GEDI04_A", bn)) {
        "L4A"
      } else if (grepl("GEDI_L4C|GEDI04_C", bn)) {
        "L4C"
      } else {
        rlang::abort(c(
          "Cannot detect GEDI product from filename.",
          "i" = "Pass {.arg product} explicitly."
        ))
      }
    }
  } else if (grepl("^ATL", bn, ignore.case = TRUE)) {
    read_fn <- read_icesat2
    if (is.null(product)) {
      product <- if (grepl("ATL03", bn)) {
        "ATL03"
      } else if (grepl("ATL06", bn)) {
        "ATL06"
      } else if (grepl("ATL07", bn)) {
        "ATL07"
      } else if (grepl("ATL08", bn)) {
        "ATL08"
      } else if (grepl("ATL10", bn)) {
        "ATL10"
      } else if (grepl("ATL13", bn)) {
        "ATL13"
      } else if (grepl("ATL24", bn)) {
        "ATL24"
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
    if (product %in% c("L1B", "L2A", "L2B", "L4A", "L4C")) {
      read_fn <- read_gedi
    } else if (product %in% c("ATL03", "ATL06", "ATL07", "ATL08", "ATL10", "ATL13", "ATL24")) {
      read_fn <- read_icesat2
    } else {
      rlang::abort("Unknown product {.val {product}}.")
    }
  }

  list(read_fn = read_fn, product = product)
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

#' Check that one bbox is fully contained within another.
#'
#' Used by `sl_read.sl_*_search()` methods to ensure a user-supplied `bbox`
#' does not extend outside the bbox the search was performed with. Equality
#' on any edge is allowed.
#'
#' @param inner The candidate bbox (will be coerced via `validate_bbox`).
#' @param outer The reference bbox (already an `sl_bbox`).
#' @noRd
check_bbox_within <- function(inner, outer, call = rlang::caller_env()) {
  i <- unclass(validate_bbox(inner))
  o <- unclass(outer)
  if (
    i[["xmin"]] < o[["xmin"]] ||
      i[["ymin"]] < o[["ymin"]] ||
      i[["xmax"]] > o[["xmax"]] ||
      i[["ymax"]] > o[["ymax"]]
  ) {
    cli::cli_abort(
      c(
        "{.arg bbox} extends outside the search bbox.",
        "i" = "Search bbox: {format(outer)}",
        "x" = "Grab bbox:   {format(validate_bbox(inner))}",
        "i" = "Re-run {.fun sl_search} with a wider bbox to avoid silently missing data."
      ),
      call = call
    )
  }
  invisible(inner)
}

# ---------------------------------------------------------------------------
# Internal: shared product reader
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Internal: shared product reader (single-URL and multi-URL)
# ---------------------------------------------------------------------------

#' Validate inputs and resolve columns, credentials, and pool specs.
#'
#' Returns a list consumed by `read_product` / `read_product_multi`.
#' @noRd
prepare_read_params <- function(product, bbox, columns, lat_col, lon_col) {
  bbox <- validate_bbox(bbox)
  columns <- validate_columns(columns, product)
  columns <- ensure_lat_lon(columns, lat_col, lon_col)
  # Split into: transposed 2D columns, pool columns, and regular scalars
  trans_split <- split_transposed_columns(columns, product)
  pool_split <- split_pool_columns(trans_split$scalar, product)
  scalar_cols <- ensure_pool_indices(pool_split$scalar, pool_split$pool_short, product)
  pool_specs <- build_pool_specs(pool_split$pool_short, pool_split$pool_paths, product)
  transposed_specs <- build_transposed_specs(trans_split$transposed)
  list(
    bbox = unclass(bbox),
    scalar_cols = scalar_cols,
    pool_specs = pool_specs,
    pool_short = pool_split$pool_short,
    pool_idx_map = product_pool_index_map(product),
    transposed_specs = transposed_specs,
    fill_values = product_fill_values(product),
    scale_factors = product_scale_factors(product),
    creds = sl_earthdata_creds()
  )
}

#' Call the Rust FFI reader with the prepared parameters.
#'
#' `target` is either a single URL (for `rust_read_gedi`) or a character
#' vector of URLs (for `rust_read_gedi_multi`).
#' @noRd
call_rust_reader <- function(rust_fn, target, product, params) {
  rust_fn(
    target,
    product,
    params$bbox[["xmin"]],
    params$bbox[["ymin"]],
    params$bbox[["xmax"]],
    params$bbox[["ymax"]],
    params$scalar_cols,
    NULL,
    params$creds$username,
    params$creds$password,
    if (length(params$pool_specs) > 0L) params$pool_specs else NULL,
    if (length(params$transposed_specs) > 0L) params$transposed_specs else NULL
  )
}

#' Convert the Rust reader's raw output into a combined data frame.
#'
#' Processes each group (beam / track), builds a tibble with geometry
#' and pool list columns, strips subgroup path prefixes, and row-binds.
#' @noRd
assemble_read_result <- function(
  raw_result,
  bbox,
  lat_col,
  lon_col,
  group_label,
  element_label,
  pool_short,
  pool_idx_map,
  fill_values = numeric(0),
  scale_factors = list()
) {
  if (length(raw_result) == 0L) {
    cli::cli_inform("No {element_label}s found within the bounding box.")
    return(tibble::tibble())
  }

  group_tbls <- lapply(raw_result, function(gd) {
    tbl <- build_tibble(
      gd,
      lat_col = lat_col,
      lon_col = lon_col,
      pool_short = pool_short,
      pool_index_map = pool_idx_map,
      fill_values = fill_values,
      scale_factors = scale_factors
    )
    tbl[[group_label]] <- gd$group_name
    tbl
  })

  result <- vctrs::vec_rbind(!!!group_tbls)

  # Post-filter: ensure all rows are within the bbox. This is a no-op
  # for products with direct lat/lon scanning (every row already
  # passes), but matters for ATL03 which uses segment-level spatial
  # filtering (~20m resolution). Edge segments may include a few
  # photons slightly outside the bbox.
  lat_short <- sub(".*/", "", lat_col)
  lon_short <- sub(".*/", "", lon_col)
  if (lat_short %in% names(result) && lon_short %in% names(result) && nrow(result) > 0) {
    b <- unclass(bbox)
    lat <- result[[lat_short]]
    lon <- result[[lon_short]]
    keep <- !is.na(lat) & !is.na(lon) &
      lat >= b[["ymin"]] & lat <= b[["ymax"]] &
      lon >= b[["xmin"]] & lon <= b[["xmax"]]
    if (!all(keep)) {
      result <- result[keep, ]
    }
  }

  n <- nrow(result)
  cli::cli_progress_done()
  cli::cli_inform(c(
    "v" = "Read {n} {element_label}{?s} from {length(group_tbls)} {group_label}{?s}."
  ))

  result
}

#' Read a single granule.
#' @noRd
read_product <- function(
  url,
  product,
  bbox,
  columns,
  rust_fn,
  lat_col,
  lon_col,
  group_label,
  element_label
) {
  params <- prepare_read_params(product, bbox, columns, lat_col, lon_col)
  cli::cli_progress_step("Reading {product} from {.url {basename(url)}}")
  raw_result <- call_rust_reader(rust_fn, url, product, params)
  assemble_read_result(
    raw_result, bbox, lat_col, lon_col, group_label, element_label,
    params$pool_short, params$pool_idx_map,
    params$fill_values, params$scale_factors
  )
}

#' Read multiple granules concurrently via Rust.
#'
#' All URLs are passed to a single Rust function that processes them in
#' parallel within one async runtime.
#' @noRd
read_product_multi <- function(
  urls,
  product,
  bbox,
  columns,
  rust_multi_fn,
  lat_col,
  lon_col,
  group_label,
  element_label
) {
  urls <- urls[!is.na(urls)]
  if (length(urls) == 0L) {
    cli::cli_inform("No URLs to read.")
    return(tibble::tibble())
  }

  params <- prepare_read_params(product, bbox, columns, lat_col, lon_col)
  cli::cli_progress_step("Reading {product} from {length(urls)} granule{?s}")
  raw_result <- call_rust_reader(rust_multi_fn, urls, product, params)
  assemble_read_result(
    raw_result, bbox, lat_col, lon_col, group_label, element_label,
    params$pool_short, params$pool_idx_map,
    params$fill_values, params$scale_factors
  )
}

# ---------------------------------------------------------------------------
# Internal: lat/lon column helpers (shared between read_* and sl_read)
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
    "L4C" = list(lat = "lat_lowestmode", lon = "lon_lowestmode"),
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
    ),
    ATL07 = list(
      lat = "sea_ice_segments/latitude",
      lon = "sea_ice_segments/longitude"
    ),
    ATL10 = list(
      lat = "freeboard_segment/latitude",
      lon = "freeboard_segment/longitude"
    ),
    ATL13 = list(lat = "segment_lat", lon = "segment_lon"),
    ATL24 = list(lat = "lat_ph", lon = "lon_ph")
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
    if (elem_size <= 4 && signed) {
      readBin(raw_bytes, what = "integer", n = n, size = elem_size, endian = "little")
    } else if (elem_size == 1) {
      as.integer(raw_bytes)
    } else if (elem_size == 2 && !signed) {
      readBin(raw_bytes, what = "integer", n = n, size = 2L, endian = "little", signed = FALSE)
    } else if (elem_size <= 4) {
      # uint32: R only supports signed = FALSE for sizes 1 and 2.
      # Read as signed; values > 2^31 - 1 are rare in practice.
      readBin(raw_bytes, what = "integer", n = n, size = elem_size, endian = "little")
    } else {
      # int64 / uint64: R has no native 64-bit integer. Convert by
      # reading pairs of 32-bit words and combining into doubles.
      # Exact for values up to 2^53 (~9e15), which covers all
      # GEDI/ICESat-2 integer columns (shot numbers, indices, counts).
      pairs <- readBin(raw_bytes, what = "integer", n = n * 2L, size = 4L, endian = "little")
      lo <- pairs[seq(1L, length(pairs), 2L)]
      hi <- pairs[seq(2L, length(pairs), 2L)]
      # Unsigned interpretation of the low word (R int32 wraps at 2^31)
      lo_d <- ifelse(lo < 0L, as.double(lo) + 4294967296, as.double(lo))
      lo_d + as.double(hi) * 4294967296
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
#' The Rust side returns pre-typed R vectors (doubles/integers) with
#' fill values already replaced by NA and column names already stripped
#' of HDF5 subgroup prefixes. This function just assembles them into
#' a data frame, applies scale factors, attaches geometry, and slices
#' pool columns into per-shot list columns.
#' @noRd
build_tibble <- function(
  group_data,
  lat_col,
  lon_col,
  pool_short = character(0),
  pool_index_map = list(),
  fill_values = numeric(0),
  scale_factors = list()
) {
  # Columns are already typed R vectors (Doubles/Integers) from Rust,
  # with fill values → NA and name prefixes stripped.
  cols <- as.list(group_data$columns)
  col_names <- names(cols)

  n <- group_data$n_elements %||% 0L
  if (n == 0L && length(cols) > 0L) {
    n <- length(cols[[1]])
  }

  tbl <- tibble::new_tibble(cols, nrow = n)

  # Apply per-column scale factors (R-side only: product-specific,
  # currently only L2B rh100 cm→m).
  if (length(scale_factors) > 0L) {
    tbl_names <- names(tbl)
    for (base_name in names(scale_factors)) {
      factor <- scale_factors[[base_name]]
      pattern <- paste0("^", base_name, "\\d*$")
      matching <- grep(pattern, tbl_names, value = TRUE)
      for (nm in matching) {
        if (is.numeric(tbl[[nm]])) {
          tbl[[nm]] <- tbl[[nm]] * factor
        }
      }
    }
  }

  # Strip subgroup prefix from lat/lon for geometry lookup
  lat_short <- sub(".*/", "", lat_col)
  lon_short <- sub(".*/", "", lon_col)
  if (lat_short %in% col_names && lon_short %in% col_names) {
    tbl[["geometry"]] <- wk::xy(
      x = tbl[[lon_short]],
      y = tbl[[lat_short]],
      crs = wk::wk_crs_longlat()
    )
  }

  # Pool columns: Rust returns raw bytes (not typed) because the R
  # side needs to slice per-shot. Parse, apply fill-value replacement,
  # then slice using count vectors.
  if (length(pool_short) > 0L) {
    pool_raw <- group_data$pool_columns %||% list()
    pool_info <- group_data$pool_col_info %||% list()
    for (pc in pool_short) {
      if (is.null(pool_raw[[pc]])) next
      flat <- parse_column(pool_raw[[pc]], pool_info[[pc]])
      if (length(fill_values) > 0L && is.numeric(flat)) {
        for (fv in fill_values) {
          flat[!is.na(flat) & flat == fv] <- NA
        }
      }
      spec <- pool_index_map[[pc]]
      if (is.null(spec)) next
      counts <- tbl[[spec$count]]
      if (is.null(counts)) next

      slices <- vector("list", n)
      pos <- 1L
      for (i in seq_len(n)) {
        k <- as.integer(counts[[i]])
        if (is.na(k) || k <= 0L) {
          slices[[i]] <- flat[integer(0)]
        } else {
          slices[[i]] <- flat[pos:(pos + k - 1L)]
          pos <- pos + k
        }
      }
      tbl[[pc]] <- slices
    }
  }

  tbl
}

#' @importFrom rlang `%||%`
NULL
