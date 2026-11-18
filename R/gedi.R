#' Read a single GEDI granule.
#'
#' Internal helper used by [sl_read()] when given a character URL.
#' Encapsulates GEDI-specific knowledge: lat/lon paths (which differ
#' per-product — see `gedi_lat_lon()`) and the `beam` group label.
#' Reads all of GEDI's 8 beams that exist in the file (degraded
#' periods may have fewer); users filter post-hoc on the returned
#' `beam` column.
#'
#' @param url Character. URL of the GEDI HDF5 file. HTTP/HTTPS or a
#'   local filesystem path (`file://` or bare path) are both accepted.
#' @param product Character. GEDI product level: one of `"L1B"`,
#'   `"L2A"`, `"L2B"`, `"L4A"`, or `"L4C"`.
#' @param bbox An `sl_bbox` or numeric `c(xmin, ymin, xmax, ymax)`.
#' @param columns Character vector of short column names, or `NULL`
#'   for the product's curated default set.
#' @param convert_time Logical. See [sl_read()].
#'
#' @importFrom rlang check_required arg_match
#' @noRd
read_gedi <- function(url,
                      product = c("L1B", "L2A", "L2B", "L4A", "L4C"),
                      bbox,
                      columns = NULL,
                      convert_time = TRUE) {
  rlang::check_required(url)
  rlang::check_required(bbox)
  product <- rlang::arg_match(product)

  lat_lon <- gedi_lat_lon(product, url)

  read_product(
    url = url,
    product = product,
    bbox = bbox,
    columns = columns,
    rust_fn = rust_read_gedi,
    lat_col = lat_lon$lat,
    lon_col = lat_lon$lon,
    group_label = "beam",
    element_label = "footprint",
    convert_time = convert_time
  )
}

#' Validate and coerce bbox input.
#'
#' Accepts an `sl_bbox`, or a numeric vector of length 4. A vector named
#' `xmin`, `ymin`, `xmax`, `ymax` in any order (an `sf::st_bbox()`, say)
#' is read by name; otherwise the order is `c(xmin, ymin, xmax, ymax)`.
#' @noRd
validate_bbox <- function(bbox, call = rlang::caller_env()) {
  if (inherits(bbox, "sl_bbox")) {
    return(bbox)
  }
  if (is.numeric(bbox) && length(bbox) == 4L) {
    corners <- c("xmin", "ymin", "xmax", "ymax")
    b <- if (setequal(names(bbox), corners)) {
      unclass(bbox)[corners]
    } else {
      unclass(bbox)
    }
    b <- as.double(b)
    return(sl_bbox(b[[1L]], b[[2L]], b[[3L]], b[[4L]]))
  }
  cli::cli_abort(
    "{.arg bbox} must be an {.cls sl_bbox} or a numeric vector of length 4.",
    call = call
  )
}
