#' Read a single GEDI granule.
#'
#' Internal helper used by [sl_read()] when given a character URL. Encapsulates
#' GEDI-specific knowledge: lat/lon paths and the `beam` group label. Always
#' reads all 8 beams; users filter post-hoc on the returned `beam` column.
#'
#' @param url Character. URL of the GEDI HDF5 file (HTTPS or S3).
#' @param product Character. GEDI product level: one of `"L1B"`, `"L2A"`,
#'   `"L2B"`, `"L4A"`, or `"L4C"`.
#' @param bbox An `sl_bbox` or numeric `c(xmin, ymin, xmax, ymax)`.
#' @param columns Character vector of short column names, or `NULL` for the
#'   product default registry.
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

  lat_lon <- gedi_lat_lon(product)

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
#' @noRd
validate_bbox <- function(bbox) {
  if (inherits(bbox, "sl_bbox")) {
    return(bbox)
  }
  if (is.numeric(bbox) && length(bbox) == 4L) {
    return(sl_bbox(bbox[1], bbox[2], bbox[3], bbox[4]))
  }
  cli::cli_abort(
    "{.arg bbox} must be an {.cls sl_bbox} or a numeric vector of length 4."
  )
}
