#' Read GEDI data with spatial subsetting
#'
#' Reads GEDI satellite lidar data directly from a remote HDF5 file, fetching
#' only the footprints that fall within the specified bounding box. No file
#' download required.
#'
#' @param url Character. URL of the GEDI HDF5 file (HTTPS or S3).
#' @param product Character. GEDI product level: `"L2A"`, `"L2B"`, `"L4A"`,
#'   or `"L1B"`.
#' @param bbox An `sl_bbox` object created by [sl_bbox()], or a numeric vector
#'   `c(xmin, ymin, xmax, ymax)`.
#' @param columns Character vector of dataset names to read. If `NULL` (the
#'   default), reads the standard columns for the specified product.
#' @param beams Character vector of beam names to read (e.g., `"BEAM0101"`).
#'   If `NULL`, reads all 8 beams.
#' @param token Bearer token for NASA Earthdata authentication. If `NULL`,
#'   uses [sl_earthdata_token()] to find credentials.
#'
#' @returns A data frame (tibble-like) with one row per footprint. Columns
#'   include the requested datasets plus a `beam` identifier and a `geometry`
#'   column (`wk_xy`). If multiple beams contain data, rows are combined.
#'
#' @details
#' The function uses HTTP Range requests to read only the metadata and data
#' chunks needed for the spatial subset. For a typical bounding box query on a
#' ~2 GB GEDI file, this requires ~10-30 small HTTP requests instead of
#' downloading the entire file.
#'
#' ## Columns
#'
#' Default columns vary by product. Use `columns` to override. Column names
#' correspond to HDF5 dataset names within each beam group (e.g.,
#' `"lat_lowestmode"`, `"rh"`, `"quality_flag"`).
#'
#' @importFrom rlang check_required arg_match abort inform warn is_string
#' @export
grab_gedi <- function(url,
                      product = c("L2A", "L2B", "L4A", "L1B"),
                      bbox,
                      columns = NULL,
                      beams = NULL,
                      token = NULL) {
  rlang::check_required(url)
  rlang::check_required(bbox)
  product <- rlang::arg_match(product)

  lat_lon <- gedi_lat_lon(product)

  grab_product(
    url = url,
    product = product,
    bbox = bbox,
    columns = columns,
    groups = beams,
    token = token,
    rust_fn = rust_read_gedi,
    lat_col = lat_lon$lat,
    lon_col = lat_lon$lon,
    group_label = "beam",
    element_label = "footprint"
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
