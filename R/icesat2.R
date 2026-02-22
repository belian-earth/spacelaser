#' Read ICESat-2 data with spatial subsetting
#'
#' Reads ICESat-2 satellite lidar data directly from a remote HDF5 file,
#' fetching only the data that falls within the specified bounding box.
#'
#' @param url Character. URL of the ICESat-2 HDF5 file (HTTPS or S3).
#' @param product Character. ICESat-2 product: `"ATL08"`, `"ATL03"`, or
#'   `"ATL06"`.
#' @param bbox An `sl_bbox` object created by [sl_bbox()], or a numeric vector
#'   `c(xmin, ymin, xmax, ymax)`.
#' @param columns Character vector of dataset paths to read (relative to the
#'   ground track group, e.g., `"land_segments/canopy/h_canopy"`). If `NULL`,
#'   reads the standard columns for the specified product.
#' @param tracks Character vector of ground track names (e.g., `"gt1l"`). If
#'   `NULL`, reads all 6 tracks.
#' @param token Bearer token for NASA Earthdata authentication.
#'
#' @returns A data frame with one row per element (segment, photon, etc.).
#'   Columns include the requested datasets plus a `track` identifier and a
#'   `geometry` column (`wk_xy`).
#'
#' @details
#' ## Products
#'
#' - **ATL08**: Land/vegetation segments at 100m resolution. Best for canopy
#'   height and terrain analysis.
#' - **ATL06**: Land ice elevation at ~40m resolution.
#' - **ATL03**: Individual photon heights. Very large datasets (millions of
#'   rows per track). Use a small bounding box.
#'
#' @importFrom rlang check_required arg_match
#' @export
grab_icesat2 <- function(url,
                         product = c("ATL08", "ATL03", "ATL06"),
                         bbox,
                         columns = NULL,
                         tracks = NULL,
                         token = NULL) {
  rlang::check_required(url)
  rlang::check_required(bbox)
  product <- rlang::arg_match(product)

  geo_cols <- switch(product,
    ATL03 = list(lat = "heights/lat_ph", lon = "heights/lon_ph"),
    ATL06 = list(lat = "land_ice_segments/latitude",
                 lon = "land_ice_segments/longitude"),
    ATL08 = list(lat = "land_segments/latitude",
                 lon = "land_segments/longitude")
  )

  grab_product(
    url = url,
    product = product,
    bbox = bbox,
    columns = columns,
    groups = tracks,
    token = token,
    rust_fn = rust_read_icesat2,
    lat_col = geo_cols$lat,
    lon_col = geo_cols$lon,
    group_label = "track",
    element_label = "element"
  )
}
