#' Read a single ICESat-2 granule.
#'
#' Internal helper used by [sl_read()] when given a character URL. Encapsulates
#' ICESat-2-specific knowledge: lat/lon paths and the `track` group label.
#' Always reads all 6 ground tracks; users filter post-hoc on the returned
#' `track` column.
#'
#' @param url Character. URL of the ICESat-2 HDF5 file (HTTPS or S3).
#' @param product Character. ICESat-2 product: one of `"ATL03"`, `"ATL06"`,
#'   `"ATL07"`, `"ATL08"`, `"ATL10"`, `"ATL13"`, or `"ATL24"`.
#' @param bbox An `sl_bbox` or numeric `c(xmin, ymin, xmax, ymax)`.
#' @param columns Character vector of short column names, or `NULL` for the
#'   product default registry.
#'
#' @importFrom rlang check_required arg_match
#' @noRd
read_icesat2 <- function(url,
                         product = c("ATL03", "ATL06", "ATL07", "ATL08",
                                     "ATL10", "ATL13", "ATL24"),
                         bbox,
                         columns = NULL) {
  rlang::check_required(url)
  rlang::check_required(bbox)
  product <- rlang::arg_match(product)

  geo_cols <- icesat2_lat_lon(product)

  read_product(
    url = url,
    product = product,
    bbox = bbox,
    columns = columns,
    rust_fn = rust_read_icesat2,
    lat_col = geo_cols$lat,
    lon_col = geo_cols$lon,
    group_label = "track",
    element_label = "element"
  )
}
