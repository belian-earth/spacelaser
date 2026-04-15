#' Read a single ICESat-2 granule.
#'
#' Internal helper used by [sl_read()] when given a character URL.
#' Encapsulates ICESat-2-specific knowledge: lat/lon paths (which
#' differ per-product — see `icesat2_lat_lon()`) and the `track`
#' group label. Reads all 6 ground tracks (`gt1l`..`gt3r`) that exist
#' in the file; users filter post-hoc on the returned `track` column.
#'
#' @param url Character. URL of the ICESat-2 HDF5 file. HTTP/HTTPS or
#'   a local filesystem path (`file://` or bare path) are both accepted.
#' @param product Character. ICESat-2 product: one of `"ATL03"`,
#'   `"ATL06"`, `"ATL07"`, `"ATL08"`, `"ATL10"`, `"ATL13"`, or `"ATL24"`.
#' @param bbox An `sl_bbox` or numeric `c(xmin, ymin, xmax, ymax)`.
#' @param columns Character vector of short column names, or `NULL`
#'   for the product's curated default set.
#' @param convert_time Logical. See [sl_read()].
#'
#' @importFrom rlang check_required arg_match
#' @noRd
read_icesat2 <- function(url,
                         product = c("ATL03", "ATL06", "ATL07", "ATL08",
                                     "ATL10", "ATL13", "ATL24"),
                         bbox,
                         columns = NULL,
                         convert_time = TRUE) {
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
    element_label = "element",
    convert_time = convert_time
  )
}
