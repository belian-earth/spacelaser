#' List groups in a remote HDF5 file
#'
#' Explore the internal structure of a remote HDF5 file by listing the
#' members of a group. Useful for discovering available datasets and beams.
#'
#' @param url Character. URL of the HDF5 file.
#' @param path Character. HDF5 group path to list (default: `"/"`).
#' @param token Bearer token for NASA Earthdata authentication.
#' @returns A character vector of group/dataset names.
#' @export
sl_hdf5_groups <- function(url, path = "/", token = NULL) {
  rlang::check_required(url)
  creds <- sl_earthdata_creds(token)
  rust_hdf5_groups(url = url, path = path,
                   username = creds$username, password = creds$password)
}

#' Read a single dataset from a remote HDF5 file
#'
#' Low-level function to read an arbitrary dataset from a remote HDF5 file.
#' Returns the data as an R vector with appropriate type conversion.
#'
#' @param url Character. URL of the HDF5 file.
#' @param dataset Character. Full HDF5 path to the dataset (e.g.,
#'   `"/BEAM0101/lat_lowestmode"`).
#' @param token Bearer token for NASA Earthdata authentication.
#' @returns An R vector (numeric, integer, or raw depending on the datatype).
#' @export
sl_hdf5_read <- function(url, dataset, token = NULL) {
  rlang::check_required(url)
  rlang::check_required(dataset)
  creds <- sl_earthdata_creds(token)

  result <- rust_hdf5_dataset(
    url = url,
    dataset_path = dataset,
    username = creds$username,
    password = creds$password
  )

  parse_column(result$data, result$info)
}
