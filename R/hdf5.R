#' List groups in a remote HDF5 file
#'
#' Low-level helper for exploring the internal structure of a remote
#' HDF5 file. Useful for discovering available datasets, beams, or
#' subgroups when working outside the curated [sl_columns()] registry.
#' Earthdata credentials are required (see [sl_search()] for setup).
#'
#' @param url Character. HTTPS URL of the HDF5 file.
#' @param path Character. HDF5 group path to list (default: `"/"`).
#' @returns A character vector of group/dataset names.
#' @examplesIf interactive()
#' url <- paste0(
#'   "https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected/",
#'   "GEDI02_A.002/GEDI02_A_2020009130403_O06095_03_T02944_02_003_01_V002/",
#'   "GEDI02_A_2020009130403_O06095_03_T02944_02_003_01_V002.h5"
#' )
#' sl_hdf5_groups(url)
#' sl_hdf5_groups(url, path = "BEAM0000/geolocation")
#' @export
sl_hdf5_groups <- function(url, path = "/") {
  rlang::check_required(url)
  creds <- sl_earthdata_creds()
  rust_hdf5_groups(
    url = url,
    path = path,
    username = creds$username,
    password = creds$password
  )
}

#' Read a single dataset from a remote HDF5 file
#'
#' Low-level helper that reads an arbitrary HDF5 dataset by its full
#' path and returns it as an appropriately-typed R vector. Useful for
#' pulling individual fields not covered by the curated registry, or
#' for inspecting a dataset before requesting it via [sl_read()].
#' Earthdata credentials are required (see [sl_search()] for setup).
#'
#' @param url Character. HTTPS URL of the HDF5 file.
#' @param dataset Character. Full HDF5 path to the dataset (e.g.,
#'   `"/BEAM0101/lat_lowestmode"`).
#' @returns An R vector — numeric, integer, or raw, depending on the
#'   underlying HDF5 datatype.
#' @examplesIf interactive()
#' url <- paste0(
#'   "https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected/",
#'   "GEDI02_A.002/GEDI02_A_2020009130403_O06095_03_T02944_02_003_01_V002/",
#'   "GEDI02_A_2020009130403_O06095_03_T02944_02_003_01_V002.h5"
#' )
#' lat <- sl_hdf5_read(url, "BEAM0000/lat_lowestmode")
#' str(lat)
#' @export
sl_hdf5_read <- function(url, dataset) {
  rlang::check_required(url)
  rlang::check_required(dataset)
  creds <- sl_earthdata_creds()

  result <- rust_hdf5_dataset(
    url = url,
    dataset_path = dataset,
    username = creds$username,
    password = creds$password
  )

  parse_column(result$data, result$info)
}
