#' @title spacelaser: Fast Remote Reads of GEDI and ICESat-2 Lidar Data
#'
#' @description
#' R bindings for a pure-Rust HDF5 parser (via extendr) that fetches
#' spatial subsets of NASA GEDI and ICESat-2 lidar granules over HTTP
#' range requests, avoiding multi-gigabyte downloads. Returns tibbles
#' with `wk` geometry columns, ready for analysis.
#'
#' Supported products: GEDI L1B / L2A / L2B / L4A / L4C and ICESat-2
#' ATL03 / ATL06 / ATL07 / ATL08 / ATL10 / ATL13 / ATL24.
#'
#' @section Search:
#' - [sl_search()] --- find granules via NASA CMR
#' - [sl_bbox()] --- construct a bounding box for spatial queries
#'
#' @section Read:
#' - [sl_read()] --- pull spatial subsets from remote HDF5 granules
#'   (dispatches on [`sl_gedi_search`][sl_search] /
#'   [`sl_icesat2_search`][sl_search] objects or a character vector of
#'   URLs)
#'
#' @section Columns:
#' - [sl_columns()] --- inspect the curated column registry for a
#'   given product; returns available column names, HDF5 paths, types,
#'   and whether each is in the default read set
#'
#' @section Waveforms:
#' - [sl_extract_waveforms()] --- expand GEDI L1B rxwaveform list
#'   columns to long form with interpolated elevation per sample, for
#'   direct plotting or downstream analysis
#'
#' @section Low-level HDF5:
#' - [sl_hdf5_groups()] --- list groups and datasets inside a remote
#'   HDF5 file
#' - [sl_hdf5_read()] --- read a single dataset by full HDF5 path,
#'   returning a typed R vector
#'
#' @section Authentication:
#' A NASA Earthdata bearer token is required for any read that hits a
#' DAAC endpoint. spacelaser reads it from the `EARTHDATA_TOKEN`
#' environment variable and sends it as `Authorization: Bearer` to the
#' DAAC. Mint a token with [generate_ed_token()] (or at
#' <https://urs.earthdata.nasa.gov/>, Generate Token) and set it in
#' `~/.Renviron` as `EARTHDATA_TOKEN=<token>`. Tokens expire after 60
#' days; an expired token surfaces a clear error at read time.
#'
#' @keywords internal
"_PACKAGE"

#' @useDynLib spacelaser, .registration = TRUE
#' @importFrom bit64 as.integer64
NULL
