# ---------------------------------------------------------------------------
# Column registries
# ---------------------------------------------------------------------------
# Named character vectors: names = short user-facing names,
# values = full HDF5 paths (relative to beam/track group).
# fmt: skip
.gedi_l1b_columns <- c(
  latitude_bin0                      = "geolocation/latitude_bin0",
  longitude_bin0                     = "geolocation/longitude_bin0",
  shot_number                        = "shot_number",
  channel                            = "channel",
  delta_time                         = "delta_time",
  master_frac                        = "master_frac",
  master_int                         = "master_int",
  stale_return_flag                  = "stale_return_flag",
  all_samples_sum                    = "all_samples_sum",
  noise_mean_corrected               = "noise_mean_corrected",
  noise_stddev_corrected             = "noise_stddev_corrected",
  nsemean_even                       = "nsemean_even",
  nsemean_odd                        = "nsemean_odd",
  rx_energy                          = "rx_energy",
  rx_offset                          = "rx_offset",
  rx_open                            = "rx_open",
  rx_sample_count                    = "rx_sample_count",
  rx_sample_start_index              = "rx_sample_start_index",
  selection_stretchers_x             = "selection_stretchers_x",
  selection_stretchers_y             = "selection_stretchers_y",
  th_left_used                       = "th_left_used",
  tx_egamplitude                     = "tx_egamplitude",
  tx_egamplitude_error               = "tx_egamplitude_error",
  tx_egbias                          = "tx_egbias",
  tx_egbias_error                    = "tx_egbias_error",
  tx_egflag                          = "tx_egflag",
  tx_eggamma                         = "tx_eggamma",
  tx_eggamma_error                   = "tx_eggamma_error",
  tx_egsigma                         = "tx_egsigma",
  tx_egsigma_error                   = "tx_egsigma_error",
  tx_gloc                            = "tx_gloc",
  tx_gloc_error                      = "tx_gloc_error",
  tx_pulseflag                       = "tx_pulseflag",
  tx_sample_count                    = "tx_sample_count",
  tx_sample_start_index              = "tx_sample_start_index",
  altitude_instrument                = "geolocation/altitude_instrument",
  altitude_instrument_error          = "geolocation/altitude_instrument_error",
  bounce_time_offset_bin0            = "geolocation/bounce_time_offset_bin0",
  bounce_time_offset_bin0_error      = "geolocation/bounce_time_offset_bin0_error",
  bounce_time_offset_lastbin         = "geolocation/bounce_time_offset_lastbin",
  bounce_time_offset_lastbin_error   = "geolocation/bounce_time_offset_lastbin_error",
  degrade                            = "geolocation/degrade",
  digital_elevation_model            = "geolocation/digital_elevation_model",
  digital_elevation_model_srtm       = "geolocation/digital_elevation_model_srtm",
  elevation_bin0                     = "geolocation/elevation_bin0",
  elevation_bin0_error               = "geolocation/elevation_bin0_error",
  elevation_lastbin                  = "geolocation/elevation_lastbin",
  elevation_lastbin_error            = "geolocation/elevation_lastbin_error",
  latitude_bin0_error                = "geolocation/latitude_bin0_error",
  latitude_instrument                = "geolocation/latitude_instrument",
  latitude_instrument_error          = "geolocation/latitude_instrument_error",
  latitude_lastbin                   = "geolocation/latitude_lastbin",
  latitude_lastbin_error             = "geolocation/latitude_lastbin_error",
  local_beam_azimuth                 = "geolocation/local_beam_azimuth",
  local_beam_azimuth_error           = "geolocation/local_beam_azimuth_error",
  local_beam_elevation               = "geolocation/local_beam_elevation",
  local_beam_elevation_error         = "geolocation/local_beam_elevation_error",
  longitude_bin0_error               = "geolocation/longitude_bin0_error",
  longitude_instrument               = "geolocation/longitude_instrument",
  longitude_instrument_error         = "geolocation/longitude_instrument_error",
  longitude_lastbin                  = "geolocation/longitude_lastbin",
  longitude_lastbin_error            = "geolocation/longitude_lastbin_error",
  mean_sea_surface                   = "geolocation/mean_sea_surface",
  neutat_delay_derivative_bin0       = "geolocation/neutat_delay_derivative_bin0",
  neutat_delay_derivative_lastbin    = "geolocation/neutat_delay_derivative_lastbin",
  neutat_delay_total_bin0            = "geolocation/neutat_delay_total_bin0",
  neutat_delay_total_lastbin         = "geolocation/neutat_delay_total_lastbin",
  range_bias_correction              = "geolocation/range_bias_correction",
  solar_azimuth                      = "geolocation/solar_azimuth",
  solar_elevation                    = "geolocation/solar_elevation",
  dynamic_atmosphere_correction      = "geolocation/dynamic_atmosphere_correction",
  geoid                              = "geolocation/geoid",
  tide_earth                         = "geolocation/tide_earth",
  tide_load                          = "geolocation/tide_load",
  tide_ocean                         = "geolocation/tide_ocean",
  tide_ocean_pole                    = "geolocation/tide_ocean_pole",
  tide_pole                          = "geolocation/tide_pole"
)
# fmt: skip
.gedi_l2a_columns <- c(
  lat_lowestmode                 = "lat_lowestmode",
  lon_lowestmode                 = "lon_lowestmode",
  shot_number                    = "shot_number",
  channel                        = "channel",
  delta_time                     = "delta_time",
  master_frac                    = "master_frac",
  master_int                     = "master_int",
  degrade_flag                   = "degrade_flag",
  quality_flag                   = "quality_flag",
  sensitivity                    = "sensitivity",
  solar_azimuth                  = "solar_azimuth",
  solar_elevation                = "solar_elevation",
  surface_flag                   = "surface_flag",
  elevation_bias_flag            = "elevation_bias_flag",
  elev_lowestmode                = "elev_lowestmode",
  elev_highestreturn             = "elev_highestreturn",
  energy_total                   = "energy_total",
  num_detectedmodes              = "num_detectedmodes",
  rh                             = "rh",
  selected_algorithm             = "selected_algorithm",
  selected_mode                  = "selected_mode",
  selected_mode_flag             = "selected_mode_flag",
  digital_elevation_model        = "digital_elevation_model",
  digital_elevation_model_srtm   = "digital_elevation_model_srtm",
  mean_sea_surface               = "mean_sea_surface",
  elevation_bin0_error           = "elevation_bin0_error",
  lat_highestreturn              = "lat_highestreturn",
  latitude_bin0_error            = "latitude_bin0_error",
  lon_highestreturn              = "lon_highestreturn",
  longitude_bin0_error           = "longitude_bin0_error",
  landsat_treecover              = "land_cover_data/landsat_treecover",
  landsat_water_persistence      = "land_cover_data/landsat_water_persistence",
  leaf_off_doy                   = "land_cover_data/leaf_off_doy",
  leaf_off_flag                  = "land_cover_data/leaf_off_flag",
  leaf_on_cycle                  = "land_cover_data/leaf_on_cycle",
  leaf_on_doy                    = "land_cover_data/leaf_on_doy",
  modis_nonvegetated             = "land_cover_data/modis_nonvegetated",
  modis_nonvegetated_sd          = "land_cover_data/modis_nonvegetated_sd",
  modis_treecover                = "land_cover_data/modis_treecover",
  modis_treecover_sd             = "land_cover_data/modis_treecover_sd",
  pft_class                      = "land_cover_data/pft_class",
  region_class                   = "land_cover_data/region_class",
  urban_focal_window_size        = "land_cover_data/urban_focal_window_size",
  urban_proportion               = "land_cover_data/urban_proportion"
)
# fmt: skip
.gedi_l2b_columns <- c(
  lat_lowestmode                 = "geolocation/lat_lowestmode",
  lon_lowestmode                 = "geolocation/lon_lowestmode",
  shot_number                    = "shot_number",
  channel                        = "channel",
  delta_time                     = "delta_time",
  master_frac                    = "master_frac",
  master_int                     = "master_int",
  algorithmrun_flag              = "algorithmrun_flag",
  l2a_quality_flag               = "l2a_quality_flag",
  l2b_quality_flag               = "l2b_quality_flag",
  num_detectedmodes              = "num_detectedmodes",
  sensitivity                    = "sensitivity",
  stale_return_flag              = "stale_return_flag",
  surface_flag                   = "surface_flag",
  cover                          = "cover",
  cover_z                        = "cover_z",
  fhd_normal                     = "fhd_normal",
  omega                          = "omega",
  pai                            = "pai",
  pai_z                          = "pai_z",
  pavd_z                         = "pavd_z",
  pgap_theta                     = "pgap_theta",
  pgap_theta_error               = "pgap_theta_error",
  pgap_theta_z                   = "pgap_theta_z",
  rg                             = "rg",
  rh100                          = "rh100",
  rhog                           = "rhog",
  rhog_error                     = "rhog_error",
  rhov                           = "rhov",
  rhov_error                     = "rhov_error",
  rossg                          = "rossg",
  rv                             = "rv",
  rx_range_highestreturn         = "rx_range_highestreturn",
  rx_sample_count                = "rx_sample_count",
  rx_sample_start_index          = "rx_sample_start_index",
  selected_l2a_algorithm         = "selected_l2a_algorithm",
  selected_mode                  = "selected_mode",
  selected_mode_flag             = "selected_mode_flag",
  selected_rg_algorithm          = "selected_rg_algorithm",
  degrade_flag                   = "geolocation/degrade_flag",
  digital_elevation_model        = "geolocation/digital_elevation_model",
  elev_highestreturn             = "geolocation/elev_highestreturn",
  elev_lowestmode                = "geolocation/elev_lowestmode",
  elevation_bin0                 = "geolocation/elevation_bin0",
  elevation_bin0_error           = "geolocation/elevation_bin0_error",
  elevation_lastbin              = "geolocation/elevation_lastbin",
  elevation_lastbin_error        = "geolocation/elevation_lastbin_error",
  height_bin0                    = "geolocation/height_bin0",
  height_lastbin                 = "geolocation/height_lastbin",
  lat_highestreturn              = "geolocation/lat_highestreturn",
  latitude_bin0                  = "geolocation/latitude_bin0",
  latitude_bin0_error            = "geolocation/latitude_bin0_error",
  latitude_lastbin               = "geolocation/latitude_lastbin",
  latitude_lastbin_error         = "geolocation/latitude_lastbin_error",
  local_beam_azimuth             = "geolocation/local_beam_azimuth",
  local_beam_elevation           = "geolocation/local_beam_elevation",
  lon_highestreturn              = "geolocation/lon_highestreturn",
  longitude_bin0                 = "geolocation/longitude_bin0",
  longitude_bin0_error           = "geolocation/longitude_bin0_error",
  longitude_lastbin              = "geolocation/longitude_lastbin",
  longitude_lastbin_error        = "geolocation/longitude_lastbin_error",
  solar_azimuth                  = "geolocation/solar_azimuth",
  solar_elevation                = "geolocation/solar_elevation",
  landsat_treecover              = "land_cover_data/landsat_treecover",
  landsat_water_persistence      = "land_cover_data/landsat_water_persistence",
  leaf_off_doy                   = "land_cover_data/leaf_off_doy",
  leaf_off_flag                  = "land_cover_data/leaf_off_flag",
  leaf_on_cycle                  = "land_cover_data/leaf_on_cycle",
  leaf_on_doy                    = "land_cover_data/leaf_on_doy",
  modis_nonvegetated             = "land_cover_data/modis_nonvegetated",
  modis_nonvegetated_sd          = "land_cover_data/modis_nonvegetated_sd",
  modis_treecover                = "land_cover_data/modis_treecover",
  modis_treecover_sd             = "land_cover_data/modis_treecover_sd",
  pft_class                      = "land_cover_data/pft_class",
  region_class                   = "land_cover_data/region_class",
  urban_focal_window_size        = "land_cover_data/urban_focal_window_size",
  urban_proportion               = "land_cover_data/urban_proportion"
)
# fmt: skip
.gedi_l4a_columns <- c(
  lat_lowestmode                 = "lat_lowestmode",
  lon_lowestmode                 = "lon_lowestmode",
  shot_number                    = "shot_number",
  channel                        = "channel",
  delta_time                     = "delta_time",
  master_frac                    = "master_frac",
  master_int                     = "master_int",
  algorithm_run_flag             = "algorithm_run_flag",
  degrade_flag                   = "degrade_flag",
  l2_quality_flag                = "l2_quality_flag",
  l4_quality_flag                = "l4_quality_flag",
  sensitivity                    = "sensitivity",
  solar_elevation                = "solar_elevation",
  surface_flag                   = "surface_flag",
  agbd                           = "agbd",
  agbd_pi_lower                  = "agbd_pi_lower",
  agbd_pi_upper                  = "agbd_pi_upper",
  agbd_se                        = "agbd_se",
  agbd_t                         = "agbd_t",
  agbd_t_se                      = "agbd_t_se",
  elev_lowestmode                = "elev_lowestmode",
  predict_stratum                = "predict_stratum",
  predictor_limit_flag           = "predictor_limit_flag",
  response_limit_flag            = "response_limit_flag",
  selected_algorithm             = "selected_algorithm",
  selected_mode                  = "selected_mode",
  selected_mode_flag             = "selected_mode_flag",
  landsat_treecover              = "land_cover_data/landsat_treecover",
  landsat_water_persistence      = "land_cover_data/landsat_water_persistence",
  leaf_off_doy                   = "land_cover_data/leaf_off_doy",
  leaf_off_flag                  = "land_cover_data/leaf_off_flag",
  leaf_on_cycle                  = "land_cover_data/leaf_on_cycle",
  leaf_on_doy                    = "land_cover_data/leaf_on_doy",
  pft_class                      = "land_cover_data/pft_class",
  region_class                   = "land_cover_data/region_class",
  urban_focal_window_size        = "land_cover_data/urban_focal_window_size",
  urban_proportion               = "land_cover_data/urban_proportion"
)
# fmt: skip
.icesat2_atl03_columns <- c(
  lat_ph           = "heights/lat_ph",
  lon_ph           = "heights/lon_ph",
  h_ph             = "heights/h_ph",
  signal_conf_ph   = "heights/signal_conf_ph",
  delta_time       = "heights/delta_time"
)
# fmt: skip
.icesat2_atl06_columns <- c(
  latitude               = "land_ice_segments/latitude",
  longitude              = "land_ice_segments/longitude",
  h_li                   = "land_ice_segments/h_li",
  h_li_sigma             = "land_ice_segments/h_li_sigma",
  atl06_quality_summary  = "land_ice_segments/atl06_quality_summary",
  delta_time             = "land_ice_segments/delta_time",
  segment_id             = "land_ice_segments/segment_id"
)
# fmt: skip
.icesat2_atl08_columns <- c(
  latitude         = "land_segments/latitude",
  longitude        = "land_segments/longitude",
  h_canopy         = "land_segments/canopy/h_canopy",
  canopy_openness  = "land_segments/canopy/canopy_openness",
  h_te_best_fit    = "land_segments/terrain/h_te_best_fit",
  h_te_uncertainty = "land_segments/terrain/h_te_uncertainty",
  delta_time       = "land_segments/delta_time",
  segment_id_beg   = "land_segments/segment_id_beg",
  night_flag       = "land_segments/night_flag"
)

# ---------------------------------------------------------------------------
# Column lookup tables (product -> registry)
# ---------------------------------------------------------------------------

.gedi_column_registry <- list(
  L1B = .gedi_l1b_columns,
  L2A = .gedi_l2a_columns,
  L2B = .gedi_l2b_columns,
  L4A = .gedi_l4a_columns
)

.icesat2_column_registry <- list(
  ATL03 = .icesat2_atl03_columns,
  ATL06 = .icesat2_atl06_columns,
  ATL08 = .icesat2_atl08_columns
)

# ---------------------------------------------------------------------------
# Exported: column discovery
# ---------------------------------------------------------------------------

#' List available columns for a GEDI or ICESat-2 product
#'
#' Returns a named character vector of all available columns for the given
#' product. Names are the short user-facing column names (used in the
#' `columns` argument of [sl_read()]). Values are the full HDF5 dataset
#' paths.
#'
#' @param product Character. One of:
#'   * GEDI: `"L1B"`, `"L2A"`, `"L2B"`, `"L4A"`
#'   * ICESat-2: `"ATL03"`, `"ATL06"`, `"ATL08"`
#' @returns A named character vector.
#'
#' @examples
#' sl_columns("L2A")
#' names(sl_columns("ATL08"))
#'
#' @export
sl_columns <- function(
  product = c("L2A", "L2B", "L4A", "L1B", "ATL08", "ATL03", "ATL06")
) {
  product <- rlang::arg_match(product)
  .gedi_column_registry[[product]] %||% .icesat2_column_registry[[product]]
}

# ---------------------------------------------------------------------------
# Internal: validation
# ---------------------------------------------------------------------------

#' Validate and resolve column names to full HDF5 paths.
#'
#' When `columns` is `NULL`, returns the full default column set for the
#' product (R is the single source of truth — Rust receives explicit paths).
#'
#' Short user-facing names are matched against the registry. Names that look
#' like expanded 2D columns (e.g., `"rh90"`) are resolved by stripping
#' trailing digits to find the base column (e.g., `"rh"`). Columns containing
#' `/` are treated as raw HDF5 paths and passed through without validation.
#'
#' @param columns Character vector of short column names, or `NULL`.
#' @param product Character. Product identifier (e.g., `"L2A"`, `"ATL08"`).
#' @returns Character vector of full HDF5 paths (always non-NULL).
#' @noRd
validate_columns <- function(columns, product) {
  registry <- .gedi_column_registry[[product]] %||%
    .icesat2_column_registry[[product]]

  if (is.null(registry)) {
    cli::cli_abort("Unknown product {.val {product}}.")
  }

  # NULL → all defaults
  if (is.null(columns)) {
    return(unique(unname(registry)))
  }

  # Columns containing "/" are raw HDF5 paths — pass through unchanged
  is_raw <- grepl("/", columns, fixed = TRUE)
  short_cols <- columns[!is_raw]
  raw_cols <- columns[is_raw]

  valid_names <- names(registry)
  resolved <- character(length(short_cols))

  for (i in seq_along(short_cols)) {
    col <- short_cols[[i]]
    if (col %in% valid_names) {
      resolved[[i]] <- unname(registry[[col]])
    } else {
      # Try 2D expansion: strip trailing digits (e.g., rh90 -> rh)
      base <- sub("\\d+$", "", col)
      if (nchar(base) > 0L && base != col && base %in% valid_names) {
        resolved[[i]] <- unname(registry[[base]])
      } else {
        # Trigger rlang error with fuzzy "did you mean?" suggestions
        rlang::arg_match(col, values = valid_names)
      }
    }
  }

  unique(c(resolved, raw_cols))
}

#' Ensure lat/lon columns are present in the column list.
#'
#' Prepends lat/lon paths if they are not already included.
#'
#' @param columns Character vector of HDF5 paths.
#' @param lat_col,lon_col Full HDF5 paths for latitude/longitude.
#' @returns Updated character vector.
#' @noRd
ensure_lat_lon <- function(columns, lat_col, lon_col) {
  missing <- setdiff(c(lat_col, lon_col), columns)
  c(missing, columns)
}
