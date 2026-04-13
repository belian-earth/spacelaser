# ---------------------------------------------------------------------------
# Column registries
# ---------------------------------------------------------------------------
# Named character vectors: names = short user-facing names,
# values = full HDF5 paths (relative to beam/track group).
# fmt: skip
.gedi_l1b_columns <- c(
  latitude_bin0                      = "geolocation/latitude_bin0",
  longitude_bin0                     = "geolocation/longitude_bin0",
  beam                               = "beam",
  shot_number                        = "shot_number",
  channel                            = "channel",
  # delta_time is a soft link at the beam root pointing to
  # geolocation/delta_time. Use the direct path until transparent
  # soft-link resolution is implemented in the Rust parser.
  delta_time                         = "geolocation/delta_time",
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
  surface_type                       = "geolocation/surface_type",
  # Geophysical corrections live under geophys_corr/, not geolocation/.
  dynamic_atmosphere_correction      = "geophys_corr/dynamic_atmosphere_correction",
  geoid                              = "geophys_corr/geoid",
  tide_earth                         = "geophys_corr/tide_earth",
  tide_load                          = "geophys_corr/tide_load",
  tide_ocean                         = "geophys_corr/tide_ocean",
  tide_ocean_pole                    = "geophys_corr/tide_ocean_pole",
  tide_pole                          = "geophys_corr/tide_pole",
  # ------------------------------------------------------------------
  # Pool datasets (variable-length per shot; opt-in via `columns`).
  # These are flat 1D arrays concatenating all shots' waveform samples
  # across the beam. The reader fetches them in full and the R side
  # slices each shot's waveform into a list column using the
  # rx_/tx_sample_start_index and rx_/tx_sample_count vectors.
  # Not returned when `columns = NULL` (too expensive, and produces
  # list columns which most downstream code doesn't expect).
  # ------------------------------------------------------------------
  rxwaveform                         = "rxwaveform",
  txwaveform                         = "txwaveform"
)

#' L1B pool columns — opt-in waveform datasets.
#'
#' Short names of the L1B columns that live at the beam root as flat
#' 1D arrays of variable-length-per-shot samples rather than shot-rate
#' values. They are read in full by the Rust layer and sliced into
#' per-shot list columns by `build_tibble()` using the map below.
#' Excluded from the default column set so a plain `sl_read(granules)`
#' does not silently download tens of megabytes per beam.
#' @noRd
.gedi_l1b_pool_columns <- c("rxwaveform", "txwaveform")

#' Index columns needed to slice each pool column into per-shot vectors.
#'
#' Keys are pool column short names; values are lists with `start` and
#' `count` short names giving the HDF5 start-index and count vectors
#' that describe where each shot's data lives inside the pool.
#' @noRd
.gedi_l1b_pool_index_map <- list(
  rxwaveform = list(start = "rx_sample_start_index", count = "rx_sample_count"),
  txwaveform = list(start = "tx_sample_start_index", count = "tx_sample_count")
)

# fmt: skip
.gedi_l2a_columns <- c(
  lat_lowestmode                 = "lat_lowestmode",
  lon_lowestmode                 = "lon_lowestmode",
  beam                           = "beam",
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
  beam                           = "beam",
  shot_number                    = "shot_number",
  channel                        = "channel",
  # delta_time is a soft link at the beam root pointing to
  # geolocation/delta_time. Use the direct path until transparent
  # soft-link resolution is implemented in the Rust parser.
  delta_time                     = "geolocation/delta_time",
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
  beam                           = "beam",
  shot_number                    = "shot_number",
  channel                        = "channel",
  delta_time                     = "delta_time",
  master_frac                    = "master_frac",
  master_int                     = "master_int",
  stale_return_flag              = "stale_return_flag",
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
#' product, excluding any pool columns (see `.gedi_l1b_pool_columns`).
#' Pool columns are opt-in because they are expensive and produce list
#' columns.
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

  # NULL → all scalar defaults, with pool columns excluded
  if (is.null(columns)) {
    pool_short <- product_pool_columns(product)
    default_short <- setdiff(names(registry), pool_short)
    return(unique(unname(registry[default_short])))
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

# ---------------------------------------------------------------------------
# Internal: pool column handling (GEDI L1B waveforms)
# ---------------------------------------------------------------------------

#' Short names of pool columns for a given product.
#'
#' Only GEDI L1B has pool columns in the current registry set. Other
#' products return `character(0)`.
#' @noRd
product_pool_columns <- function(product) {
  switch(
    product,
    L1B = .gedi_l1b_pool_columns,
    character(0)
  )
}

#' Index-column map for pool columns in a given product.
#'
#' Returns a named list keyed by pool column short name; each entry has
#' `start` and `count` short names naming the HDF5 index columns needed
#' to slice that pool into per-shot vectors.
#' @noRd
product_pool_index_map <- function(product) {
  switch(
    product,
    L1B = .gedi_l1b_pool_index_map,
    list()
  )
}

#' Ensure that pool-index columns are included in the scalar column list.
#'
#' For each pool column requested, the R side needs the corresponding
#' `*_sample_start_index` and `*_sample_count` vectors to slice the
#' per-shot waveforms out of the flat pool. This helper inspects the
#' requested pool columns and prepends the required index columns
#' (using the registry's HDF5 paths) if they are not already present.
#'
#' @param columns Character vector of resolved scalar HDF5 paths.
#' @param pool_short Character vector of requested pool column short names.
#' @param product Character product identifier.
#' @returns Updated scalar-column character vector.
#' @noRd
ensure_pool_indices <- function(columns, pool_short, product) {
  if (length(pool_short) == 0L) {
    return(columns)
  }
  registry <- .gedi_column_registry[[product]] %||%
    .icesat2_column_registry[[product]]
  idx_map <- product_pool_index_map(product)

  required_short <- character(0)
  for (pc in pool_short) {
    spec <- idx_map[[pc]]
    if (is.null(spec)) next
    required_short <- c(required_short, spec$start, spec$count)
  }
  required_paths <- unname(registry[unique(required_short)])
  c(setdiff(required_paths, columns), columns)
}

#' Build colon-delimited pool specs for the Rust FFI.
#'
#' Each spec is `"hdf5_path:start_col:count_col"` so the Rust side can
#' parse the start/count index columns it already has from the scalar
#' read and compute targeted sample-level byte ranges into the pool
#' dataset, instead of reading the entire (potentially 50+ MB) pool.
#'
#' @param pool_short Character vector of pool column short names.
#' @param pool_paths Character vector of pool column HDF5 paths (parallel).
#' @param product Character product identifier.
#' @returns Character vector of colon-delimited specs, or `character(0)`.
#' @noRd
build_pool_specs <- function(pool_short, pool_paths, product) {
  if (length(pool_short) == 0L) {
    return(character(0))
  }
  idx_map <- product_pool_index_map(product)
  vapply(seq_along(pool_short), function(i) {
    spec <- idx_map[[pool_short[[i]]]]
    if (is.null(spec)) return(NA_character_)
    paste(pool_paths[[i]], spec$start, spec$count, sep = ":")
  }, character(1))
}

#' Split a resolved column list into scalar and pool components.
#'
#' Pool columns are identified by their registry path appearing in the
#' product's pool-column list. Returns paths for the Rust FFI call and
#' short names for `ensure_pool_indices()` / `build_tibble()` slicing.
#'
#' @param columns Character vector of resolved HDF5 paths.
#' @param product Character product identifier.
#' @returns A list with `scalar` (paths), `pool_paths` (paths), and
#'   `pool_short` (short names, parallel to `pool_paths`).
#' @noRd
split_pool_columns <- function(columns, product) {
  pool_short_all <- product_pool_columns(product)
  if (length(pool_short_all) == 0L) {
    return(list(
      scalar = columns,
      pool_paths = character(0),
      pool_short = character(0)
    ))
  }
  registry <- .gedi_column_registry[[product]] %||%
    .icesat2_column_registry[[product]]
  pool_paths_all <- unname(registry[pool_short_all])

  is_pool <- columns %in% pool_paths_all
  requested_paths <- unname(columns[is_pool])
  requested_short <- pool_short_all[match(requested_paths, pool_paths_all)]

  list(
    scalar = unname(columns[!is_pool]),
    pool_paths = requested_paths,
    pool_short = requested_short
  )
}
