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
  # surface_type is transposed 2D [5, N]. It expands into 5 boolean
  # columns named surface_type_{land,ocean,sea_ice,land_ice,inland_water}
  # via the transposed-columns machinery in product_transposed_columns().
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

#' L2B pool columns — variable-length per-shot profile datasets.
#'
#' `pgap_theta_z` is stored as a flat 1D array of concatenated per-shot
#' values (like L1B's rxwaveform), NOT as a 2D matrix like cover_z/pai_z.
#' Each shot contributes a variable number of height bins. The index
#' columns are the same as L1B waveforms: `rx_sample_start_index` and
#' `rx_sample_count`.
#' @noRd
.gedi_l2b_pool_columns <- c("pgap_theta_z")

#' @noRd
.gedi_l2b_pool_index_map <- list(
  pgap_theta_z = list(start = "rx_sample_start_index", count = "rx_sample_count")
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

#' L1B transposed 2D datasets.
#'
#' These are datasets stored as `[K, N]` in HDF5 (K categories × N shots)
#' rather than the usual `[N, K]` (shots × bins). They can't be handled
#' by the standard 2D expansion (which assumes dim 0 is the shot
#' dimension). Instead, the Rust reader reads the full dataset and
#' emits K separate columns named `{base}_{label1}`, `{base}_{label2}`,
#' etc. using the labels here.
#'
#' surface_type with shape `(5, N)` is the only known transposed dataset across all
#' currently supported products. The other candidates from the data
#' dictionaries (ATL03 signal_conf_ph, ATL08 canopy_h_metrics, etc.)
#' are stored `[N, K]` in the actual files despite being described
#' `(K, :)` in the docs, and use the standard 2D expansion.
#' @noRd
.gedi_l1b_transposed_columns <- list(
  surface_type = list(
    path = "geolocation/surface_type",
    labels = c("land", "ocean", "sea_ice", "land_ice", "inland_water")
  )
)

#' Index columns needed to slice each pool column into per-shot vectors.
#'
#' Keys are pool column short names; values are lists with `start` and
#' `count` short names giving the HDF5 start-index and count vectors
#' that describe where each shot's data lives inside the pool.
#' @noRd
.gedi_l1b_pool_index_map <- list(
  rxwaveform = list(
    start = "rx_sample_start_index",
    count = "rx_sample_count",
    # Additional columns auto-added when this pool column is requested.
    # elevation_bin0/lastbin are needed by sl_extract_waveforms() to
    # compute per-sample elevation profiles.
    deps = c("elevation_bin0", "elevation_lastbin")
  ),
  txwaveform = list(
    start = "tx_sample_start_index",
    count = "tx_sample_count"
  )
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
  stale_return_flag              = "geolocation/stale_return_flag",
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
.gedi_l4c_columns <- c(
  lat_lowestmode                 = "lat_lowestmode",
  lon_lowestmode                 = "lon_lowestmode",
  beam                           = "beam",
  shot_number                    = "shot_number",
  channel                        = "channel",
  delta_time                     = "delta_time",
  master_frac                    = "master_frac",
  master_int                     = "master_int",
  algorithm_run_flag             = "algorithm_run_flag",
  degrade_flag                   = "degrade_flag",
  l2_quality_flag                = "l2_quality_flag",
  wsci_quality_flag              = "wsci_quality_flag",
  sensitivity                    = "sensitivity",
  solar_elevation                = "solar_elevation",
  surface_flag                   = "surface_flag",
  elev_lowestmode                = "elev_lowestmode",
  elev_outlier_flag              = "elev_outlier_flag",
  fhd_normal                     = "fhd_normal",
  selected_algorithm             = "selected_algorithm",
  wsci                           = "wsci",
  wsci_pi_lower                  = "wsci_pi_lower",
  wsci_pi_upper                  = "wsci_pi_upper",
  wsci_xy                        = "wsci_xy",
  wsci_xy_pi_lower               = "wsci_xy_pi_lower",
  wsci_xy_pi_upper               = "wsci_xy_pi_upper",
  wsci_z                         = "wsci_z",
  wsci_z_pi_lower                = "wsci_z_pi_lower",
  wsci_z_pi_upper                = "wsci_z_pi_upper",
  stale_return_flag              = "geolocation/stale_return_flag",
  landsat_treecover              = "land_cover_data/landsat_treecover",
  landsat_water_persistence      = "land_cover_data/landsat_water_persistence",
  leaf_off_doy                   = "land_cover_data/leaf_off_doy",
  leaf_off_flag                  = "land_cover_data/leaf_off_flag",
  leaf_on_cycle                  = "land_cover_data/leaf_on_cycle",
  leaf_on_doy                    = "land_cover_data/leaf_on_doy",
  pft_class                      = "land_cover_data/pft_class",
  region_class                   = "land_cover_data/region_class",
  urban_focal_window_size        = "land_cover_data/urban_focal_window_size",
  urban_proportion               = "land_cover_data/urban_proportion",
  worldcover_class               = "land_cover_data/worldcover_class"
)

# fmt: skip
.icesat2_atl03_columns <- c(
  lat_ph             = "heights/lat_ph",
  lon_ph             = "heights/lon_ph",
  h_ph               = "heights/h_ph",
  delta_time         = "heights/delta_time",
  signal_conf_ph     = "heights/signal_conf_ph",
  dist_ph_across     = "heights/dist_ph_across",
  dist_ph_along      = "heights/dist_ph_along",
  pce_mframe_cnt     = "heights/pce_mframe_cnt",
  ph_id_channel      = "heights/ph_id_channel",
  ph_id_count        = "heights/ph_id_count",
  ph_id_pulse        = "heights/ph_id_pulse",
  quality_ph         = "heights/quality_ph",
  signal_class_ph    = "heights/signal_class_ph",
  weight_ph          = "heights/weight_ph"
)
# fmt: skip
.icesat2_atl06_columns <- c(
  latitude                 = "land_ice_segments/latitude",
  longitude                = "land_ice_segments/longitude",
  h_li                     = "land_ice_segments/h_li",
  h_li_sigma               = "land_ice_segments/h_li_sigma",
  atl06_quality_summary    = "land_ice_segments/atl06_quality_summary",
  delta_time               = "land_ice_segments/delta_time",
  segment_id               = "land_ice_segments/segment_id",
  sigma_geo_h              = "land_ice_segments/sigma_geo_h",
  # fit_statistics
  dh_fit_dx                = "land_ice_segments/fit_statistics/dh_fit_dx",
  dh_fit_dx_sigma          = "land_ice_segments/fit_statistics/dh_fit_dx_sigma",
  dh_fit_dy                = "land_ice_segments/fit_statistics/dh_fit_dy",
  n_fit_photons            = "land_ice_segments/fit_statistics/n_fit_photons",
  h_robust_sprd            = "land_ice_segments/fit_statistics/h_robust_sprd",
  h_rms_misfit             = "land_ice_segments/fit_statistics/h_rms_misfit",
  h_mean                   = "land_ice_segments/fit_statistics/h_mean",
  snr                      = "land_ice_segments/fit_statistics/snr",
  snr_significance         = "land_ice_segments/fit_statistics/snr_significance",
  signal_selection_source  = "land_ice_segments/fit_statistics/signal_selection_source",
  w_surface_window_final   = "land_ice_segments/fit_statistics/w_surface_window_final",
  n_seg_pulses             = "land_ice_segments/fit_statistics/n_seg_pulses",
  # dem
  dem_h                    = "land_ice_segments/dem/dem_h",
  dem_flag                 = "land_ice_segments/dem/dem_flag",
  geoid_h                  = "land_ice_segments/dem/geoid_h",
  geoid_free2mean          = "land_ice_segments/dem/geoid_free2mean",
  # geophysical
  bckgrd                   = "land_ice_segments/geophysical/bckgrd",
  cloud_flg_asr            = "land_ice_segments/geophysical/cloud_flg_asr",
  cloud_flg_atm            = "land_ice_segments/geophysical/cloud_flg_atm",
  layer_flag               = "land_ice_segments/geophysical/layer_flag",
  msw_flag                 = "land_ice_segments/geophysical/msw_flag",
  r_eff                    = "land_ice_segments/geophysical/r_eff",
  solar_azimuth            = "land_ice_segments/geophysical/solar_azimuth",
  solar_elevation          = "land_ice_segments/geophysical/solar_elevation",
  tide_earth               = "land_ice_segments/geophysical/tide_earth",
  tide_load                = "land_ice_segments/geophysical/tide_load",
  tide_ocean               = "land_ice_segments/geophysical/tide_ocean",
  tide_pole                = "land_ice_segments/geophysical/tide_pole",
  dac                      = "land_ice_segments/geophysical/dac",
  neutat_delay_total       = "land_ice_segments/geophysical/neutat_delay_total",
  # ground_track
  ref_azimuth              = "land_ice_segments/ground_track/ref_azimuth",
  sigma_geo_at             = "land_ice_segments/ground_track/sigma_geo_at",
  sigma_geo_xt             = "land_ice_segments/ground_track/sigma_geo_xt",
  x_atc                    = "land_ice_segments/ground_track/x_atc",
  y_atc                    = "land_ice_segments/ground_track/y_atc"
)
# fmt: skip
.icesat2_atl08_columns <- c(
  latitude              = "land_segments/latitude",
  longitude             = "land_segments/longitude",
  delta_time            = "land_segments/delta_time",
  segment_id_beg        = "land_segments/segment_id_beg",
  segment_id_end        = "land_segments/segment_id_end",
  night_flag            = "land_segments/night_flag",
  asr                   = "land_segments/asr",
  brightness_flag       = "land_segments/brightness_flag",
  cloud_flag_atm        = "land_segments/cloud_flag_atm",
  cloud_fold_flag       = "land_segments/cloud_fold_flag",
  dem_flag              = "land_segments/dem_flag",
  dem_h                 = "land_segments/dem_h",
  dem_removal_flag      = "land_segments/dem_removal_flag",
  h_dif_ref             = "land_segments/h_dif_ref",
  layer_flag            = "land_segments/layer_flag",
  msw_flag              = "land_segments/msw_flag",
  n_seg_ph              = "land_segments/n_seg_ph",
  rgt                   = "land_segments/rgt",
  sat_flag              = "land_segments/sat_flag",
  segment_landcover     = "land_segments/segment_landcover",
  segment_snowcover     = "land_segments/segment_snowcover",
  segment_watermask     = "land_segments/segment_watermask",
  sigma_atlas_land      = "land_segments/sigma_atlas_land",
  sigma_h               = "land_segments/sigma_h",
  sigma_topo            = "land_segments/sigma_topo",
  solar_azimuth         = "land_segments/solar_azimuth",
  solar_elevation       = "land_segments/solar_elevation",
  surf_type             = "land_segments/surf_type",
  terrain_flg           = "land_segments/terrain_flg",
  urban_flag            = "land_segments/urban_flag",
  # canopy
  h_canopy              = "land_segments/canopy/h_canopy",
  h_canopy_abs          = "land_segments/canopy/h_canopy_abs",
  h_canopy_quad         = "land_segments/canopy/h_canopy_quad",
  h_canopy_uncertainty  = "land_segments/canopy/h_canopy_uncertainty",
  h_dif_canopy          = "land_segments/canopy/h_dif_canopy",
  h_max_canopy          = "land_segments/canopy/h_max_canopy",
  h_max_canopy_abs      = "land_segments/canopy/h_max_canopy_abs",
  h_mean_canopy         = "land_segments/canopy/h_mean_canopy",
  h_mean_canopy_abs     = "land_segments/canopy/h_mean_canopy_abs",
  h_median_canopy       = "land_segments/canopy/h_median_canopy",
  h_median_canopy_abs   = "land_segments/canopy/h_median_canopy_abs",
  h_min_canopy          = "land_segments/canopy/h_min_canopy",
  h_min_canopy_abs      = "land_segments/canopy/h_min_canopy_abs",
  canopy_h_metrics      = "land_segments/canopy/canopy_h_metrics",
  canopy_h_metrics_abs  = "land_segments/canopy/canopy_h_metrics_abs",
  canopy_openness       = "land_segments/canopy/canopy_openness",
  canopy_rh_conf        = "land_segments/canopy/canopy_rh_conf",
  centroid_height       = "land_segments/canopy/centroid_height",
  n_ca_photons          = "land_segments/canopy/n_ca_photons",
  n_toc_photons         = "land_segments/canopy/n_toc_photons",
  photon_rate_can       = "land_segments/canopy/photon_rate_can",
  segment_cover         = "land_segments/canopy/segment_cover",
  toc_roughness         = "land_segments/canopy/toc_roughness",
  # terrain
  h_te_best_fit         = "land_segments/terrain/h_te_best_fit",
  h_te_interp           = "land_segments/terrain/h_te_interp",
  h_te_max              = "land_segments/terrain/h_te_max",
  h_te_mean             = "land_segments/terrain/h_te_mean",
  h_te_median           = "land_segments/terrain/h_te_median",
  h_te_min              = "land_segments/terrain/h_te_min",
  h_te_mode             = "land_segments/terrain/h_te_mode",
  h_te_rh25             = "land_segments/terrain/h_te_rh25",
  h_te_skew             = "land_segments/terrain/h_te_skew",
  h_te_std              = "land_segments/terrain/h_te_std",
  h_te_uncertainty      = "land_segments/terrain/h_te_uncertainty",
  n_te_photons          = "land_segments/terrain/n_te_photons",
  photon_rate_te        = "land_segments/terrain/photon_rate_te",
  terrain_slope         = "land_segments/terrain/terrain_slope"
)

# fmt: skip
.icesat2_atl13_columns <- c(
  # ATL13 datasets live directly under /gtx/ (no intermediate subgroup).
  # anom_ssegs/ subgroup excluded: different row dimension.
  # Core
  segment_lat                    = "segment_lat",
  segment_lon                    = "segment_lon",
  delta_time                     = "delta_time",
  ht_water_surf                  = "ht_water_surf",
  ht_ortho                       = "ht_ortho",
  stdev_water_surf               = "stdev_water_surf",
  significant_wave_ht            = "significant_wave_ht",
  water_depth                    = "water_depth",
  err_ht_water_surf              = "err_ht_water_surf",
  # Segment geometry
  sseg_mean_lat                  = "sseg_mean_lat",
  sseg_mean_lon                  = "sseg_mean_lon",
  sseg_sig_ph_cnt                = "sseg_sig_ph_cnt",
  # Water body ID
  inland_water_body_id           = "inland_water_body_id",
  inland_water_body_type         = "inland_water_body_type",
  inland_water_body_size         = "inland_water_body_size",
  inland_water_body_source       = "inland_water_body_source",
  atl13refid                     = "atl13refid",
  transect_id                    = "transect_id",
  # Height corrections
  segment_geoid                  = "segment_geoid",
  segment_dem_ht                 = "segment_dem_ht",
  segment_dac                    = "segment_dac",
  segment_tide_ocean             = "segment_tide_ocean",
  segment_bias_em                = "segment_bias_em",
  segment_bias_fit               = "segment_bias_fit",
  segment_fpb_correction         = "segment_fpb_correction",
  # Metadata
  segment_id_beg                 = "segment_id_beg",
  segment_id_end                 = "segment_id_end",
  segment_slope_trk_bdy          = "segment_slope_trk_bdy",
  err_slope_trk                  = "err_slope_trk",
  cycle_number                   = "cycle_number",
  rgt                            = "rgt",
  # Subsurface
  subsurface_attenuation         = "subsurface_attenuation",
  subsurface_backscat_ampltd     = "subsurface_backscat_ampltd",
  bottom_lat                     = "bottom_lat",
  bottom_lon                     = "bottom_lon",
  # Quality flags
  segment_quality                = "segment_quality",
  qf_bckgrd                     = "qf_bckgrd",
  qf_bias_em                    = "qf_bias_em",
  qf_bias_fit                   = "qf_bias_fit",
  qf_cloud                      = "qf_cloud",
  qf_ice                        = "qf_ice",
  qf_subsurf_anomaly            = "qf_subsurf_anomaly",
  cloud_flag_asr_atl09           = "cloud_flag_asr_atl09",
  ice_flag                       = "ice_flag",
  # Meteorology
  met_wind10_atl13               = "met_wind10_atl13"
)

# fmt: skip
.icesat2_atl24_columns <- c(
  # ATL24 datasets live directly under /gtx/ (no subgroups).
  # All per-photon, 1D.
  lat_ph                         = "lat_ph",
  lon_ph                         = "lon_ph",
  delta_time                     = "delta_time",
  ortho_h                        = "ortho_h",
  ellipse_h                      = "ellipse_h",
  surface_h                      = "surface_h",
  class_ph                       = "class_ph",
  confidence                     = "confidence",
  sigma_thu                      = "sigma_thu",
  sigma_tvu                      = "sigma_tvu",
  x_atc                          = "x_atc",
  y_atc                          = "y_atc",
  index_ph                       = "index_ph",
  index_seg                      = "index_seg",
  night_flag                     = "night_flag",
  low_confidence_flag            = "low_confidence_flag",
  invalid_kd                     = "invalid_kd",
  invalid_wind_speed             = "invalid_wind_speed",
  sensor_depth_exceeded          = "sensor_depth_exceeded"
)

# fmt: skip
.icesat2_atl07_columns <- c(
  # ATL07 main data under sea_ice_segments/ and its subgroups.
  # sea_ice_segments_10m/ and dda_surface_segments/ excluded (different
  # row dimensions).
  latitude                         = "sea_ice_segments/latitude",
  longitude                        = "sea_ice_segments/longitude",
  delta_time                       = "sea_ice_segments/delta_time",
  height_segment_id                = "sea_ice_segments/height_segment_id",
  seg_dist_x                       = "sea_ice_segments/seg_dist_x",
  # heights
  height_segment_height            = "sea_ice_segments/heights/height_segment_height",
  height_segment_confidence        = "sea_ice_segments/heights/height_segment_confidence",
  height_segment_fit_quality_flag  = "sea_ice_segments/heights/height_segment_fit_quality_flag",
  height_segment_quality           = "sea_ice_segments/heights/height_segment_quality",
  height_segment_length_seg        = "sea_ice_segments/heights/height_segment_length_seg",
  height_segment_type              = "sea_ice_segments/heights/height_segment_type",
  height_segment_ssh_flag          = "sea_ice_segments/heights/height_segment_ssh_flag",
  height_segment_surface_error_est = "sea_ice_segments/heights/height_segment_surface_error_est",
  height_segment_w_gaussian        = "sea_ice_segments/heights/height_segment_w_gaussian",
  height_segment_asr_calc          = "sea_ice_segments/heights/height_segment_asr_calc",
  height_segment_rms               = "sea_ice_segments/heights/height_segment_rms",
  height_segment_n_pulse_seg       = "sea_ice_segments/heights/height_segment_n_pulse_seg",
  across_track_distance            = "sea_ice_segments/heights/across_track_distance",
  # geolocation
  solar_azimuth                    = "sea_ice_segments/geolocation/solar_azimuth",
  solar_elevation                  = "sea_ice_segments/geolocation/solar_elevation",
  sigma_h                          = "sea_ice_segments/geolocation/sigma_h",
  # geophysical
  height_segment_mss               = "sea_ice_segments/geophysical/height_segment_mss",
  height_segment_ocean             = "sea_ice_segments/geophysical/height_segment_ocean",
  height_segment_dac               = "sea_ice_segments/geophysical/height_segment_dac",
  height_segment_earth             = "sea_ice_segments/geophysical/height_segment_earth",
  height_segment_geoid             = "sea_ice_segments/geophysical/height_segment_geoid",
  height_segment_load              = "sea_ice_segments/geophysical/height_segment_load",
  # stats
  photon_rate                      = "sea_ice_segments/stats/photon_rate",
  cloud_flag_asr                   = "sea_ice_segments/stats/cloud_flag_asr",
  cloud_flag_atm                   = "sea_ice_segments/stats/cloud_flag_atm",
  layer_flag                       = "sea_ice_segments/stats/layer_flag",
  n_photons_actual                 = "sea_ice_segments/stats/n_photons_actual",
  n_photons_used                   = "sea_ice_segments/stats/n_photons_used",
  ice_conc_amsr2                   = "sea_ice_segments/stats/ice_conc_amsr2",
  fpb_corr                         = "sea_ice_segments/stats/fpb_corr",
  dist2land                        = "sea_ice_segments/stats/dist2land"
)

# fmt: skip
.icesat2_atl10_columns <- c(
  # ATL10 main data under freeboard_segment/ and its subgroups.
  # leads/ and reference_surface_section/ excluded (different row
  # dimensions from the freeboard segment rate).
  latitude                         = "freeboard_segment/latitude",
  longitude                        = "freeboard_segment/longitude",
  delta_time                       = "freeboard_segment/delta_time",
  beam_fb_height                   = "freeboard_segment/beam_fb_height",
  beam_fb_confidence               = "freeboard_segment/beam_fb_confidence",
  beam_fb_quality_flag             = "freeboard_segment/beam_fb_quality_flag",
  beam_fb_sigma                    = "freeboard_segment/beam_fb_sigma",
  height_segment_id                = "freeboard_segment/height_segment_id",
  seg_dist_x                       = "freeboard_segment/seg_dist_x",
  # heights (ATL07 height segment parameters at freeboard rate)
  height_segment_height            = "freeboard_segment/heights/height_segment_height",
  height_segment_confidence        = "freeboard_segment/heights/height_segment_confidence",
  height_segment_type              = "freeboard_segment/heights/height_segment_type",
  height_segment_ssh_flag          = "freeboard_segment/heights/height_segment_ssh_flag",
  height_segment_length_seg        = "freeboard_segment/heights/height_segment_length_seg",
  height_segment_sigma             = "freeboard_segment/heights/height_segment_sigma",
  ice_conc_ssmi                    = "freeboard_segment/heights/ice_conc_ssmi",
  photon_rate                      = "freeboard_segment/heights/photon_rate",
  layer_flag                       = "freeboard_segment/heights/layer_flag",
  # geophysical
  height_segment_dac               = "freeboard_segment/geophysical/height_segment_dac",
  height_segment_earth             = "freeboard_segment/geophysical/height_segment_earth",
  height_segment_geoid             = "freeboard_segment/geophysical/height_segment_geoid",
  height_segment_mss               = "freeboard_segment/geophysical/height_segment_mss",
  height_segment_ocean             = "freeboard_segment/geophysical/height_segment_ocean",
  height_segment_load              = "freeboard_segment/geophysical/height_segment_load"
)

# ---------------------------------------------------------------------------
# Column lookup tables (product -> registry)
# ---------------------------------------------------------------------------

.gedi_column_registry <- list(
  L1B = .gedi_l1b_columns,
  L2A = .gedi_l2a_columns,
  L2B = .gedi_l2b_columns,
  L4A = .gedi_l4a_columns,
  L4C = .gedi_l4c_columns
)

.icesat2_column_registry <- list(
  ATL03 = .icesat2_atl03_columns,
  ATL06 = .icesat2_atl06_columns,
  ATL08 = .icesat2_atl08_columns,
  ATL07 = .icesat2_atl07_columns,
  ATL10 = .icesat2_atl10_columns,
  ATL13 = .icesat2_atl13_columns,
  ATL24 = .icesat2_atl24_columns
)

# ---------------------------------------------------------------------------
# Default column sets (curated subsets for "grab and go" reads)
# ---------------------------------------------------------------------------
# These define what sl_read() returns when columns = NULL. The full
# registry is available via sl_columns(product) or
# sl_columns(product, set = "all"). Default sets include the primary
# science variables, key quality flags, and basic context. They exclude
# geophysical corrections, instrument details, error/uncertainty
# columns, pool/list columns, and niche QC flags.

# fmt: skip
.gedi_l1b_default <- c(
  "shot_number", "beam", "channel", "delta_time",
  "rx_energy", "stale_return_flag",
  "elevation_bin0", "elevation_lastbin",
  "rx_sample_count", "rx_sample_start_index",
  "digital_elevation_model", "solar_elevation",
  "rxwaveform"
)
# fmt: skip
.gedi_l2a_default <- c(
  "shot_number", "beam", "delta_time",
  "quality_flag", "degrade_flag", "sensitivity",
  "elev_lowestmode", "elev_highestreturn", "energy_total",
  "num_detectedmodes", "rh", "selected_algorithm",
  "solar_elevation", "digital_elevation_model",
  "landsat_treecover", "modis_treecover",
  "pft_class", "region_class"
)
# fmt: skip
.gedi_l2b_default <- c(
  "shot_number", "beam", "delta_time",
  "l2b_quality_flag", "l2a_quality_flag", "sensitivity",
  "cover", "fhd_normal", "pai", "rh100",
  "cover_z", "pai_z", "pavd_z",
  "elev_lowestmode", "solar_elevation",
  "digital_elevation_model",
  "landsat_treecover", "modis_treecover"
)
# fmt: skip
.gedi_l4a_default <- c(
  "shot_number", "beam", "delta_time",
  "l4_quality_flag", "degrade_flag", "sensitivity",
  "agbd", "agbd_se", "agbd_pi_lower", "agbd_pi_upper",
  "elev_lowestmode", "solar_elevation",
  "landsat_treecover", "pft_class", "region_class"
)
# fmt: skip
.gedi_l4c_default <- c(
  "shot_number", "beam", "delta_time",
  "wsci_quality_flag", "degrade_flag", "sensitivity",
  "wsci", "wsci_pi_lower", "wsci_pi_upper",
  "wsci_xy", "wsci_z", "fhd_normal",
  "elev_lowestmode", "solar_elevation",
  "landsat_treecover", "pft_class", "region_class"
)
# fmt: skip
.icesat2_atl03_default <- c(
  "h_ph", "delta_time",
  "signal_conf_ph", "quality_ph", "signal_class_ph"
)
# fmt: skip
.icesat2_atl06_default <- c(
  "h_li", "h_li_sigma", "atl06_quality_summary",
  "delta_time", "segment_id",
  "sigma_geo_h", "n_fit_photons", "h_robust_sprd", "snr",
  "dem_h"
)
# fmt: skip
.icesat2_atl08_default <- c(
  "delta_time", "segment_id_beg", "night_flag",
  "h_canopy", "h_canopy_uncertainty",
  "h_max_canopy", "h_mean_canopy", "h_median_canopy",
  "canopy_openness", "canopy_h_metrics",
  "centroid_height", "n_ca_photons", "toc_roughness",
  "h_te_best_fit", "h_te_uncertainty", "terrain_slope",
  "n_te_photons", "dem_h",
  "segment_landcover", "solar_elevation"
)

# fmt: skip
.icesat2_atl13_default <- c(
  "delta_time", "ht_water_surf", "ht_ortho",
  "stdev_water_surf", "significant_wave_ht", "water_depth",
  "err_ht_water_surf",
  "inland_water_body_id", "inland_water_body_type",
  "sseg_sig_ph_cnt", "segment_geoid", "segment_dem_ht",
  "segment_id_beg", "segment_slope_trk_bdy",
  "qf_cloud", "qf_ice", "ice_flag"
)
# fmt: skip
.icesat2_atl24_default <- c(
  "delta_time", "ortho_h", "ellipse_h", "surface_h",
  "class_ph", "confidence",
  "sigma_thu", "sigma_tvu",
  "night_flag", "low_confidence_flag",
  "sensor_depth_exceeded"
)

# fmt: skip
.icesat2_atl07_default <- c(
  "delta_time", "height_segment_id",
  "height_segment_height", "height_segment_confidence",
  "height_segment_quality", "height_segment_type",
  "height_segment_ssh_flag", "height_segment_length_seg",
  "height_segment_surface_error_est",
  "photon_rate", "n_photons_used",
  "ice_conc_amsr2", "cloud_flag_asr", "layer_flag",
  "solar_elevation", "dist2land"
)
# fmt: skip
.icesat2_atl10_default <- c(
  "delta_time",
  "beam_fb_height", "beam_fb_quality_flag", "beam_fb_confidence",
  "beam_fb_sigma",
  "height_segment_height", "height_segment_type",
  "height_segment_ssh_flag", "ice_conc_ssmi",
  "photon_rate", "layer_flag"
)

.default_column_registry <- list(
  L1B   = .gedi_l1b_default,
  L2A   = .gedi_l2a_default,
  L2B   = .gedi_l2b_default,
  L4A   = .gedi_l4a_default,
  L4C   = .gedi_l4c_default,
  ATL03 = .icesat2_atl03_default,
  ATL06 = .icesat2_atl06_default,
  ATL08 = .icesat2_atl08_default,
  ATL07 = .icesat2_atl07_default,
  ATL10 = .icesat2_atl10_default,
  ATL13 = .icesat2_atl13_default,
  ATL24 = .icesat2_atl24_default
)

#' Short names of the default column set for a product.
#' @noRd
product_default_columns <- function(product) {
  .default_column_registry[[product]] %||% character(0)
}

# ---------------------------------------------------------------------------
# Exported: column discovery
# ---------------------------------------------------------------------------

#' List available columns for a GEDI or ICESat-2 product
#'
#' Returns a named character vector of columns for the given product.
#' Names are the short user-facing column names (used in the `columns`
#' argument of [sl_read()]). Values are the full HDF5 dataset paths.
#'
#' @param product Character. One of:
#'   * GEDI: `"L1B"`, `"L2A"`, `"L2B"`, `"L4A"`
#'   * ICESat-2: `"ATL03"`, `"ATL06"`, `"ATL08"`
#' @param set Character. Which column set to return:
#'   * `"all"` (default): every column in the registry, including
#'     geophysical corrections, instrument details, and pool columns.
#'   * `"default"`: a curated subset of commonly useful science
#'     variables, quality flags, and context columns. This is what
#'     [sl_read()] returns when `columns` is not specified.
#' @returns A named character vector.
#'
#' @examples
#' sl_columns("L2A")
#' sl_columns("L2A", set = "default")
#'
#' @export
sl_columns <- function(
  product = c("L2A", "L2B", "L4A", "L4C", "L1B", "ATL03", "ATL06", "ATL07", "ATL08", "ATL10", "ATL13", "ATL24"),
  set = c("all", "default")
) {
  product <- rlang::arg_match(product)
  set <- rlang::arg_match(set)
  registry <- .gedi_column_registry[[product]] %||%
    .icesat2_column_registry[[product]]
  if (set == "default") {
    default_short <- product_default_columns(product)
    registry[intersect(names(registry), default_short)]
  } else {
    registry
  }
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

  # NULL → curated default set for the product
  if (is.null(columns)) {
    default_short <- product_default_columns(product)
    if (length(default_short) > 0L) {
      return(unique(unname(registry[intersect(names(registry), default_short)])))
    }
    # Fallback: all columns minus pool columns
    pool_short <- product_pool_columns(product)
    return(unique(unname(registry[setdiff(names(registry), pool_short)])))
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
    L2B = .gedi_l2b_pool_columns,
    character(0)
  )
}

#' Transposed-column registry for a given product.
#'
#' Returns a named list where each entry describes a transposed 2D
#' dataset: `list(path = "hdf5/path", labels = c("cat1", "cat2", ...))`.
#' Only GEDI L1B has transposed datasets in the current registry
#' (surface_type). Other products return an empty list.
#' @noRd
product_transposed_columns <- function(product) {
  switch(
    product,
    L1B = .gedi_l1b_transposed_columns,
    list()
  )
}

#' Split a resolved column list into scalar and transposed components.
#'
#' Transposed columns are identified by their HDF5 path matching an entry
#' in the product's transposed registry.
#' @noRd
split_transposed_columns <- function(columns, product) {
  trans_map <- product_transposed_columns(product)
  if (length(trans_map) == 0L) {
    return(list(scalar = columns, transposed = list()))
  }
  trans_paths <- vapply(trans_map, function(e) e$path, character(1))
  is_trans <- columns %in% trans_paths
  # Build a list of (path, labels) entries for the matched columns
  matched <- lapply(columns[is_trans], function(p) {
    idx <- match(p, trans_paths)
    list(path = p, labels = trans_map[[idx]]$labels)
  })
  list(
    scalar = columns[!is_trans],
    transposed = matched
  )
}

#' Build colon-delimited transposed column specs for the Rust FFI.
#'
#' Each spec is `"path:label1,label2,label3,..."`. The Rust side parses
#' these, reads the full transposed dataset per beam, and emits one
#' column per label with values extracted for the selected shots.
#' @noRd
build_transposed_specs <- function(transposed_entries) {
  if (length(transposed_entries) == 0L) {
    return(character(0))
  }
  vapply(transposed_entries, function(e) {
    paste(e$path, paste(e$labels, collapse = ","), sep = ":")
  }, character(1))
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
    L2B = .gedi_l2b_pool_index_map,
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
    if (!is.null(spec$deps)) {
      required_short <- c(required_short, spec$deps)
    }
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

# ---------------------------------------------------------------------------
# Internal: fill values and scale factors
# ---------------------------------------------------------------------------

#' Known fill-value sentinels for each sensor family.
#'
#' GEDI uses -9999 (most columns) and -999999 (DEM columns). ICESat-2
#' ATL08 uses HDF5 float max (3.4028235e+38). ATL06 uses IEEE NaN, which
#' R already represents as NA. ATL03 photon-level data has no fill values.
#'
#' These are applied to all numeric/integer columns after parsing. The
#' sentinel values are exact IEEE 754 representations, so equality
#' comparison is correct.
#' @noRd
product_fill_values <- function(product) {
  if (product %in% c("L1B", "L2A", "L2B", "L4A", "L4C")) {
    c(-9999.0, -999999.0)
  } else if (product %in% c("ATL08", "ATL13")) {
    c(3.4028235e+38)
  } else {
    numeric(0)
  }
}

#' Per-column scale factors that convert raw HDF5 values to physical units.
#'
#' Only L2B currently needs this:
#'   - `pgap_theta_z`: stored as DN (digital number), true value = DN / 10000
#'   - `rh100`: stored in centimetres, convert to metres
#'
#' Keys are short column names (after subgroup prefix stripping). For 2D
#' columns that expand to `{name}0`, `{name}1`, ..., the factor is applied
#' to all expanded variants via prefix matching.
#' @noRd
product_scale_factors <- function(product) {
  # L2B rh100 is stored in centimetres; convert to metres for consistency
  # with L2A's rh (which is already in metres as FLOAT32).
  # Note: pgap_theta_z was documented as "DN / 10000" in the dictionary
  # but the HDF5 file stores it as a float already in the 0-1 range, so
  # no conversion is needed despite the documentation.
  switch(
    product,
    L2B = list(rh100 = 1 / 100),
    list()
  )
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
