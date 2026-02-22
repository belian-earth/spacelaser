//! GEDI product-aware reader.
//!
//! Knows the internal structure of GEDI L1B, L2A, L2B, and L4A HDF5 files
//! and provides spatial subsetting: given a bounding box, reads only the
//! footprints that fall within it.
//!
//! ## Design choice: product-specific lat/lon paths
//!
//! GEDI products store coordinates in different locations:
//! - L2A / L4A: `lat_lowestmode` / `lon_lowestmode` at the beam root
//! - L2B: `geolocation/lat_lowestmode` / `geolocation/lon_lowestmode`
//! - L1B: `geolocation/latitude_bin0` / `geolocation/longitude_bin0`
//!
//! The `SatelliteProduct` trait lets us express this without branching
//! in the core reader.
//!
//! ## File structures
//!
//! L2A / L4A (lat/lon at root):
//! ```text
//! /BEAM0000/
//! ├── lat_lowestmode          (f64)
//! ├── lon_lowestmode          (f64)
//! ├── rh                      (f32, n×101)   [L2A only]
//! ├── quality_flag / ...
//! ├── land_cover_data/
//! └── geolocation/
//! ```
//!
//! L2B (lat/lon inside geolocation/):
//! ```text
//! /BEAM0000/
//! ├── shot_number, delta_time, cover, pai, ...
//! ├── land_cover_data/
//! └── geolocation/
//!     ├── lat_lowestmode      (f64)
//!     ├── lon_lowestmode      (f64)
//!     └── degrade_flag, elev_lowestmode, ...
//! ```
//!
//! L1B (latitude_bin0 / longitude_bin0 in geolocation/):
//! ```text
//! /BEAM0000/
//! ├── shot_number, channel, delta_time, ...
//! └── geolocation/
//!     ├── latitude_bin0       (f64)
//!     ├── longitude_bin0      (f64)
//!     └── elevation_bin0, solar_elevation, ...
//! ```

use super::common::{self, BBox, GroupData, SatelliteProduct};
use crate::hdf5::file::Hdf5File;
use crate::hdf5::types::Hdf5Error;

// Re-export common types so existing callers still find them here.
pub use super::common::ColumnData;

/// The 8 GEDI beam group names.
pub const BEAM_NAMES: [&str; 8] = [
    "BEAM0000", "BEAM0001", "BEAM0010", "BEAM0011",
    "BEAM0100", "BEAM0101", "BEAM0110", "BEAM1011",
];

/// Full-power beams (higher signal-to-noise).
pub const FULL_POWER_BEAMS: [&str; 4] = ["BEAM0101", "BEAM0110", "BEAM1011", "BEAM0010"];

/// Coverage beams (lower power).
pub const COVERAGE_BEAMS: [&str; 4] = ["BEAM0000", "BEAM0001", "BEAM0011", "BEAM0100"];

/// GEDI product type.
#[derive(Debug, Clone, Copy)]
pub enum GediProduct {
    L1B,
    L2A,
    L2B,
    L4A,
}

impl SatelliteProduct for GediProduct {
    fn group_names(&self) -> Vec<&'static str> {
        BEAM_NAMES.to_vec()
    }

    fn lat_dataset(&self) -> &'static str {
        match self {
            GediProduct::L1B => "geolocation/latitude_bin0",
            GediProduct::L2B => "geolocation/lat_lowestmode",
            _ => "lat_lowestmode",
        }
    }

    fn lon_dataset(&self) -> &'static str {
        match self {
            GediProduct::L1B => "geolocation/longitude_bin0",
            GediProduct::L2B => "geolocation/lon_lowestmode",
            _ => "lon_lowestmode",
        }
    }

    /// Default columns to read for each product level.
    ///
    /// These lists match the scalar variables returned by the `chewie` R
    /// package.  Multi-dimensional arrays (e.g. `rh`, `cover_z`, `pai_z`,
    /// `pavd_z`, `pgap_theta_z`) are included where the reader already
    /// supports them.  The `beam` column is *not* listed here because the
    /// R wrapper adds it from the beam group name.
    ///
    /// chewie reads ALL 1D datasets and drops specific groups; the lists
    /// below are the result of that logic.  We omit the 606 `rx_cumulative`
    /// columns from L2A and the `xvar` matrix from L4A since those are
    /// better handled as post-processing steps.
    fn default_columns(&self) -> Vec<&'static str> {
        match self {
            // L2A: 29 root scalars + rh (2D) + 14 land_cover_data
            GediProduct::L2A => vec![
                "lat_lowestmode",
                "lon_lowestmode",
                "shot_number",
                "channel",
                "delta_time",
                "master_frac",
                "master_int",
                "degrade_flag",
                "quality_flag",
                "sensitivity",
                "solar_azimuth",
                "solar_elevation",
                "surface_flag",
                "elevation_bias_flag",
                "elev_lowestmode",
                "elev_highestreturn",
                "energy_total",
                "num_detectedmodes",
                "rh",
                "selected_algorithm",
                "selected_mode",
                "selected_mode_flag",
                "digital_elevation_model",
                "digital_elevation_model_srtm",
                "mean_sea_surface",
                "elevation_bin0_error",
                "lat_highestreturn",
                "latitude_bin0_error",
                "lon_highestreturn",
                "longitude_bin0_error",
                "land_cover_data/landsat_treecover",
                "land_cover_data/landsat_water_persistence",
                "land_cover_data/leaf_off_doy",
                "land_cover_data/leaf_off_flag",
                "land_cover_data/leaf_on_cycle",
                "land_cover_data/leaf_on_doy",
                "land_cover_data/modis_nonvegetated",
                "land_cover_data/modis_nonvegetated_sd",
                "land_cover_data/modis_treecover",
                "land_cover_data/modis_treecover_sd",
                "land_cover_data/pft_class",
                "land_cover_data/region_class",
                "land_cover_data/urban_focal_window_size",
                "land_cover_data/urban_proportion",
            ],
            // L2B: root scalars + arrays + geolocation/ + land_cover_data/
            GediProduct::L2B => vec![
                "geolocation/lat_lowestmode",
                "geolocation/lon_lowestmode",
                "shot_number",
                "channel",
                "delta_time",
                "master_frac",
                "master_int",
                "algorithmrun_flag",
                "l2a_quality_flag",
                "l2b_quality_flag",
                "num_detectedmodes",
                "sensitivity",
                "stale_return_flag",
                "surface_flag",
                "cover",
                "cover_z",
                "fhd_normal",
                "omega",
                "pai",
                "pai_z",
                "pavd_z",
                "pgap_theta",
                "pgap_theta_error",
                "pgap_theta_z",
                "rg",
                "rh100",
                "rhog",
                "rhog_error",
                "rhov",
                "rhov_error",
                "rossg",
                "rv",
                "rx_range_highestreturn",
                "rx_sample_count",
                "rx_sample_start_index",
                "selected_l2a_algorithm",
                "selected_mode",
                "selected_mode_flag",
                "selected_rg_algorithm",
                "geolocation/degrade_flag",
                "geolocation/digital_elevation_model",
                "geolocation/elev_highestreturn",
                "geolocation/elev_lowestmode",
                "geolocation/elevation_bin0",
                "geolocation/elevation_bin0_error",
                "geolocation/elevation_lastbin",
                "geolocation/elevation_lastbin_error",
                "geolocation/height_bin0",
                "geolocation/height_lastbin",
                "geolocation/lat_highestreturn",
                "geolocation/latitude_bin0",
                "geolocation/latitude_bin0_error",
                "geolocation/latitude_lastbin",
                "geolocation/latitude_lastbin_error",
                "geolocation/local_beam_azimuth",
                "geolocation/local_beam_elevation",
                "geolocation/lon_highestreturn",
                "geolocation/longitude_bin0",
                "geolocation/longitude_bin0_error",
                "geolocation/longitude_lastbin",
                "geolocation/longitude_lastbin_error",
                "geolocation/solar_azimuth",
                "geolocation/solar_elevation",
                "land_cover_data/landsat_treecover",
                "land_cover_data/landsat_water_persistence",
                "land_cover_data/leaf_off_doy",
                "land_cover_data/leaf_off_flag",
                "land_cover_data/leaf_on_cycle",
                "land_cover_data/leaf_on_doy",
                "land_cover_data/modis_nonvegetated",
                "land_cover_data/modis_nonvegetated_sd",
                "land_cover_data/modis_treecover",
                "land_cover_data/modis_treecover_sd",
                "land_cover_data/pft_class",
                "land_cover_data/region_class",
                "land_cover_data/urban_focal_window_size",
                "land_cover_data/urban_proportion",
            ],
            // L4A: root scalars + land_cover_data/ (no modis_* fields)
            GediProduct::L4A => vec![
                "lat_lowestmode",
                "lon_lowestmode",
                "shot_number",
                "channel",
                "delta_time",
                "master_frac",
                "master_int",
                "algorithm_run_flag",
                "degrade_flag",
                "l2_quality_flag",
                "l4_quality_flag",
                "sensitivity",
                "solar_elevation",
                "surface_flag",
                "agbd",
                "agbd_pi_lower",
                "agbd_pi_upper",
                "agbd_se",
                "agbd_t",
                "agbd_t_se",
                "elev_lowestmode",
                "predict_stratum",
                "predictor_limit_flag",
                "response_limit_flag",
                "selected_algorithm",
                "selected_mode",
                "selected_mode_flag",
                "land_cover_data/landsat_treecover",
                "land_cover_data/landsat_water_persistence",
                "land_cover_data/leaf_off_doy",
                "land_cover_data/leaf_off_flag",
                "land_cover_data/leaf_on_cycle",
                "land_cover_data/leaf_on_doy",
                "land_cover_data/pft_class",
                "land_cover_data/region_class",
                "land_cover_data/urban_focal_window_size",
                "land_cover_data/urban_proportion",
            ],
            // L1B: root scalars + geolocation/ scalars
            GediProduct::L1B => vec![
                "geolocation/latitude_bin0",
                "geolocation/longitude_bin0",
                "shot_number",
                "channel",
                "delta_time",
                "master_frac",
                "master_int",
                "stale_return_flag",
                "all_samples_sum",
                "noise_mean_corrected",
                "noise_stddev_corrected",
                "nsemean_even",
                "nsemean_odd",
                "rx_energy",
                "rx_offset",
                "rx_open",
                "rx_sample_count",
                "rx_sample_start_index",
                "selection_stretchers_x",
                "selection_stretchers_y",
                "th_left_used",
                "tx_egamplitude",
                "tx_egamplitude_error",
                "tx_egbias",
                "tx_egbias_error",
                "tx_egflag",
                "tx_eggamma",
                "tx_eggamma_error",
                "tx_egsigma",
                "tx_egsigma_error",
                "tx_gloc",
                "tx_gloc_error",
                "tx_pulseflag",
                "tx_sample_count",
                "tx_sample_start_index",
                "geolocation/altitude_instrument",
                "geolocation/altitude_instrument_error",
                "geolocation/bounce_time_offset_bin0",
                "geolocation/bounce_time_offset_bin0_error",
                "geolocation/bounce_time_offset_lastbin",
                "geolocation/bounce_time_offset_lastbin_error",
                "geolocation/degrade",
                "geolocation/digital_elevation_model",
                "geolocation/digital_elevation_model_srtm",
                "geolocation/elevation_bin0",
                "geolocation/elevation_bin0_error",
                "geolocation/elevation_lastbin",
                "geolocation/elevation_lastbin_error",
                "geolocation/latitude_bin0_error",
                "geolocation/latitude_instrument",
                "geolocation/latitude_instrument_error",
                "geolocation/latitude_lastbin",
                "geolocation/latitude_lastbin_error",
                "geolocation/local_beam_azimuth",
                "geolocation/local_beam_azimuth_error",
                "geolocation/local_beam_elevation",
                "geolocation/local_beam_elevation_error",
                "geolocation/longitude_bin0_error",
                "geolocation/longitude_instrument",
                "geolocation/longitude_instrument_error",
                "geolocation/longitude_lastbin",
                "geolocation/longitude_lastbin_error",
                "geolocation/mean_sea_surface",
                "geolocation/neutat_delay_derivative_bin0",
                "geolocation/neutat_delay_derivative_lastbin",
                "geolocation/neutat_delay_total_bin0",
                "geolocation/neutat_delay_total_lastbin",
                "geolocation/range_bias_correction",
                "geolocation/solar_azimuth",
                "geolocation/solar_elevation",
                "geolocation/dynamic_atmosphere_correction",
                "geolocation/geoid",
                "geolocation/tide_earth",
                "geolocation/tide_load",
                "geolocation/tide_ocean",
                "geolocation/tide_ocean_pole",
                "geolocation/tide_pole",
            ],
        }
    }
}

/// Read GEDI data with spatial subsetting.
///
/// Thin wrapper around [`common::read_product_groups`] that supplies
/// GEDI-specific product metadata.
pub async fn read_gedi(
    file: &mut Hdf5File,
    product: GediProduct,
    bbox: BBox,
    columns: Option<Vec<String>>,
    beams: Option<Vec<String>>,
) -> Result<Vec<GroupData>, Hdf5Error> {
    common::read_product_groups(file, &product, bbox, columns, beams).await
}
