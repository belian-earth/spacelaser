//! GEDI product-aware reader.
//!
//! Knows the internal structure of GEDI L1B, L2A, L2B, and L4A HDF5 files
//! and provides spatial subsetting: given a bounding box, reads only the
//! footprints that fall within it.
//!
//! File structure (L2A / L4A — lat/lon at root):
//! ```text
//! /BEAM0000/
//! │   ├── lat_lowestmode          (f64)
//! │   ├── lon_lowestmode          (f64)
//! │   ├── rh                      (f32, n×101)   [L2A only]
//! │   ├── quality_flag / ...
//! │   ├── land_cover_data/
//! │   └── geolocation/
//! ```
//!
//! File structure (L2B — lat/lon inside geolocation/, but shot_number at root):
//! ```text
//! /BEAM0000/
//! │   ├── shot_number, delta_time, cover, pai, ...
//! │   ├── land_cover_data/
//! │   └── geolocation/
//! │       ├── lat_lowestmode      (f64)
//! │       ├── lon_lowestmode      (f64)
//! │       └── degrade_flag, elev_lowestmode, ...
//! ```
//!
//! File structure (L1B — uses latitude_bin0 / longitude_bin0 in geolocation/):
//! ```text
//! /BEAM0000/
//! │   ├── shot_number, channel, delta_time, ...
//! │   ├── rx_sample_count, tx_sample_count, ...
//! │   └── geolocation/
//! │       ├── latitude_bin0       (f64)
//! │       ├── longitude_bin0      (f64)
//! │       ├── elevation_bin0, solar_elevation, ...
//! │       └── (44 geolocation datasets)
//! ```

use crate::hdf5::file::Hdf5File;
use crate::hdf5::types::Hdf5Error;
use std::collections::HashMap;

/// The 8 GEDI beam group names.
pub const BEAM_NAMES: [&str; 8] = [
    "BEAM0000", "BEAM0001", "BEAM0010", "BEAM0011",
    "BEAM0100", "BEAM0101", "BEAM0110", "BEAM1011",
];

/// Full-power beams (higher signal-to-noise).
pub const FULL_POWER_BEAMS: [&str; 4] = ["BEAM0101", "BEAM0110", "BEAM1011", "BEAM0010"];

/// Coverage beams (lower power).
pub const COVERAGE_BEAMS: [&str; 4] = ["BEAM0000", "BEAM0001", "BEAM0011", "BEAM0100"];

/// Bounding box for spatial queries [xmin, ymin, xmax, ymax] (lon/lat).
#[derive(Debug, Clone, Copy)]
pub struct BBox {
    pub xmin: f64,
    pub ymin: f64,
    pub xmax: f64,
    pub ymax: f64,
}

impl BBox {
    pub fn new(xmin: f64, ymin: f64, xmax: f64, ymax: f64) -> Self {
        Self { xmin, ymin, xmax, ymax }
    }

    /// Check if a point falls within this bounding box.
    pub fn contains(&self, lon: f64, lat: f64) -> bool {
        lon >= self.xmin && lon <= self.xmax && lat >= self.ymin && lat <= self.ymax
    }
}

/// Result from reading a GEDI beam: column name → raw byte data + metadata.
#[derive(Debug)]
pub struct BeamData {
    pub beam_name: String,
    /// Column name → (raw bytes, element size, num elements, type description)
    pub columns: HashMap<String, ColumnData>,
    /// Row indices (in the original file) that matched the spatial filter.
    pub selected_indices: Vec<u64>,
}

/// Raw column data extracted from HDF5.
#[derive(Debug, Clone)]
pub struct ColumnData {
    pub bytes: Vec<u8>,
    pub element_size: usize,
    pub num_elements: usize,
    pub dtype_desc: String,
}

/// GEDI product type.
#[derive(Debug, Clone, Copy)]
pub enum GediProduct {
    L1B,
    L2A,
    L2B,
    L4A,
}

impl GediProduct {
    /// Default columns to read for each product level.
    ///
    /// These lists match the scalar variables returned by the `chewie` R
    /// package.  Multi-dimensional arrays (e.g. `rh`, `cover_z`, `pai_z`,
    /// `pavd_z`, `pgap_theta_z`) are included where the reader already supports
    /// them.  The `beam` column is *not* listed here because the R wrapper adds
    /// it from the beam group name.
    ///
    /// chewie reads ALL 1D datasets and drops specific groups; the lists below
    /// are the result of that logic.  We omit the 606 `rx_cumulative` columns
    /// from L2A and the `xvar` matrix from L4A since those are better handled
    /// as post-processing steps (chewie expands them into named columns).
    pub fn default_columns(&self) -> Vec<&'static str> {
        match self {
            // -----------------------------------------------------------
            // L2A: 29 root scalars + rh (2D) + 14 land_cover_data
            // (chewie also expands rx_cumulative_a1–a6 × 101 — omitted)
            // -----------------------------------------------------------
            GediProduct::L2A => vec![
                // -- spatial / id --
                "lat_lowestmode",
                "lon_lowestmode",
                "shot_number",
                "channel",
                "delta_time",
                "master_frac",
                "master_int",
                // -- quality / flags --
                "degrade_flag",
                "quality_flag",
                "sensitivity",
                "solar_azimuth",
                "solar_elevation",
                "surface_flag",
                "elevation_bias_flag",
                // -- science --
                "elev_lowestmode",
                "elev_highestreturn",
                "energy_total",
                "num_detectedmodes",
                "rh",
                "selected_algorithm",
                "selected_mode",
                "selected_mode_flag",
                // -- ancillary elevation / coordinates --
                "digital_elevation_model",
                "digital_elevation_model_srtm",
                "mean_sea_surface",
                "elevation_bin0_error",
                "lat_highestreturn",
                "latitude_bin0_error",
                "lon_highestreturn",
                "longitude_bin0_error",
                // -- land cover (14) --
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
            // -----------------------------------------------------------
            // L2B: 33 root scalars + 4 root arrays (cover_z, pai_z,
            //       pavd_z, pgap_theta_z) + 26 geolocation/ + 14
            //       land_cover_data/
            // Note: shot_number & delta_time are at beam root for L2B,
            //       while lat/lon are in geolocation/.
            // -----------------------------------------------------------
            GediProduct::L2B => vec![
                // -- spatial / id (lat/lon in geolocation/) --
                "geolocation/lat_lowestmode",
                "geolocation/lon_lowestmode",
                "shot_number",
                "channel",
                "delta_time",
                "master_frac",
                "master_int",
                // -- quality / flags --
                "algorithmrun_flag",
                "l2a_quality_flag",
                "l2b_quality_flag",
                "num_detectedmodes",
                "sensitivity",
                "stale_return_flag",
                "surface_flag",
                // -- science (root) --
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
                // -- geolocation/ (26) --
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
                // -- land cover (14) --
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
            // -----------------------------------------------------------
            // L4A: 27 root scalars + 10 land_cover_data/
            // (chewie also expands xvar into xvar_pred_1..N — omitted)
            // Note: L4A land_cover_data has no modis_* fields.
            // -----------------------------------------------------------
            GediProduct::L4A => vec![
                // -- spatial / id --
                "lat_lowestmode",
                "lon_lowestmode",
                "shot_number",
                "channel",
                "delta_time",
                "master_frac",
                "master_int",
                // -- quality / flags --
                "algorithm_run_flag",
                "degrade_flag",
                "l2_quality_flag",
                "l4_quality_flag",
                "sensitivity",
                "solar_elevation",
                "surface_flag",
                // -- science --
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
                // -- land cover (10 — no modis_* in L4A) --
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
            // -----------------------------------------------------------
            // L1B: 33 root scalars + 44 geolocation/ scalars
            // (chewie also adds rxwaveform as a list-column and expands
            // surface_type into 5 named columns — omitted here, the raw
            // arrays can be requested via `columns`)
            // -----------------------------------------------------------
            GediProduct::L1B => vec![
                // -- spatial / id (in geolocation/ subgroup) --
                "geolocation/latitude_bin0",
                "geolocation/longitude_bin0",
                "shot_number",
                "channel",
                "delta_time",
                "master_frac",
                "master_int",
                // -- quality / flags --
                "stale_return_flag",
                // -- waveform metadata --
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
                // -- geolocation/ (44) --
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

    /// The lat/lon dataset names used for spatial indexing.
    ///
    /// These are relative to the beam group (e.g. `BEAM0101/<lat_dataset>`).
    /// L2B stores coordinates under `geolocation/`, while L1B uses
    /// `latitude_bin0` / `longitude_bin0` in the `geolocation/` subgroup.
    pub fn lat_dataset(&self) -> &'static str {
        match self {
            GediProduct::L1B => "geolocation/latitude_bin0",
            GediProduct::L2B => "geolocation/lat_lowestmode",
            _ => "lat_lowestmode",
        }
    }

    pub fn lon_dataset(&self) -> &'static str {
        match self {
            GediProduct::L1B => "geolocation/longitude_bin0",
            GediProduct::L2B => "geolocation/lon_lowestmode",
            _ => "lon_lowestmode",
        }
    }
}

/// Read GEDI data with spatial subsetting.
///
/// This is the main entry point. It:
/// 1. Opens the HDF5 file
/// 2. For each beam (or selected beams), reads lat/lon
/// 3. Determines which footprints fall in the bounding box
/// 4. Reads only the requested columns for those footprints
pub async fn read_gedi(
    file: &mut Hdf5File,
    product: GediProduct,
    bbox: BBox,
    columns: Option<Vec<String>>,
    beams: Option<Vec<String>>,
) -> Result<Vec<BeamData>, Hdf5Error> {
    let columns = columns.unwrap_or_else(|| {
        product.default_columns().into_iter().map(String::from).collect()
    });

    let beam_list: Vec<String> = beams.unwrap_or_else(|| {
        BEAM_NAMES.iter().map(|s| s.to_string()).collect()
    });

    let mut results = Vec::new();

    for beam_name in &beam_list {
        // Check if this beam exists in the file
        let beam_path = format!("/{}", beam_name);

        // Read lat/lon to find matching footprints
        let lat_path = format!("{}/{}", beam_path, product.lat_dataset());
        let lon_path = format!("{}/{}", beam_path, product.lon_dataset());

        let lat_result = file.read_dataset(&lat_path).await;
        let lon_result = file.read_dataset(&lon_path).await;

        let (lat_meta, lat_bytes) = match lat_result {
            Ok(v) => v,
            Err(Hdf5Error::PathNotFound(_)) => continue,
            Err(e) => return Err(e),
        };
        let (_lon_meta, lon_bytes) = match lon_result {
            Ok(v) => v,
            Err(Hdf5Error::PathNotFound(_)) => continue,
            Err(e) => return Err(e),
        };

        // Parse lat/lon as f64 arrays and find matching indices
        let num_footprints = lat_meta.dataspace.dims[0] as usize;
        let lat_elem_size = lat_meta.datatype.size();
        let selected_indices = find_matching_indices(
            &lat_bytes,
            &lon_bytes,
            lat_elem_size,
            num_footprints,
            &bbox,
        );

        if selected_indices.is_empty() {
            continue;
        }

        // Convert selected indices to row ranges for efficient chunk reading
        let row_ranges = indices_to_ranges(&selected_indices);

        // Read each requested column for the selected rows
        let mut col_data = HashMap::new();

        for col_name in &columns {
            let col_path = format!("{}/{}", beam_path, col_name);

            let result = file.read_dataset_rows(&col_path, &row_ranges).await;

            match result {
                Ok((meta, bytes)) => {
                    let elem_size = meta.datatype.size();
                    let num_elements = bytes.len() / elem_size;
                    let dtype_desc = format!("{:?}", meta.datatype);

                    col_data.insert(
                        col_name.clone(),
                        ColumnData {
                            bytes,
                            element_size: elem_size,
                            num_elements,
                            dtype_desc,
                        },
                    );
                }
                Err(Hdf5Error::PathNotFound(_)) => {
                    // Column doesn't exist in this beam, skip
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        results.push(BeamData {
            beam_name: beam_name.clone(),
            columns: col_data,
            selected_indices,
        });
    }

    Ok(results)
}

/// Find footprint indices that fall within the bounding box.
fn find_matching_indices(
    lat_bytes: &[u8],
    lon_bytes: &[u8],
    elem_size: usize,
    num_elements: usize,
    bbox: &BBox,
) -> Vec<u64> {
    let mut indices = Vec::new();

    for i in 0..num_elements {
        let offset = i * elem_size;
        if offset + elem_size > lat_bytes.len() || offset + elem_size > lon_bytes.len() {
            break;
        }

        let lat = read_float(lat_bytes, offset, elem_size);
        let lon = read_float(lon_bytes, offset, elem_size);

        if bbox.contains(lon, lat) {
            indices.push(i as u64);
        }
    }

    indices
}

/// Read a float value from bytes (supports f32 and f64).
fn read_float(bytes: &[u8], offset: usize, size: usize) -> f64 {
    match size {
        4 => {
            let arr = [
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ];
            f32::from_le_bytes(arr) as f64
        }
        8 => {
            let arr = [
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7],
            ];
            f64::from_le_bytes(arr)
        }
        _ => f64::NAN,
    }
}

/// Convert a sorted list of indices into contiguous (start, end) ranges.
///
/// This minimizes the number of chunk reads: consecutive indices are merged
/// into a single range request.
fn indices_to_ranges(indices: &[u64]) -> Vec<(u64, u64)> {
    if indices.is_empty() {
        return Vec::new();
    }

    let mut ranges = Vec::new();
    let mut start = indices[0];
    let mut end = indices[0] + 1;

    for &idx in &indices[1..] {
        if idx == end {
            end = idx + 1;
        } else {
            ranges.push((start, end));
            start = idx;
            end = idx + 1;
        }
    }
    ranges.push((start, end));
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bbox_contains() {
        let bbox = BBox::new(-10.0, -5.0, 10.0, 5.0);
        assert!(bbox.contains(0.0, 0.0));
        assert!(bbox.contains(-10.0, -5.0));
        assert!(!bbox.contains(11.0, 0.0));
        assert!(!bbox.contains(0.0, 6.0));
    }

    #[test]
    fn test_indices_to_ranges() {
        assert_eq!(indices_to_ranges(&[0, 1, 2, 5, 6, 10]),
            vec![(0, 3), (5, 7), (10, 11)]);
        assert_eq!(indices_to_ranges(&[42]),
            vec![(42, 43)]);
        assert!(indices_to_ranges(&[]).is_empty());
    }

    #[test]
    fn test_read_float_f32() {
        let val: f32 = 3.14;
        let bytes = val.to_le_bytes();
        let result = read_float(&bytes, 0, 4);
        assert!((result - 3.14).abs() < 0.001);
    }

    #[test]
    fn test_read_float_f64() {
        let val: f64 = -122.456;
        let bytes = val.to_le_bytes();
        let result = read_float(&bytes, 0, 8);
        assert!((result - (-122.456)).abs() < 1e-10);
    }
}
