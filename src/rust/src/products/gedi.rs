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
//! File structure (L2B — lat/lon inside geolocation/):
//! ```text
//! /BEAM0000/
//! │   ├── cover, pai, fhd_normal, ...
//! │   ├── land_cover_data/
//! │   └── geolocation/
//! │       ├── lat_lowestmode      (f64)
//! │       ├── lon_lowestmode      (f64)
//! │       └── shot_number, ...
//! ```
//!
//! File structure (L1B — uses latitude_bin0 / longitude_bin0):
//! ```text
//! /BEAM0000/
//! │   ├── shot_number, channel, ...
//! │   └── geolocation/
//! │       ├── latitude_bin0       (f64)
//! │       ├── longitude_bin0      (f64)
//! │       └── elevation_bin0, ...
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
    /// These lists are aligned with the variables returned by the `chewie` R
    /// package.  Multi-dimensional arrays (e.g. `rh`, `cover_z`, `pai_z`,
    /// `pavd_z`, `pgap_theta_z`) are included where the reader already supports
    /// them.  The `beam` column is *not* listed here because the R wrapper adds
    /// it from the beam group name.
    pub fn default_columns(&self) -> Vec<&'static str> {
        match self {
            GediProduct::L2A => vec![
                // -- spatial / id --
                "lat_lowestmode",
                "lon_lowestmode",
                "shot_number",
                "channel",
                "delta_time",
                // -- quality / flags --
                "degrade_flag",
                "quality_flag",
                "sensitivity",
                "solar_elevation",
                "stale_return_flag",
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
                // -- ancillary elevation --
                "digital_elevation_model",
                "digital_elevation_model_srtm",
                // -- land cover --
                "land_cover_data/landsat_treecover",
                "land_cover_data/modis_nonvegetated",
                "land_cover_data/modis_nonvegetated_sd",
                "land_cover_data/modis_treecover",
                "land_cover_data/modis_treecover_sd",
            ],
            GediProduct::L2B => vec![
                // -- spatial / id  (in geolocation/ subgroup) --
                "geolocation/lat_lowestmode",
                "geolocation/lon_lowestmode",
                "geolocation/shot_number",
                "geolocation/degrade_flag",
                "geolocation/delta_time",
                "geolocation/digital_elevation_model",
                "geolocation/elev_lowestmode",
                "geolocation/solar_elevation",
                "channel",
                // -- quality / flags --
                "algorithmrun_flag",
                "l2a_quality_flag",
                "l2b_quality_flag",
                "sensitivity",
                "stale_return_flag",
                "surface_flag",
                // -- science --
                "cover",
                "cover_z",
                "fhd_normal",
                "omega",
                "pai",
                "pai_z",
                "pavd_z",
                "pgap_theta",
                "pgap_theta_z",
                "rh100",
                "rhog",
                "rhov",
                // -- land cover --
                "land_cover_data/landsat_treecover",
                "land_cover_data/modis_nonvegetated",
                "land_cover_data/modis_nonvegetated_sd",
                "land_cover_data/modis_treecover",
                "land_cover_data/modis_treecover_sd",
            ],
            GediProduct::L4A => vec![
                // -- spatial / id --
                "lat_lowestmode",
                "lon_lowestmode",
                "shot_number",
                "delta_time",
                // -- quality / flags --
                "degrade_flag",
                "l2_quality_flag",
                "l4_quality_flag",
                "sensitivity",
                "solar_elevation",
                "surface_flag",
                "algorithm_run_flag",
                // -- science --
                "agbd",
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
                // -- land cover --
                "land_cover_data/landsat_treecover",
                "land_cover_data/modis_nonvegetated",
                "land_cover_data/modis_nonvegetated_sd",
                "land_cover_data/modis_treecover",
                "land_cover_data/modis_treecover_sd",
            ],
            GediProduct::L1B => vec![
                // -- spatial / id (in geolocation/ subgroup) --
                "geolocation/latitude_bin0",
                "geolocation/longitude_bin0",
                "shot_number",
                "channel",
                // -- quality / flags --
                "stale_return_flag",
                "rx_sample_count",
                "tx_sample_count",
                "noise_mean_corrected",
                // -- geolocation metadata --
                "geolocation/degrade",
                "geolocation/delta_time",
                "geolocation/digital_elevation_model",
                "geolocation/digital_elevation_model_srtm",
                "geolocation/elevation_bin0",
                "geolocation/elevation_lastbin",
                "geolocation/local_beam_elevation",
                "geolocation/solar_elevation",
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
