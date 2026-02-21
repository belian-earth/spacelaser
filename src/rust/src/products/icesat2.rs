//! ICESat-2 product-aware reader.
//!
//! Knows the internal structure of ICESat-2 ATL03, ATL06, ATL08, etc.
//! and provides spatial subsetting by ground track and bounding box.
//!
//! ATL08 structure (100m segment-level, most common for vegetation):
//! ```text
//! /
//! ├── gt1l/
//! │   ├── land_segments/
//! │   │   ├── latitude          (f64, per segment)
//! │   │   ├── longitude         (f64, per segment)
//! │   │   ├── canopy/
//! │   │   │   ├── h_canopy      (f32)
//! │   │   │   ├── canopy_openness (f32)
//! │   │   │   └── ...
//! │   │   ├── terrain/
//! │   │   │   ├── h_te_best_fit (f32)
//! │   │   │   └── ...
//! │   │   └── ...
//! │   └── signal_photons/
//! ├── gt1r/ ... gt3r/
//! ├── orbit_info/
//! └── ancillary_data/
//! ```
//!
//! ATL03 structure (photon-level):
//! ```text
//! /
//! ├── gt1l/
//! │   ├── heights/
//! │   │   ├── lat_ph            (f64, per photon)
//! │   │   ├── lon_ph            (f64, per photon)
//! │   │   ├── h_ph              (f32)
//! │   │   ├── signal_conf_ph    (i8, per photon x 5 surface types)
//! │   │   └── ...
//! │   ├── geolocation/
//! │   │   ├── segment_ph_cnt    (number of photons per segment)
//! │   │   └── ...
//! │   └── ...
//! ├── gt1r/ ... gt3r/
//! └── ...
//! ```

use crate::hdf5::file::Hdf5File;
use crate::hdf5::types::Hdf5Error;
use crate::products::gedi::{BBox, ColumnData};
use std::collections::HashMap;

/// The 6 ICESat-2 ground track group names.
pub const GROUND_TRACKS: [&str; 6] = ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"];

/// Strong beam ground tracks (left beams in standard orientation).
pub const STRONG_BEAMS: [&str; 3] = ["gt1l", "gt2l", "gt3l"];

/// Weak beam ground tracks.
pub const WEAK_BEAMS: [&str; 3] = ["gt1r", "gt2r", "gt3r"];

/// ICESat-2 product type.
#[derive(Debug, Clone, Copy)]
pub enum IceSat2Product {
    ATL03,
    ATL06,
    ATL08,
}

/// Result from reading an ICESat-2 ground track.
#[derive(Debug)]
pub struct TrackData {
    pub track_name: String,
    pub columns: HashMap<String, ColumnData>,
    pub selected_indices: Vec<u64>,
}

impl IceSat2Product {
    /// Lat/lon dataset paths relative to the ground track group.
    pub fn lat_path(&self) -> &'static str {
        match self {
            IceSat2Product::ATL03 => "heights/lat_ph",
            IceSat2Product::ATL06 => "land_ice_segments/latitude",
            IceSat2Product::ATL08 => "land_segments/latitude",
        }
    }

    pub fn lon_path(&self) -> &'static str {
        match self {
            IceSat2Product::ATL03 => "heights/lon_ph",
            IceSat2Product::ATL06 => "land_ice_segments/longitude",
            IceSat2Product::ATL08 => "land_segments/longitude",
        }
    }

    /// Default columns to read for each product.
    /// Paths are relative to the ground track group.
    pub fn default_columns(&self) -> Vec<&'static str> {
        match self {
            IceSat2Product::ATL03 => vec![
                "heights/lat_ph",
                "heights/lon_ph",
                "heights/h_ph",
                "heights/signal_conf_ph",
                "heights/delta_time",
            ],
            IceSat2Product::ATL06 => vec![
                "land_ice_segments/latitude",
                "land_ice_segments/longitude",
                "land_ice_segments/h_li",
                "land_ice_segments/h_li_sigma",
                "land_ice_segments/atl06_quality_summary",
                "land_ice_segments/delta_time",
                "land_ice_segments/segment_id",
            ],
            IceSat2Product::ATL08 => vec![
                "land_segments/latitude",
                "land_segments/longitude",
                "land_segments/canopy/h_canopy",
                "land_segments/canopy/canopy_openness",
                "land_segments/terrain/h_te_best_fit",
                "land_segments/terrain/h_te_uncertainty",
                "land_segments/delta_time",
                "land_segments/segment_id_beg",
                "land_segments/night_flag",
            ],
        }
    }
}

/// Read ICESat-2 data with spatial subsetting.
pub async fn read_icesat2(
    file: &mut Hdf5File,
    product: IceSat2Product,
    bbox: BBox,
    columns: Option<Vec<String>>,
    tracks: Option<Vec<String>>,
) -> Result<Vec<TrackData>, Hdf5Error> {
    let columns = columns.unwrap_or_else(|| {
        product
            .default_columns()
            .into_iter()
            .map(String::from)
            .collect()
    });

    let track_list: Vec<String> = tracks.unwrap_or_else(|| {
        GROUND_TRACKS.iter().map(|s| s.to_string()).collect()
    });

    let mut results = Vec::new();

    for track_name in &track_list {
        let track_path = format!("/{}", track_name);

        // Read lat/lon for spatial filtering
        let lat_path = format!("{}/{}", track_path, product.lat_path());
        let lon_path = format!("{}/{}", track_path, product.lon_path());

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

        let num_elements = lat_meta.dataspace.dims[0] as usize;
        let elem_size = lat_meta.datatype.size();

        let selected_indices =
            find_matching_indices(&lat_bytes, &lon_bytes, elem_size, num_elements, &bbox);

        if selected_indices.is_empty() {
            continue;
        }

        let row_ranges = indices_to_ranges(&selected_indices);

        // Read requested columns
        let mut col_data = HashMap::new();

        for col_name in &columns {
            let col_path = format!("{}/{}", track_path, col_name);

            match file.read_dataset_rows(&col_path, &row_ranges).await {
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
                Err(Hdf5Error::PathNotFound(_)) => continue,
                Err(e) => return Err(e),
            }
        }

        results.push(TrackData {
            track_name: track_name.clone(),
            columns: col_data,
            selected_indices,
        });
    }

    Ok(results)
}

/// Find indices of elements whose lat/lon fall within the bounding box.
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
