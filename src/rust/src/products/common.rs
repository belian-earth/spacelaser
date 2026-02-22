//! Shared types and spatial utilities for product readers.
//!
//! Both GEDI and ICESat-2 readers follow the same pattern:
//!   1. Iterate over groups (beams / ground tracks)
//!   2. Read lat/lon datasets for spatial filtering
//!   3. Determine which rows fall within the query bounding box
//!   4. Read only the matching rows from each requested column
//!
//! This module contains the types and functions common to that workflow,
//! eliminating duplication between `gedi.rs` and `icesat2.rs`.

use crate::hdf5::file::Hdf5File;
use crate::hdf5::types::Hdf5Error;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Bounding box
// ---------------------------------------------------------------------------

/// Bounding box for spatial queries [xmin, ymin, xmax, ymax] (lon/lat, WGS 84).
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

    /// Check if a point (lon, lat) falls within this bounding box.
    pub fn contains(&self, lon: f64, lat: f64) -> bool {
        lon >= self.xmin && lon <= self.xmax && lat >= self.ymin && lat <= self.ymax
    }
}

// ---------------------------------------------------------------------------
// Column data (output of a single dataset read)
// ---------------------------------------------------------------------------

/// Raw column data extracted from an HDF5 dataset.
///
/// Bytes are returned unparsed — the R side uses `element_size` and
/// `dtype_desc` to reinterpret them as the correct R vector type.
#[derive(Debug, Clone)]
pub struct ColumnData {
    pub bytes: Vec<u8>,
    pub element_size: usize,
    pub num_elements: usize,
    pub dtype_desc: String,
}

// ---------------------------------------------------------------------------
// GroupData — unified output for one beam / ground track
// ---------------------------------------------------------------------------

/// Result of reading a single HDF5 group (a GEDI beam or ICESat-2 ground
/// track) with spatial subsetting applied.
#[derive(Debug)]
pub struct GroupData {
    /// Name of the group (e.g. `"BEAM0101"` or `"gt1l"`).
    pub group_name: String,
    /// Column name → extracted data for the selected rows.
    pub columns: HashMap<String, ColumnData>,
    /// Row indices (in the original file) that passed the spatial filter.
    pub selected_indices: Vec<u64>,
}

// ---------------------------------------------------------------------------
// Product trait — defines the per-product metadata needed by the generic reader
// ---------------------------------------------------------------------------

/// Trait implemented by each satellite product type to supply the metadata
/// that the generic [`read_product_groups`] function needs.
pub trait SatelliteProduct {
    /// Names of the HDF5 groups to iterate over (beams or ground tracks).
    fn group_names(&self) -> Vec<&'static str>;

    /// Latitude dataset path relative to the group root.
    fn lat_dataset(&self) -> &'static str;

    /// Longitude dataset path relative to the group root.
    fn lon_dataset(&self) -> &'static str;

    /// Default column names to read when the user doesn't specify any.
    fn default_columns(&self) -> Vec<&'static str>;
}

// ---------------------------------------------------------------------------
// Generic spatial reader (core algorithm shared by GEDI and ICESat-2)
// ---------------------------------------------------------------------------

/// Read data from groups in an HDF5 file with spatial subsetting.
///
/// This is the unified implementation behind both `read_gedi` and
/// `read_icesat2`. The algorithm is:
///
/// 1. For each group, read the full lat/lon datasets.
/// 2. Identify which rows fall within `bbox`.
/// 3. Convert matching row indices into contiguous ranges.
/// 4. Read only those ranges from each requested column.
///
/// # Design choice: full lat/lon scan
///
/// We read the *entire* lat/lon dataset for each group rather than
/// sampling or using an index. GEDI lat/lon arrays are typically 1–2 MB
/// (~250 K footprints × 8 bytes), which is small compared to the file
/// (~2 GB) and takes only 1–3 range requests thanks to chunked storage
/// and block caching. This keeps the logic simple and correct without
/// requiring any pre-built spatial index.
pub async fn read_product_groups(
    file: &mut Hdf5File,
    product: &dyn SatelliteProduct,
    bbox: BBox,
    columns: Option<Vec<String>>,
    groups: Option<Vec<String>>,
) -> Result<Vec<GroupData>, Hdf5Error> {
    let columns = columns.unwrap_or_else(|| {
        product.default_columns().into_iter().map(String::from).collect()
    });

    let group_list: Vec<String> = groups.unwrap_or_else(|| {
        product.group_names().into_iter().map(String::from).collect()
    });

    let mut results = Vec::new();

    for group_name in &group_list {
        let group_path = format!("/{}", group_name);

        // 1. Read lat/lon for spatial filtering
        let lat_path = format!("{}/{}", group_path, product.lat_dataset());
        let lon_path = format!("{}/{}", group_path, product.lon_dataset());

        let (lat_meta, lat_bytes) = match file.read_dataset(&lat_path).await {
            Ok(v) => v,
            Err(Hdf5Error::PathNotFound(_)) => continue,
            Err(e) => return Err(e),
        };
        let (_lon_meta, lon_bytes) = match file.read_dataset(&lon_path).await {
            Ok(v) => v,
            Err(Hdf5Error::PathNotFound(_)) => continue,
            Err(e) => return Err(e),
        };

        // 2. Find rows within the bounding box
        let num_elements = lat_meta.dataspace.dims[0] as usize;
        let elem_size = lat_meta.datatype.size();
        let selected_indices =
            find_matching_indices(&lat_bytes, &lon_bytes, elem_size, num_elements, &bbox);

        if selected_indices.is_empty() {
            continue;
        }

        // 3. Convert indices → contiguous ranges for efficient chunk reads
        let row_ranges = indices_to_ranges(&selected_indices);

        // 4. Read each requested column for the selected rows
        let mut col_data = HashMap::new();

        for col_name in &columns {
            let col_path = format!("{}/{}", group_path, col_name);

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

        results.push(GroupData {
            group_name: group_name.clone(),
            columns: col_data,
            selected_indices,
        });
    }

    Ok(results)
}

// ---------------------------------------------------------------------------
// Spatial helper functions
// ---------------------------------------------------------------------------

/// Find row indices whose (lat, lon) fall within the bounding box.
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

/// Read a float from a byte slice (supports f32 and f64, little-endian).
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

/// Convert a sorted list of row indices into contiguous `(start, end)` ranges.
///
/// Consecutive indices are merged so that the chunk reader can issue
/// minimal range requests. For example, `[0, 1, 2, 5, 6, 10]` becomes
/// `[(0, 3), (5, 7), (10, 11)]`.
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
        assert_eq!(
            indices_to_ranges(&[0, 1, 2, 5, 6, 10]),
            vec![(0, 3), (5, 7), (10, 11)]
        );
        assert_eq!(indices_to_ranges(&[42]), vec![(42, 43)]);
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
