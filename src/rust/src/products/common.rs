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
use futures::stream::{self, StreamExt};
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
    /// Pool datasets read in full (no row filtering). Used for GEDI L1B
    /// variable-length waveforms (`rxwaveform`, `txwaveform`), which are
    /// flat 1D arrays of concatenated samples across *all* shots in the
    /// beam. The R side slices these into per-shot list columns using
    /// `rx_sample_start_index` / `rx_sample_count` (and the tx equivalents).
    pub pool_columns: HashMap<String, ColumnData>,
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
    file: &Hdf5File,
    product: &dyn SatelliteProduct,
    bbox: BBox,
    columns: Option<Vec<String>>,
    groups: Option<Vec<String>>,
    pool_columns: Option<Vec<String>>,
) -> Result<Vec<GroupData>, Hdf5Error> {
    // R always resolves defaults; empty vec is a no-op safety net.
    let columns: Vec<String> = columns.unwrap_or_default();
    let pool_columns: Vec<String> = pool_columns.unwrap_or_default();

    let group_list: Vec<String> = groups.unwrap_or_else(|| {
        product.group_names().into_iter().map(String::from).collect()
    });

    // Process all beams/tracks concurrently.
    let beam_futures: Vec<_> = group_list.iter().map(|group_name| {
        let columns = &columns;
        let pool_columns = &pool_columns;
        let group_name = group_name.clone();
        async move {
            read_single_group(file, &group_name, product, bbox, columns, pool_columns).await
        }
    }).collect();

    let beam_results = futures::future::join_all(beam_futures).await;

    let mut results = Vec::new();
    for result in beam_results {
        match result {
            Ok(Some(gd)) => results.push(gd),
            Ok(None) => {}     // no matching rows in this beam
            Err(e) => return Err(e),
        }
    }

    Ok(results)
}

/// Read a single beam/track with spatial subsetting and concurrent column reads.
async fn read_single_group(
    file: &Hdf5File,
    group_name: &str,
    product: &dyn SatelliteProduct,
    bbox: BBox,
    columns: &[String],
    pool_columns: &[String],
) -> Result<Option<GroupData>, Hdf5Error> {
    let group_path = format!("/{}", group_name);

    // 1. Read lat/lon concurrently for spatial filtering
    let lat_path = format!("{}/{}", group_path, product.lat_dataset());
    let lon_path = format!("{}/{}", group_path, product.lon_dataset());

    let (lat_result, lon_result) = tokio::join!(
        file.read_dataset(&lat_path),
        file.read_dataset(&lon_path),
    );

    let (lat_meta, lat_bytes) = match lat_result {
        Ok(v) => v,
        Err(Hdf5Error::PathNotFound(_)) => return Ok(None),
        Err(e) => return Err(e),
    };
    let (_lon_meta, lon_bytes) = match lon_result {
        Ok(v) => v,
        Err(Hdf5Error::PathNotFound(_)) => return Ok(None),
        Err(e) => return Err(e),
    };

    // 2. Find rows within the bounding box
    let num_elements = lat_meta.dataspace.dims[0] as usize;
    let elem_size = lat_meta.datatype.size();
    let selected_indices =
        find_matching_indices(&lat_bytes, &lon_bytes, elem_size, num_elements, &bbox);

    if selected_indices.is_empty() {
        return Ok(None);
    }

    // 3. Convert indices → contiguous ranges for efficient chunk reads
    let row_ranges = indices_to_ranges(&selected_indices);
    let n_selected = selected_indices.len();

    // 4. Read all requested columns concurrently (buffered to limit
    //    the number of in-flight HTTP requests).
    let row_ranges_ref = &row_ranges;
    let col_results: Vec<_> = stream::iter(columns.iter())
        .map(|col_name| {
            let col_path = format!("{}/{}", group_path, col_name);
            let col_name = col_name.clone();
            async move {
                match file.read_dataset_rows(&col_path, row_ranges_ref).await {
                    Ok((meta, bytes)) => Ok(Some((col_name, meta, bytes))),
                    Err(Hdf5Error::PathNotFound(_)) => Ok(None),
                    Err(e) => Err(e),
                }
            }
        })
        .buffer_unordered(16)
        .collect()
        .await;

    // Process results: expand 2D datasets, build HashMap
    let mut col_data = HashMap::new();
    for result in col_results {
        if let Some((col_name, meta, bytes)) = result? {
            let elem_size = meta.datatype.size();
            let dims = &meta.dataspace.dims;
            let dtype_desc = format!("{:?}", meta.datatype);

            if dims.len() >= 2 {
                // 2D dataset (e.g. rh [N, 101]): expand into separate
                // columns named {col}{0}, {col}{1}, … {col}{ncols-1},
                // matching chewie's naming convention (rh0 … rh100).
                let ncols = dims[1] as usize;
                let row_size = ncols * elem_size;

                log::debug!(
                    "2D column '{}': dims={:?}, dtype={}, bytes={}, expected={}",
                    col_name, dims, dtype_desc, bytes.len(),
                    n_selected * row_size,
                );
                let nonzero = bytes.iter().filter(|&&b| b != 0).count();
                log::debug!(
                    "  non-zero bytes: {}/{} ({:.1}%)",
                    nonzero, bytes.len(),
                    100.0 * nonzero as f64 / bytes.len().max(1) as f64,
                );

                for j in 0..ncols {
                    let mut col_bytes = Vec::with_capacity(n_selected * elem_size);
                    for i in 0..n_selected {
                        let offset = i * row_size + j * elem_size;
                        let end = offset + elem_size;
                        if end <= bytes.len() {
                            col_bytes.extend_from_slice(&bytes[offset..end]);
                        }
                    }
                    let expanded_name = format!("{}{}", col_name, j);
                    col_data.insert(
                        expanded_name,
                        ColumnData {
                            bytes: col_bytes,
                            element_size: elem_size,
                            num_elements: n_selected,
                            dtype_desc: dtype_desc.clone(),
                        },
                    );
                }
            } else {
                // 1D dataset: pass through as-is
                let num_elements = bytes.len() / elem_size;
                col_data.insert(
                    col_name,
                    ColumnData {
                        bytes,
                        element_size: elem_size,
                        num_elements,
                        dtype_desc,
                    },
                );
            }
        }
    }

    // 5. Pool columns: targeted reads using the start-index / count
    //    columns that were already read as scalar columns above.
    //
    //    Each pool_columns entry is a colon-delimited spec:
    //      "dataset_name:start_index_col:count_col"
    //    e.g. "rxwaveform:rx_sample_start_index:rx_sample_count"
    //
    //    The start/count columns give per-shot positions into the pool
    //    dataset (a flat 1D array of concatenated samples). We parse
    //    them, compute sample-level ranges for just the selected shots,
    //    and issue targeted chunk reads rather than downloading the
    //    entire pool (which can be 50-200 MB per beam).
    let mut pool_data = HashMap::new();
    for spec in pool_columns.iter() {
        let parts: Vec<&str> = spec.splitn(3, ':').collect();
        if parts.len() != 3 {
            log::warn!("Invalid pool spec (expected name:start:count): {}", spec);
            continue;
        }
        let pool_name = parts[0];
        let start_col = parts[1];
        let count_col = parts[2];

        let start_data = match col_data.get(start_col) {
            Some(d) => d,
            None => {
                log::warn!("Pool index column '{}' not in scalar results", start_col);
                continue;
            }
        };
        let count_data = match col_data.get(count_col) {
            Some(d) => d,
            None => {
                log::warn!("Pool index column '{}' not in scalar results", count_col);
                continue;
            }
        };

        let starts = parse_as_u64_vec(start_data);
        let counts = parse_as_u64_vec(count_data);

        if starts.len() != counts.len() || starts.is_empty() {
            continue;
        }

        let sample_ranges: Vec<(u64, u64)> = starts
            .iter()
            .zip(counts.iter())
            .filter(|(_, &c)| c > 0)
            .map(|(&s, &c)| (s, s + c))
            .collect();

        if sample_ranges.is_empty() {
            continue;
        }

        let col_path = format!("{}/{}", group_path, pool_name);
        match file.read_dataset_rows(&col_path, &sample_ranges).await {
            Ok((meta, bytes)) => {
                let elem_size = meta.datatype.size();
                let num_elements = bytes.len() / elem_size.max(1);
                pool_data.insert(
                    pool_name.to_string(),
                    ColumnData {
                        bytes,
                        element_size: elem_size,
                        num_elements,
                        dtype_desc: format!("{:?}", meta.datatype),
                    },
                );
            }
            Err(Hdf5Error::PathNotFound(_)) => {}
            Err(e) => {
                log::warn!("Failed to read pool column '{}': {}", pool_name, e);
            }
        }
    }

    Ok(Some(GroupData {
        group_name: group_name.to_string(),
        columns: col_data,
        selected_indices,
        pool_columns: pool_data,
    }))
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

/// Parse a `ColumnData` (raw bytes from an HDF5 integer or float dataset)
/// into a `Vec<u64>` for use as pool column sample indices. Handles the
/// common integer and float types found in GEDI start-index / count columns.
fn parse_as_u64_vec(col: &ColumnData) -> Vec<u64> {
    let n = col.num_elements;
    let s = col.element_size;
    let b = &col.bytes;

    (0..n)
        .map(|i| {
            let off = i * s;
            if off + s > b.len() {
                return 0;
            }
            match s {
                1 => b[off] as u64,
                2 => u16::from_le_bytes([b[off], b[off + 1]]) as u64,
                4 => {
                    if col.dtype_desc.contains("FloatingPoint") {
                        f32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]) as u64
                    } else {
                        u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]) as u64
                    }
                }
                8 => {
                    if col.dtype_desc.contains("FloatingPoint") {
                        f64::from_le_bytes([
                            b[off], b[off + 1], b[off + 2], b[off + 3], b[off + 4], b[off + 5],
                            b[off + 6], b[off + 7],
                        ]) as u64
                    } else {
                        u64::from_le_bytes([
                            b[off], b[off + 1], b[off + 2], b[off + 3], b[off + 4], b[off + 5],
                            b[off + 6], b[off + 7],
                        ])
                    }
                }
                _ => 0,
            }
        })
        .collect()
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
