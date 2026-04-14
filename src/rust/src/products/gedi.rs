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
    L4C,
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
}

/// Read GEDI data with spatial subsetting.
///
/// Thin wrapper around [`common::read_product_groups`] that supplies
/// GEDI-specific product metadata. `pool_columns` is used for L1B
/// variable-length waveforms (`rxwaveform`, `txwaveform`).
pub async fn read_gedi(
    file: &Hdf5File,
    product: GediProduct,
    bbox: BBox,
    columns: Option<Vec<String>>,
    beams: Option<Vec<String>>,
    pool_columns: Option<Vec<String>>,
    transposed_columns: Option<Vec<String>>,
) -> Result<Vec<GroupData>, Hdf5Error> {
    common::read_product_groups(file, &product, bbox, columns, beams, pool_columns, transposed_columns).await
}
