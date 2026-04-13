//! ICESat-2 product-aware reader.
//!
//! Knows the internal structure of ICESat-2 ATL03, ATL06, and ATL08 HDF5
//! files and provides spatial subsetting by ground track and bounding box.
//!
//! ## File structures
//!
//! ATL08 (100m segment-level — most common for vegetation):
//! ```text
//! /gt1l/
//! ├── land_segments/
//! │   ├── latitude              (f64, per segment)
//! │   ├── longitude             (f64, per segment)
//! │   ├── canopy/
//! │   │   ├── h_canopy          (f32)
//! │   │   └── canopy_openness   (f32)
//! │   └── terrain/
//! │       └── h_te_best_fit     (f32)
//! └── signal_photons/
//! ```
//!
//! ATL03 (photon-level — very large datasets):
//! ```text
//! /gt1l/
//! ├── heights/
//! │   ├── lat_ph                (f64, per photon)
//! │   ├── lon_ph                (f64, per photon)
//! │   ├── h_ph                  (f32)
//! │   └── signal_conf_ph        (i8, per photon × 5 surface types)
//! └── geolocation/
//! ```

use super::common::{self, BBox, GroupData, SatelliteProduct};
use crate::hdf5::file::Hdf5File;
use crate::hdf5::types::Hdf5Error;

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
    ATL13,
    ATL24,
}

impl SatelliteProduct for IceSat2Product {
    fn group_names(&self) -> Vec<&'static str> {
        GROUND_TRACKS.to_vec()
    }

    fn lat_dataset(&self) -> &'static str {
        match self {
            IceSat2Product::ATL03 => "heights/lat_ph",
            IceSat2Product::ATL06 => "land_ice_segments/latitude",
            IceSat2Product::ATL08 => "land_segments/latitude",
            IceSat2Product::ATL13 => "segment_lat",
            IceSat2Product::ATL24 => "lat_ph",
        }
    }

    fn lon_dataset(&self) -> &'static str {
        match self {
            IceSat2Product::ATL03 => "heights/lon_ph",
            IceSat2Product::ATL06 => "land_ice_segments/longitude",
            IceSat2Product::ATL08 => "land_segments/longitude",
            IceSat2Product::ATL13 => "segment_lon",
            IceSat2Product::ATL24 => "lon_ph",
        }
    }
}

/// Read ICESat-2 data with spatial subsetting.
///
/// Thin wrapper around [`common::read_product_groups`] that supplies
/// ICESat-2-specific product metadata. `pool_columns` is accepted for
/// signature symmetry with GEDI but ICESat-2 has no pool datasets in
/// the currently supported products (ATLAS is photon-counting, not
/// analog-waveform), so it is typically `None`.
pub async fn read_icesat2(
    file: &Hdf5File,
    product: IceSat2Product,
    bbox: BBox,
    columns: Option<Vec<String>>,
    tracks: Option<Vec<String>>,
    pool_columns: Option<Vec<String>>,
) -> Result<Vec<GroupData>, Hdf5Error> {
    common::read_product_groups(file, &product, bbox, columns, tracks, pool_columns).await
}
