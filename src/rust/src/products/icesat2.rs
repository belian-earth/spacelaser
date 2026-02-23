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
        }
    }

    fn lon_dataset(&self) -> &'static str {
        match self {
            IceSat2Product::ATL03 => "heights/lon_ph",
            IceSat2Product::ATL06 => "land_ice_segments/longitude",
            IceSat2Product::ATL08 => "land_segments/longitude",
        }
    }

    /// Default columns for each product (paths relative to the track group).
    fn default_columns(&self) -> Vec<&'static str> {
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
///
/// Thin wrapper around [`common::read_product_groups`] that supplies
/// ICESat-2-specific product metadata.
pub async fn read_icesat2(
    file: &Hdf5File,
    product: IceSat2Product,
    bbox: BBox,
    columns: Option<Vec<String>>,
    tracks: Option<Vec<String>>,
) -> Result<Vec<GroupData>, Hdf5Error> {
    common::read_product_groups(file, &product, bbox, columns, tracks).await
}
