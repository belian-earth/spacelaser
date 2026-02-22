//! FFI bindings for R via extendr.
//!
//! These functions are called from R through the rextendr framework.
//! Data is transferred as raw byte vectors + JSON metadata strings,
//! which the R side converts to properly typed R vectors.
//!
//! ## Design choice: raw bytes + JSON metadata (not Arrow)
//!
//! We initially planned to use Arrow RecordBatches for zero-copy transfer,
//! but the extendr ↔ arrow interop is immature.  Instead, each column is
//! returned as a raw byte vector with a tiny JSON sidecar describing its
//! HDF5 datatype and element count.  The R side (`parse_column()`) uses
//! `readBin()` to reinterpret the bytes — this is fast and avoids any
//! additional compiled dependency beyond what extendr already provides.

use extendr_api::prelude::*;

use crate::hdf5::file::Hdf5File;
use crate::io::source::DataSource;
use crate::products::common::GroupData;
use crate::products::gedi::{self, GediProduct};
use crate::products::icesat2::{self, IceSat2Product};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a tokio runtime for blocking on async operations.
///
/// We use a single-threaded runtime because R is single-threaded.
/// All async concurrency happens within this runtime via `block_on`.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
}

/// Build a `DataSource` from URL and optional bearer token.
///
/// Extracts the repeated pattern that was previously inlined in every
/// exported function.
fn make_source(url: &str, bearer_token: Nullable<&str>) -> DataSource {
    match bearer_token {
        Nullable::NotNull(token) => DataSource::http_with_token(url, token),
        Nullable::Null => DataSource::http(url),
    }
}

/// Convert a `GroupData` (beam or track) into an R list.
///
/// Output structure (consumed by `build_tibble()` on the R side):
/// ```text
/// list(
///   group_name   = "BEAM0101",
///   n_elements   = 1234L,
///   columns      = list(col1 = <raw>, col2 = <raw>, ...),
///   col_info     = list(col1 = "<json>", col2 = "<json>", ...)
/// )
/// ```
fn group_data_to_list(gd: GroupData) -> List {
    let n_elements = gd.selected_indices.len() as i32;
    let group_name = gd.group_name;

    let mut col_names: Vec<String> = Vec::new();
    let mut col_bytes: Vec<Raw> = Vec::new();
    let mut col_info_strs: Vec<String> = Vec::new();

    for (name, cdata) in gd.columns {
        col_names.push(name);
        col_bytes.push(Raw::from_bytes(&cdata.bytes));
        col_info_strs.push(
            serde_json::json!({
                "element_size": cdata.element_size,
                "num_elements": cdata.num_elements,
                "dtype": cdata.dtype_desc,
            })
            .to_string(),
        );
    }

    let columns = List::from_names_and_values(
        col_names.iter().map(|s| s.as_str()),
        col_bytes,
    )
    .unwrap_or_else(|_| List::new(0));

    let col_info = List::from_names_and_values(
        col_names.iter().map(|s| s.as_str()),
        col_info_strs,
    )
    .unwrap_or_else(|_| List::new(0));

    // Use generic field names so the R side doesn't need to branch
    // on GEDI vs ICESat-2.  The R wrapper renames `group_name` to
    // `beam` or `track` as appropriate.
    list!(
        group_name = group_name,
        n_elements = n_elements,
        columns = columns,
        col_info = col_info
    )
}

// ---------------------------------------------------------------------------
// Exported functions (called from R)
// ---------------------------------------------------------------------------

/// Read GEDI data from a remote HDF5 file with spatial subsetting.
/// @export
#[extendr]
fn rust_read_gedi(
    url: &str,
    product: &str,
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
    columns: Nullable<Vec<String>>,
    beams: Nullable<Vec<String>>,
    bearer_token: Nullable<&str>,
) -> extendr_api::Result<List> {
    let rt = runtime();
    let source = make_source(url, bearer_token);

    let product_type = match product {
        "L1B" | "l1b" => GediProduct::L1B,
        "L2A" | "l2a" => GediProduct::L2A,
        "L2B" | "l2b" => GediProduct::L2B,
        "L4A" | "l4a" => GediProduct::L4A,
        _ => return Err(extendr_api::Error::Other(format!("Unknown GEDI product: {product}"))),
    };

    let bbox = crate::BBox::new(xmin, ymin, xmax, ymax);
    let cols = match columns { Nullable::NotNull(c) => Some(c), Nullable::Null => None };
    let bms = match beams { Nullable::NotNull(b) => Some(b), Nullable::Null => None };

    let result = rt.block_on(async {
        let mut file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
        gedi::read_gedi(&mut file, product_type, bbox, cols, bms)
            .await
            .map_err(|e| e.to_string())
    });

    match result {
        Ok(groups) => {
            let lists: Vec<List> = groups.into_iter().map(group_data_to_list).collect();
            Ok(List::from_values(lists))
        }
        Err(e) => Err(extendr_api::Error::Other(e)),
    }
}

/// Read ICESat-2 data from a remote HDF5 file with spatial subsetting.
/// @export
#[extendr]
fn rust_read_icesat2(
    url: &str,
    product: &str,
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
    columns: Nullable<Vec<String>>,
    tracks: Nullable<Vec<String>>,
    bearer_token: Nullable<&str>,
) -> extendr_api::Result<List> {
    let rt = runtime();
    let source = make_source(url, bearer_token);

    let product_type = match product {
        "ATL03" | "atl03" => IceSat2Product::ATL03,
        "ATL06" | "atl06" => IceSat2Product::ATL06,
        "ATL08" | "atl08" => IceSat2Product::ATL08,
        _ => return Err(extendr_api::Error::Other(format!("Unknown ICESat-2 product: {product}"))),
    };

    let bbox = crate::BBox::new(xmin, ymin, xmax, ymax);
    let cols = match columns { Nullable::NotNull(c) => Some(c), Nullable::Null => None };
    let trks = match tracks { Nullable::NotNull(t) => Some(t), Nullable::Null => None };

    let result = rt.block_on(async {
        let mut file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
        icesat2::read_icesat2(&mut file, product_type, bbox, cols, trks)
            .await
            .map_err(|e| e.to_string())
    });

    match result {
        Ok(groups) => {
            let lists: Vec<List> = groups.into_iter().map(group_data_to_list).collect();
            Ok(List::from_values(lists))
        }
        Err(e) => Err(extendr_api::Error::Other(e)),
    }
}

/// List available groups in an HDF5 file (for exploration).
/// @export
#[extendr]
fn rust_hdf5_groups(url: &str, path: &str, bearer_token: Nullable<&str>) -> extendr_api::Result<Vec<String>> {
    let rt = runtime();
    let source = make_source(url, bearer_token);

    let result = rt.block_on(async {
        let mut file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
        file.list_group(path).await.map_err(|e| e.to_string())
    });

    match result {
        Ok(members) => Ok(members.into_iter().map(|(name, _)| name).collect()),
        Err(e) => Err(extendr_api::Error::Other(e)),
    }
}

/// Read a single dataset from an HDF5 file and return raw bytes + metadata.
/// @export
#[extendr]
fn rust_hdf5_dataset(
    url: &str,
    dataset_path: &str,
    bearer_token: Nullable<&str>,
) -> extendr_api::Result<List> {
    let rt = runtime();
    let source = make_source(url, bearer_token);

    let result = rt.block_on(async {
        let mut file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
        file.read_dataset(dataset_path)
            .await
            .map_err(|e| e.to_string())
    });

    match result {
        Ok((meta, bytes)) => {
            let info = serde_json::json!({
                "dtype": format!("{:?}", meta.datatype),
                "shape": meta.dataspace.dims,
                "element_size": meta.datatype.size(),
                "num_elements": meta.dataspace.num_elements(),
            })
            .to_string();

            Ok(list!(data = Raw::from_bytes(&bytes), info = info))
        }
        Err(e) => Err(extendr_api::Error::Other(e)),
    }
}

/// Exchange Earthdata username/password for a bearer token.
///
/// Calls the NASA Earthdata Login token API. Returns the access token string.
/// @export
#[extendr]
fn rust_earthdata_token(username: &str, password: &str) -> extendr_api::Result<String> {
    let rt = runtime();
    rt.block_on(async {
        crate::auth::fetch_earthdata_token(username, password)
            .await
            .map_err(extendr_api::Error::Other)
    })
}

extendr_module! {
    mod spacelaser;
    fn rust_read_gedi;
    fn rust_read_icesat2;
    fn rust_hdf5_groups;
    fn rust_hdf5_dataset;
    fn rust_earthdata_token;
}
