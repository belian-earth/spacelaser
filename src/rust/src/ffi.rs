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

/// Simple stderr logger for debug diagnostics.
struct SimpleLogger;
impl log::Log for SimpleLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::max_level()
    }
    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!("[spacelaser:{}] {}", record.level(), record.args());
        }
    }
    fn flush(&self) {}
}
static LOGGER: SimpleLogger = SimpleLogger;

/// Initialize logging (once).
fn init_logging() {
    // Only enable debug logging when SPACELASER_DEBUG is set
    let level = if std::env::var("SPACELASER_DEBUG").is_ok() {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Warn
    };
    let _ = log::set_logger(&LOGGER).map(|()| log::set_max_level(level));
}

/// Create a tokio runtime for blocking on async operations.
///
/// We use a single-threaded runtime because R is single-threaded.
/// All async concurrency happens within this runtime via `block_on`.
fn runtime() -> tokio::runtime::Runtime {
    init_logging();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime")
}

/// Build a `DataSource` from URL and optional Earthdata credentials.
fn make_source(url: &str, username: Nullable<&str>, password: Nullable<&str>) -> DataSource {
    match (username, password) {
        (Nullable::NotNull(u), Nullable::NotNull(p)) => {
            DataSource::http_with_auth(url, u, p)
        }
        _ => DataSource::http(url),
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
    username: Nullable<&str>,
    password: Nullable<&str>,
) -> extendr_api::Result<List> {
    let rt = runtime();
    let source = make_source(url, username, password);

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
        let file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
        gedi::read_gedi(&file, product_type, bbox, cols, bms)
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
    username: Nullable<&str>,
    password: Nullable<&str>,
) -> extendr_api::Result<List> {
    let rt = runtime();
    let source = make_source(url, username, password);

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
        let file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
        icesat2::read_icesat2(&file, product_type, bbox, cols, trks)
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
fn rust_hdf5_groups(
    url: &str,
    path: &str,
    username: Nullable<&str>,
    password: Nullable<&str>,
) -> extendr_api::Result<Vec<String>> {
    let rt = runtime();
    let source = make_source(url, username, password);

    let result = rt.block_on(async {
        let file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
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
    username: Nullable<&str>,
    password: Nullable<&str>,
) -> extendr_api::Result<List> {
    let rt = runtime();
    let source = make_source(url, username, password);

    let result = rt.block_on(async {
        let file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
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

/// Read GEDI data from multiple remote HDF5 files concurrently.
///
/// All files are processed in parallel within a single async runtime.
/// Returns a list of per-file results (each is a list of beam data).
/// @export
#[extendr]
fn rust_read_gedi_multi(
    urls: Vec<String>,
    product: &str,
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
    columns: Nullable<Vec<String>>,
    beams: Nullable<Vec<String>>,
    username: Nullable<&str>,
    password: Nullable<&str>,
) -> extendr_api::Result<List> {
    let rt = runtime();

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

    // Extract auth strings so they can be shared across closures.
    let user = match username { Nullable::NotNull(u) => Some(u.to_string()), Nullable::Null => None };
    let pass = match password { Nullable::NotNull(p) => Some(p.to_string()), Nullable::Null => None };

    let results = rt.block_on(async {
        let futs: Vec<_> = urls.iter().map(|url| {
            let source = match (&user, &pass) {
                (Some(u), Some(p)) => DataSource::http_with_auth(url, u, p),
                _ => DataSource::http(url),
            };
            let cols = cols.clone();
            let bms = bms.clone();
            async move {
                let file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
                gedi::read_gedi(&file, product_type, bbox, cols, bms)
                    .await
                    .map_err(|e| e.to_string())
            }
        }).collect();

        futures::future::join_all(futs).await
    });

    // Flatten: each file's result is a Vec<GroupData>
    let mut all_groups = Vec::new();
    for result in results {
        match result {
            Ok(groups) => all_groups.extend(groups),
            Err(e) => {
                log::warn!("File read failed: {}", e);
            }
        }
    }

    let lists: Vec<List> = all_groups.into_iter().map(group_data_to_list).collect();
    Ok(List::from_values(lists))
}

/// Read ICESat-2 data from multiple remote HDF5 files concurrently.
/// @export
#[extendr]
fn rust_read_icesat2_multi(
    urls: Vec<String>,
    product: &str,
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
    columns: Nullable<Vec<String>>,
    tracks: Nullable<Vec<String>>,
    username: Nullable<&str>,
    password: Nullable<&str>,
) -> extendr_api::Result<List> {
    let rt = runtime();

    let product_type = match product {
        "ATL03" | "atl03" => IceSat2Product::ATL03,
        "ATL06" | "atl06" => IceSat2Product::ATL06,
        "ATL08" | "atl08" => IceSat2Product::ATL08,
        _ => return Err(extendr_api::Error::Other(format!("Unknown ICESat-2 product: {product}"))),
    };

    let bbox = crate::BBox::new(xmin, ymin, xmax, ymax);
    let cols = match columns { Nullable::NotNull(c) => Some(c), Nullable::Null => None };
    let trks = match tracks { Nullable::NotNull(t) => Some(t), Nullable::Null => None };

    let user = match username { Nullable::NotNull(u) => Some(u.to_string()), Nullable::Null => None };
    let pass = match password { Nullable::NotNull(p) => Some(p.to_string()), Nullable::Null => None };

    let results = rt.block_on(async {
        let futs: Vec<_> = urls.iter().map(|url| {
            let source = match (&user, &pass) {
                (Some(u), Some(p)) => DataSource::http_with_auth(url, u, p),
                _ => DataSource::http(url),
            };
            let cols = cols.clone();
            let trks = trks.clone();
            async move {
                let file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
                icesat2::read_icesat2(&file, product_type, bbox, cols, trks)
                    .await
                    .map_err(|e| e.to_string())
            }
        }).collect();

        futures::future::join_all(futs).await
    });

    let mut all_groups = Vec::new();
    for result in results {
        match result {
            Ok(groups) => all_groups.extend(groups),
            Err(e) => {
                log::warn!("File read failed: {}", e);
            }
        }
    }

    let lists: Vec<List> = all_groups.into_iter().map(group_data_to_list).collect();
    Ok(List::from_values(lists))
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
    fn rust_read_gedi_multi;
    fn rust_read_icesat2_multi;
    fn rust_hdf5_groups;
    fn rust_hdf5_dataset;
    fn rust_earthdata_token;
}
