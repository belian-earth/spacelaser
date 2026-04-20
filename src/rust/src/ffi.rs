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

/// Build a `DataSource` from URL/path and optional Earthdata credentials.
///
/// Routing:
///   - `http://` / `https://` → HTTP (authenticated if creds supplied)
///   - `file://<path>` → Local (prefix stripped)
///   - anything else → Local (treated as a filesystem path)
///
/// The Local route is for synthetic test fixtures and local HDF5 caches; it
/// shares the full reader pipeline with the HTTP route, so parser correctness
/// tests can exercise the real code paths without a network.
fn make_source(url: &str, username: Nullable<&str>, password: Nullable<&str>) -> DataSource {
    if let Some(path) = url.strip_prefix("file://") {
        return DataSource::local(path);
    }
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return DataSource::local(url);
    }
    match (username, password) {
        (Nullable::NotNull(u), Nullable::NotNull(p)) => {
            DataSource::http_with_auth(url, u, p)
        }
        _ => DataSource::http(url),
    }
}

/// Fill-value sentinels for each sensor family. Matching values are
/// replaced with NA during byte-to-typed-vector conversion so the R
/// side never sees raw sentinels.
fn fill_values_for_product(product: &str) -> Vec<f64> {
    match product {
        "L1B" | "l1b" | "L2A" | "l2a" | "L2B" | "l2b" | "L4A" | "l4a" | "L4C" | "l4c" => {
            vec![-9999.0, -999999.0]
        }
        "ATL08" | "atl08" | "ATL13" | "atl13" => {
            vec![3.4028235e+38_f64]
        }
        _ => Vec::new(),
    }
}

/// Convert a `GroupData` (beam or track) into an R list with typed
/// vectors (not raw bytes). Fill-value sentinels are replaced with NA
/// during conversion. Column name prefixes are stripped.
///
/// Output structure (consumed by `build_tibble()` on the R side):
/// ```text
/// list(
///   group_name   = "BEAM0101",
///   n_elements   = 1234L,
///   columns      = list(col1 = <dbl/int>, col2 = <dbl/int>, ...),
///   pool_columns = list(rxwaveform = <dbl>, ...),
///   pool_col_info = list(rxwaveform = "<json>", ...),
/// )
/// ```
fn group_data_to_list(gd: GroupData, fill_values: &[f64]) -> List {
    let n_elements = gd.selected_indices.len() as i32;
    let group_name = gd.group_name;

    let columns = columns_to_typed_list(gd.columns, fill_values);
    // Pool columns stay as raw bytes + JSON: the R side needs to
    // slice them per-shot using count vectors, and the flat byte
    // buffer is the right format for that (avoids double-conversion).
    let (pool_columns, pool_col_info) = columns_to_raw_lists(gd.pool_columns);

    list!(
        group_name = group_name,
        n_elements = n_elements,
        columns = columns,
        pool_columns = pool_columns,
        pool_col_info = pool_col_info
    )
}

/// Convert a `HashMap<String, ColumnData>` into a named R list of
/// typed vectors. Float columns become `Doubles` (with fill → NA),
/// integer columns become `Integers` (with fill → NA_integer).
/// Column name prefixes (e.g. "geolocation/") are stripped.
fn columns_to_typed_list(
    cols: std::collections::HashMap<String, crate::products::common::ColumnData>,
    fill_values: &[f64],
) -> List {
    let mut names: Vec<String> = Vec::new();
    let mut values: Vec<Robj> = Vec::new();

    for (name, cdata) in cols {
        // Strip subgroup prefix (e.g. "geolocation/solar_elevation" → "solar_elevation")
        let short_name = match name.rfind('/') {
            Some(pos) => name[pos + 1..].to_string(),
            None => name,
        };
        let robj = column_data_to_robj(&cdata, fill_values);
        names.push(short_name);
        values.push(robj);
    }

    List::from_names_and_values(names.iter().map(|s| s.as_str()), values)
        .unwrap_or_else(|_| List::new(0))
}

/// Convert raw HDF5 bytes + dtype into a typed R vector.
///
/// Float columns: f32→f64 widening, fill values → `NA_real_`.
/// Integer columns (≤4 bytes): direct cast to i32, fill → `NA_integer_`.
/// Integer columns (8 bytes): int64 → f64 arithmetic conversion.
fn column_data_to_robj(
    cdata: &crate::products::common::ColumnData,
    fill_values: &[f64],
) -> Robj {
    let b = &cdata.bytes;
    let s = cdata.element_size;
    let n = cdata.num_elements;
    let is_float = cdata.dtype_desc.contains("FloatingPoint");
    let is_signed = cdata.dtype_desc.contains("signed: true");

    if is_float {
        let doubles: Vec<Rfloat> = if s == 4 {
            b.chunks_exact(4)
                .take(n)
                .map(|chunk| {
                    let val = f32::from_le_bytes(chunk.try_into().unwrap()) as f64;
                    if fill_values.iter().any(|&fv| val == fv) {
                        Rfloat::na()
                    } else {
                        Rfloat::from(val)
                    }
                })
                .collect()
        } else {
            b.chunks_exact(8)
                .take(n)
                .map(|chunk| {
                    let val = f64::from_le_bytes(chunk.try_into().unwrap());
                    if val.is_nan() || fill_values.iter().any(|&fv| val == fv) {
                        Rfloat::na()
                    } else {
                        Rfloat::from(val)
                    }
                })
                .collect()
        };
        Doubles::from_values(doubles).into()
    } else if s <= 4 {
        // Integer (signed or unsigned, ≤ 32 bits)
        //
        // Signed → R integer (i32). Unsigned 1-2 bytes → R integer
        // (fits in i32). Unsigned 4 bytes → R double (u32 max 4.3e9
        // exceeds i32 max 2.1e9 but is exact in f64).
        if !is_signed && s == 4 {
            let doubles: Vec<Rfloat> = b.chunks_exact(4).take(n).map(|chunk| {
                let val = u32::from_le_bytes(chunk.try_into().unwrap()) as f64;
                if fill_values.iter().any(|&fv| val == fv) {
                    Rfloat::na()
                } else {
                    Rfloat::from(val)
                }
            }).collect();
            return Doubles::from_values(doubles).into();
        }
        let fill_ints: Vec<i32> = fill_values.iter().map(|&fv| fv as i32).collect();
        let ints: Vec<Rint> = match s {
            1 => {
                if is_signed {
                    b.iter().take(n).map(|&byte| {
                        let val = byte as i8 as i32;
                        if fill_ints.contains(&val) { Rint::na() } else { Rint::from(val) }
                    }).collect()
                } else {
                    b.iter().take(n).map(|&byte| {
                        let val = byte as i32;
                        if fill_ints.contains(&val) { Rint::na() } else { Rint::from(val) }
                    }).collect()
                }
            }
            2 => {
                b.chunks_exact(2).take(n).map(|chunk| {
                    let val = if is_signed {
                        i16::from_le_bytes(chunk.try_into().unwrap()) as i32
                    } else {
                        u16::from_le_bytes(chunk.try_into().unwrap()) as i32
                    };
                    if fill_ints.contains(&val) { Rint::na() } else { Rint::from(val) }
                }).collect()
            }
            _ => {
                // 3 or 4 bytes, signed → i32
                b.chunks_exact(s).take(n).map(|chunk| {
                    let mut buf = [0u8; 4];
                    buf[..s].copy_from_slice(chunk);
                    let val = i32::from_le_bytes(buf);
                    if fill_ints.contains(&val) { Rint::na() } else { Rint::from(val) }
                }).collect()
            }
        };
        Integers::from_values(ints).into()
    } else {
        // int64 / uint64 → bit64::integer64.
        //
        // bit64 stores int64 values as doubles where the 8 bytes ARE
        // the raw int64 bit pattern (a transmute, not a conversion).
        // NA is represented by i64::MIN (0x8000_0000_0000_0000).
        //
        // All current unsigned 64-bit values in GEDI/ICESat-2
        // (shot_number max ~2.3e17) fit within signed int64 range
        // (max 9.2e18), so treating uint64 as int64 is safe.
        const BIT64_NA_BITS: u64 = 0x8000_0000_0000_0000;
        let doubles: Vec<Rfloat> = b.chunks_exact(8).take(n).map(|chunk| {
            let bits = u64::from_le_bytes(chunk.try_into().unwrap());
            // Fill-value check: interpret raw bits as i64, cast to f64,
            // compare against the f64 fill values from the product spec.
            let as_f64 = (bits as i64) as f64;
            if bits == BIT64_NA_BITS || fill_values.iter().any(|&fv| as_f64 == fv) {
                Rfloat::from(f64::from_bits(BIT64_NA_BITS))
            } else {
                Rfloat::from(f64::from_bits(bits))
            }
        }).collect();
        let mut robj: Robj = Doubles::from_values(doubles).into();
        robj.set_attrib(class_symbol(), "integer64").unwrap();
        robj
    }
}

/// Convert a `HashMap<String, ColumnData>` into two parallel R lists:
/// one of raw byte vectors, one of JSON info strings. Used for pool
/// columns which the R side still needs to slice per-shot.
fn columns_to_raw_lists(
    cols: std::collections::HashMap<String, crate::products::common::ColumnData>,
) -> (List, List) {
    let mut names: Vec<String> = Vec::new();
    let mut bytes: Vec<Raw> = Vec::new();
    let mut info_strs: Vec<String> = Vec::new();

    for (name, cdata) in cols {
        names.push(name);
        bytes.push(Raw::from_bytes(&cdata.bytes));
        info_strs.push(
            serde_json::json!({
                "element_size": cdata.element_size,
                "num_elements": cdata.num_elements,
                "dtype": cdata.dtype_desc,
            })
            .to_string(),
        );
    }

    let col_list = List::from_names_and_values(
        names.iter().map(|s| s.as_str()),
        bytes,
    )
    .unwrap_or_else(|_| List::new(0));

    let info_list = List::from_names_and_values(
        names.iter().map(|s| s.as_str()),
        info_strs,
    )
    .unwrap_or_else(|_| List::new(0));

    (col_list, info_list)
}

// ---------------------------------------------------------------------------
// Exported functions (called from R)
// ---------------------------------------------------------------------------

/// Read GEDI data from a remote HDF5 file with spatial subsetting.
/// @noRd
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
    pool_columns: Nullable<Vec<String>>,
    transposed_columns: Nullable<Vec<String>>,
) -> extendr_api::Result<List> {
    let rt = runtime();
    let source = make_source(url, username, password);

    let product_type = match product {
        "L1B" | "l1b" => GediProduct::L1B,
        "L2A" | "l2a" => GediProduct::L2A,
        "L2B" | "l2b" => GediProduct::L2B,
        "L4A" | "l4a" => GediProduct::L4A,
        "L4C" | "l4c" => GediProduct::L4C,
        _ => return Err(extendr_api::Error::Other(format!("Unknown GEDI product: {product}"))),
    };

    let bbox = crate::BBox::new(xmin, ymin, xmax, ymax);
    let cols = match columns { Nullable::NotNull(c) => Some(c), Nullable::Null => None };
    let bms = match beams { Nullable::NotNull(b) => Some(b), Nullable::Null => None };
    let pool = match pool_columns { Nullable::NotNull(p) => Some(p), Nullable::Null => None };
    let trans = match transposed_columns { Nullable::NotNull(t) => Some(t), Nullable::Null => None };

    let result = rt.block_on(async {
        let file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
        gedi::read_gedi(&file, product_type, bbox, cols, bms, pool, trans)
            .await
            .map_err(|e| e.to_string())
    });

    let fill_vals = fill_values_for_product(product);
    match result {
        Ok(groups) => {
            let lists: Vec<List> = groups.into_iter().map(|gd| group_data_to_list(gd, &fill_vals)).collect();
            Ok(List::from_values(lists))
        }
        Err(e) => Err(extendr_api::Error::Other(e)),
    }
}

/// Read ICESat-2 data from a remote HDF5 file with spatial subsetting.
/// @noRd
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
    pool_columns: Nullable<Vec<String>>,
    transposed_columns: Nullable<Vec<String>>,
) -> extendr_api::Result<List> {
    let rt = runtime();
    let source = make_source(url, username, password);

    let product_type = match product {
        "ATL03" | "atl03" => IceSat2Product::ATL03,
        "ATL06" | "atl06" => IceSat2Product::ATL06,
        "ATL07" | "atl07" => IceSat2Product::ATL07,
        "ATL08" | "atl08" => IceSat2Product::ATL08,
        "ATL10" | "atl10" => IceSat2Product::ATL10,
        "ATL13" | "atl13" => IceSat2Product::ATL13,
        "ATL24" | "atl24" => IceSat2Product::ATL24,
        _ => return Err(extendr_api::Error::Other(format!("Unknown ICESat-2 product: {product}"))),
    };

    let bbox = crate::BBox::new(xmin, ymin, xmax, ymax);
    let cols = match columns { Nullable::NotNull(c) => Some(c), Nullable::Null => None };
    let trks = match tracks { Nullable::NotNull(t) => Some(t), Nullable::Null => None };
    let pool = match pool_columns { Nullable::NotNull(p) => Some(p), Nullable::Null => None };
    let trans = match transposed_columns { Nullable::NotNull(t) => Some(t), Nullable::Null => None };

    let result = rt.block_on(async {
        let file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
        icesat2::read_icesat2(&file, product_type, bbox, cols, trks, pool, trans)
            .await
            .map_err(|e| e.to_string())
    });

    let fill_vals = fill_values_for_product(product);
    match result {
        Ok(groups) => {
            let lists: Vec<List> = groups.into_iter().map(|gd| group_data_to_list(gd, &fill_vals)).collect();
            Ok(List::from_values(lists))
        }
        Err(e) => Err(extendr_api::Error::Other(e)),
    }
}

/// List available groups in an HDF5 file (for exploration).
/// @noRd
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
/// @noRd
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
/// @noRd
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
    pool_columns: Nullable<Vec<String>>,
    transposed_columns: Nullable<Vec<String>>,
) -> extendr_api::Result<List> {
    let rt = runtime();

    let product_type = match product {
        "L1B" | "l1b" => GediProduct::L1B,
        "L2A" | "l2a" => GediProduct::L2A,
        "L2B" | "l2b" => GediProduct::L2B,
        "L4A" | "l4a" => GediProduct::L4A,
        "L4C" | "l4c" => GediProduct::L4C,
        _ => return Err(extendr_api::Error::Other(format!("Unknown GEDI product: {product}"))),
    };

    let bbox = crate::BBox::new(xmin, ymin, xmax, ymax);
    let cols = match columns { Nullable::NotNull(c) => Some(c), Nullable::Null => None };
    let bms = match beams { Nullable::NotNull(b) => Some(b), Nullable::Null => None };
    let pool = match pool_columns { Nullable::NotNull(p) => Some(p), Nullable::Null => None };
    let trans = match transposed_columns { Nullable::NotNull(t) => Some(t), Nullable::Null => None };

    // Extract auth strings so they can be shared across closures.
    let user = match username { Nullable::NotNull(u) => Some(u.to_string()), Nullable::Null => None };
    let pass = match password { Nullable::NotNull(p) => Some(p.to_string()), Nullable::Null => None };

    crate::io::reader::reset_request_counter();
    let t_start = std::time::Instant::now();

    // Process granules with bounded concurrency (4 at a time) to avoid
    // overwhelming the server.  Each granule internally runs its beams and
    // columns concurrently, so 4 granules is already a high request fan-out.
    let all_groups = rt.block_on(async {
        use futures::stream::{self, StreamExt};

        let results: Vec<_> = stream::iter(urls.iter())
            .map(|url| {
                let source = match (&user, &pass) {
                    (Some(u), Some(p)) => DataSource::http_with_auth(url, u, p),
                    _ => DataSource::http(url),
                };
                let cols = cols.clone();
                let bms = bms.clone();
                let pool = pool.clone();
                let trans = trans.clone();
                async move {
                    let file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
                    gedi::read_gedi(&file, product_type, bbox, cols, bms, pool, trans)
                        .await
                        .map_err(|e| e.to_string())
                }
            })
            // Up from 4. Multi-granule reads are dominated by HTTP
            // round-trips; bumping concurrency lets us saturate NASA
            // DAAC endpoints more completely on fast connections.
            .buffer_unordered(8)
            .collect()
            .await;

        let mut all_groups = Vec::new();
        for result in results {
            match result {
                Ok(groups) => all_groups.extend(groups),
                Err(e) => {
                    log::warn!("File read failed: {}", e);
                }
            }
        }
        all_groups
    });

    let (req_count, byte_count) = crate::io::reader::request_counter();
    log::info!(
        "http summary: {} requests, {:.1} MB, {:.2}s wall, \
         {:.1} req/s, {:.2} MB/req",
        req_count,
        byte_count as f64 / 1_048_576.0,
        t_start.elapsed().as_secs_f64(),
        req_count as f64 / t_start.elapsed().as_secs_f64(),
        (byte_count as f64 / 1_048_576.0) / req_count.max(1) as f64,
    );

    let fill_vals = fill_values_for_product(product);
    let lists: Vec<List> = all_groups.into_iter().map(|gd| group_data_to_list(gd, &fill_vals)).collect();
    Ok(List::from_values(lists))
}

/// Read ICESat-2 data from multiple remote HDF5 files concurrently.
/// @noRd
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
    pool_columns: Nullable<Vec<String>>,
    transposed_columns: Nullable<Vec<String>>,
) -> extendr_api::Result<List> {
    let rt = runtime();

    let product_type = match product {
        "ATL03" | "atl03" => IceSat2Product::ATL03,
        "ATL06" | "atl06" => IceSat2Product::ATL06,
        "ATL07" | "atl07" => IceSat2Product::ATL07,
        "ATL08" | "atl08" => IceSat2Product::ATL08,
        "ATL10" | "atl10" => IceSat2Product::ATL10,
        "ATL13" | "atl13" => IceSat2Product::ATL13,
        "ATL24" | "atl24" => IceSat2Product::ATL24,
        _ => return Err(extendr_api::Error::Other(format!("Unknown ICESat-2 product: {product}"))),
    };

    let bbox = crate::BBox::new(xmin, ymin, xmax, ymax);
    let cols = match columns { Nullable::NotNull(c) => Some(c), Nullable::Null => None };
    let trks = match tracks { Nullable::NotNull(t) => Some(t), Nullable::Null => None };
    let pool = match pool_columns { Nullable::NotNull(p) => Some(p), Nullable::Null => None };
    let trans = match transposed_columns { Nullable::NotNull(t) => Some(t), Nullable::Null => None };

    let user = match username { Nullable::NotNull(u) => Some(u.to_string()), Nullable::Null => None };
    let pass = match password { Nullable::NotNull(p) => Some(p.to_string()), Nullable::Null => None };

    let all_groups = rt.block_on(async {
        use futures::stream::{self, StreamExt};

        let results: Vec<_> = stream::iter(urls.iter())
            .map(|url| {
                let source = match (&user, &pass) {
                    (Some(u), Some(p)) => DataSource::http_with_auth(url, u, p),
                    _ => DataSource::http(url),
                };
                let cols = cols.clone();
                let trks = trks.clone();
                let pool = pool.clone();
                let trans = trans.clone();
                async move {
                    let file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
                    icesat2::read_icesat2(&file, product_type, bbox, cols, trks, pool, trans)
                        .await
                        .map_err(|e| e.to_string())
                }
            })
            // Up from 4. Multi-granule reads are dominated by HTTP
            // round-trips; bumping concurrency lets us saturate NASA
            // DAAC endpoints more completely on fast connections.
            .buffer_unordered(8)
            .collect()
            .await;

        let mut all_groups = Vec::new();
        for result in results {
            match result {
                Ok(groups) => all_groups.extend(groups),
                Err(e) => {
                    log::warn!("File read failed: {}", e);
                }
            }
        }
        all_groups
    });

    let fill_vals = fill_values_for_product(product);
    let lists: Vec<List> = all_groups.into_iter().map(|gd| group_data_to_list(gd, &fill_vals)).collect();
    Ok(List::from_values(lists))
}

/// Returns true when this Rust crate was compiled with debug assertions
/// enabled (i.e. without --release). Used by the benchmark setup to
/// refuse to run against an unoptimised binary, since rextendr::document()
/// generates a debug Makevars by default during dev iteration.
/// @noRd
#[extendr]
fn rust_is_debug() -> bool {
    cfg!(debug_assertions)
}

extendr_module! {
    mod spacelaser;
    fn rust_is_debug;
    fn rust_read_gedi;
    fn rust_read_icesat2;
    fn rust_read_gedi_multi;
    fn rust_read_icesat2_multi;
    fn rust_hdf5_groups;
    fn rust_hdf5_dataset;
}
