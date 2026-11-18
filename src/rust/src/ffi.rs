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
use crate::io::batch::{read_granules, SigpipeGuard};
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

/// R list of the per-group results, with the granules that failed after
/// the retries attached as attributes `failed_urls` and `failed_errors`.
fn groups_with_failures(groups: Vec<GroupData>, failed: Vec<(String, String)>, family: FillFamily) -> List {
    let lists: Vec<List> = groups.into_iter().map(|gd| group_data_to_list(gd, family)).collect();
    let mut out = List::from_values(lists);
    let (urls, errors): (Vec<String>, Vec<String>) = failed.into_iter().unzip();
    out.set_attrib("failed_urls", urls).unwrap();
    out.set_attrib("failed_errors", errors).unwrap();
    out
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
fn make_source(url: &str, token: Nullable<&str>) -> DataSource {
    if let Some(path) = url.strip_prefix("file://") {
        return DataSource::local(path);
    }
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return DataSource::local(url);
    }
    match token {
        Nullable::NotNull(t) if !t.is_empty() => DataSource::http_with_token(url, t),
        _ => DataSource::http(url),
    }
}

/// Sensor family, for the missing-data sentinels that a product uses
/// across all its datasets.
///
/// The first authority is each dataset's own `_FillValue` attribute
/// (`ColumnData::fill`), compared in the dataset's native type. The
/// family sentinels below are the fallback for datasets that carry the
/// sentinel without declaring it. They are restricted to values that
/// cannot be data: integer type maxima such as 127 or 255 are *not*
/// family sentinels, because they are legitimate values in columns that
/// declare no fill.
#[derive(Clone, Copy, PartialEq)]
enum FillFamily {
    /// -9999 and -999999, in any numeric type that can hold them.
    Gedi,
    /// The ATLAS `INVALID_R4B` / `INVALID_R8B` / `INVALID_I4B` constants:
    /// the largest finite float of each width and the largest int32.
    Icesat2,
    None,
}

/// The GEDI product for a registry key from R: a product name, with a
/// `_V3` suffix for the version 3 file layout. Only L2B reads differently
/// between versions (its coordinates moved to the beam root).
fn parse_gedi_product(product: &str) -> extendr_api::Result<GediProduct> {
    let upper = product.to_ascii_uppercase();
    Ok(match upper.as_str() {
        "L2B_V3" => GediProduct::L2BV3,
        other => match other.strip_suffix("_V3").unwrap_or(other) {
            "L1B" => GediProduct::L1B,
            "L2A" => GediProduct::L2A,
            "L2B" => GediProduct::L2B,
            "L4A" => GediProduct::L4A,
            "L4C" => GediProduct::L4C,
            _ => return Err(extendr_api::Error::Other(format!("Unknown GEDI product: {product}"))),
        },
    })
}

fn fill_family(product: &str) -> FillFamily {
    let upper = product.to_ascii_uppercase();
    match upper.strip_suffix("_V3").unwrap_or(&upper) {
        "L1B" | "L2A" | "L2B" | "L4A" | "L4C" => FillFamily::Gedi,
        "ATL03" | "ATL06" | "ATL07" | "ATL08" | "ATL10" | "ATL13" | "ATL24" => {
            FillFamily::Icesat2
        }
        _ => FillFamily::None,
    }
}

const GEDI_FILLS: [i64; 2] = [-9999, -999999];

/// GEDI integer columns whose files hold a type-maximum sentinel without
/// declaring it in a `_FillValue` attribute. Listed by name, because 255
/// or 32767 is not safe to treat as missing in GEDI integer columns at
/// large. Each value lies outside its column's valid range (flags of 0
/// or 1, a cycle count, a day of year, a calendar year).
fn gedi_column_fill(short_name: &str) -> Option<i64> {
    match short_name {
        "leaf_off_flag" | "leaf_on_cycle" | "predictor_limit_flag" | "response_limit_flag"
        | "phenology_phase" => Some(255),
        "leaf_off_doy" | "leaf_on_doy" => Some(32767),
        "phenology_year" => Some(65535),
        _ => None,
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
///   columns      = list(col1 = <dbl/int/chr>, col2 = ..., ...),
///   pool_columns = list(rxwaveform = <raw>, ...),
///   pool_col_info = list(rxwaveform = "<json>", ...),
///   skipped      = c(col = "reason", ...)
/// )
/// ```
fn group_data_to_list(gd: GroupData, family: FillFamily) -> List {
    let n_elements = gd.selected_indices.len() as i32;
    let group_name = gd.group_name;
    let mut skipped = gd.skipped;

    let columns = columns_to_typed_list(gd.columns, family, &mut skipped);
    // Pool columns stay as raw bytes + JSON: the R side needs to
    // slice them per-shot using count vectors, and the flat byte
    // buffer is the right format for that (avoids double-conversion).
    let (pool_columns, pool_col_info) = columns_to_raw_lists(gd.pool_columns);

    let (skipped_names, skipped_reasons): (Vec<String>, Vec<String>) =
        skipped.into_iter().unzip();
    let mut skipped_robj: Robj = skipped_reasons.into();
    skipped_robj.set_names(skipped_names).unwrap();

    list!(
        group_name = group_name,
        n_elements = n_elements,
        columns = columns,
        pool_columns = pool_columns,
        pool_col_info = pool_col_info,
        skipped = skipped_robj
    )
}

/// Convert a `HashMap<String, ColumnData>` into a named R list of
/// typed vectors. Column name prefixes (e.g. "geolocation/") are
/// stripped. Columns whose datatype has no R representation are left
/// out and recorded in `skipped`.
fn columns_to_typed_list(
    cols: std::collections::HashMap<String, crate::products::common::ColumnData>,
    family: FillFamily,
    skipped: &mut Vec<(String, String)>,
) -> List {
    // Sort by full key so output column order is deterministic across
    // runs; HashMap iteration order is randomised per process.
    let mut entries: Vec<_> = cols.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let mut names: Vec<String> = Vec::with_capacity(entries.len());
    let mut values: Vec<Robj> = Vec::with_capacity(entries.len());

    for (name, cdata) in entries {
        // Strip subgroup prefix (e.g. "geolocation/solar_elevation" → "solar_elevation")
        let short_name = match name.rfind('/') {
            Some(pos) => name[pos + 1..].to_string(),
            None => name,
        };
        let named_fill = if family == FillFamily::Gedi {
            gedi_column_fill(&short_name)
        } else {
            None
        };
        match column_data_to_robj(&cdata, family, named_fill) {
            Ok(robj) => {
                names.push(short_name);
                values.push(robj);
            }
            Err(why) => skipped.push((short_name, why)),
        }
    }

    List::from_names_and_values(names.iter().map(|s| s.as_str()), values)
        .unwrap_or_else(|_| List::new(0))
}

/// Convert raw HDF5 bytes + dtype into a typed R vector.
///
/// - Floats (4 or 8 bytes) become doubles.
/// - Integers up to 32 bits become R integers, except uint32, which
///   becomes double (exact; it does not fit a signed 32-bit integer).
/// - 64-bit integers become `bit64::integer64`.
/// - Fixed-length strings become character.
///
/// An element becomes NA when it equals the dataset's declared
/// `_FillValue`, when it is a family sentinel (see [`FillFamily`]) or the
/// column's own undeclared sentinel (`named_fill`, integers up to 32
/// bits), or when it is a float NaN. Any other datatype is an `Err` naming it.
fn column_data_to_robj(
    cdata: &crate::products::common::ColumnData,
    family: FillFamily,
    named_fill: Option<i64>,
) -> std::result::Result<Robj, String> {
    let b = &cdata.bytes;
    let s = cdata.element_size;
    let n = cdata.num_elements;
    let desc = cdata.dtype_desc.as_str();
    let is_float = desc.starts_with("FloatingPoint");
    let is_int = desc.starts_with("FixedPoint");
    let is_string = desc.starts_with("String");
    let is_signed = desc.contains("signed: true");

    // Declared sentinel: byte-for-byte equality in the native type, so
    // no widening or rounding can make it miss.
    let declared: Option<&[u8]> = cdata.fill.as_deref().filter(|f| f.len() == s);
    let is_declared = |chunk: &[u8]| declared == Some(chunk);
    let gedi = family == FillFamily::Gedi;
    let icesat2 = family == FillFamily::Icesat2;

    if is_string && s > 0 {
        // Fixed-length string: NUL-terminated, NUL-padded or space-padded.
        let strings: Vec<String> = b
            .chunks_exact(s)
            .take(n)
            .map(|chunk| {
                let end = chunk.iter().position(|&c| c == 0).unwrap_or(s);
                String::from_utf8_lossy(&chunk[..end]).trim_end().to_string()
            })
            .collect();
        return Ok(strings.into());
    }

    if is_float && s == 4 {
        let doubles: Vec<Rfloat> = b
            .chunks_exact(4)
            .take(n)
            .map(|chunk| {
                let val = f32::from_le_bytes(chunk.try_into().unwrap());
                let fill = is_declared(chunk)
                    || val.is_nan()
                    || (icesat2 && val == f32::MAX)
                    || (gedi && GEDI_FILLS.iter().any(|&fv| val == fv as f32));
                if fill { Rfloat::na() } else { Rfloat::from(val as f64) }
            })
            .collect();
        return Ok(Doubles::from_values(doubles).into());
    }
    if is_float && s == 8 {
        let doubles: Vec<Rfloat> = b
            .chunks_exact(8)
            .take(n)
            .map(|chunk| {
                let val = f64::from_le_bytes(chunk.try_into().unwrap());
                let fill = is_declared(chunk)
                    || val.is_nan()
                    || (icesat2 && val == f64::MAX)
                    || (gedi && GEDI_FILLS.iter().any(|&fv| val == fv as f64));
                if fill { Rfloat::na() } else { Rfloat::from(val) }
            })
            .collect();
        return Ok(Doubles::from_values(doubles).into());
    }

    if is_int && !is_signed && s == 4 {
        // uint32 → double (max 4.3e9 exceeds i32 but is exact in f64).
        let doubles: Vec<Rfloat> = b
            .chunks_exact(4)
            .take(n)
            .map(|chunk| {
                if is_declared(chunk) {
                    Rfloat::na()
                } else {
                    Rfloat::from(u32::from_le_bytes(chunk.try_into().unwrap()) as f64)
                }
            })
            .collect();
        return Ok(Doubles::from_values(doubles).into());
    }
    if is_int && (1..=4).contains(&s) {
        let ints: Vec<Rint> = b
            .chunks_exact(s)
            .take(n)
            .map(|chunk| {
                let val: i32 = match (s, is_signed) {
                    (1, true) => chunk[0] as i8 as i32,
                    (1, false) => chunk[0] as i32,
                    (2, true) => i16::from_le_bytes(chunk.try_into().unwrap()) as i32,
                    (2, false) => u16::from_le_bytes(chunk.try_into().unwrap()) as i32,
                    _ => {
                        // 3 or 4 bytes. For 3-byte signed values the top
                        // byte must be sign-extended, otherwise negatives
                        // deserialise to large positive i32.
                        let mut buf = [0u8; 4];
                        buf[..s].copy_from_slice(chunk);
                        if s == 3 && is_signed && (chunk[2] & 0x80) != 0 {
                            buf[3] = 0xFF;
                        }
                        i32::from_le_bytes(buf)
                    }
                };
                let fill = is_declared(chunk)
                    || named_fill == Some(val as i64)
                    || (icesat2 && s == 4 && val == i32::MAX)
                    || (gedi && is_signed && GEDI_FILLS.contains(&(val as i64)));
                // i32::MIN is R's own NA_integer_.
                if fill { Rint::na() } else { Rint::from(val) }
            })
            .collect();
        return Ok(Integers::from_values(ints).into());
    }
    if is_int && s == 8 {
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
        let doubles: Vec<Rfloat> = b
            .chunks_exact(8)
            .take(n)
            .map(|chunk| {
                let bits = u64::from_le_bytes(chunk.try_into().unwrap());
                let fill = is_declared(chunk)
                    || bits == BIT64_NA_BITS
                    || (gedi && is_signed && GEDI_FILLS.contains(&(bits as i64)));
                Rfloat::from(f64::from_bits(if fill { BIT64_NA_BITS } else { bits }))
            })
            .collect();
        let mut robj: Robj = Doubles::from_values(doubles).into();
        robj.set_attrib(class_symbol(), "integer64").unwrap();
        return Ok(robj);
    }

    Err(format!("its datatype is not supported ({desc})"))
}

/// Convert a `HashMap<String, ColumnData>` into two parallel R lists:
/// one of raw byte vectors, one of JSON info strings. Used for pool
/// columns which the R side still needs to slice per-shot.
fn columns_to_raw_lists(
    cols: std::collections::HashMap<String, crate::products::common::ColumnData>,
) -> (List, List) {
    let mut entries: Vec<_> = cols.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let mut names: Vec<String> = Vec::with_capacity(entries.len());
    let mut bytes: Vec<Raw> = Vec::with_capacity(entries.len());
    let mut info_strs: Vec<String> = Vec::with_capacity(entries.len());

    for (name, cdata) in entries {
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
    token: Nullable<&str>,
    pool_columns: Nullable<Vec<String>>,
    transposed_columns: Nullable<Vec<String>>,
) -> extendr_api::Result<List> {
    let _sigpipe = SigpipeGuard::new();
    let rt = runtime();
    let source = make_source(url, token);

    let product_type = parse_gedi_product(product)?;

    let bbox = crate::BBox::new(xmin, ymin, xmax, ymax);
    let cols = match columns { Nullable::NotNull(c) => Some(c), Nullable::Null => None };
    let bms = match beams { Nullable::NotNull(b) => Some(b), Nullable::Null => None };
    let pool = match pool_columns { Nullable::NotNull(p) => Some(p), Nullable::Null => None };
    let trans = match transposed_columns { Nullable::NotNull(t) => Some(t), Nullable::Null => None };

    crate::io::reader::reset_request_counter();
    let result = rt.block_on(async {
        let file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
        gedi::read_gedi(&file, product_type, bbox, cols, bms, pool, trans)
            .await
            .map_err(|e| e.to_string())
    });

    let family = fill_family(product);
    match result {
        Ok(groups) => {
            let lists: Vec<List> = groups.into_iter().map(|gd| group_data_to_list(gd, family)).collect();
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
    token: Nullable<&str>,
    pool_columns: Nullable<Vec<String>>,
    transposed_columns: Nullable<Vec<String>>,
) -> extendr_api::Result<List> {
    let _sigpipe = SigpipeGuard::new();
    let rt = runtime();
    let source = make_source(url, token);

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

    crate::io::reader::reset_request_counter();
    let result = rt.block_on(async {
        let file = Hdf5File::open(source).await.map_err(|e| e.to_string())?;
        icesat2::read_icesat2(&file, product_type, bbox, cols, trks, pool, trans)
            .await
            .map_err(|e| e.to_string())
    });

    let family = fill_family(product);
    match result {
        Ok(groups) => {
            let lists: Vec<List> = groups.into_iter().map(|gd| group_data_to_list(gd, family)).collect();
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
    token: Nullable<&str>,
) -> extendr_api::Result<Vec<String>> {
    let _sigpipe = SigpipeGuard::new();
    let rt = runtime();
    let source = make_source(url, token);

    crate::io::reader::reset_request_counter();
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
    token: Nullable<&str>,
) -> extendr_api::Result<List> {
    let _sigpipe = SigpipeGuard::new();
    let rt = runtime();
    let source = make_source(url, token);

    crate::io::reader::reset_request_counter();
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
    token: Nullable<&str>,
    pool_columns: Nullable<Vec<String>>,
    transposed_columns: Nullable<Vec<String>>,
) -> extendr_api::Result<List> {
    let _sigpipe = SigpipeGuard::new();
    let rt = runtime();

    let product_type = parse_gedi_product(product)?;

    let bbox = crate::BBox::new(xmin, ymin, xmax, ymax);
    let cols = match columns { Nullable::NotNull(c) => Some(c), Nullable::Null => None };
    let bms = match beams { Nullable::NotNull(b) => Some(b), Nullable::Null => None };
    let pool = match pool_columns { Nullable::NotNull(p) => Some(p), Nullable::Null => None };
    let trans = match transposed_columns { Nullable::NotNull(t) => Some(t), Nullable::Null => None };

    // Extract the bearer token so it can be shared across closures.
    let token = match token {
        Nullable::NotNull(t) if !t.is_empty() => Some(t.to_string()),
        _ => None,
    };

    crate::io::reader::reset_request_counter();
    let t_start = std::time::Instant::now();

    // Process granules with bounded concurrency to avoid overwhelming the
    // server; each granule internally runs its beams and columns
    // concurrently. Granules that fail are retried (read_granules).
    let (all_groups, failed) = rt.block_on(read_granules(&urls, |url| {
        // Same routing as the single-granule readers: local paths and
        // file:// URLs read from disk, http(s) URLs over the network.
        let source = match &token {
            Some(t) => make_source(&url, Nullable::NotNull(t.as_str())),
            None => make_source(&url, Nullable::Null),
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
    }));

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

    let family = fill_family(product);
    Ok(groups_with_failures(all_groups, failed, family))
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
    token: Nullable<&str>,
    pool_columns: Nullable<Vec<String>>,
    transposed_columns: Nullable<Vec<String>>,
) -> extendr_api::Result<List> {
    let _sigpipe = SigpipeGuard::new();
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

    // Extract the bearer token so it can be shared across closures.
    let token = match token {
        Nullable::NotNull(t) if !t.is_empty() => Some(t.to_string()),
        _ => None,
    };

    crate::io::reader::reset_request_counter();
    let (all_groups, failed) = rt.block_on(read_granules(&urls, |url| {
        // Same routing as the single-granule readers: local paths and
        // file:// URLs read from disk, http(s) URLs over the network.
        let source = match &token {
            Some(t) => make_source(&url, Nullable::NotNull(t.as_str())),
            None => make_source(&url, Nullable::Null),
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
    }));

    let family = fill_family(product);
    Ok(groups_with_failures(all_groups, failed, family))
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
