# spacelaser

Cloud-optimized partial reading of GEDI and ICESat-2 HDF5 data from R.

Read satellite lidar data directly from NASA Earthdata cloud storage using
HTTP range requests.  Only fetches the bytes needed for the requested spatial
subset, avoiding multi-gigabyte file downloads.

## Installation

Requires [Rust](https://www.rust-lang.org/tools/install) (cargo + rustc):

```r
# install.packages("pak")
pak::pak("belian-earth/spacelaser")
```

## Quick start

```r
library(spacelaser)

bbox <- sl_bbox(-55.5, -12.5, -55.0, -12.0)

# Search, then read. The sensor is determined from the product string.
granules <- sl_search(bbox, product = "L2A")
gedi <- sl_read(granules)

# ICESat-2 works the same way; same two verbs, no sensor in the call.
granules <- sl_search(bbox, product = "ATL08")
icesat <- sl_read(granules)

# Or pass URLs directly. Sensor and product are auto-detected from the
# filename, and bbox is required because there's no search result to defer to.
gedi <- sl_read(
  "https://e4ftl01.cr.usgs.gov/.../GEDI02_A_2024100.h5",
  bbox = bbox
)
```

### Search and read workflow

`sl_search()` returns a classed data frame (`sl_gedi_search` or
`sl_icesat2_search`) that carries the product **and** the bbox the search
was performed with. `sl_read()` is an S3 generic that dispatches on these
classes, so you never need to re-specify either:

```r
granules <- sl_search(bbox, product = "L2A", date_start = "2024-01-01")
granules
#> <sl_gedi_search> | GEDI L2A | 12 granules | (-55.5000, -12.5000) - (-55.0000, -12.0000)
#>   id         time_start          url                          geometry
#>   G1234...   2024-01-03 12:00:00 https://e4ftl01.cr.usgs.gov/...
#>   ...

# Reads all granules in parallel, combining into one data frame.
# Uses the search bbox automatically.
data <- sl_read(granules)
```

You can pass an explicit `bbox` to `sl_read()` to subset further within the
search area, but it must be fully contained within the search bbox.
Supplying a wider bbox is an error: this is by design, to prevent silently
missing data outside the original search.

```r
# OK: tighter bbox inside the search area
data <- sl_read(granules, bbox = sl_bbox(-55.3, -12.4, -55.1, -12.1))

# Error: wider bbox would silently miss granules
data <- sl_read(granules, bbox = sl_bbox(-56, -13, -54, -11))
#> Error in `sl_read()`:
#> ! `bbox` extends outside the search bbox.
#> i Re-run `sl_search()` with a wider bbox to avoid silently missing data.
```

### Filtering after reading

All beams (GEDI) and ground tracks (ICESat-2) are read by default. The
returned data frame includes a `beam` (GEDI) or `track` (ICESat-2)
identifier column, so you can filter post-hoc using your preferred tools:

```r
library(dplyr)

# GEDI: keep only the 4 power beams (better canopy penetration)
data |> filter(beam %in% c("BEAM0101","BEAM0110","BEAM1000","BEAM1011"))

# Combine with quality filtering in one step
data |> filter(quality_flag == 1, degrade_flag == 0)
```

This is a deliberate API choice. The cost of reading 4 beams versus 8 is
small (HTTP latency dominates over data volume), and post-hoc filtering
keeps the spatial-subset reader uncoupled from the user's analytical
filtering. See the *Power beams and ground tracks* vignette for details.

### Discovering columns

```r
sl_columns("L2A")    # GEDI L2A: 44 columns
sl_columns("ATL08")  # ICESat-2 ATL08: 9 columns
```

`sl_read()` accepts the short names from these registries via its
`columns` argument:

```r
sl_read(granules, columns = c("rh", "quality_flag", "solar_elevation"))
```

If `columns` is omitted, the full registry for the product is read.

## Architecture

spacelaser is an R package backed by a pure-Rust HDF5 partial reader
(no libhdf5 dependency).  The Rust core is compiled via
[rextendr](https://extendr.github.io/rextendr/) and linked as a static
library.

### Design rationale

GEDI granules are ~2 GB and ICESat-2 ATL03 files reach 7 GB, but a typical
spatial query needs only a few thousand rows out of millions.  Downloading
entire files is wasteful by orders of magnitude.

The standard HDF5 C library (libhdf5) was designed for local I/O and makes
hundreds of tiny reads to navigate B-trees and object headers.  Each becomes
a separate HTTP round trip, making remote reads painfully slow (NSIDC measured
19 minutes for a single ATL03 read via S3).

Instead, spacelaser implements an **h5coro-style** reader: a purpose-built
partial HDF5 parser that uses block-aligned caching and request coalescing to
minimize HTTP round trips.  This approach was pioneered by the
[SlideRule](https://slideruleearth.io/) project and achieves 77-132x speedup
over libhdf5 for remote reads.

### How remote partial reading works

```
  User (R)                           spacelaser (Rust)                          NASA Earthdata (HTTPS)
  ========                           =================                          ======================

  sl_read(granules)
       |
       |  validate bbox, resolve
       |  Earthdata creds (.netrc)
       |
       +----> rust_read_gedi() ----+
              (via extendr FFI)    |
                                   v
                         +--------------------+
                         | 1. OPEN FILE       |   --- GET Range: bytes=0-255 ------->  superblock
                         |    Parse superblock|   <-- 206 Partial Content -----------
                         |    (file offset    |
                         |     sizes, root    |
                         |     group address) |
                         +--------+-----------+
                                  |
                                  v
                         +--------------------+
                         | 2. NAVIGATE TO     |   --- GET Range: bytes=X-Y ---------->  object headers,
                         |    BEAM GROUP      |   <-- 206 Partial Content -----------   B-tree nodes,
                         |                    |                                        local heap
                         |  Walk: root group  |   (Block cache: 256 KiB aligned
                         |  -> object header  |    reads absorb nearby metadata,
                         |  -> B-tree / SNOD  |    reducing round trips to ~3-5)
                         |  -> BEAM0101       |
                         +--------+-----------+
                                  |
                                  v
                         +--------------------+
                         | 3. READ LAT/LON    |   --- GET Range: bytes=A-B ---------->  lat_lowestmode
                         |                    |   --- GET Range: bytes=C-D ---------->  lon_lowestmode
                         |  Read full lat/lon |   <-- 206 (chunked, decompressed) ---
                         |  arrays for beam   |
                         |  (~1-2 MB each,    |   Decompress: deflate -> unshuffle
                         |   1-3 range reqs)  |
                         +--------+-----------+
                                  |
                                  v
                         +--------------------+
                         | 4. SPATIAL FILTER  |   (no network -- pure CPU)
                         |                    |
                         |  For each footprint|   lat/lon arrays are small enough
                         |  check if (lon,lat)|   to scan linearly (~250K elements).
                         |  falls within bbox |   No spatial index needed.
                         |                    |
                         |  Result: row index |   Consecutive indices are merged
                         |  list, e.g.        |   into contiguous ranges:
                         |  [100..150, 300..  |     [100,101,...,150] -> (100,151)
                         |   320]             |
                         +--------+-----------+
                                  |
                                  v
                         +--------------------+
                         | 5. READ COLUMNS    |   For each requested column:
                         |    (selected rows  |
                         |     only)          |   a) Navigate to dataset's B-tree
                         |                    |      (chunk index)
                         |  For chunked data: |
                         |  - Find which      |   b) Identify which chunks overlap
                         |    chunks overlap  |      the selected row ranges
                         |    our row ranges  |
                         |  - Fetch + decomp  |   --- GET Range: bytes=E-F --------->  chunk data
                         |    only those      |   <-- 206 Partial Content -----------
                         |    chunks          |
                         |  - Extract the     |   c) Decompress (shuffle + deflate)
                         |    exact rows      |      and extract matching rows
                         |                    |
                         |  For contiguous:   |   Direct byte-range read of just
                         |  - Compute byte    |   the needed row slice
                         |    offset & length |
                         |  - Single range    |
                         |    request         |
                         +--------+-----------+
                                  |
                                  v
                         +--------------------+
                         | 6. RETURN TO R     |
                         |                    |   Each column: raw bytes + JSON
                         |  Repeat steps 2-5  |   metadata (dtype, element count).
                         |  for each beam     |
                         |  (8 beams total,   |   R side (build_tibble):
                         |   skip empty ones) |   - readBin() to typed vector
                         |                    |   - wk::xy() for geometry
                         +--------+-----------+   - vctrs::vec_rbind() to combine
                                  |
                                  v
       <---- data.frame ----------+
       (n footprints x m columns,
        geometry column,
        beam identifier)
```

### Layer diagram

```
+-------------------------------------------------------+
|                    R Package Layer                     |
|                                                       |
|  sl_search()             Search (CMR API)              |
|       |  returns sl_gedi_search / sl_icesat2_search    |
|       |  carrying product + bbox attributes            |
|       v                                               |
|  sl_read()               S3 generic dispatch           |
|       |  methods: sl_gedi_search, sl_icesat2_search,   |
|       |           character (URL auto-detect)           |
|       v                                               |
|  read_gedi() / read_icesat2()    Per-sensor internals  |
|       |                  (lat/lon paths, group label)  |
|       v                                               |
|  read_product() / read_product_multi()                 |
|       |                  Shared workflow:              |
|       |                  validate, call Rust,          |
|       |                  build tibble, combine         |
|       |                                               |
|  sl_columns()            Column registry per product   |
|  sl_bbox()               Bounding box constructor      |
|  sl_hdf5_groups()        Low-level HDF5 exploration    |
|  sl_hdf5_read()                                        |
+-------------------+-----------------------------------+
                    |  extendr FFI (.Call)
+-------------------+-----------------------------------+
|                    Rust Core Library                   |
|                                                       |
|  products/                                            |
|    common.rs        SatelliteProduct trait, BBox,      |
|                     GroupData, spatial filter,          |
|                     generic read_product_groups()       |
|    gedi.rs          GEDI product metadata + defaults   |
|    icesat2.rs       ICESat-2 product metadata          |
|                                                       |
|  hdf5/                                                |
|    file.rs          Hdf5File: open, navigate, read     |
|    superblock.rs    Superblock v0/v1/v2 parser         |
|    object_header.rs Object header + message parsers    |
|    btree.rs         B-tree v1 (groups + chunks)        |
|    heap.rs          Local heap + fractal heap          |
|    dataset.rs       Dataset: read_all / read_rows      |
|    chunk.rs         Chunk selection + extraction        |
|    types.rs         Datatype, DataLayout, Filter, etc  |
|                                                       |
|  io/                                                  |
|    source.rs        DataSource (HTTP / local)          |
|    reader.rs        Block-cached Range request reader  |
|    cache.rs         LRU block cache + coalescing       |
|                                                       |
|  filters/                                             |
|    mod.rs           Deflate, shuffle, Fletcher32       |
|                                                       |
|  ffi.rs             R <-> Rust bridge (extendr)       |
+-------------------------------------------------------+
```

### Key design decisions

| Decision | Rationale |
|----------|-----------|
| **Pure Rust, no libhdf5** | libhdf5's internal navigation pattern issues hundreds of tiny reads, each becoming an HTTP round trip. A purpose-built parser controls I/O granularity. Also eliminates the C build dependency. |
| **Block-aligned LRU cache** | HDF5 metadata is scattered but spatially clustered. 256 KiB block reads with LRU caching absorb nearby metadata lookups, reducing B-tree traversal from ~20 requests to ~3-5. |
| **Request coalescing** | Adjacent missing cache blocks are merged into single HTTP Range requests, minimizing round trips further. |
| **Full lat/lon scan** | GEDI lat/lon arrays are ~1-2 MB per beam, small relative to the file. Scanning linearly is simpler and more robust than maintaining a spatial index. |
| **Raw bytes + JSON metadata (not Arrow)** | The extendr <-> arrow interop is immature. Raw bytes + `readBin()` is fast, dependency-free, and works reliably across platforms. |
| **`SatelliteProduct` trait** | GEDI and ICESat-2 share the same read algorithm (iterate groups, filter spatially, read columns). The trait captures per-product metadata (group names, lat/lon paths, default columns) without duplicating the core logic. |
| **Product-specific lat/lon paths** | GEDI products inconsistently store coordinates (root vs `geolocation/` subgroup, different variable names by product). The trait method captures this per-product knowledge. |
| **Async I/O via tokio** | HTTP requests benefit from async concurrency. The single-threaded tokio runtime integrates cleanly with R's single-threaded model via `block_on`. |

## Supported HDF5 features

The reader implements the subset of the HDF5 specification used by
GEDI and ICESat-2 files:

- Superblock versions 0, 1, 2, 3
- B-tree v1 (group nodes and raw data chunk nodes)
- Object header versions 1 and 2 (with continuation blocks)
- Symbol table nodes (SNOD) and local heaps
- Fractal heaps (for v2 group link messages)
- Data layouts: compact, contiguous, chunked
- Datatypes: fixed-point (signed/unsigned), floating-point, string
- Filter pipeline: deflate (zlib), shuffle, Fletcher32 checksum

## Authentication

spacelaser uses NASA Earthdata Basic auth with cookie-based session
following (not bearer tokens). Credentials are resolved in this order:

1. `EARTHDATA_USERNAME` + `EARTHDATA_PASSWORD` environment variables.
2. A `.netrc` file containing an entry for `urs.earthdata.nasa.gov`.
   Both `~/.netrc` and the path in `GDAL_HTTP_NETRC_FILE` are checked, so
   credentials set up via [earthdatalogin](https://github.com/boettiger-lab/earthdatalogin)
   work out of the box.

The simplest setup, if you already have an Earthdata account:

```r
# Once per machine: writes ~/.netrc and sets GDAL_HTTP_NETRC_FILE
earthdatalogin::edl_netrc()
```

Credentials are cached in the package namespace for the R session after
first successful resolution. No bearer token, no token endpoint, no
interactive prompt.

Register at <https://urs.earthdata.nasa.gov/> if you don't have an account.

## API reference

### Search

- `sl_search(bbox, product, date_start = NULL, date_end = NULL)`: search
  CMR for GEDI or ICESat-2 granules. Returns an `sl_gedi_search` or
  `sl_icesat2_search` (chosen automatically from `product`) carrying the
  product and bbox as attributes.

### Read

- `sl_read(x, bbox = NULL, columns = NULL, ...)`: S3 generic that reads
  satellite lidar data. Dispatches on:
  - `sl_gedi_search` / `sl_icesat2_search`: reads all granules; bbox
    defaults to the search bbox.
  - `character`: a vector of HDF5 URLs; auto-detects sensor and product
    from the filename. `bbox` is required.

### Utilities

- `sl_bbox(xmin, ymin, xmax, ymax)`: create a bounding box (WGS84).
- `sl_columns(product)`: list the column registry for a product.
- `sl_hdf5_groups(url, path)`: list groups in a remote HDF5 file.
- `sl_hdf5_read(url, dataset)`: read a single dataset (low-level).

Valid `product` values: `"L1B"`, `"L2A"`, `"L2B"`, `"L4A"` (GEDI);
`"ATL03"`, `"ATL06"`, `"ATL08"` (ICESat-2).

## License

MIT
