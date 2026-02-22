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

# Search for granules, then grab the data
granules <- find_gedi(bbox, product = "L2A")
gedi <- sl_grab(granules, bbox = bbox)

# Or pass URLs directly — product is auto-detected from the filename
gedi <- sl_grab(
  "https://e4ftl01.cr.usgs.gov/.../GEDI02_A_2024100.h5",
  bbox = bbox
)

# ICESat-2 works the same way
granules <- find_icesat2(bbox, product = "ATL08")
icesat <- sl_grab(granules, bbox = bbox)
```

### Search and grab workflow

`find_gedi()` and `find_icesat2()` return S3 classed data frames
(`sl_gedi_search` / `sl_icesat2_search`) that carry the product type.
`sl_grab()` is an S3 generic that dispatches on these classes, so you
never need to re-specify the product:

```r
# find_*() returns a typed search result
granules <- find_gedi(bbox, product = "L2A", date_start = "2024-01-01")
granules
#> <sl_gedi_search> | GEDI L2A | 12 granules
#>   id         time_start          url                          geometry
#>   G1234...   2024-01-03 12:00:00 https://e4ftl01.cr.usgs.gov/...
#>   ...

# sl_grab() reads all granules, combining into one data frame
data <- sl_grab(granules, bbox = bbox)
```

`sl_grab()` also accepts a plain character vector of URLs:

```r
sl_grab("https://.../GEDI02_A_2024100.h5", bbox = bbox)
sl_grab(c(url1, url2, url3), bbox = bbox, product = "ATL08")
```

The underlying single-file readers (`grab_gedi()` / `grab_icesat2()`)
remain exported for direct use when you already have a URL and product.

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

  grab_gedi(url, bbox)
       |
       |  validate bbox, obtain
       |  Earthdata bearer token
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
|  find_gedi() / find_icesat2()    Search (CMR API)      |
|       |  returns sl_*_search S3 class                  |
|       v                                               |
|  sl_grab()               S3 generic dispatch           |
|       |  methods: sl_gedi_search, sl_icesat2_search,   |
|       |           character (URL auto-detect)           |
|       v                                               |
|  grab_gedi() / grab_icesat2()    Single-file readers   |
|       |                                               |
|  grab_product()          Shared internal workflow      |
|       |                  (validate, call Rust,         |
|       |                   build tibble, combine)       |
|       |                                               |
|  sl_earthdata_token()    Auth: env vars / .netrc /    |
|  sl_bbox()               interactive / token API      |
|  sl_hdf5_groups()        Low-level HDF5 exploration   |
|  sl_hdf5_read()                                       |
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
|  auth.rs            Earthdata token API client         |
|  ffi.rs             R <-> Rust bridge (extendr)        |
+-------------------------------------------------------+
```

### Key design decisions

| Decision | Rationale |
|----------|-----------|
| **Pure Rust, no libhdf5** | libhdf5's internal navigation pattern issues hundreds of tiny reads, each becoming an HTTP round trip. A purpose-built parser controls I/O granularity. Also eliminates the C build dependency. |
| **Block-aligned LRU cache** | HDF5 metadata is scattered but spatially clustered. 256 KiB block reads with LRU caching absorb nearby metadata lookups, reducing B-tree traversal from ~20 requests to ~3-5. |
| **Request coalescing** | Adjacent missing cache blocks are merged into single HTTP Range requests, minimizing round trips further. |
| **Full lat/lon scan** | GEDI lat/lon arrays are ~1-2 MB per beam — small relative to the file. Scanning linearly is simpler and more robust than maintaining a spatial index. |
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

spacelaser authenticates with NASA Earthdata in this order:

1. `token` argument (pass-through)
2. `EARTHDATA_TOKEN` environment variable (pre-existing bearer token)
3. `EARTHDATA_USERNAME` + `EARTHDATA_PASSWORD` environment variables
4. `~/.netrc` entry for `urs.earthdata.nasa.gov`
5. Interactive prompt (if running interactively)

For options 3-5, credentials are exchanged for a bearer token via the
Earthdata Login token API.  The token is cached for the R session.

Register at <https://urs.earthdata.nasa.gov/> if you don't have an account.

## API reference

### Search

- `find_gedi(bbox, product, ...)` — Search CMR for GEDI granules → `sl_gedi_search`
- `find_icesat2(bbox, product, ...)` — Search CMR for ICESat-2 granules → `sl_icesat2_search`

### Read

- `sl_grab(x, bbox, ...)` — S3 generic: read data from search results or URL(s)
- `grab_gedi(url, product, bbox)` — Read a single GEDI file (L1B/L2A/L2B/L4A)
- `grab_icesat2(url, product, bbox)` — Read a single ICESat-2 file (ATL03/ATL06/ATL08)

### Utilities

- `sl_bbox(xmin, ymin, xmax, ymax)` — Create a bounding box
- `sl_earthdata_token()` — Obtain/cache a bearer token
- `sl_hdf5_groups(url, path)` — List groups in a remote HDF5 file
- `sl_hdf5_read(url, dataset)` — Read a single dataset (low-level)

## License

MIT
