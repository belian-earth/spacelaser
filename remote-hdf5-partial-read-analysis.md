# Remote Partial Reading of GEDI & ICESat-2 HDF5 Files

## Technical Analysis: Architecture Options for a Next-Generation R Package

---

## 1. The Problem

GEDI granules are ~2 GB each (quarter-orbit, 8 beams x ~156 datasets). ICESat-2
ATL03 files reach 7 GB. A typical use case—extracting data for a small spatial
region—might need a few thousand footprints out of millions. Downloading the
entire file to read a fraction of it is wasteful by orders of magnitude.

**Goal**: Read only the requested data directly from remote HDF5 files, similar
to how Parquet or Cloud-Optimized GeoTIFF enables partial reads.

---

## 2. The Data Landscape

### GEDI Files (L1B/L2A/L2B/L4A)

- HDF5 with 8 beam groups (`BEAM0000`...`BEAM1011`)
- Each beam contains flat arrays where array index = footprint
- Datasets include: lat/lon, elevation, rh metrics, quality flags (~156 per beam)
- V2 granules are ~2 GB (one quarter of an ISS orbit)
- Hosted on NASA Earthdata (LP DAAC), cloud-hosted in AWS us-west-2
- **Not** cloud-optimized

```
/
├── METADATA/
├── ANCILLARY/
├── BEAM0000/
│   ├── lat_lowestmode       (1-D float array, per footprint)
│   ├── lon_lowestmode
│   ├── elev_lowestmode
│   ├── rh                   (2-D: footprint x percentile)
│   ├── quality_flag
│   └── geolocation/
│       └── ...
├── BEAM0001/
│   └── ...
└── ... (8 beams total)
```

### ICESat-2 Files (ATL03/ATL08/etc.)

- HDF5 with ground tracks (gt1l, gt1r, gt2l, gt2r, gt3l, gt3r)
- ATL03: photon-level, up to 7 GB per file, 1003 variables
- ATL08: 100m segment-level terrain and canopy heights
- As of July 2025, ATL03 Release 007 is available in **cloud-optimized HDF5**
- Other products expected to follow

### Network Access Paths

| Method | Where | Performance | Auth |
|--------|-------|-------------|------|
| S3 direct | In AWS us-west-2 only | Best (~60ms first-byte) | Earthdata Login → temp S3 creds |
| HTTPS | Anywhere | Higher latency (~100-200ms/request) | Earthdata Login via netrc |

Both S3 and HTTPS endpoints support byte-range requests, which is the
fundamental enabler for partial reading.

---

## 3. Approaches Evaluated

### Approach 1: HDF5 Virtual File Driver (VFD) via libhdf5

Register a custom file driver through HDF5's Virtual File Layer that implements
`read()` using HTTP Range requests instead of POSIX I/O.

**How it works**: The `hdf5-sys` Rust crate exposes `H5FDregister` and the
`H5FD_class_t` struct. You'd fill in C-compatible callback functions that
delegate to an HTTP or S3 backend.

**Why it fails**: libhdf5 was designed for local, low-latency I/O. Its internal
navigation (superblock → B-trees → object headers → chunk index) makes hundreds
of tiny reads. Each becomes a separate HTTP round trip. NSIDC's evaluation found
that reading ATL03 from S3 via the standard HDF5 library took **19 minutes**.
Even with page buffering and cloud-optimized files, you're fighting the
library's fundamental architecture.

Additional downsides:
- Inherits the libhdf5 C dependency (build/distribution nightmare)
- The Rust `hdf5` crate's maintainer is no longer active
- No safe Rust abstraction for custom VFDs; requires `unsafe extern "C"` code

**Verdict**: Not recommended.

### Approach 2: Kerchunk Sidecar Index

Kerchunk scans an HDF5 file's internal structure and produces a JSON or Parquet
reference file that maps every data chunk to a `(url, byte_offset,
byte_length)` triplet. Reads then use the Zarr library + fsspec to issue
targeted range requests.

**How it works**: `SingleHdf5ToZarr` translates HDF5 metadata into Zarr
metadata. Chunks below a threshold are inlined; larger chunks are referenced by
byte range. The reference file enables any Zarr-compatible reader to access the
original HDF5 data via range requests.

**Pros**:
- Works on unmodified files
- Standard Zarr ecosystem
- Lightweight reference files

**Cons**:
- Requires a separate indexing step that itself must read the file's metadata
  (many small requests — the chicken-and-egg problem)
- Index must be generated and stored somewhere
- Python-centric ecosystem (kerchunk, fsspec, xarray)
- Doesn't handle all HDF5 structures (nested groups, compound types)

**Verdict**: Worth considering as a complement (pre-computed index = instant
metadata), but not viable as the primary strategy. Cannot depend on indices
existing for all files.

### Approach 3: Cloud-Optimized HDF5

Data providers repack files with:
- **Paged aggregation** (`H5F_FSPACE_STRATEGY_PAGE`): consolidates metadata into
  fixed-size pages at the front of the file
- **Larger chunk sizes**: 100K elements instead of 10K (ATL03)
- **Defragmentation**: `h5repack` to eliminate gaps

This achieves Zarr-comparable performance with standard tools. The metadata
pages can be read in 2-3 large requests; data chunks are sized for efficient
HTTP fetches (1-10 MB each).

**Status**:
- ICESat-2 ATL03 R007: cloud-optimized (July 2025)
- GEDI products: **not** cloud-optimized
- More ICESat-2 products expected late 2025

**Verdict**: Excellent when available. The package should detect and exploit
cloud-optimized files when present, but must also handle non-optimized files.

### Approach 4: h5coro-Style Direct HDF5 Reader

h5coro (SlideRule project, NASA/GSFC + University of Washington) is a partial
HDF5 implementation purpose-built for cloud reading. It originated as C++ in the
SlideRule server, then was re-implemented in Python.

**Key design principles**:
- **Concurrent reads**: each dataset read in its own thread
- **Intelligent range gets**: batch adjacent chunks into single requests
- **Block cache**: large reads (e.g., 256 KB blocks) cached locally via LRU,
  amortizing S3's ~60ms first-byte latency
- **Metadata caching**: file structure cached as navigated, reused across
  dataset reads within the same file
- **No sidecar files needed**: self-contained, serverless

**Performance**: 77–132x faster than libhdf5 for S3 reads of unmodified files.
Scales near-linearly with parallel requests.

**Supported HDF5 features** (sufficient for GEDI/ICESat-2):
- Superblock v0 and v2
- B-tree v1
- Fractal heaps
- Chunked, contiguous, and compact storage
- Fixed-point, floating-point, string data types
- Deflate + shuffle filters

**Verdict**: This is the approach. Proven on exactly the data we care about,
delivers transformative performance, avoids the libhdf5 dependency entirely.

---

## 4. Recommended Architecture

### Core Strategy: h5coro-style partial HDF5 reader implemented in Rust

Reimplement the h5coro reading approach in Rust rather than Python or C++ for:
- **No libhdf5 dependency**: pure Rust, straightforward cross-platform builds
- **Async concurrent I/O**: tokio + reqwest for HTTP, aws-sdk-s3 for in-region
- **Memory safety**: Rust's guarantees vs. C pointer gymnastics
- **Performance**: competitive with C++, far better than Python h5coro
- **Arrow interop**: build RecordBatches in Rust, zero-copy transfer to R

### Architecture Layers

```
┌──────────────────────────────────────┐
│         R Package (spacelaser)        │
│  - Earthdata CMR discovery/search    │
│  - Authentication (earthdatalogin)   │
│  - User-facing API: spatial query,   │
│    product selection, column select  │
│  - Returns arrow / sf / data.frame   │
└──────────────┬───────────────────────┘
               │ rextendr FFI
┌──────────────┴───────────────────────┐
│         Rust Core Library             │
│                                       │
│  ┌─────────────────────────────────┐  │
│  │ HDF5 Partial Reader             │  │
│  │ - Superblock parser (v0, v2)    │  │
│  │ - B-tree v1/v2 navigator        │  │
│  │ - Object header decoder         │  │
│  │ - Fractal heap reader           │  │
│  │ - Chunk locator + index         │  │
│  │ - Filter pipeline:              │  │
│  │   deflate, shuffle, (lz4)       │  │
│  │ - Datatype conversion:          │  │
│  │   int, float, string            │  │
│  └─────────────────────────────────┘  │
│  ┌─────────────────────────────────┐  │
│  │ I/O Backend (async, tokio)      │  │
│  │ - HTTPS + Range header driver   │  │
│  │ - S3 byte-range driver          │  │
│  │ - Local file fallback           │  │
│  │ - Block-level LRU cache         │  │
│  │ - Request coalescing/batching   │  │
│  │ - Connection pooling            │  │
│  └─────────────────────────────────┘  │
│  ┌─────────────────────────────────┐  │
│  │ Product-Aware Readers           │  │
│  │ - GEDI L1B/L2A/L2B/L4A layout  │  │
│  │ - ICESat-2 ATL03/08/etc layout  │  │
│  │ - Spatial subset: read lat/lon  │  │
│  │   → row ranges → read columns   │  │
│  │   for matching rows only        │  │
│  └─────────────────────────────────┘  │
│  ┌─────────────────────────────────┐  │
│  │ Arrow Output                    │  │
│  │ - Build RecordBatches in Rust   │  │
│  │ - Zero-copy transfer to R via   │  │
│  │   Arrow C Data Interface        │  │
│  └─────────────────────────────────┘  │
└───────────────────────────────────────┘
```

### The Smart Read Strategy

For a spatial query (bounding box or polygon) against a GEDI file:

1. **Read superblock** (bytes 0–96): 1 range request → root group address
2. **Navigate to beam group**: walk object headers + B-trees. With block
   caching (256 KB–1 MB blocks), this is ~3-5 range requests
3. **Read lat/lon dataset chunk index**: locate via B-tree, identify which
   chunks overlap the spatial query region
4. **Fetch lat/lon chunks**: determine exact row indices within the query area
5. **Read requested columns**: for only those row ranges, fetch the specific
   chunks of each requested dataset variable
6. **Decompress and return**: apply filter pipeline (shuffle → deflate),
   build Arrow arrays, return to R

For a non-cloud-optimized GEDI file (~2 GB), expect roughly 10–30 range
requests for navigation + N requests for data chunks. At ~100-200ms HTTPS
latency per request from outside AWS, that's 2–6 seconds for navigation + data
proportional to result size. Compare to downloading 2 GB (~minutes to hours
depending on connection).

For cloud-optimized ICESat-2 files, metadata is front-loaded in 8 MiB pages.
Navigation drops to 2–3 requests.

### Key Rust Crates

| Crate | Purpose |
|-------|---------|
| `tokio` | Async runtime for concurrent I/O |
| `reqwest` | HTTP client with Range header support |
| `aws-sdk-s3` | S3 byte-range gets for in-region access |
| `flate2` | Deflate decompression |
| `arrow` (arrow-rs) | Arrow RecordBatch construction |
| `lru` | Block cache implementation |
| `extendr-api` | R ↔ Rust FFI (rextendr) |

---

## 5. Risks and Mitigations

| Risk | Severity | Mitigation |
|------|----------|------------|
| HDF5 spec complexity / edge cases | Medium | GEDI and ICESat-2 are produced by known pipelines with consistent settings. Test against real files from each product. Scope to the h5coro-supported subset. |
| HTTPS latency outside us-west-2 | Medium | Aggressive block caching, request batching, connection pooling. Still orders of magnitude better than full download. Document that in-region access is fastest. |
| NASA Earthdata authentication | Low | Lean on earthdatalogin R package for token/cookie management; pass auth headers to Rust HTTP client. |
| Non-cloud-optimized GEDI files | Medium | The h5coro approach was designed for this exact case and achieved 77–132x speedup on non-optimized files. |
| Maintenance burden of partial HDF5 impl | Medium | Scope tightly. Document supported features. These file formats are stable (GEDI/ICESat-2 producers don't change HDF5 settings often). |
| Cross-platform Rust builds for R | Low | rextendr handles this; Rust's cross-compilation is mature. No C library dependencies to manage. |

---

## 6. What to Deprioritize

- **libhdf5 dependency / VFD approach**: Dead end for performance; build
  complexity not worth it.
- **Full HDF5 specification support**: Don't try. Support the subset used by
  GEDI/ICESat-2. Users with exotic HDF5 files have rhdf5.
- **HSDS (HDF5 REST server)**: Requires server infrastructure. NASA doesn't
  serve GEDI/ICESat-2 via HSDS.
- **Format conversion (→ Parquet/GeoParquet)**: Could be an output option but
  not the access strategy — the point is avoiding format conversion.

---

## 7. Optional Future Enhancements

### Kerchunk Index Generation / Caching

The Rust reader could generate kerchunk-compatible reference indices as a
byproduct of first reading a file. Cache these locally so subsequent reads of
the same file skip metadata navigation entirely. This is pure optimization, not
a requirement for v1.

### GeoParquet Export

Once data is in Arrow RecordBatches, writing GeoParquet is trivial. Could be a
convenience function for users who want to cache subsets locally in a modern
format.

### Parallel File Processing

For queries spanning multiple granules, dispatch reads to multiple files
concurrently. The async architecture supports this naturally.

---

## 8. Key References

- h5coro (Python): https://github.com/SlideRuleEarth/h5coro
- h5coro JOSS paper: https://joss.theoj.org/papers/10.21105/joss.04982
- SlideRule documentation: https://slideruleearth.io/web/rtd/developer_guide/articles/h5coro.html
- NSIDC Cloud-Optimized ICESat-2 evaluation: https://nsidc.github.io/cloud-optimized-icesat2/
- NASA Earthdata ICESat-2 CO-HDF5 announcement: https://www.earthdata.nasa.gov/data/alerts-outages/icesat-2-data-now-cloud-optimized-hdf5
- Cloud-Optimized HDF/NetCDF guide: https://guide.cloudnativegeo.org/cloud-optimized-netcdf4-hdf5/
- Kerchunk: https://fsspec.github.io/kerchunk/
- h5cloud project: https://github.com/ICESAT-2HackWeek/h5cloud
- HDF5 Virtual File Layer: https://support.hdfgroup.org/documentation/hdf5/latest/_v_f_l_t_n.html
- hdf5-rust crate: https://github.com/aldanor/hdf5-rust
- earthdatalogin R package: https://boettiger-lab.github.io/earthdatalogin/
- NASA Earthdata Cloud Access Guide: https://nsidc.org/data/user-resources/help-center/nasa-earthdata-cloud-data-access-guide
- chewie R package: https://github.com/Permian-Global-Research/chewie
