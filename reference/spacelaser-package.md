# spacelaser: Fast Remote Reads of GEDI and ICESat-2 Lidar Data

R bindings for a pure-Rust HDF5 parser (via extendr) that fetches
spatial subsets of NASA GEDI and ICESat-2 lidar granules over HTTP range
requests, avoiding multi-gigabyte downloads. Returns tibbles with `wk`
geometry columns, ready for analysis.

Supported products: GEDI L1B / L2A / L2B / L4A / L4C and ICESat-2 ATL03
/ ATL06 / ATL07 / ATL08 / ATL10 / ATL13 / ATL24.

## Search

- [`sl_search()`](https://belian-earth.github.io/spacelaser/reference/sl_search.md)
  — find granules via NASA CMR

- [`sl_bbox()`](https://belian-earth.github.io/spacelaser/reference/sl_bbox.md)
  — construct a bounding box for spatial queries

## Read

- [`sl_read()`](https://belian-earth.github.io/spacelaser/reference/sl_read.md)
  — pull spatial subsets from remote HDF5 granules (dispatches on
  [`sl_gedi_search`](https://belian-earth.github.io/spacelaser/reference/sl_search.md)
  /
  [`sl_icesat2_search`](https://belian-earth.github.io/spacelaser/reference/sl_search.md)
  objects or a character vector of URLs)

## Columns

- [`sl_columns()`](https://belian-earth.github.io/spacelaser/reference/sl_columns.md)
  — inspect the curated column registry for a given product; returns
  available column names, HDF5 paths, types, and whether each is in the
  default read set

## Waveforms

- [`sl_extract_waveforms()`](https://belian-earth.github.io/spacelaser/reference/sl_extract_waveforms.md)
  — expand GEDI L1B rxwaveform list columns to long form with
  interpolated elevation per sample, for direct plotting or downstream
  analysis

## Low-level HDF5

- [`sl_hdf5_groups()`](https://belian-earth.github.io/spacelaser/reference/sl_hdf5_groups.md)
  — list groups and datasets inside a remote HDF5 file

- [`sl_hdf5_read()`](https://belian-earth.github.io/spacelaser/reference/sl_hdf5_read.md)
  — read a single dataset by full HDF5 path, returning a typed R vector

## Authentication

NASA Earthdata credentials are required for any read that hits a DAAC
endpoint. spacelaser resolves them from, in order,
`EARTHDATA_USERNAME` + `EARTHDATA_PASSWORD` environment variables or a
`.netrc` file (via `GDAL_HTTP_NETRC_FILE` or `~/.netrc`). Register at
<https://urs.earthdata.nasa.gov/> and set up with
[`earthdatalogin::edl_netrc()`](https://boettiger-lab.github.io/earthdatalogin/reference/edl_netrc.html)
if preferred.

## See also

Useful links:

- <https://github.com/belian-earth/spacelaser>

- <https://belian-earth.github.io/spacelaser/>

- Report bugs at <https://github.com/belian-earth/spacelaser/issues>

## Author

**Maintainer**: Hugh Graham <hugh@belian.earth>

Other contributors:

- belian.earth \[copyright holder\]
