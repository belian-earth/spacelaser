# Package index

## Search

Find GEDI and ICESat-2 granules via NASA CMR.

- [`sl_search()`](https://belian-earth.github.io/spacelaser/reference/sl_search.md)
  : Search NASA CMR for GEDI or ICESat-2 granules
- [`sl_bbox()`](https://belian-earth.github.io/spacelaser/reference/sl_bbox.md)
  : Create a bounding box for spatial queries

## Read

Pull spatial subsets from remote HDF5 granules.

- [`sl_read()`](https://belian-earth.github.io/spacelaser/reference/sl_read.md)
  : Read satellite lidar data

## Columns

Inspect the curated column registry for each product.

- [`sl_columns()`](https://belian-earth.github.io/spacelaser/reference/sl_columns.md)
  : List available columns for a GEDI or ICESat-2 product

## Waveforms

Post-processing helpers for GEDI L1B waveform data.

- [`sl_extract_waveforms()`](https://belian-earth.github.io/spacelaser/reference/sl_extract_waveforms.md)
  : Extract L1B waveforms to long form with elevation profile

## Low-level HDF5

Explore arbitrary datasets in a remote HDF5 file.

- [`sl_hdf5_groups()`](https://belian-earth.github.io/spacelaser/reference/sl_hdf5_groups.md)
  : List groups in a remote HDF5 file
- [`sl_hdf5_read()`](https://belian-earth.github.io/spacelaser/reference/sl_hdf5_read.md)
  : Read a single dataset from a remote HDF5 file
