# Read a single dataset from a remote HDF5 file

Low-level helper that reads an arbitrary HDF5 dataset by its full path
and returns it as an appropriately-typed R vector. Useful for pulling
individual fields not covered by the curated registry, or for inspecting
a dataset before requesting it via
[`sl_read()`](https://belian-earth.github.io/spacelaser/reference/sl_read.md).
Earthdata credentials are required (see
[`sl_search()`](https://belian-earth.github.io/spacelaser/reference/sl_search.md)
for setup).

## Usage

``` r
sl_hdf5_read(url, dataset)
```

## Arguments

- url:

  Character. HTTPS URL of the HDF5 file.

- dataset:

  Character. Full HDF5 path to the dataset (e.g.,
  `"/BEAM0101/lat_lowestmode"`).

## Value

An R vector — numeric, integer, or raw, depending on the underlying HDF5
datatype.
