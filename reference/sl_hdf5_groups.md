# List groups in a remote HDF5 file

Low-level helper for exploring the internal structure of a remote HDF5
file. Useful for discovering available datasets, beams, or subgroups
when working outside the curated
[`sl_columns()`](https://belian-earth.github.io/spacelaser/reference/sl_columns.md)
registry. Earthdata credentials are required (see
[`sl_search()`](https://belian-earth.github.io/spacelaser/reference/sl_search.md)
for setup).

## Usage

``` r
sl_hdf5_groups(url, path = "/")
```

## Arguments

- url:

  Character. HTTPS URL of the HDF5 file.

- path:

  Character. HDF5 group path to list (default: `"/"`).

## Value

A character vector of group/dataset names.

## Examples

``` r
if (FALSE) { # interactive()
url <- paste0(
  "https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected/",
  "GEDI02_A.002/GEDI02_A_2020009130403_O06095_03_T02944_02_003_01_V002/",
  "GEDI02_A_2020009130403_O06095_03_T02944_02_003_01_V002.h5"
)
sl_hdf5_groups(url)
sl_hdf5_groups(url, path = "BEAM0000/geolocation")
}
```
