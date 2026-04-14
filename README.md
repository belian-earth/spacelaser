
<!-- README.md is generated from README.Rmd. Please edit that file -->

# spacelaser

<!-- badges: start -->

[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
<!-- badges: end -->

Cloud-optimized partial reading of GEDI and ICESat-2 HDF5 data from R.
Only the bytes needed for the requested spatial subset are fetched over
HTTP, avoiding multi-gigabyte downloads.

Supports GEDI L1B, L2A, L2B, L4A, L4C and ICESat-2 ATL03, ATL06, ATL07,
ATL08, ATL10, ATL13, ATL24.

## Installation

Requires a [Rust toolchain](https://www.rust-lang.org/tools/install)
(cargo + rustc).

``` r
# install.packages("pak")
pak::pak("belian-earth/spacelaser")
```

## Authentication

NASA Earthdata credentials are resolved from `EARTHDATA_USERNAME` /
`EARTHDATA_PASSWORD` or a `~/.netrc` entry for `urs.earthdata.nasa.gov`.
Register at <https://urs.earthdata.nasa.gov/> if you don’t have an
account. The simplest setup:

``` r
earthdatalogin::edl_netrc()
```

## Example

``` r
library(spacelaser)

bbox <- sl_bbox(-124.04, 41.39, -124.01, 41.42)

# GEDI
granules <- sl_search(
  bbox, 
  product = "L2A", 
  date_start = "2022-01-01", 
  date_end = "2023-01-01")
#> ℹ Searching CMR for GEDI L2A granules
#> ✔ Searching CMR for GEDI L2A granules [9.5s]
#> 
#> ✔ Found 9 GEDI L2A granule.
gedi2a <- sl_read(granules)
#> ℹ Reading L2A from 9 granules
#> ✔ Read 579 footprint from 17 beam.✔ Reading L2A from 9 granules [1m 13.5s]

gedi2a
#> # A tibble: 579 × 121
#>     rh98  rh61  rh64  rh26  rh25  rh55 selected_algorithm quality_flag  rh47
#>    <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>              <int>        <int> <dbl>
#>  1  84.8  71.9  72.6 39.5  30.6   70.5                  2            1  67.3
#>  2  73.8  57.3  58.9  6.58  6.36  53.7                  2            1  45.9
#>  3  76.0  45.7  48.1  6.06  5.68  39.6                  2            1  24.8
#>  4  20.2  13.0  13.5  5.76  5.53  11.7                  2            1  10.2
#>  5  80.1  63.1  64.4 10.6   9.95  58.0                  2            1  49.6
#>  6  62.0  51.8  52.3 23.5  22.4   50.7                  2            1  48.5
#>  7  20.6  12.9  13.3  7.85  7.70  12.2                  2            1  11.1
#>  8  30.6  20.4  21.2  7.82  7.10  18.9                  2            1  16.7
#>  9  71.1  59.2  59.9 51.2  51.0   58.1                  2            1  56.2
#> 10  63.8  53.8  54.5 39.6  38.8   52.3                  1            1  49.8
#> # ℹ 569 more rows
#> # ℹ 112 more variables: rh3 <dbl>, rh45 <dbl>, rh99 <dbl>, rh91 <dbl>,
#> #   rh9 <dbl>, rh65 <dbl>, pft_class <int>, rh71 <dbl>, rh17 <dbl>, rh20 <dbl>,
#> #   rh75 <dbl>, rh21 <dbl>, rh87 <dbl>, rh22 <dbl>, rh68 <dbl>, rh48 <dbl>,
#> #   rh82 <dbl>, modis_treecover <dbl>, rh1 <dbl>, energy_total <dbl>,
#> #   rh85 <dbl>, rh57 <dbl>, rh46 <dbl>, rh62 <dbl>, rh6 <dbl>, rh41 <dbl>,
#> #   rh80 <dbl>, beam <chr>, solar_elevation <dbl>, region_class <int>, …

# # ICESat-2 uses the same two verbs
# granules <- sl_search(bbox, product = "ATL08")
# icesat <- sl_read(granules)
```

Columns available for a product:

``` r
sl_columns("L2A")
#>                              lat_lowestmode 
#>                            "lat_lowestmode" 
#>                              lon_lowestmode 
#>                            "lon_lowestmode" 
#>                                        beam 
#>                                      "beam" 
#>                                 shot_number 
#>                               "shot_number" 
#>                                     channel 
#>                                   "channel" 
#>                                  delta_time 
#>                                "delta_time" 
#>                                 master_frac 
#>                               "master_frac" 
#>                                  master_int 
#>                                "master_int" 
#>                                degrade_flag 
#>                              "degrade_flag" 
#>                                quality_flag 
#>                              "quality_flag" 
#>                                 sensitivity 
#>                               "sensitivity" 
#>                               solar_azimuth 
#>                             "solar_azimuth" 
#>                             solar_elevation 
#>                           "solar_elevation" 
#>                                surface_flag 
#>                              "surface_flag" 
#>                         elevation_bias_flag 
#>                       "elevation_bias_flag" 
#>                             elev_lowestmode 
#>                           "elev_lowestmode" 
#>                          elev_highestreturn 
#>                        "elev_highestreturn" 
#>                                energy_total 
#>                              "energy_total" 
#>                           num_detectedmodes 
#>                         "num_detectedmodes" 
#>                                          rh 
#>                                        "rh" 
#>                          selected_algorithm 
#>                        "selected_algorithm" 
#>                               selected_mode 
#>                             "selected_mode" 
#>                          selected_mode_flag 
#>                        "selected_mode_flag" 
#>                     digital_elevation_model 
#>                   "digital_elevation_model" 
#>                digital_elevation_model_srtm 
#>              "digital_elevation_model_srtm" 
#>                            mean_sea_surface 
#>                          "mean_sea_surface" 
#>                        elevation_bin0_error 
#>                      "elevation_bin0_error" 
#>                           lat_highestreturn 
#>                         "lat_highestreturn" 
#>                         latitude_bin0_error 
#>                       "latitude_bin0_error" 
#>                           lon_highestreturn 
#>                         "lon_highestreturn" 
#>                        longitude_bin0_error 
#>                      "longitude_bin0_error" 
#>                           landsat_treecover 
#>         "land_cover_data/landsat_treecover" 
#>                   landsat_water_persistence 
#> "land_cover_data/landsat_water_persistence" 
#>                                leaf_off_doy 
#>              "land_cover_data/leaf_off_doy" 
#>                               leaf_off_flag 
#>             "land_cover_data/leaf_off_flag" 
#>                               leaf_on_cycle 
#>             "land_cover_data/leaf_on_cycle" 
#>                                 leaf_on_doy 
#>               "land_cover_data/leaf_on_doy" 
#>                          modis_nonvegetated 
#>        "land_cover_data/modis_nonvegetated" 
#>                       modis_nonvegetated_sd 
#>     "land_cover_data/modis_nonvegetated_sd" 
#>                             modis_treecover 
#>           "land_cover_data/modis_treecover" 
#>                          modis_treecover_sd 
#>        "land_cover_data/modis_treecover_sd" 
#>                                   pft_class 
#>                 "land_cover_data/pft_class" 
#>                                region_class 
#>              "land_cover_data/region_class" 
#>                     urban_focal_window_size 
#>   "land_cover_data/urban_focal_window_size" 
#>                            urban_proportion 
#>          "land_cover_data/urban_proportion"
```
