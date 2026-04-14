
<!-- README.md is generated from README.Rmd. Please edit that file -->

# spacelaser

<!-- badges: start -->

[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![extendr](https://img.shields.io/badge/extendr-*-276DC2)](https://extendr.github.io/extendr/extendr_api/)
[![License:Apache](https://img.shields.io/github/license/belian-earth/a5R)](https://www.apache.org/licenses/LICENSE-2.0)
<!-- badges: end -->

Cloud-optimized partial reading of GEDI and ICESat-2 HDF5 data from R.
Only the bytes needed for the requested spatial subset are fetched over
HTTP, avoiding multi-gigabyte downloads.

## Supported products

| Sensor   | Product | Description                                   |
|:---------|:--------|:----------------------------------------------|
| GEDI     | L1B     | Geolocated received and transmitted waveforms |
| GEDI     | L2A     | Elevation and canopy relative height metrics  |
| GEDI     | L2B     | Canopy cover fraction and vertical profile    |
| GEDI     | L4A     | Footprint-level aboveground biomass density   |
| GEDI     | L4C     | Waveform structural complexity index          |
| ICESat-2 | ATL03   | Geolocated photon heights                     |
| ICESat-2 | ATL06   | Land ice surface elevation                    |
| ICESat-2 | ATL07   | Sea ice surface elevation                     |
| ICESat-2 | ATL08   | Land and vegetation height segments           |
| ICESat-2 | ATL10   | Sea ice freeboard                             |
| ICESat-2 | ATL13   | Inland water surface heights                  |
| ICESat-2 | ATL24   | Near-shore bathymetric photons                |

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
#> ✔ Searching CMR for GEDI L2A granules [2.6s]
#> 
#> ✔ Found 9 GEDI L2A granule.
gedi2a <- sl_read(granules)
#> ℹ Reading L2A from 9 granules
#> ✔ Read 579 footprint from 17 beam.✔ Reading L2A from 9 granules [1m 17.6s]

gedi2a
#> # A tibble: 579 × 121
#>     rh98  rh61 energy_total  rh40      rh4  rh41  rh24    rh10  rh43  rh62  rh80
#>    <dbl> <dbl>        <dbl> <dbl>    <dbl> <dbl> <dbl>   <dbl> <dbl> <dbl> <dbl>
#>  1  84.8  71.9        5121. 63.8    0.890  64.3  27.4    8.11  65.3   72.1  77.2
#>  2  73.8  57.3        4769. 18.7   -0.0300 19.9   6.13   1.90  23.3   57.9  67.3
#>  3  76.0  45.7        4804. 15.2   -2.13   17.1   5.38   0.440 20.4   46.8  61.6
#>  4  20.2  13.0        5023.  8.56  -1.01    8.79  5.27   0.590  9.24  13.2  16.0
#>  5  80.1  63.1        4304. 25.8   -0.0300 29.5   9.42   2.05  31.6   63.6  70.3
#>  6  62.0  51.8        5889. 43.8   -0.290  44.6  20.9    2.88  46.1   52.0  55.8
#>  7  20.6  12.9        4695. 10.2    0.590  10.3   7.55   4.26  10.6   13.1  16.2
#>  8  30.6  20.4        5299. 14.3  -36.9    14.7   6.17 -12.6   15.4   20.6  24.7
#>  9  71.1  59.2        4393. 54.3    0.180  54.5  50.7   34.3   55.0   59.4  64.8
#> 10  63.8  53.8        6143. 47.1   -0.480  47.5  37.9    1.08  48.3   54.0  58.2
#> # ℹ 569 more rows
#> # ℹ 110 more variables: rh39 <dbl>, rh23 <dbl>, rh42 <dbl>, rh87 <dbl>,
#> #   rh85 <dbl>, rh63 <dbl>, rh97 <dbl>, rh18 <dbl>, num_detectedmodes <int>,
#> #   rh56 <dbl>, rh48 <dbl>, rh47 <dbl>, rh7 <dbl>, rh84 <dbl>, rh38 <dbl>,
#> #   rh96 <dbl>, sensitivity <dbl>, landsat_treecover <dbl>, rh79 <dbl>,
#> #   rh99 <dbl>, rh81 <dbl>, rh91 <dbl>, rh74 <dbl>, rh2 <dbl>, rh95 <dbl>,
#> #   rh57 <dbl>, rh60 <dbl>, rh19 <dbl>, lon_lowestmode <dbl>, rh30 <dbl>, …

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
