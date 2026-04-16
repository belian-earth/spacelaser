# List available columns for a GEDI or ICESat-2 product

Returns a named character vector of columns for the given product. Names
are the short user-facing column names (used in the `columns` argument
of
[`sl_read()`](https://belian-earth.github.io/spacelaser/reference/sl_read.md)).
Values are the full HDF5 dataset paths.

## Usage

``` r
sl_columns(
  product = c("L2A", "L2B", "L4A", "L4C", "L1B", "ATL03", "ATL06", "ATL07", "ATL08",
    "ATL10", "ATL13", "ATL24"),
  set = c("all", "default")
)
```

## Arguments

- product:

  Character. One of:

  - GEDI: `"L1B"`, `"L2A"`, `"L2B"`, `"L4A"`, `"L4C"`

  - ICESat-2: `"ATL03"`, `"ATL06"`, `"ATL07"`, `"ATL08"`, `"ATL10"`,
    `"ATL13"`, `"ATL24"`

- set:

  Character. Which column set to return:

  - `"all"` (default): every column in the registry, including
    geophysical corrections, instrument details, and pool columns.

  - `"default"`: a curated subset of commonly useful science variables,
    quality flags, and context columns. This is what
    [`sl_read()`](https://belian-earth.github.io/spacelaser/reference/sl_read.md)
    returns when `columns` is not specified.

## Value

A named character vector.

## Examples

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
sl_columns("L2A", set = "default")
#>                                beam                         shot_number 
#>                              "beam"                       "shot_number" 
#>                          delta_time                        degrade_flag 
#>                        "delta_time"                      "degrade_flag" 
#>                        quality_flag                         sensitivity 
#>                      "quality_flag"                       "sensitivity" 
#>                     solar_elevation                     elev_lowestmode 
#>                   "solar_elevation"                   "elev_lowestmode" 
#>                  elev_highestreturn                        energy_total 
#>                "elev_highestreturn"                      "energy_total" 
#>                   num_detectedmodes                                  rh 
#>                 "num_detectedmodes"                                "rh" 
#>                  selected_algorithm             digital_elevation_model 
#>                "selected_algorithm"           "digital_elevation_model" 
#>                   landsat_treecover                     modis_treecover 
#> "land_cover_data/landsat_treecover"   "land_cover_data/modis_treecover" 
#>                           pft_class                        region_class 
#>         "land_cover_data/pft_class"      "land_cover_data/region_class" 
```
