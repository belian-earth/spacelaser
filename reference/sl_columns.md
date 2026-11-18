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
  set = c("all", "default"),
  version = NULL
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

- version:

  Character or `NULL`. Product version, for the products published in
  more than one: `"3"` (the default) or `"2"` for GEDI L1B, L2A, L2B and
  L4C, `"3"` or `"2.1"` for L4A. GEDI L2A, L2B, L4A and L4C variables
  differ between versions; see
  [`sl_search()`](https://belian-earth.github.io/spacelaser/reference/sl_search.md).
  [`sl_read()`](https://belian-earth.github.io/spacelaser/reference/sl_read.md)
  picks the version from the granule file names.

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
#>                        degrade_include_flag 
#>          "geolocation/degrade_include_flag" 
#>                               l2_algrunflag 
#>                             "l2_algrunflag" 
#>                       l2a_quality_flag_rel3 
#>                     "l2a_quality_flag_rel3" 
#>                       l2a_quality_flag_rel2 
#>                     "l2a_quality_flag_rel2" 
#>                                 sensitivity 
#>                               "sensitivity" 
#>                               solar_azimuth 
#>                             "solar_azimuth" 
#>                             solar_elevation 
#>                           "solar_elevation" 
#>                                surface_flag 
#>                              "surface_flag" 
#>                           stale_return_flag 
#>             "geolocation/stale_return_flag" 
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
#>                          local_beam_azimuth 
#>            "geolocation/local_beam_azimuth" 
#>                        local_beam_elevation 
#>          "geolocation/local_beam_elevation" 
#>                           landsat_treecover 
#>         "land_cover_data/landsat_treecover" 
#>                   landsat_water_persistence 
#> "land_cover_data/landsat_water_persistence" 
#>                               leaf_off_flag 
#>             "land_cover_data/leaf_off_flag" 
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
#>                             phenology_phase 
#>           "land_cover_data/phenology_phase" 
#>                              phenology_year 
#>            "land_cover_data/phenology_year" 
#>                                region_class 
#>              "land_cover_data/region_class" 
#>                     urban_focal_window_size 
#>   "land_cover_data/urban_focal_window_size" 
#>                            urban_proportion 
#>          "land_cover_data/urban_proportion" 
#>                            worldcover_class 
#>          "land_cover_data/worldcover_class" 
sl_columns("L2A", set = "default")
#>                                beam                         shot_number 
#>                              "beam"                       "shot_number" 
#>                          delta_time                        degrade_flag 
#>                        "delta_time"                      "degrade_flag" 
#>               l2a_quality_flag_rel3               l2a_quality_flag_rel2 
#>             "l2a_quality_flag_rel3"             "l2a_quality_flag_rel2" 
#>                         sensitivity                     solar_elevation 
#>                       "sensitivity"                   "solar_elevation" 
#>                     elev_lowestmode                  elev_highestreturn 
#>                   "elev_lowestmode"                "elev_highestreturn" 
#>                        energy_total                   num_detectedmodes 
#>                      "energy_total"                 "num_detectedmodes" 
#>                                  rh                  selected_algorithm 
#>                                "rh"                "selected_algorithm" 
#>             digital_elevation_model                   landsat_treecover 
#>           "digital_elevation_model" "land_cover_data/landsat_treecover" 
#>                     modis_treecover                           pft_class 
#>   "land_cover_data/modis_treecover"         "land_cover_data/pft_class" 
#>                        region_class 
#>      "land_cover_data/region_class" 
```
