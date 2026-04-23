
<!-- README.md is generated from README.Rmd. Please edit that file -->

# spacelaser <a href="https://belian-earth.github.io/spacelaser/"><img src="man/figures/logo.png" align="right" height="138" alt="spacelaser website" /></a>

<!-- badges: start -->

[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![extendr](https://img.shields.io/badge/extendr-*-276DC2)](https://extendr.github.io/extendr/extendr_api/)
[![License:Apache](https://img.shields.io/github/license/belian-earth/spacelaser)](https://www.apache.org/licenses/LICENSE-2.0)
[![Codecov test
coverage](https://codecov.io/gh/belian-earth/spacelaser/graph/badge.svg)](https://app.codecov.io/gh/belian-earth/spacelaser)
[![R-CMD-check](https://github.com/belian-earth/spacelaser/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/belian-earth/spacelaser/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

Cloud-optimized partial reading of GEDI and ICESat-2 HDF5 data from R.
Only the bytes needed for the requested spatial/temporal subset are
fetched over HTTP, avoiding multi-gigabyte downloads.

## Installation

Requires a [Rust toolchain](https://www.rust-lang.org/tools/install)
(cargo + rustc).

``` r
# install.packages("pak")
pak::pak("belian-earth/spacelaser")
```

## Authentication

All reads go through NASA Earthdata, which requires a free account.
Register at <https://urs.earthdata.nasa.gov/>.

Credentials can be supplied in any of the following ways:

- **Environment variables** — set `EARTHDATA_USERNAME` and
  `EARTHDATA_PASSWORD`. Convenient for CI and shell sessions.

- **A `.netrc` file** — add an entry for `urs.earthdata.nasa.gov` to
  `~/.netrc` (or `_netrc` on Windows). spacelaser will read it directly.

- [**`earthdatalogin`**](https://boettiger-lab.github.io/earthdatalogin/)
  — the simplest option if you don’t already have a netrc set up:

``` r
# install.packages("earthdatalogin")
earthdatalogin::edl_netrc()
```

This writes a netrc for you and is interoperable with other R Earthdata
tools.

## Example with GEDI L2A

``` r
library(spacelaser)

bbox <- sl_bbox(-124.04, 41.39, -124.01, 41.42)

granules <- sl_search(
  bbox,
  product = "L2A",
  date_start = "2022-01-01",
  date_end = "2023-01-01"
)
#> ℹ Searching CMR for GEDI L2A granules
#> ✔ Searching CMR for GEDI L2A granules [6.2s]
#> 
#> ✔ Found 9 GEDI L2A granules.
gedi2a <- sl_read(granules)
#> ℹ Reading L2A from 9 granules
#> ✔ Read 647 footprints from 20 beams.✔ Reading L2A from 9 granules [45.7s]

gedi2a
#> # A tibble: 647 × 121
#>    beam     shot_number time                lat_lowestmode lon_lowestmode
#>    <chr>        <int64> <dttm>                       <dbl>          <dbl>
#>  1 BEAM0000       1.e17 2022-01-22 01:46:51           41.4          -124.
#>  2 BEAM0000       1.e17 2022-01-22 01:46:51           41.4          -124.
#>  3 BEAM0000       1.e17 2022-01-22 01:46:51           41.4          -124.
#>  4 BEAM0000       1.e17 2022-01-22 01:46:51           41.4          -124.
#>  5 BEAM0000       1.e17 2022-01-22 01:46:51           41.4          -124.
#>  6 BEAM0000       1.e17 2022-01-22 01:46:51           41.4          -124.
#>  7 BEAM0000       1.e17 2022-01-22 01:46:51           41.4          -124.
#>  8 BEAM0000       1.e17 2022-01-22 01:46:51           41.4          -124.
#>  9 BEAM0000       1.e17 2022-01-22 01:46:51           41.4          -124.
#> 10 BEAM0000       1.e17 2022-01-22 01:46:51           41.4          -124.
#> # ℹ 637 more rows
#> # ℹ 116 more variables: degrade_flag <int>, quality_flag <int>,
#> #   sensitivity <dbl>, solar_elevation <dbl>, elev_lowestmode <dbl>,
#> #   elev_highestreturn <dbl>, energy_total <dbl>, num_detectedmodes <int>,
#> #   rh0 <dbl>, rh1 <dbl>, rh2 <dbl>, rh3 <dbl>, rh4 <dbl>, rh5 <dbl>,
#> #   rh6 <dbl>, rh7 <dbl>, rh8 <dbl>, rh9 <dbl>, rh10 <dbl>, rh11 <dbl>,
#> #   rh12 <dbl>, rh13 <dbl>, rh14 <dbl>, rh15 <dbl>, rh16 <dbl>, rh17 <dbl>, …

g <- gedi2a[gedi2a$quality_flag == 1, ]

plot(
  g$geometry,
  pch = 21,
  cex = 1.5,
  bg = hcl.colors(100, "Viridis", alpha = 0.7)[
    findInterval(g$rh98, seq(0, 100), all.inside = TRUE)
  ]
)
```

<img src="man/figures/README-example-1.png" alt="" width="100%" />

## Exploring other products

`sl_columns()` lists what a product offers. All 12 GEDI and ICESat-2
products supported by spacelaser use the same two verbs (`sl_search()` →
`sl_read()`); only the product string and column names change.

``` r
# ICESat-2 photon-level data — full column inventory
sl_columns("ATL03", set = "all")
#>                    lat_ph                    lon_ph                      h_ph 
#>          "heights/lat_ph"          "heights/lon_ph"            "heights/h_ph" 
#>                delta_time            signal_conf_ph            dist_ph_across 
#>      "heights/delta_time"  "heights/signal_conf_ph"  "heights/dist_ph_across" 
#>             dist_ph_along            pce_mframe_cnt             ph_id_channel 
#>   "heights/dist_ph_along"  "heights/pce_mframe_cnt"   "heights/ph_id_channel" 
#>               ph_id_count               ph_id_pulse                quality_ph 
#>     "heights/ph_id_count"     "heights/ph_id_pulse"      "heights/quality_ph" 
#>           signal_class_ph                 weight_ph 
#> "heights/signal_class_ph"       "heights/weight_ph"
```

## Supported products

### GEDI

| Product | Description                                 |
|:--------|:--------------------------------------------|
| L1B     | Geolocated waveforms                        |
| L2A     | Ground elevation, relative height metrics   |
| L2B     | Canopy cover fraction and vertical profile  |
| L4A     | Footprint-level aboveground biomass density |
| L4C     | Waveform structural complexity index        |

### ICESat-2

| Product | Description                                     |
|:--------|:------------------------------------------------|
| ATL03   | Geolocated photon heights                       |
| ATL06   | Land ice surface elevation                      |
| ATL07   | Sea ice surface elevation                       |
| ATL08   | Terrain height, canopy height, and canopy cover |
| ATL10   | Sea ice freeboard                               |
| ATL13   | Inland water surface data                       |
| ATL24   | Coastal and Nearshore bathymetry                |

## Why spacelaser

The standard R workflow for GEDI / ICESat-2 data is to download whole
HDF5 granules, then filter locally. For a typical spatial subset query
that wastes minutes and gigabytes — the file you’re filtering is usually
orders of magnitude larger than the answer you actually want.

Spacelaser sends HTTP range requests against the remote files and
returns just the rows that fall inside your bounding box, with no local
caching needed.

On a representative Mondah Forest workload (11 GEDI L2A granules, two
years of coverage, 1,376 matching shots) spacelaser completes in ~60 s
versus ~1,170 s for a full-granule download + hdf5r read — around **19×
quicker** for the same 112 shared columns, bit-for-bit identical output.
See
[`benchmarks/`](https://github.com/belian-earth/spacelaser/tree/main/benchmarks)
for methodology and comparisons with other partial-read and pre-indexed
approaches.

## Acknowledgements

spacelaser is a Rust reimplementation of the partial-HDF5 reading
approach pioneered by
**[h5coro](https://github.com/SlideRuleEarth/h5coro)** (NASA SlideRule
Earth). The core idea is theirs: targeted HTTP range requests against
cloud-hosted HDF5 granules rather than downloading whole files. This
package brings that idea to R with a GEDI/ICESat-2-specific API and a
ground-up Rust parser.

### Data

- [**GEDI**](https://www.earthdata.nasa.gov/data/instruments/gedi-lidar):
  NASA GEDI Science Team, distributed by LP DAAC and ORNL DAAC
- [**ICESat-2**](https://icesat-2.gsfc.nasa.gov/): NASA ATLAS Instrument
  and Science Teams, distributed by NSIDC DAAC
