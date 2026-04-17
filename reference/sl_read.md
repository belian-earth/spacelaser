# Read satellite lidar data

`sl_read()` is an S3 generic that reads GEDI or ICESat-2 data from
remote HDF5 files using HTTP range requests. Only the chunks
intersecting the bounding box are fetched; no full-file download. It
dispatches on the type of its first argument:

## Usage

``` r
sl_read(x, bbox, ...)

# S3 method for class 'sl_gedi_search'
sl_read(x, bbox = NULL, columns = NULL, convert_time = TRUE, ...)

# S3 method for class 'sl_icesat2_search'
sl_read(x, bbox = NULL, columns = NULL, convert_time = TRUE, ...)

# S3 method for class 'character'
sl_read(x, bbox, ..., product = NULL)
```

## Arguments

- x:

  An `sl_gedi_search`, `sl_icesat2_search`, or character vector of HDF5
  URLs.

- bbox:

  An `sl_bbox` or numeric `c(xmin, ymin, xmax, ymax)`. Required when `x`
  is a character vector. Optional when `x` is a search result: it
  defaults to the bbox the search was performed with. If supplied, it
  must be fully contained within the search bbox; supplying a wider bbox
  is an error to avoid silently missing data outside the original
  search.

- ...:

  Reserved for method-specific arguments and forwarding.

- columns:

  Character vector of column names to read (short names from
  [`sl_columns()`](https://belian-earth.github.io/spacelaser/reference/sl_columns.md)).
  Latitude and longitude are always included automatically. `NULL`
  (default) reads the curated default column set for the product (see
  [`sl_columns()`](https://belian-earth.github.io/spacelaser/reference/sl_columns.md)
  with `set = "default"`). Pass `names(sl_columns(product))` to read all
  available columns.

- convert_time:

  Logical. If `TRUE` (default), the raw `delta_time` column (seconds
  since the GEDI / ICESat-2 reference epoch of 2018-01-01 00:00:00 UTC)
  is converted to a POSIXct column named `time`. Set to `FALSE` to keep
  `delta_time` as the raw numeric seconds-since-epoch value, e.g. if you
  need to compare against the file-level epoch exactly, want to avoid
  the POSIXct conversion overhead on very large photon-level reads, or
  want to preserve the original HDF5 column name.

- product:

  Character. Product level (e.g., `"L2A"`, `"ATL08"`). Required when `x`
  is a character vector and the product cannot be inferred from the file
  name.

## Value

A data frame with one row per footprint (GEDI) or segment/photon
(ICESat-2). Columns depend on the product and the `columns` argument.
Fill-value sentinels (-9999, 3.4e38, etc.) are automatically replaced
with `NA`.

## Details

- An `sl_gedi_search` or `sl_icesat2_search` object (from
  [`sl_search()`](https://belian-earth.github.io/spacelaser/reference/sl_search.md)):
  reads all granules in the search result, combining rows into a single
  data frame. The search bbox is used by default; an explicit `bbox` may
  be supplied to subset further but must be contained within it.

- A character vector of URLs: auto-detects the sensor and product from
  the file name, or uses the explicit `product` argument.

All beams (GEDI) or ground tracks (ICESat-2) are always read; the
returned data frame includes a `beam` (GEDI) or `track` (ICESat-2)
identifier column for post-hoc filtering with
[`dplyr::filter()`](https://dplyr.tidyverse.org/reference/filter.html)
or base subsetting.

### Default columns

When `columns = NULL`, a curated default set is returned for each
product. Use `sl_columns(product, set = "default")` to see which columns
are included. The defaults are designed to cover the primary science
variables, key quality flags, and basic context without surprises. Use
`sl_columns(product, set = "all")` to discover everything available.

### Product-specific notes

**GEDI L1B**: The default set includes `rxwaveform`, which is a **list
column** (one numeric vector per shot containing the received waveform).
Use
[`sl_extract_waveforms()`](https://belian-earth.github.io/spacelaser/reference/sl_extract_waveforms.md)
to expand this into a long-form data frame with per-sample elevations.
The transmitted waveform (`txwaveform`) is available via explicit
request but not included in defaults. L1B reads are slower than other
products because waveform data requires targeted chunk reads into the
pool dataset.

**GEDI L2A**: The `rh` column is a 2D dataset \[N, 101\] that expands
into 101 columns (`rh0` through `rh100`), representing relative height
percentiles in metres. This is included in the default set.

**GEDI L2B**: The default set includes `cover_z`, `pai_z`, and `pavd_z`,
which are 2D datasets \[N, 30\] representing canopy vertical profiles at
5 m height bins. Each expands to 30 columns (e.g. `cover_z0` through
`cover_z29`), adding 90 columns to the output. `pgap_theta_z` is a
variable-length list column (similar to L1B waveforms) and is not
included in defaults; request it explicitly when needed.

**GEDI L2B `rh100`**: Stored in centimetres in the HDF5 file;
automatically converted to metres for consistency with L2A.

**GEDI L4A**: The `agbd` column is above-ground biomass density in
Mg/ha. Prediction intervals (`agbd_pi_lower`, `agbd_pi_upper`) and
standard error (`agbd_se`) are included in defaults.

**GEDI L4C**: Waveform Structural Complexity Index. The `wsci` column is
the headline metric, with prediction intervals (`wsci_pi_lower`,
`wsci_pi_upper`) and decomposed XY/Z components (`wsci_xy`, `wsci_z`) in
defaults. `worldcover_class` provides the ESA WorldCover land-cover
class.

**ICESat-2 ATL03**: Photon-level data. A single granule can contain
millions of photons. The reader uses ATL03's segment-level spatial index
(`geolocation/reference_photon_lat` etc.) to filter at segment rate
before reading photon-level columns, so spatial subsets stay fast even
on large bboxes. `signal_conf_ph` is a 2D column \[N, 5\] (5 surface
types: land, ocean, sea ice, land ice, inland water) that expands to 5
columns.

**ICESat-2 ATL06**: Land ice elevation segments. The default set
includes fit statistics (`n_fit_photons`, `h_robust_sprd`, `snr`) and
reference DEM height (`dem_h`). Tidal and geophysical corrections are
available via `sl_columns("ATL06")` but not in defaults.

**ICESat-2 ATL07**: Sea-ice height segments. Defaults include segment
height + confidence + quality, photon rate, AMSR2 ice concentration, and
atmospheric flags. Geolocation parameters (`solar_*`, `sigma_h`) and
finer geophysical corrections live under
`sea_ice_segments/{geolocation,geophysical,stats}/` and are available
via `sl_columns("ATL07", set = "all")`.

**ICESat-2 ATL08**: The default set includes `canopy_h_metrics`, a 2D
dataset \[N, 18\] of canopy height percentiles (P10 through P95) that
expands to 18 columns. Terrain slope, photon counts, and land cover are
also included. `*_abs` (absolute height) variants and the secondary
canopy / terrain metrics are in the registry but not in defaults.

**ICESat-2 ATL10**: Sea-ice freeboard. Defaults include `beam_fb_height`
(per-beam freeboard) plus quality, confidence, and the underlying ATL07
height-segment context.

**ICESat-2 ATL13**: Inland water surface heights. Defaults include
water-surface height + standard deviation, significant wave height,
water depth, segment provenance (`inland_water_body_*`), and quality
flags. Geometry uses the segment centroid (`segment_lat`,
`segment_lon`).

**ICESat-2 ATL24**: Near-shore bathymetry, photon-level. Defaults
include orthometric / ellipsoidal / surface heights, photon class and
confidence, and the THU / TVU positional uncertainty pair. Geometry uses
photon coordinates (`lat_ph`, `lon_ph`).

## See also

[`sl_search()`](https://belian-earth.github.io/spacelaser/reference/sl_search.md),
[`sl_columns()`](https://belian-earth.github.io/spacelaser/reference/sl_columns.md),
[`sl_extract_waveforms()`](https://belian-earth.github.io/spacelaser/reference/sl_extract_waveforms.md)
