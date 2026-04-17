# Search NASA CMR for GEDI or ICESat-2 granules

Searches NASA's Common Metadata Repository (CMR) for satellite lidar
granules that overlap a bounding box and optional date range. The sensor
(GEDI or ICESat-2) is determined automatically from `product`, since the
valid product strings do not overlap.

## Usage

``` r
sl_search(
  bbox,
  product = c("L2A", "L2B", "L4A", "L4C", "L1B", "ATL03", "ATL06", "ATL07", "ATL08",
    "ATL10", "ATL13", "ATL24"),
  date_start = NULL,
  date_end = NULL
)
```

## Arguments

- bbox:

  An `sl_bbox` object created by
  [`sl_bbox()`](https://belian-earth.github.io/spacelaser/reference/sl_bbox.md),
  or a numeric vector `c(xmin, ymin, xmax, ymax)`.

- product:

  Character. One of:

  - GEDI: `"L1B"`, `"L2A"`, `"L2B"`, `"L4A"`, `"L4C"`

  - ICESat-2: `"ATL03"`, `"ATL06"`, `"ATL07"`, `"ATL08"`, `"ATL10"`,
    `"ATL13"`, `"ATL24"`

- date_start, date_end:

  Either a `Date` object or a character string in strict `"YYYY-MM-DD"`
  format (e.g. `"2020-06-01"`). Other character forms (e.g.
  `"01/06/2020"`, `"June 1 2020"`, `"2020-06"`) are rejected with an
  informative error to avoid ambiguity. `POSIXct` inputs are not
  accepted — format them explicitly with `format(x, "%Y-%m-%d")`.

  Bounds are treated as UTC and inclusive of the end date: a range of
  `"2020-06-01"` to `"2020-06-30"` covers every granule whose start time
  falls on any of those 30 days.

  `date_start` defaults to the mission start (2019-03-25 for GEDI,
  2018-10-14 for ICESat-2). `date_end` defaults to today.

## Value

A classed data frame (`sl_gedi_search` or `sl_icesat2_search`) with
columns:

- id:

  CMR granule identifier.

- time_start:

  POSIXct start time of the granule.

- time_end:

  POSIXct end time of the granule.

- url:

  HTTPS data URL.

- geometry:

  `wk_wkt` polygon of the granule swath footprint.

The returned object carries `bbox` and `product` as attributes so
[`sl_read()`](https://belian-earth.github.io/spacelaser/reference/sl_read.md)
can dispatch without re-specifying them.

## Details

No authentication is needed for the search itself; Earthdata credentials
are only required when reading data via
[`sl_read()`](https://belian-earth.github.io/spacelaser/reference/sl_read.md).

The CMR search filters by bounding box on the server side. For finer
spatial filtering (e.g. against an irregular polygon), filter the
returned `geometry` column with your favourite spatial package.

## See also

[`sl_read()`](https://belian-earth.github.io/spacelaser/reference/sl_read.md)
to read data from the returned granules.

## Examples

``` r
if (FALSE) { # interactive()
# GEDI L2A over a small Pacific Northwest forest bbox, summer 2020.
granules <- sl_search(
  sl_bbox(-124.04, 41.39, -124.01, 41.42),
  product    = "L2A",
  date_start = "2020-06-01",
  date_end   = "2020-09-01"
)
granules

# ICESat-2 ATL08 (land + canopy segments) over the same bbox, 2020.
sl_search(
  sl_bbox(-124.10, 41.36, -124.00, 41.45),
  product    = "ATL08",
  date_start = "2020-01-01",
  date_end   = "2021-01-01"
)
}
```
