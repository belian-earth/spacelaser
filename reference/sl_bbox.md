# Create a bounding box for spatial queries

Wraps four corner coordinates into an `sl_bbox` vector used by
[`sl_search()`](https://belian-earth.github.io/spacelaser/reference/sl_search.md)
and
[`sl_read()`](https://belian-earth.github.io/spacelaser/reference/sl_read.md).
The main value is up-front validation: arguments are checked for correct
ordering (`xmin < xmax`, `ymin < ymax`) and for coordinates falling
within WGS84 bounds (latitude in \[-90, 90\], longitude in \[-180,
180\]), so mistakes surface here rather than as a silent empty search or
a failed HTTP request.

## Usage

``` r
sl_bbox(xmin, ymin, xmax, ymax)
```

## Arguments

- xmin:

  Minimum longitude (western boundary).

- ymin:

  Minimum latitude (southern boundary).

- xmax:

  Maximum longitude (eastern boundary).

- ymax:

  Maximum latitude (northern boundary).

## Value

A named double vector of class `sl_bbox`.

## Examples

``` r
# Construct a bounding box over a Pacific Northwest forest site.
sl_bbox(-124.04, 41.39, -124.01, 41.42)
#> <sl_bbox>: (-124.0400, 41.3900) - (-124.0100, 41.4200)

# Validation catches common mistakes before they reach the search
# or reader. Wrap in try() so the example chunk keeps running.
try(sl_bbox(-124.01, 41.39, -124.04, 41.42))  # xmin >= xmax
#> Error in sl_bbox(-124.01, 41.39, -124.04, 41.42) : 
#>   `xmin` must be less than `xmax`.
try(sl_bbox(0, -100, 1, 1))                   # latitude out of range
#> Error in sl_bbox(0, -100, 1, 1) : 
#>   Latitude values must be between -90 and 90.
```
