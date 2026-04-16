# Extract L1B waveforms to long form with elevation profile

Converts GEDI L1B received waveform list columns into a long-form data
frame with one row per waveform sample. Each sample is assigned an
elevation by linearly interpolating between `elevation_bin0` (waveform
top) and `elevation_lastbin` (waveform bottom).

## Usage

``` r
sl_extract_waveforms(x)
```

## Arguments

- x:

  A data frame from
  [`sl_read()`](https://belian-earth.github.io/spacelaser/reference/sl_read.md)
  containing GEDI L1B data. Must include columns: `shot_number`,
  `elevation_bin0`, `elevation_lastbin`, `rx_sample_count`, and
  `rxwaveform` (list column). If `beam` is present, it is carried
  through to the output.

  The L1B default column set already includes `rxwaveform` plus the
  `elevation_bin0` / `elevation_lastbin` / `rx_sample_count` columns it
  depends on, so a plain `sl_read(granules)` is enough. When narrowing
  the read, request `rxwaveform` and the dependency columns are
  auto-added:

      d <- sl_read(granules, columns = c("shot_number", "rxwaveform"))

## Value

A data frame with one row per waveform sample:

- shot_number:

  Shot identifier (repeated per sample).

- beam:

  Beam name, if present in input.

- elevation:

  Sample elevation in metres (WGS-84 ellipsoidal), interpolated from
  `elevation_bin0` and `elevation_lastbin`.

- amplitude:

  Waveform return amplitude (raw float from HDF5).

## Details

This is the standard preprocessing step for waveform visualisation and
analysis. The output can be plotted directly with ggplot2:

    library(ggplot2)
    wf <- sl_extract_waveforms(gedi_l1b)
    ggplot(wf, aes(amplitude, elevation)) +
      geom_path() +
      facet_wrap(~shot_number)

The elevation for each sample is computed as:

`elevation[i] = elevation_bin0 - i * (elevation_bin0 - elevation_lastbin) / rx_sample_count`

where `i` runs from 1 to `rx_sample_count`. This places sample 1 one
step below `elevation_bin0` and sample `rx_sample_count` at
`elevation_lastbin`, matching the chewie convention.

Shots with `NA` elevation or zero `rx_sample_count` are dropped.

## See also

[`sl_read()`](https://belian-earth.github.io/spacelaser/reference/sl_read.md)
with `columns = c("rxwaveform", ...)`
