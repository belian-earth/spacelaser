# Extract L1B waveforms to long form with elevation profile

Converts GEDI L1B received-waveform list columns into a long-form data
frame with one row per waveform sample, with each sample assigned an
elevation by linearly interpolating between `elevation_bin0` (waveform
top) and `elevation_lastbin` (waveform bottom). The elevation reference
is configurable via `height_ref` and the amplitude normalisation via
`normalise_amplitude`.

## Usage

``` r
sl_extract_waveforms(
  x,
  height_ref = c("ellipsoid", "geoid", "tandemx", "srtm"),
  normalise_amplitude = c("raw", "noise", "snr")
)
```

## Arguments

- x:

  A data frame from
  [`sl_read()`](https://belian-earth.github.io/spacelaser/reference/sl_read.md)
  containing GEDI L1B data. Must include `shot_number`,
  `elevation_bin0`, `elevation_lastbin`, `rx_sample_count`, and
  `rxwaveform` (list column). If `beam` is present, it is carried
  through to the output. Additional columns are required for non-default
  `height_ref` and `normalise_amplitude` values — see the description of
  each mode below.

- height_ref:

  Reference surface for the returned `elevation` column:

  `"ellipsoid"`

  :   (default) WGS-84 ellipsoidal elevation, exactly as reported by
      L1B. No extra columns needed.

  `"geoid"`

  :   Height above the EGM2008 geoid (mean sea level):
      `elevation - geoid` per shot. Requires `geoid`.

  `"tandemx"`

  :   Height above the TanDEM-X 90 m DEM at the footprint:
      `elevation - digital_elevation_model` per shot. A rough proxy for
      height above ground when an L2A ground return isn't available.
      Requires `digital_elevation_model`.

  `"srtm"`

  :   Same as `"tandemx"`, but against the SRTM DEM. Requires
      `digital_elevation_model_srtm`.

- normalise_amplitude:

  Baseline correction for the returned `amplitude` column. Raw GEDI
  waveform samples carry a per-shot pedestal that differs by digitiser
  channel, so raw amplitudes from different shots / beams / channels are
  not directly comparable.

  `"raw"`

  :   (default) Sample values straight from `rxwaveform`. No extra
      columns needed.

  `"noise"`

  :   Baseline-subtracted amplitude: `rxwaveform - noise_mean_corrected`
      per shot, so every shot's free-atmosphere noise floor sits at
      zero. Requires `noise_mean_corrected`.

  `"snr"`

  :   Noise-normalised amplitude:
      `(rxwaveform - noise_mean_corrected) / noise_stddev_corrected` per
      shot, expressed in units of the per-shot noise standard deviation.
      Useful for peak-detection. Requires `noise_mean_corrected` and
      `noise_stddev_corrected`.

## Value

A data frame with one row per waveform sample:

- shot_number:

  Shot identifier (repeated per sample).

- beam:

  Beam name, if present in input.

- elevation:

  Sample elevation in metres, referenced to the surface chosen by
  `height_ref`.

- amplitude:

  Waveform return amplitude, adjusted according to
  `normalise_amplitude`.

## Details

The per-sample elevation is computed as

`elevation[i] = elevation_bin0 - i * (elevation_bin0 - elevation_lastbin) / rx_sample_count`

for `i` in `1..rx_sample_count`, placing sample 1 one step below
`elevation_bin0` and sample `rx_sample_count` at `elevation_lastbin`
(chewie convention). The `height_ref` argument then subtracts the
corresponding per-shot reference (geoid, DEM, or nothing) from every
sample.

The L1B default column set includes `rxwaveform`, the structural
dependency columns (`elevation_bin0`, `elevation_lastbin`,
`rx_sample_count`, `shot_number`), the height references (`geoid`,
`digital_elevation_model`, `digital_elevation_model_srtm`), and the
noise statistics (`noise_mean_corrected`, `noise_stddev_corrected`).
Every `height_ref` and `normalise_amplitude` mode therefore works out of
the box against a plain `sl_read(granules)`. When narrowing the read
with `columns = c(...)`, request the columns needed for your chosen
modes.

Shots with `NA` reference elevations, `NA` noise statistics (when
requested), or zero `rx_sample_count` are dropped silently.

## See also

[`sl_read()`](https://belian-earth.github.io/spacelaser/reference/sl_read.md)

## Examples

``` r
if (FALSE) { # interactive()
bbox <- sl_bbox(-124.04, 41.39, -124.01, 41.42)
granules <- sl_search(bbox, product = "L1B",
                      date_start = "2020-06-01",
                      date_end   = "2020-06-30")
d <- sl_read(granules[1L, ], bbox = bbox)

# Absolute ellipsoidal elevations, raw amplitudes (defaults).
wf <- sl_extract_waveforms(d)

# Height above the TanDEM-X DEM with baseline-subtracted amplitude —
# waveforms from different beams / channels line up at zero.
wf_agl <- sl_extract_waveforms(
  d,
  height_ref         = "tandemx",
  normalise_amplitude = "noise"
)

# SNR units: amplitudes in multiples of per-shot noise σ.
wf_snr <- sl_extract_waveforms(d, normalise_amplitude = "snr")
}
```
