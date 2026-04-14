# ---------------------------------------------------------------------------
# GEDI L1B fixture
# ---------------------------------------------------------------------------
#
# L1B is the most structurally complex GEDI product, exercising paths
# no other product uses:
#   - lat/lon at `geolocation/latitude_bin0` / `geolocation/longitude_bin0`
#     (bin0, not lowestmode)
#   - two pool columns: `rxwaveform` (long, ~500 samples/shot) and
#     `txwaveform` (short, ~128 samples/shot), with distinct index
#     column pairs
#   - transposed 2D `surface_type [5, N]` under `geolocation/`
#   - a second subgroup, `geophys_corr/`, for tidal / atmospheric
#     corrections

make_gedi_l1b <- function(path, n_shots = N_SHOTS,
                          beams = c("BEAM0000", "BEAM0101"),
                          rx_samples_per_shot = 500L,
                          tx_samples_per_shot = 128L) {
  if (file.exists(path)) file.remove(path)
  f <- hdf5r::H5File$new(path, mode = "w")
  on.exit(f$close_all(), add = TRUE)

  registry <- spacelaser:::.gedi_l1b_columns
  trans_reg <- spacelaser:::.gedi_l1b_transposed_columns

  special_cols <- c(
    # lat/lon use L1B-specific paths
    "latitude_bin0", "longitude_bin0",
    # Pool columns + their index pairs
    "rxwaveform", "txwaveform",
    "rx_sample_start_index", "rx_sample_count",
    "tx_sample_start_index", "tx_sample_count",
    # Transposed
    "surface_type",
    # R-side synthesized
    "beam"
  )

  for (beam_name in beams) {
    beam <- f$create_group(beam_name)

    # lat/lon: L1B uses latitude_bin0 / longitude_bin0 under geolocation/
    write_latlon(beam, subgroup = "geolocation",
                 lat_name = "latitude_bin0", lon_name = "longitude_bin0",
                 n_shots = n_shots)

    # rxwaveform pool: ~500 samples/shot
    write_pool(beam, "rxwaveform",
               start_col = "rx_sample_start_index",
               count_col = "rx_sample_count",
               n_shots = n_shots,
               samples_per_shot = rx_samples_per_shot,
               min = 0, max = 1000)

    # txwaveform pool: ~128 samples/shot (shorter transmitted pulse)
    write_pool(beam, "txwaveform",
               start_col = "tx_sample_start_index",
               count_col = "tx_sample_count",
               n_shots = n_shots,
               samples_per_shot = tx_samples_per_shot,
               min = 0, max = 1000)

    # Transposed `surface_type [5, N]`. Geographic semantics: this is a
    # PNW-redwoods bbox, so all shots are marked `land`; a small
    # fraction are also flagged `inland_water` (rivers). Other
    # categories stay 0. That lets tests assert meaningful counts per
    # category rather than "some rows are 1".
    spec <- trans_reg$surface_type
    n_cats <- length(spec$labels)  # 5: land, ocean, sea_ice, land_ice, inland_water
    surface_data <- matrix(0L, nrow = n_shots, ncol = n_cats)
    surface_data[, 1] <- 1L  # column 1 = land
    water_shots <- sample.int(n_shots, size = n_shots %/% 10)
    surface_data[water_shots, 5] <- 1L  # column 5 = inland_water
    # chunk along the shot axis so multi-chunk navigation is exercised
    write_transposed_2d(beam, spec$path, surface_data, chunk_shots = 100L)

    # Scalar columns (root + geolocation/ + geophys_corr/ subgroups)
    write_scalar_columns(beam, registry, n_shots, skip = special_cols)
  }

  invisible(path)
}
