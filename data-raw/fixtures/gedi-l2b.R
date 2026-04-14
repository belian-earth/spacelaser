# ---------------------------------------------------------------------------
# GEDI L2B fixture
# ---------------------------------------------------------------------------
#
# L2B adds three code paths L2A doesn't cover:
#   - lat/lon under `geolocation/` (not beam root)
#   - three 2D [N, 30] profile datasets (cover_z, pai_z, pavd_z)
#   - `pgap_theta_z` pool column, indexed by rx_sample_start_index
#     (1-based) + rx_sample_count

make_gedi_l2b <- function(path, n_shots = N_SHOTS,
                          beams = c("BEAM0000", "BEAM0101"),
                          n_profile_bins = 30L, samples_per_shot = 30L) {
  if (file.exists(path)) file.remove(path)
  f <- hdf5r::H5File$new(path, mode = "w")
  on.exit(f$close_all(), add = TRUE)

  registry <- spacelaser:::.gedi_l2b_columns

  special_cols <- c(
    "lat_lowestmode", "lon_lowestmode",
    "cover_z", "pai_z", "pavd_z",
    "pgap_theta_z",
    "rx_sample_start_index", "rx_sample_count",
    "beam"
  )

  for (beam_name in beams) {
    beam <- f$create_group(beam_name)

    # lat/lon under the geolocation/ subgroup
    write_latlon(beam, subgroup = "geolocation",
                 lat_name = "lat_lowestmode", lon_name = "lon_lowestmode",
                 n_shots = n_shots)

    # 2D profile datasets, HDF5 shape [n_shots, 30]
    for (nm in c("cover_z", "pai_z", "pavd_z")) {
      write_2d(beam, nm, n_shots, n_profile_bins, max = 10)
    }

    # Pool: pgap_theta_z indexed by rx_sample_start_index / rx_sample_count
    write_pool(beam, "pgap_theta_z",
               start_col = "rx_sample_start_index",
               count_col = "rx_sample_count",
               n_shots = n_shots,
               samples_per_shot = samples_per_shot)

    write_scalar_columns(beam, registry, n_shots, skip = special_cols)
  }

  invisible(path)
}
