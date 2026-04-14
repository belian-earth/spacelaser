# ---------------------------------------------------------------------------
# GEDI L2A fixture
# ---------------------------------------------------------------------------
#
# L2A is the baseline GEDI pattern:
#   - lat/lon at beam root (lat_lowestmode / lon_lowestmode)
#   - 2D `rh [N, 101]`
#   - one subgroup for land_cover_data
#   - no pool columns, no transposed columns

make_gedi_l2a <- function(path, n_shots = N_SHOTS, n_rh_bins = 101L,
                          beams = c("BEAM0000", "BEAM0101")) {
  if (file.exists(path)) file.remove(path)
  f <- hdf5r::H5File$new(path, mode = "w")
  on.exit(f$close_all(), add = TRUE)

  registry <- spacelaser:::.gedi_l2a_columns

  for (beam_name in beams) {
    beam <- f$create_group(beam_name)

    write_latlon(beam, subgroup = "",
                 lat_name = "lat_lowestmode", lon_name = "lon_lowestmode",
                 n_shots = n_shots)

    # rh: HDF5 shape [n_shots, 101], values in 0-50 m (plausible heights)
    write_2d(beam, "rh", n_shots, n_rh_bins, max = 50)

    write_scalar_columns(
      beam, registry, n_shots,
      skip = c("lat_lowestmode", "lon_lowestmode", "rh",
               "beam")  # `beam` is R-side synthesized from the group name
    )
  }

  invisible(path)
}
