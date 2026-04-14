# ---------------------------------------------------------------------------
# GEDI L4C fixture
# ---------------------------------------------------------------------------
#
# L4C (waveform structural complexity index) follows the same shape as
# L4A: no 2D / pool / transposed, lat/lon at beam root, scalar columns
# split between root, geolocation/, and land_cover_data/. The only
# schema difference from L4A is a handful of `wsci_*` columns and
# `worldcover_class` under land_cover_data/.

make_gedi_l4c <- function(path, n_shots = N_SHOTS,
                          beams = c("BEAM0000", "BEAM0101")) {
  if (file.exists(path)) file.remove(path)
  f <- hdf5r::H5File$new(path, mode = "w")
  on.exit(f$close_all(), add = TRUE)

  registry <- spacelaser:::.gedi_l4c_columns

  for (beam_name in beams) {
    beam <- f$create_group(beam_name)

    write_latlon(beam, subgroup = "",
                 lat_name = "lat_lowestmode", lon_name = "lon_lowestmode",
                 n_shots = n_shots)

    write_scalar_columns(
      beam, registry, n_shots,
      skip = c("lat_lowestmode", "lon_lowestmode", "beam")
    )
  }

  invisible(path)
}
