# ---------------------------------------------------------------------------
# GEDI L4A fixture
# ---------------------------------------------------------------------------
#
# L4A (footprint-level aboveground biomass density) is the simplest
# GEDI pattern: no 2D profiles, no pool columns, no transposed.
# lat/lon at beam root, one column under geolocation/, rest under
# land_cover_data/ or at the root.

make_gedi_l4a <- function(path, n_shots = N_SHOTS,
                          beams = c("BEAM0000", "BEAM0101")) {
  if (file.exists(path)) file.remove(path)
  f <- hdf5r::H5File$new(path, mode = "w")
  on.exit(f$close_all(), add = TRUE)

  registry <- spacelaser:::.gedi_l4a_columns

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
