# ---------------------------------------------------------------------------
# ICESat-2 ATL07 fixture
# ---------------------------------------------------------------------------
#
# ATL07 (sea ice surface) — same shape as ATL06 but under
# `sea_ice_segments/` with nested heights/, geolocation/,
# geophysical/, stats/ subgroups. No 2D, no pool, no transposed.

make_icesat2_atl07 <- function(path, n_segments = N_SHOTS,
                               tracks = c("gt1l", "gt2r")) {
  if (file.exists(path)) file.remove(path)
  f <- hdf5r::H5File$new(path, mode = "w")
  on.exit(f$close_all(), add = TRUE)

  registry <- spacelaser:::.icesat2_atl07_columns

  for (track_name in tracks) {
    track <- f$create_group(track_name)
    write_latlon(track, subgroup = "sea_ice_segments",
                 lat_name = "latitude", lon_name = "longitude",
                 n_shots = n_segments)
    write_scalar_columns(track, registry, n_segments,
                         skip = c("latitude", "longitude", "track"))
  }

  invisible(path)
}
