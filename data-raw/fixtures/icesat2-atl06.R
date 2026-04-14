# ---------------------------------------------------------------------------
# ICESat-2 ATL06 fixture
# ---------------------------------------------------------------------------
#
# ATL06 is the land-ice equivalent of ATL08: all columns live under a
# single subgroup (`land_ice_segments/`) with nested subgroups for
# fit_statistics/, dem/, geophysical/, ground_track/. No 2D, no pool,
# no transposed.

make_icesat2_atl06 <- function(path, n_segments = N_SHOTS,
                               tracks = c("gt1l", "gt2r")) {
  if (file.exists(path)) file.remove(path)
  f <- hdf5r::H5File$new(path, mode = "w")
  on.exit(f$close_all(), add = TRUE)

  registry <- spacelaser:::.icesat2_atl06_columns

  for (track_name in tracks) {
    track <- f$create_group(track_name)
    write_latlon(track, subgroup = "land_ice_segments",
                 lat_name = "latitude", lon_name = "longitude",
                 n_shots = n_segments)
    write_scalar_columns(track, registry, n_segments,
                         skip = c("latitude", "longitude", "track"))
  }

  invisible(path)
}
