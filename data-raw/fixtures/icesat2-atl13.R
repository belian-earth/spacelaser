# ---------------------------------------------------------------------------
# ICESat-2 ATL13 fixture
# ---------------------------------------------------------------------------
#
# ATL13 (inland water surface) — flat layout, everything at track
# root including lat/lon under the `segment_lat` / `segment_lon`
# names (not `latitude` / `longitude`). Tests the root-level lat/lon
# path for ICESat-2, which ATL24 also uses but with yet different
# names. No 2D, no pool, no transposed.

make_icesat2_atl13 <- function(path, n_segments = N_SHOTS,
                               tracks = c("gt1l", "gt2r")) {
  if (file.exists(path)) file.remove(path)
  f <- hdf5r::H5File$new(path, mode = "w")
  on.exit(f$close_all(), add = TRUE)

  registry <- spacelaser:::.icesat2_atl13_columns

  for (track_name in tracks) {
    track <- f$create_group(track_name)
    write_latlon(track, subgroup = "",
                 lat_name = "segment_lat", lon_name = "segment_lon",
                 n_shots = n_segments)
    write_scalar_columns(track, registry, n_segments,
                         skip = c("segment_lat", "segment_lon", "track"))
  }

  invisible(path)
}
