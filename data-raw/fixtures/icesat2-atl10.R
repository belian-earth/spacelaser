# ---------------------------------------------------------------------------
# ICESat-2 ATL10 fixture
# ---------------------------------------------------------------------------
#
# ATL10 (sea ice freeboard) — everything under `freeboard_segment/`
# with nested heights/ and geophysical/. No 2D, no pool, no transposed.

make_icesat2_atl10 <- function(path, n_segments = N_SHOTS,
                               tracks = c("gt1l", "gt2r")) {
  if (file.exists(path)) file.remove(path)
  f <- hdf5r::H5File$new(path, mode = "w")
  on.exit(f$close_all(), add = TRUE)

  registry <- spacelaser:::.icesat2_atl10_columns

  for (track_name in tracks) {
    track <- f$create_group(track_name)
    write_latlon(track, subgroup = "freeboard_segment",
                 lat_name = "latitude", lon_name = "longitude",
                 n_shots = n_segments)
    write_scalar_columns(track, registry, n_segments,
                         skip = c("latitude", "longitude", "track"))
  }

  invisible(path)
}
