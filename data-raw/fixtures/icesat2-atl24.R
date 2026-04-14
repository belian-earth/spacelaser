# ---------------------------------------------------------------------------
# ICESat-2 ATL24 fixture
# ---------------------------------------------------------------------------
#
# ATL24 (near-shore bathymetric photons) — flat layout at track root,
# lat/lon named `lat_ph` / `lon_ph`. Like ATL13 it doesn't use a
# subgroup; like ATL03 it's photon-level, but without the segment
# index (direct lat/lon scan, no `heights/` subgroup). No 2D, no
# pool, no transposed.

make_icesat2_atl24 <- function(path, n_segments = N_SHOTS,
                               tracks = c("gt1l", "gt2r")) {
  if (file.exists(path)) file.remove(path)
  f <- hdf5r::H5File$new(path, mode = "w")
  on.exit(f$close_all(), add = TRUE)

  registry <- spacelaser:::.icesat2_atl24_columns

  for (track_name in tracks) {
    track <- f$create_group(track_name)
    write_latlon(track, subgroup = "",
                 lat_name = "lat_ph", lon_name = "lon_ph",
                 n_shots = n_segments)
    write_scalar_columns(track, registry, n_segments,
                         skip = c("lat_ph", "lon_ph", "track"))
  }

  invisible(path)
}
