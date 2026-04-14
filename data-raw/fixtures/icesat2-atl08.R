# ---------------------------------------------------------------------------
# ICESat-2 ATL08 fixture
# ---------------------------------------------------------------------------
#
# ATL08 is the richest ICESat-2 product in terms of groups:
#   - six ground tracks (gt1l, gt1r, gt2l, gt2r, gt3l, gt3r)
#     — we use two (gt1l, gt2r) in the fixture to keep it small
#   - lat/lon at `land_segments/latitude` / `land_segments/longitude`
#   - nested subgroups: `land_segments/canopy/*` and
#     `land_segments/terrain/*`
#   - two 2D datasets under canopy/: canopy_h_metrics and
#     canopy_h_metrics_abs, each HDF5 shape [N, 18]
#
# No pool columns, no transposed columns.

make_icesat2_atl08 <- function(path, n_segments = N_SHOTS,
                               tracks = c("gt1l", "gt2r"),
                               n_canopy_metrics = 18L) {
  if (file.exists(path)) file.remove(path)
  f <- hdf5r::H5File$new(path, mode = "w")
  on.exit(f$close_all(), add = TRUE)

  registry <- spacelaser:::.icesat2_atl08_columns

  special_cols <- c(
    "latitude", "longitude",
    "canopy_h_metrics", "canopy_h_metrics_abs",
    "track"  # R-side synthesized from the group name
  )

  for (track_name in tracks) {
    track <- f$create_group(track_name)

    # lat/lon under land_segments/
    write_latlon(track, subgroup = "land_segments",
                 lat_name = "latitude", lon_name = "longitude",
                 n_shots = n_segments)

    # Two 2D canopy metric datasets, HDF5 shape [n_segments, 18],
    # under land_segments/canopy/. h_metrics is relative heights
    # (0-30 m plausible range), h_metrics_abs is absolute heights.
    write_2d(track, "land_segments/canopy/canopy_h_metrics",
             n_segments, n_canopy_metrics, max = 30)
    write_2d(track, "land_segments/canopy/canopy_h_metrics_abs",
             n_segments, n_canopy_metrics, max = 30)

    # Everything else: scalars under land_segments/,
    # land_segments/canopy/, land_segments/terrain/
    write_scalar_columns(track, registry, n_segments, skip = special_cols)
  }

  invisible(path)
}
