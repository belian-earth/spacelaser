# ---------------------------------------------------------------------------
# ICESat-2 ATL03 fixture
# ---------------------------------------------------------------------------
#
# ATL03 is structurally unique in the reader — it's the only product
# that uses the segment-index spatial filter code path rather than a
# direct lat/lon scan. The reader:
#   1. reads segment-level `geolocation/reference_photon_{lat,lon}`
#      (small, ~n_segments elements)
#   2. filters segments to bbox
#   3. reads `ph_index_beg` (1-based) and `segment_ph_cnt` for the
#      matching segments
#   4. expands those into photon-level row ranges in `heights/*`
#   5. reads the requested photon-level columns over those ranges
#
# The fixture mirrors this two-tier layout: per-segment reference
# lat/lon straddles the test bbox, each segment contains a fixed
# number of photons, and each photon inherits its segment's location
# (with light jitter) so segment-level and photon-level spatial
# assertions agree.
#
# Also exercises:
#   - `signal_conf_ph` 2D HDF5 shape [n_photons, 5]
#   - photon-level integer datasets (quality_ph, signal_class_ph, ...)

make_icesat2_atl03 <- function(path, n_segments = 200L,
                                tracks = c("gt1l", "gt2r"),
                                photons_per_segment = 20L) {
  if (file.exists(path)) file.remove(path)
  f <- hdf5r::H5File$new(path, mode = "w")
  on.exit(f$close_all(), add = TRUE)

  registry <- spacelaser:::.icesat2_atl03_columns

  for (track_name in tracks) {
    track <- f$create_group(track_name)

    # ---- Segment-level geolocation (~n_segments rows, small) ----
    #
    # Half inside bbox, half outside. Matches the pattern the other
    # fixtures use, so assertions can be written the same way.
    inside_n <- ceiling(n_segments / 2)
    outside_n <- n_segments - inside_n
    seg_lat <- c(
      runif(inside_n,  BBOX$ymin + 0.005, BBOX$ymax - 0.005),
      runif(outside_n, BBOX$ymax + 0.01,  BBOX$ymax + 0.05)
    )
    seg_lon <- c(
      runif(inside_n,  BBOX$xmin + 0.005, BBOX$xmax - 0.005),
      runif(outside_n, BBOX$xmax + 0.01,  BBOX$xmax + 0.05)
    )

    # ph_index_beg is 1-based (Fortran heritage preserved in ICESat-2
    # files); segment i starts at photon index 1 + (i - 1) * K.
    n_photons <- n_segments * photons_per_segment
    starts <- seq.int(1L, by = photons_per_segment, length.out = n_segments)
    counts <- rep(photons_per_segment, n_segments)

    geo <- track$create_group("geolocation")
    geo$create_dataset("reference_photon_lat", robj = seg_lat,
                       chunk_dims = 100L, gzip_level = 4)
    geo$create_dataset("reference_photon_lon", robj = seg_lon,
                       chunk_dims = 100L, gzip_level = 4)
    geo$create_dataset("ph_index_beg", robj = starts,
                       chunk_dims = 100L, gzip_level = 4)
    geo$create_dataset("segment_ph_cnt", robj = counts,
                       chunk_dims = 100L, gzip_level = 4)

    # ---- Photon-level data (~n_photons rows) ----
    #
    # Each photon inherits its segment's reference lat/lon with small
    # jitter — keeps the photon-level bbox assertions consistent with
    # the segment-level filter. Jitter is much smaller than the 0.005
    # safety margin used when generating seg_lat/seg_lon, so "inside"
    # segments' photons stay inside the bbox (no R-side post-filter
    # drops needed for the common case).
    ph_lat <- rep(seg_lat, each = photons_per_segment) +
              runif(n_photons, -0.0005, 0.0005)
    ph_lon <- rep(seg_lon, each = photons_per_segment) +
              runif(n_photons, -0.0005, 0.0005)

    heights <- track$create_group("heights")
    heights$create_dataset("lat_ph", robj = ph_lat,
                           chunk_dims = 500L, gzip_level = 4)
    heights$create_dataset("lon_ph", robj = ph_lon,
                           chunk_dims = 500L, gzip_level = 4)

    # 2D signal_conf_ph: HDF5 [n_photons, 5]. Integer-valued confidence
    # class in the real data (0-4); we store float here because write_2d
    # uses runif — the reader's 2D expansion doesn't care about the
    # exact dtype as long as bytes parse, and fill-value handling is
    # exercised elsewhere.
    write_2d(track, "heights/signal_conf_ph",
             n_shots = n_photons, n_cols = 5L, max = 4,
             chunk_shots = 500L)

    # Remaining heights/* photon-level columns
    write_scalar_columns(
      track, registry, n_shots = n_photons,
      skip = c("lat_ph", "lon_ph", "signal_conf_ph", "track")
    )
  }

  invisible(path)
}
