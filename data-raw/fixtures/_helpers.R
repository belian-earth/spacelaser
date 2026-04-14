# ---------------------------------------------------------------------------
# Shared helpers for synthetic fixture generation
# ---------------------------------------------------------------------------
#
# Sourced by the per-product scripts (gedi-l2a.R, gedi-l2b.R, ...). Defines
# the constants and utility functions they all rely on so the per-product
# scripts stay focused on the schema differences that actually matter.
#
# hdf5r transposition quirk: when you pass an R matrix with dim c(a, b),
# hdf5r writes an HDF5 dataset with shape [b, a]. Chunk dims transpose the
# same way. All helpers in this file handle that by taking the desired
# *HDF5* shape as input and doing the pre-transposition internally, so
# callers don't have to think about it.

# Test bbox used across fixtures. Synthesized shots straddle this: half
# inside, half outside. Kept in one place so tests and generators agree.
BBOX <- list(xmin = -124.04, ymin = 41.39, xmax = -124.01, ymax = 41.42)

# Default shot count per beam. Large enough to span multiple chunks with
# a 100-row chunk size, small enough to keep fixtures under 2 MB.
N_SHOTS <- 500L

# Synthesize plausible values for a column. Integer flag/ID names are
# listed explicitly; everything else defaults to float, which covers
# most GEDI/ICESat-2 variables.
synth_values <- function(name, n) {
  int_cols <- c(
    # Bit-flag / enumeration scalars
    "master_int", "degrade_flag", "quality_flag",
    "l2a_quality_flag", "l2b_quality_flag", "l4_quality_flag",
    "surface_flag", "elevation_bias_flag", "stale_return_flag",
    "num_detectedmodes", "selected_algorithm", "selected_mode",
    "selected_mode_flag",
    "selected_l2a_algorithm", "selected_rg_algorithm",
    "leaf_off_doy", "leaf_off_flag", "leaf_on_cycle", "leaf_on_doy",
    "pft_class", "region_class", "urban_focal_window_size",
    "urban_proportion",
    "channel", "degrade", "tx_egflag", "tx_pulseflag",
    "selection_stretchers_x", "selection_stretchers_y", "th_left_used",
    # Pool-column indices
    "rx_sample_start_index", "rx_sample_count",
    "tx_sample_start_index", "tx_sample_count"
  )

  if (name == "shot_number") {
    # 64-bit integer sequence, stored as double (hdf5r handles)
    return(seq_len(n) + 1e12)
  }
  if (name %in% int_cols) {
    return(sample.int(10L, n, replace = TRUE))
  }
  # Default: floats (most numeric science vars)
  as.numeric(runif(n, 0, 100))
}

# Write a lat/lon pair into a group (possibly under a subgroup).
# First half of shots land inside BBOX, second half outside — exercises
# spatial filtering with a contiguous-run shape (the common case).
write_latlon <- function(group, subgroup, lat_name, lon_name, n_shots) {
  inside_n <- ceiling(n_shots / 2)
  outside_n <- n_shots - inside_n
  lat <- c(
    runif(inside_n,  BBOX$ymin + 0.002, BBOX$ymax - 0.002),
    runif(outside_n, BBOX$ymax + 0.01,  BBOX$ymax + 0.05)
  )
  lon <- c(
    runif(inside_n,  BBOX$xmin + 0.002, BBOX$xmax - 0.002),
    runif(outside_n, BBOX$xmax + 0.01,  BBOX$xmax + 0.05)
  )

  target <- if (nzchar(subgroup)) {
    if (!group$exists(subgroup)) group$create_group(subgroup)
    group[[subgroup]]
  } else {
    group
  }
  target$create_dataset(lat_name, robj = lat,
                        chunk_dims = 100L, gzip_level = 4)
  target$create_dataset(lon_name, robj = lon,
                        chunk_dims = 100L, gzip_level = 4)
}

# Write a 2D dataset with HDF5 shape [n_shots, n_cols]. Callers think
# in HDF5 axes; this function handles the hdf5r transposition.
write_2d <- function(group, path, n_shots, n_cols, min = 0, max = 1,
                      chunk_shots = 100L) {
  m <- matrix(runif(n_shots * n_cols, min, max),
              nrow = n_cols, ncol = n_shots)
  group$create_dataset(path, robj = m,
                       chunk_dims = c(n_cols, chunk_shots),
                       gzip_level = 4)
}

# Write an integer 2D dataset with HDF5 shape [n_cats, n_shots] for
# the transposed-column pattern (L1B surface_type is the only user of
# this in the current registries). Data is an R matrix with dim
# c(n_shots, n_cats); hdf5r transposes it.
write_transposed_2d <- function(group, path, data, chunk_shots = 100L) {
  n_shots <- nrow(data)
  n_cats <- ncol(data)
  group$create_dataset(path, robj = data,
                       chunk_dims = c(chunk_shots, n_cats),
                       gzip_level = 4)
}

# Write a pool dataset (flat 1D array) plus its per-shot index
# columns. GEDI pool indices are 1-based (Fortran heritage); generated
# starts reflect that so the reader's 1→0 offset conversion is
# exercised realistically.
write_pool <- function(beam, pool_name, start_col, count_col,
                        n_shots, samples_per_shot, min = 0, max = 1) {
  starts <- seq.int(1L, by = samples_per_shot, length.out = n_shots)
  counts <- rep(samples_per_shot, n_shots)
  pool_len <- n_shots * samples_per_shot
  pool_values <- runif(pool_len, min, max)

  beam$create_dataset(start_col, robj = starts,
                      chunk_dims = 100L, gzip_level = 4)
  beam$create_dataset(count_col, robj = counts,
                      chunk_dims = 100L, gzip_level = 4)
  beam$create_dataset(pool_name, robj = pool_values,
                      chunk_dims = min(pool_len, 5000L),
                      gzip_level = 4)
}

# Given a column registry (named char vec short → hdf5 path), write
# every column at its registry path, creating parent subgroups on
# demand. Names in `skip` are filtered out (e.g. columns handled
# specially by the caller: lat/lon, 2D, pool, transposed).
write_scalar_columns <- function(beam, registry, n_shots, skip = character()) {
  scalar_names <- setdiff(names(registry), skip)
  for (nm in scalar_names) {
    hdf5_path <- registry[[nm]]
    vals <- synth_values(nm, n_shots)

    parts <- strsplit(hdf5_path, "/", fixed = TRUE)[[1]]
    if (length(parts) > 1L) {
      parent_path <- paste(parts[-length(parts)], collapse = "/")
      if (!beam$exists(parent_path)) beam$create_group(parent_path)
    }
    beam$create_dataset(hdf5_path, robj = vals,
                        chunk_dims = 100L, gzip_level = 4)
  }
}

# Report a generated fixture's path and size.
report_size <- function(path) {
  message(sprintf("  → %s (%s)",
                  path,
                  format(structure(file.info(path)$size, class = "object_size"),
                         units = "auto")))
}
