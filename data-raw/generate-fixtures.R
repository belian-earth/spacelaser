# ---------------------------------------------------------------------------
# Synthetic HDF5 fixture generator
# ---------------------------------------------------------------------------
#
# Builds tiny HDF5 files (~100 KB each) that mirror the real GEDI and
# ICESat-2 schemas, for offline parser/reader testing. The generated
# files are committed to tests/testthat/fixtures/ and only need to be
# regenerated when the registry schema changes.
#
# Design principles:
#   - Pull the column list and HDF5 paths directly from the R-side
#     registry so fixtures stay in lockstep.
#   - Use small chunk dimensions so the test file is small but still
#     contains multiple chunks per dataset — exercises multi-chunk
#     navigation without bloating file size.
#   - Use gzip compression to exercise the deflate+shuffle filter path.
#   - Generate lat/lon that deliberately straddles the standard test
#     bbox so that spatial filtering has something to filter.
#
# Run from the package root:
#   Rscript data-raw/generate-fixtures.R

suppressPackageStartupMessages({
  library(hdf5r)
  devtools::load_all(quiet = TRUE)
})

FIXTURE_DIR <- file.path("tests", "testthat", "fixtures")
dir.create(FIXTURE_DIR, recursive = TRUE, showWarnings = FALSE)

# Fix the RNG so regeneration is byte-stable. Changes to fixture files
# in git then reflect real schema/generator changes, not random noise.
set.seed(20260414L)

# Test bbox used by fixture tests — bakes the same extent the integration
# suite uses. Half the synthesized shots land inside, half outside, so
# spatial filtering has work to do.
BBOX <- list(xmin = -124.04, ymin = 41.39, xmax = -124.01, ymax = 41.42)

# Default synthetic dimensions. Small but big enough to span multiple
# 100-row chunks.
N_SHOTS <- 500L
N_RH_BINS <- 101L

# ---------------------------------------------------------------------------
# Per-column synthesis
# ---------------------------------------------------------------------------
#
# Given a short column name and its HDF5 path, return a plausible vector
# of length n_shots. Values don't need to be scientifically meaningful —
# just have the right dtype and range so the reader can parse them.
# Special names (lat/lon/rh) have dedicated handlers to keep the file
# spatially coherent and the 2D layout correct.

synth_values <- function(name, n) {
  # Integer-like flags / IDs (by short name, not path). Everything else
  # defaults to float, which covers most GEDI/ICESat-2 variables.
  int_cols <- c(
    "master_int", "degrade_flag", "quality_flag",
    "l2a_quality_flag", "l2b_quality_flag",
    "surface_flag", "elevation_bias_flag",
    "num_detectedmodes", "selected_algorithm", "selected_mode",
    "selected_mode_flag",
    "selected_l2a_algorithm", "selected_rg_algorithm",
    "leaf_off_doy", "leaf_off_flag", "leaf_on_cycle", "leaf_on_doy",
    "pft_class", "region_class", "urban_focal_window_size",
    "urban_proportion",
    "rx_sample_start_index", "rx_sample_count"
  )

  if (name == "shot_number") {
    # 64-bit integer sequence, stored as double (hdf5r handles)
    return(seq_len(n) + 1e12)
  }
  if (name %in% int_cols) {
    return(sample.int(10L, n, replace = TRUE))
  }
  # Fallback: floats (most numeric science vars)
  as.numeric(runif(n, 0, 100))
}

# Write a lat/lon pair into a group (possibly under a subgroup).
#
# First half of shots land inside the test bbox, second half outside,
# so spatial filtering has work to do. Interleaved positions are
# avoided on purpose so the indices-to-ranges merging gets the common
# "contiguous run" case.
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

# Write a 2D dataset with HDF5 shape [n_shots, n_cols] by passing the
# matrix in hdf5r's expected dim order (hdf5r transposes dims on
# write, so we pre-transpose to compensate).
write_2d <- function(group, path, n_shots, n_cols, min = 0, max = 1,
                      chunk_shots = 100L) {
  m <- matrix(runif(n_shots * n_cols, min, max),
              nrow = n_cols, ncol = n_shots)
  group$create_dataset(path, robj = m,
                       chunk_dims = c(n_cols, chunk_shots),
                       gzip_level = 4)
}

# Given a registry (named char vec of short -> hdf5 path), write every
# scalar column to the beam group at its registry path, creating
# parent subgroups on demand. Known-2D and pool columns listed in the
# `skip` vector are filtered out so they can be handled separately.
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

# ---------------------------------------------------------------------------
# Fixture: GEDI L2A
# ---------------------------------------------------------------------------

make_gedi_l2a <- function(path, n_shots = N_SHOTS, n_rh_bins = N_RH_BINS,
                          beams = c("BEAM0000", "BEAM0101")) {
  if (file.exists(path)) file.remove(path)
  f <- H5File$new(path, mode = "w")
  on.exit(f$close_all(), add = TRUE)

  registry <- spacelaser:::.gedi_l2a_columns

  for (beam_name in beams) {
    beam <- f$create_group(beam_name)

    # lat/lon at beam root (L2A / L4A / L4C pattern)
    write_latlon(beam, subgroup = "",
                 lat_name = "lat_lowestmode", lon_name = "lon_lowestmode",
                 n_shots = n_shots)

    # 2D rh: HDF5 shape [n_shots, 101] of plausible height values (0-50m)
    write_2d(beam, "rh", n_shots, n_rh_bins, max = 50)

    # Everything else (including land_cover_data/* subgroup)
    write_scalar_columns(beam, registry, n_shots,
                         skip = c("lat_lowestmode", "lon_lowestmode", "rh",
                                  "beam"))  # `beam` is R-side synthesized
  }

  invisible(path)
}

# ---------------------------------------------------------------------------
# Fixture: GEDI L2B
# ---------------------------------------------------------------------------
#
# L2B differs from L2A in three reader-relevant ways:
#   - lat/lon live under `geolocation/` (not beam root)
#   - `cover_z`, `pai_z`, `pavd_z` are 2D [N, 30] profile datasets
#   - `pgap_theta_z` is a pool column: a flat 1D array of concatenated
#     per-shot samples, indexed via `rx_sample_start_index`
#     (1-based) + `rx_sample_count`.

make_gedi_l2b <- function(path, n_shots = N_SHOTS,
                          beams = c("BEAM0000", "BEAM0101"),
                          n_profile_bins = 30L, samples_per_shot = 30L) {
  if (file.exists(path)) file.remove(path)
  f <- H5File$new(path, mode = "w")
  on.exit(f$close_all(), add = TRUE)

  registry <- spacelaser:::.gedi_l2b_columns

  # Columns handled specially (not by write_scalar_columns)
  special_cols <- c(
    "lat_lowestmode", "lon_lowestmode",
    "cover_z", "pai_z", "pavd_z",
    "pgap_theta_z",
    "rx_sample_start_index", "rx_sample_count",
    "beam"
  )

  for (beam_name in beams) {
    beam <- f$create_group(beam_name)

    # lat/lon in geolocation/ subgroup (L2B-specific path)
    write_latlon(beam, subgroup = "geolocation",
                 lat_name = "lat_lowestmode", lon_name = "lon_lowestmode",
                 n_shots = n_shots)

    # 2D profile datasets, HDF5 shape [n_shots, 30]
    for (nm in c("cover_z", "pai_z", "pavd_z")) {
      write_2d(beam, nm, n_shots, n_profile_bins, max = 10)
    }

    # Pool column setup: each shot contributes `samples_per_shot` samples
    # laid out contiguously in a flat pool dataset. Start indices are
    # 1-based (Fortran convention preserved in GEDI files).
    starts <- seq.int(1L, by = samples_per_shot, length.out = n_shots)
    counts <- rep(samples_per_shot, n_shots)
    pool_len <- n_shots * samples_per_shot
    pool_values <- runif(pool_len, 0, 1)

    beam$create_dataset("rx_sample_start_index", robj = starts,
                        chunk_dims = 100L, gzip_level = 4)
    beam$create_dataset("rx_sample_count", robj = counts,
                        chunk_dims = 100L, gzip_level = 4)
    beam$create_dataset("pgap_theta_z", robj = pool_values,
                        chunk_dims = min(pool_len, 1000L),
                        gzip_level = 4)

    # Everything else: scalar columns at root and under geolocation/,
    # land_cover_data/. write_scalar_columns creates parent subgroups
    # on demand.
    write_scalar_columns(beam, registry, n_shots, skip = special_cols)
  }

  invisible(path)
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

report_size <- function(path) {
  message(sprintf("  → %s (%s)",
                  path,
                  format(structure(file.info(path)$size, class = "object_size"),
                         units = "auto")))
}

message("Generating synthetic GEDI L2A fixture")
l2a_path <- file.path(FIXTURE_DIR, "gedi-l2a.h5")
make_gedi_l2a(l2a_path)
report_size(l2a_path)

message("Generating synthetic GEDI L2B fixture")
l2b_path <- file.path(FIXTURE_DIR, "gedi-l2b.h5")
make_gedi_l2b(l2b_path)
report_size(l2b_path)

message("Done.")
