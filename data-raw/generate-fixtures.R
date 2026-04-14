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
  # Float-ish columns: default to runif
  float_cols <- c(
    "delta_time", "master_frac", "sensitivity",
    "solar_azimuth", "solar_elevation",
    "elev_lowestmode", "elev_highestreturn",
    "energy_total", "elevation_bin0_error",
    "lat_highestreturn", "latitude_bin0_error",
    "lon_highestreturn", "longitude_bin0_error",
    "digital_elevation_model", "digital_elevation_model_srtm",
    "mean_sea_surface",
    "landsat_treecover", "landsat_water_persistence",
    "modis_nonvegetated", "modis_nonvegetated_sd",
    "modis_treecover", "modis_treecover_sd"
  )
  # Integer-like flags / IDs
  int_cols <- c(
    "master_int", "degrade_flag", "quality_flag",
    "surface_flag", "elevation_bias_flag",
    "num_detectedmodes", "selected_algorithm", "selected_mode",
    "selected_mode_flag",
    "leaf_off_doy", "leaf_off_flag", "leaf_on_cycle", "leaf_on_doy",
    "pft_class", "region_class", "urban_focal_window_size",
    "urban_proportion"
  )

  if (name == "shot_number") {
    # 64-bit integer sequence, stored as double (hdf5r handles)
    return(seq_len(n) + 1e12)
  }
  if (name %in% float_cols) {
    return(as.numeric(runif(n, 0, 100)))
  }
  if (name %in% int_cols) {
    return(sample.int(10L, n, replace = TRUE))
  }
  # Fallback: floats
  as.numeric(runif(n, 0, 100))
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

    # lat/lon: straddle the bbox. First half inside, second half outside.
    # Using interleaved positions exercises the indices-to-ranges merging.
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

    beam$create_dataset("lat_lowestmode", robj = lat,
                        chunk_dims = 100L, gzip_level = 4)
    beam$create_dataset("lon_lowestmode", robj = lon,
                        chunk_dims = 100L, gzip_level = 4)

    # 2D rh: HDF5 shape [n_shots, 101] (row-major: rows = shots, cols = bins).
    # hdf5r transposes dims on write, so pass R matrix with dim c(n_bins,
    # n_shots) to land on the desired HDF5 layout.
    rh <- matrix(runif(n_shots * n_rh_bins, 0, 50),
                 nrow = n_rh_bins, ncol = n_shots)
    beam$create_dataset("rh", robj = rh,
                        chunk_dims = c(n_rh_bins, 100L),
                        gzip_level = 4)

    # All other registry columns. Handle land_cover_data subgroup.
    scalar_names <- setdiff(
      names(registry),
      c("lat_lowestmode", "lon_lowestmode", "rh")
    )
    for (nm in scalar_names) {
      hdf5_path <- registry[[nm]]
      vals <- synth_values(nm, n_shots)

      # Create parent subgroup (e.g. land_cover_data/) if needed
      parts <- strsplit(hdf5_path, "/", fixed = TRUE)[[1]]
      if (length(parts) > 1L) {
        parent_path <- paste(parts[-length(parts)], collapse = "/")
        if (!beam$exists(parent_path)) {
          beam$create_group(parent_path)
        }
      }

      beam$create_dataset(hdf5_path, robj = vals,
                          chunk_dims = 100L, gzip_level = 4)
    }
  }

  invisible(path)
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

message("Generating synthetic GEDI L2A fixture")
l2a_path <- file.path(FIXTURE_DIR, "gedi-l2a.h5")
make_gedi_l2a(l2a_path)
message(sprintf("  → %s (%s)",
                l2a_path,
                format(structure(file.info(l2a_path)$size, class = "object_size"),
                       units = "auto")))

message("Done.")
