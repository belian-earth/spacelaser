# ---------------------------------------------------------------------------
# Status-quo pipeline: download + hdf5r
# ---------------------------------------------------------------------------
#
# Represents what a competent R user would write today to get a spatial
# subset of GEDI data. Uses:
#   - curl::multi_download()  — concurrent authenticated HTTPS fetches,
#     standard netrc + follow-location + cookie-jar for URS OAuth
#   - hdf5r                    — standard HDF5 reader for R
#   - data.table::rbindlist()  — fast assembly
#
# Best practices applied:
#   1. Concurrent downloads (not one-at-a-time)
#   2. Read lat/lon first, compute spatial indices, THEN targeted row
#      reads on science columns — not read-everything-then-subset
#   3. Preallocated list, rbindlist with fill = TRUE
#   4. Only reads the columns the user asked for
#
# Does NOT do caching across runs — each invocation starts with an empty
# tempdir. This matches the first-time use-case of an analyst exploring
# a region for the first time, which is the honest comparison for
# spacelaser's value proposition.

suppressPackageStartupMessages({
  library(curl)
  library(hdf5r)
  library(data.table)
})

# Read a 1D dataset at the given row indices. Thin wrapper so we have
# one obvious place to extend (e.g. add uint64 handling via bit64) later.
read_vec_hdf5r <- function(ds, idx) {
  ds[idx]
}

# Default GEDI beam names — status quo reads all 8 and lets the spatial
# filter drop empty beams.
GEDI_BEAMS <- c(
  "BEAM0000", "BEAM0001", "BEAM0010", "BEAM0011",
  "BEAM0100", "BEAM0101", "BEAM0110", "BEAM1011"
)

# Sanity-check that netrc auth is available.
check_netrc <- function() {
  home_netrc <- path.expand("~/.netrc")
  gdal_netrc <- Sys.getenv("GDAL_HTTP_NETRC_FILE", unset = "")
  for (p in c(home_netrc, gdal_netrc)) {
    if (nzchar(p) && file.exists(p)) {
      lines <- readLines(p, warn = FALSE)
      if (any(grepl("urs\\.earthdata\\.nasa\\.gov", lines))) return(invisible())
    }
  }
  cli::cli_abort(c(
    "No netrc entry for urs.earthdata.nasa.gov found.",
    "i" = "Set one up with {.code earthdatalogin::edl_netrc()}.",
    "i" = "Required for the download step of the status-quo pipeline."
  ))
}

# Concurrent authenticated downloads. curl::multi_download uses libcurl's
# native parallelism; netrc + followlocation + cookie-jar together handle
# the URS OAuth redirect chain the same way `curl --netrc --location` does
# on the command line.
download_granules <- function(urls, dest_dir) {
  dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
  paths <- file.path(dest_dir, basename(urls))
  # libcurl's cookiejar = <file> both stores and re-sends cookies across
  # the URS OAuth redirect chain; netrc = 1 sources Basic auth from
  # ~/.netrc; followlocation = 1 follows the 302s between data host
  # and urs.earthdata.nasa.gov. curl::multi_download() forwards `...`
  # straight to handle_setopt() on each internal handle.
  # If a file is already present (dev iteration, re-runs, cold-cache was
  # paid in a previous run), skip re-downloading it. Honest-benchmark
  # runs should start with an empty dest_dir; this only kicks in when
  # files are already fully there.
  needs_download <- !file.exists(paths) | file.size(paths) == 0L
  if (any(needs_download)) {
    res <- curl::multi_download(
      urls[needs_download], paths[needs_download],
      resume = FALSE,
      progress = FALSE,
      netrc = 1L,
      followlocation = 1L,
      cookiejar = tempfile(fileext = ".cookies"),
      failonerror = 1L
    )
  } else {
    res <- data.frame(success = rep(TRUE, length(urls)),
                      status_code = rep(200, length(urls)),
                      url = urls)
    cli::cli_inform("(all {length(urls)} files already cached — skipping downloads)")
  }
  failures <- res[!res$success, ]
  if (nrow(failures) > 0L) {
    cli::cli_abort(c(
      "{nrow(failures)} download{?s} failed.",
      "x" = "First: HTTP {failures$status_code[1]} from {failures$url[1]}"
    ))
  }
  paths
}

# Read one granule: open with hdf5r, iterate beams, spatial-filter via
# lat/lon read, then targeted row reads on the requested columns.
read_one_granule <- function(path, bbox, columns) {
  f <- hdf5r::H5File$new(path, mode = "r")
  on.exit(f$close_all(), add = TRUE)
  b <- unclass(bbox)

  beams_present <- intersect(GEDI_BEAMS, names(f))
  beam_tables <- vector("list", length(beams_present))
  for (i in seq_along(beams_present)) {
    bm <- beams_present[[i]]
    grp <- f[[bm]]
    # Targeted spatial filter: full read of lat/lon (~1–2 MB each),
    # then index-based reads of science columns.
    lat <- grp[["lat_lowestmode"]][]
    lon <- grp[["lon_lowestmode"]][]
    idx <- which(
      lat >= b[["ymin"]] & lat <= b[["ymax"]] &
      lon >= b[["xmin"]] & lon <= b[["xmax"]]
    )
    if (length(idx) == 0L) next

    # Preallocate columns; start with lat/lon + beam identifier.
    out <- list(
      beam = rep(bm, length(idx)),
      lat_lowestmode = lat[idx],
      lon_lowestmode = lon[idx]
    )

    for (col in columns) {
      # The column path may be under a subgroup (e.g. land_cover_data/*).
      # Resolve via spacelaser's registry so this pipeline uses exactly
      # the same HDF5 paths the spacelaser pipeline does — apples to
      # apples comparison.
      hdf5_path <- spacelaser:::.gedi_l2a_columns[[col]]
      if (is.null(hdf5_path) || !grp$exists(hdf5_path)) next
      ds <- grp[[hdf5_path]]
      dims <- ds$dims   # hdf5r reports dims in R (column-major) order:
                        # a HDF5-stored [N_shots, 101] dataset (rh) shows
                        # up here as length-2 c(101, N_shots). The SHOT
                        # dimension is always the LAST element of dims.
      shot_dim <- length(dims)  # index of the shot dimension (1 for 1D,
                                # 2 for 2D, etc.)
      vals <- if (shot_dim == 1L) {
        read_vec_hdf5r(ds, idx)
      } else {
        # 2D dataset → subset on the shot dim (last), expand bins into
        # {col}0..{col}{K-1} columns following spacelaser's convention.
        mat <- ds[, idx, drop = FALSE]   # [n_bins, length(idx)]
        n_bins <- nrow(mat)
        expanded <- lapply(seq_len(n_bins), function(j) mat[j, ])
        names(expanded) <- paste0(col, seq_len(n_bins) - 1L)
        expanded
      }
      if (shot_dim >= 2L) {
        out <- c(out, vals)
      } else {
        out[[col]] <- vals
      }
    }
    beam_tables[[i]] <- data.table::setDT(out)
  }

  data.table::rbindlist(beam_tables, fill = TRUE, use.names = TRUE)
}

# Whole pipeline: download + read, returning the combined data.table and
# phase-level timings. Does NOT include the CMR search (that's shared).
run_hdf5r_pipeline <- function(granules, bbox, columns, dest_dir) {
  check_netrc()

  t_download_start <- Sys.time()
  paths <- download_granules(granules$url, dest_dir)
  t_download_end <- Sys.time()

  t_read_start <- Sys.time()
  tables <- lapply(paths, function(p) {
    tryCatch(
      read_one_granule(p, bbox, columns),
      error = function(e) {
        cli::cli_warn("Failed to read {.file {basename(p)}}: {conditionMessage(e)}")
        NULL
      }
    )
  })
  result <- data.table::rbindlist(tables, fill = TRUE, use.names = TRUE)
  t_read_end <- Sys.time()

  bytes_downloaded <- sum(file.size(paths))

  list(
    result = result,
    seconds_download = as.numeric(t_download_end - t_download_start, units = "secs"),
    seconds_read     = as.numeric(t_read_end     - t_read_start,     units = "secs"),
    seconds_total    = as.numeric(t_read_end     - t_download_start, units = "secs"),
    bytes_downloaded = bytes_downloaded,
    n_granules       = length(paths)
  )
}
