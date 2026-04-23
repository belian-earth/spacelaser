# ---------------------------------------------------------------------------
# Download-then-read pipeline: curl::multi_download + hdf5r
# ---------------------------------------------------------------------------
#
# A well-engineered R implementation of the classic download-then-read
# approach to GEDI spatial subsetting:
#   - curl::multi_download()  — concurrent authenticated HTTPS fetches
#     (netrc + follow-location + cookie jar for URS OAuth)
#   - hdf5r                   — standard HDF5 reader for R
#   - data.table::rbindlist() — fast assembly
#
# Best practices applied:
#   1. Concurrent downloads (not one-at-a-time)
#   2. Read lat/lon first, compute spatial indices, THEN targeted row
#      reads on science columns — not read-everything-then-subset
#   3. Preallocated list, rbindlist with fill = TRUE
#   4. Only reads the columns the user asked for
#
# Invoke standalone:
#   Rscript benchmarks/bench-hdf5r.R
#
# Writes:
#   results/latest/hdf5r-timing.parquet  (1 row, tracked)
#   results/latest/hdf5r-data.parquet    (full data, gitignored)

if (!"spacelaser" %in% loadedNamespaces()) library(spacelaser)
source("benchmarks/setup.R")

suppressPackageStartupMessages({
  library(curl)
  library(hdf5r)
  library(data.table)
})

GEDI_BEAMS <- c(
  "BEAM0000", "BEAM0001", "BEAM0010", "BEAM0011",
  "BEAM0101", "BEAM0110", "BEAM1000", "BEAM1011"
)

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

download_granules <- function(urls, dest_dir) {
  dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
  paths <- file.path(dest_dir, basename(urls))

  # A cached file is only valid if it matches the server's
  # Content-Length. The earlier check (file.exists & size > 0) couldn't
  # distinguish "fully downloaded" from "partial download from an
  # interrupted prior run", which led to silent hdf5r read failures.
  # One HEAD request per cached file (~50ms each) is cheap insurance.
  needs_download <- logical(length(urls))
  for (i in seq_along(urls)) {
    if (!file.exists(paths[i]) || file.size(paths[i]) == 0L) {
      needs_download[i] <- TRUE
      next
    }
    expected <- expected_size(urls[i])
    if (is.na(expected) || file.size(paths[i]) != expected) {
      needs_download[i] <- TRUE
    }
  }

  if (any(needs_download)) {
    n <- sum(needs_download)
    cli::cli_inform("downloading {n} of {length(urls)} granule{?s}\\
                     ({length(urls) - n} cached)")
    res <- curl::multi_download(
      urls[needs_download], paths[needs_download],
      resume = FALSE,
      progress = FALSE,
      netrc = 1L,
      followlocation = 1L,
      cookiejar = tempfile(fileext = ".cookies"),
      failonerror = 1L
    )
    failures <- res[!res$success, ]
    if (nrow(failures) > 0L) {
      cli::cli_abort(c(
        "{nrow(failures)} download{?s} failed.",
        "x" = "First: HTTP {failures$status_code[1]} from {failures$url[1]}"
      ))
    }
  } else {
    cli::cli_inform("(all {length(urls)} files already cached — skipping downloads)")
  }
  paths
}

# HEAD the URL through the URS redirect chain and read the
# Content-Length of the final 200/206 response. NA on any error.
expected_size <- function(url) {
  tryCatch({
    h <- curl::new_handle(
      netrc = 1L, followlocation = 1L,
      cookiejar = tempfile(fileext = ".cookies"),
      nobody = 1L           # HEAD: skip the body, headers only
    )
    res <- curl::curl_fetch_memory(url, handle = h)
    cl <- curl::parse_headers_list(rawToChar(res$headers))[["content-length"]]
    if (is.null(cl)) return(NA_real_)
    as.numeric(cl)
  }, error = function(e) NA_real_)
}

read_vec_hdf5r <- function(ds, idx) ds[idx]

read_one_granule <- function(path, bbox, columns) {
  f <- hdf5r::H5File$new(path, mode = "r")
  on.exit(f$close_all(), add = TRUE)
  b <- unclass(bbox)

  beams_present <- intersect(GEDI_BEAMS, names(f))
  beam_tables <- vector("list", length(beams_present))
  for (i in seq_along(beams_present)) {
    bm <- beams_present[[i]]
    grp <- f[[bm]]
    lat <- grp[["lat_lowestmode"]][]
    lon <- grp[["lon_lowestmode"]][]
    idx <- which(
      lat >= b[["ymin"]] & lat <= b[["ymax"]] &
      lon >= b[["xmin"]] & lon <= b[["xmax"]]
    )
    if (length(idx) == 0L) next

    out <- list(
      beam = rep(bm, length(idx)),
      lat_lowestmode = lat[idx],
      lon_lowestmode = lon[idx]
    )
    for (col in columns) {
      hdf5_path <- spacelaser:::.gedi_l2a_columns[[col]]
      if (is.null(hdf5_path) || !grp$exists(hdf5_path)) next
      ds <- grp[[hdf5_path]]
      dims <- ds$dims
      shot_dim <- length(dims)
      if (shot_dim == 1L) {
        out[[col]] <- read_vec_hdf5r(ds, idx)
      } else {
        mat <- ds[, idx, drop = FALSE]
        n_bins <- nrow(mat)
        expanded <- lapply(seq_len(n_bins), function(j) mat[j, ])
        names(expanded) <- paste0(col, seq_len(n_bins) - 1L)
        out <- c(out, expanded)
      }
    }
    beam_tables[[i]] <- data.table::setDT(out)
  }
  data.table::rbindlist(beam_tables, fill = TRUE, use.names = TRUE)
}

run_hdf5r_pipeline <- function(granules, bbox, columns, dest_dir) {
  check_netrc()

  t_dl_start <- Sys.time()
  paths <- download_granules(granules$url, dest_dir)
  t_dl_end <- Sys.time()

  t_rd_start <- Sys.time()
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
  t_rd_end <- Sys.time()

  list(
    result = result,
    seconds_download = as.numeric(t_dl_end - t_dl_start, units = "secs"),
    seconds_read     = as.numeric(t_rd_end - t_rd_start, units = "secs"),
    seconds_total    = as.numeric(t_rd_end - t_dl_start, units = "secs"),
    bytes_downloaded = sum(file.size(paths)),
    n_granules       = length(paths)
  )
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if (sys.nframe() == 0L || !exists(".bench_sourced", inherits = FALSE)) {
  paths <- bench_paths()

  cli::cli_h2("curl::multi_download + hdf5r (download-then-read)")

  # Granule list comes from results/latest/granules.parquet, written by
  # search.R. Same input for all three pipelines, no per-pipeline CMR.
  granules <- bench_load_granules()

  # Download dir: persist across runs if requested via env var, else
  # cold cache per run.
  dest_dir <- Sys.getenv("SPACELASER_BENCH_DIR", unset = "")
  if (!nzchar(dest_dir)) {
    dest_dir <- tempfile("bench-hdf5r-")
    on.exit(unlink(dest_dir, recursive = TRUE, force = TRUE), add = TRUE)
    cli::cli_inform("download dir: {.path {dest_dir}} (cold cache)")
  } else {
    dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
    cli::cli_inform("download dir: {.path {dest_dir}} (persistent)")
  }

  run <- run_hdf5r_pipeline(granules, BENCH_BBOX, BENCH_COLUMNS, dest_dir)

  cli::cli_inform(c(
    "download: {round(run$seconds_download, 1)}s, \\
     read: {round(run$seconds_read, 1)}s, \\
     total: {round(run$seconds_total, 1)}s",
    "bytes on disk: {format(structure(run$bytes_downloaded, \\
      class = \"object_size\"), units = \"auto\")}",
    "rows: {nrow(run$result)}"
  ))

  bench_write_timing(
    pipeline         = "hdf5r",
    seconds_total    = run$seconds_total,
    seconds_download = run$seconds_download,
    seconds_read     = run$seconds_read,
    bytes_downloaded = run$bytes_downloaded,
    n_rows           = nrow(run$result),
    n_granules       = run$n_granules
  )
  bench_write_data("hdf5r", run$result)
  cli::cli_alert_success("wrote {.path {paths$timing('hdf5r')}}")
}
