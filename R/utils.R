# ---------------------------------------------------------------------------
# S3 generic: sl_read()
# ---------------------------------------------------------------------------

#' Read satellite lidar data
#'
#' `sl_read()` is an S3 generic that reads GEDI or ICESat-2 data from remote
#' HDF5 files using HTTP range requests. Only the chunks intersecting the
#' bounding box are fetched; no full-file download. It dispatches on the type
#' of its first argument:
#'
#' * An `sl_gedi_search` or `sl_icesat2_search` object (from [sl_search()]):
#'   reads all granules in the search result, combining rows into a single
#'   data frame. The search bbox is used by default; an explicit `bbox` may
#'   be supplied to subset further but must be contained within it.
#' * A character vector of URLs: auto-detects the sensor and product from the
#'   file name, or uses the explicit `product` argument.
#'
#' All beams (GEDI) or ground tracks (ICESat-2) are always read; the returned
#' data frame includes a `beam` (GEDI) or `track` (ICESat-2) identifier
#' column for post-hoc filtering with [dplyr::filter()] or base subsetting.
#'
#' @param x An `sl_gedi_search`, `sl_icesat2_search`, or character vector of
#'   HDF5 URLs.
#' @param bbox An `sl_bbox` or numeric `c(xmin, ymin, xmax, ymax)`. Required
#'   when `x` is a character vector. Optional when `x` is a search result: it
#'   defaults to the bbox the search was performed with. If supplied, it
#'   must be fully contained within the search bbox; supplying a wider bbox
#'   is an error to avoid silently missing data outside the original search.
#' @param columns Character vector of column names to read (short names from
#'   [sl_columns()]). Latitude and longitude are always included
#'   automatically. `NULL` (default) reads the curated default column set
#'   for the product (see [sl_columns()] with `set = "default"`). Pass
#'   `names(sl_columns(product))` to read all available columns.
#' @param convert_time Logical. If `TRUE` (default), the raw `delta_time`
#'   column (seconds since the GEDI / ICESat-2 reference epoch of
#'   2018-01-01 00:00:00 UTC) is converted to a POSIXct column named
#'   `time`. Set to `FALSE` to keep `delta_time` as the raw numeric
#'   seconds-since-epoch value, e.g. if you need to compare against the
#'   file-level epoch exactly, want to avoid the POSIXct conversion
#'   overhead on very large photon-level reads, or want to preserve the
#'   original HDF5 column name.
#' @param ... Reserved for method-specific arguments and forwarding.
#'
#' @returns A data frame with one row per footprint (GEDI) or
#'   segment/photon (ICESat-2). Columns depend on the product and the
#'   `columns` argument. Fill-value sentinels (-9999, 3.4e38, etc.) are
#'   automatically replaced with `NA`.
#'
#' @details
#' ## Default columns
#'
#' When `columns = NULL`, a curated default set is returned for each
#' product. Use `sl_columns(product, set = "default")` to see which
#' columns are included. The defaults are designed to cover the primary
#' science variables, key quality flags, and basic context without
#' surprises. Use `sl_columns(product, set = "all")` to discover
#' everything available.
#'
#' ## Product-specific notes
#'
#' **GEDI L1B**: The default set includes `rxwaveform`, which is a
#' **list column** (one numeric vector per shot containing the received
#' waveform). Use [sl_extract_waveforms()] to expand this into a
#' long-form data frame with per-sample elevations. The transmitted
#' waveform (`txwaveform`) is available via explicit request but not
#' included in defaults. L1B reads are slower than other products
#' because waveform data requires targeted chunk reads into the pool
#' dataset.
#'
#' **GEDI L2A**: The `rh` column is a 2D dataset \[N, 101\] that
#' expands into 101 columns (`rh0` through `rh100`), representing
#' relative height percentiles in metres. This is included in the
#' default set.
#'
#' **GEDI L2B**: The default set includes `cover_z`, `pai_z`, and
#' `pavd_z`, which are 2D datasets \[N, 30\] representing canopy
#' vertical profiles at 5 m height bins. Each expands to 30 columns
#' (e.g. `cover_z0` through `cover_z29`), adding 90 columns to the
#' output. `pgap_theta_z` is a variable-length list column (similar to
#' L1B waveforms) and is not included in defaults; request it
#' explicitly when needed.
#'
#' **GEDI L2B `rh100`**: Stored in centimetres in the HDF5 file;
#' automatically converted to metres for consistency with L2A.
#'
#' **GEDI L4A**: The `agbd` column is above-ground biomass density in
#' Mg/ha. Prediction intervals (`agbd_pi_lower`, `agbd_pi_upper`) and
#' standard error (`agbd_se`) are included in defaults.
#'
#' **GEDI L4C**: Waveform Structural Complexity Index. The `wsci`
#' column is the headline metric, with prediction intervals
#' (`wsci_pi_lower`, `wsci_pi_upper`) and decomposed XY/Z components
#' (`wsci_xy`, `wsci_z`) in defaults. `worldcover_class` provides the
#' ESA WorldCover land-cover class.
#'
#' **ICESat-2 ATL03**: Photon-level data. A single granule can contain
#' millions of photons. The reader uses ATL03's segment-level spatial
#' index (`geolocation/reference_photon_lat` etc.) to filter at segment
#' rate before reading photon-level columns, so spatial subsets stay
#' fast even on large bboxes. `signal_conf_ph` is a 2D column
#' \[N, 5\] (5 surface types: land, ocean, sea ice, land ice, inland
#' water) that expands to 5 columns.
#'
#' **ICESat-2 ATL06**: Land ice elevation segments. The default set
#' includes fit statistics (`n_fit_photons`, `h_robust_sprd`, `snr`)
#' and reference DEM height (`dem_h`). Tidal and geophysical
#' corrections are available via `sl_columns("ATL06")` but not in
#' defaults.
#'
#' **ICESat-2 ATL07**: Sea-ice height segments. Defaults include
#' segment height + confidence + quality, photon rate, AMSR2 ice
#' concentration, and atmospheric flags. Geolocation parameters
#' (`solar_*`, `sigma_h`) and finer geophysical corrections live
#' under `sea_ice_segments/{geolocation,geophysical,stats}/` and are
#' available via `sl_columns("ATL07", set = "all")`.
#'
#' **ICESat-2 ATL08**: The default set includes `canopy_h_metrics`, a
#' 2D dataset \[N, 18\] of canopy height percentiles (P10 through P95)
#' that expands to 18 columns. Terrain slope, photon counts, and land
#' cover are also included. `*_abs` (absolute height) variants and
#' the secondary canopy / terrain metrics are in the registry but not
#' in defaults.
#'
#' **ICESat-2 ATL10**: Sea-ice freeboard. Defaults include
#' `beam_fb_height` (per-beam freeboard) plus quality, confidence,
#' and the underlying ATL07 height-segment context.
#'
#' **ICESat-2 ATL13**: Inland water surface heights. Defaults include
#' water-surface height + standard deviation, significant wave height,
#' water depth, segment provenance (`inland_water_body_*`), and
#' quality flags. Geometry uses the segment centroid (`segment_lat`,
#' `segment_lon`).
#'
#' **ICESat-2 ATL24**: Near-shore bathymetry, photon-level. Defaults
#' include orthometric / ellipsoidal / surface heights, photon class
#' and confidence, and the THU / TVU positional uncertainty pair.
#' Geometry uses photon coordinates (`lat_ph`, `lon_ph`).
#'
#' ## Performance tuning
#'
#' The default read path scans every beam's full lat/lon dataset to
#' build each beam's spatial filter — a safe, simple strategy that
#' works for every product and every orbit geometry.
#'
#' An opt-in **cross-beam scan** optimisation is available for
#' workloads where one beam's shot-index range can predict the others'
#' (GEDI and most ICESat-2 products). Instead of scanning all 8 beams
#' per granule, one reference beam scans an inflated latitude band and
#' the other beams dense-read the resulting shot-index range. Output
#' is bitwise identical; HTTP request and byte counts drop by ~50 %.
#'
#' **Wall-time impact is DAAC-dependent:**
#' * ORNL-DAAC (hosts L4A / L4C): ~23 % faster on a typical small bbox
#' * LP.DAAC (hosts L1B / L2A / L2B): slightly slower (~10 %) because
#'   LP.DAAC's CloudFront distribution rate-limits aggressively and
#'   cross-beam's serial critical path pays more in underutilised
#'   pool capacity than it saves in bytes
#'
#' Enable per session:
#' ```r
#' options(spacelaser.cross_beam_scan = TRUE)
#' ```
#'
#' Or persistently for every R session by adding that line to your
#' `.Rprofile`.
#'
#' The option is off by default. Consider enabling it when your
#' workload is dominated by L4A / L4C reads, or when you care more
#' about NASA server load (both byte and request counts halve) than
#' about marginal wall-time differences on LP.DAAC.
#'
#' @seealso [sl_search()], [sl_columns()], [sl_extract_waveforms()]
#' @export
sl_read <- function(x, bbox, ...) {
  UseMethod("sl_read")
}

#' @rdname sl_read
#' @export
sl_read.sl_gedi_search <- function(x, bbox = NULL, columns = NULL,
                                   convert_time = TRUE, ...) {
  search_bbox <- attr(x, "bbox")
  bbox <- bbox %||% search_bbox
  check_bbox_within(bbox, search_bbox)
  product <- attr(x, "product")
  lat_lon <- gedi_lat_lon(product)

  read_product_multi(
    urls = x$url,
    product = product,
    bbox = bbox,
    columns = columns,
    rust_multi_fn = rust_read_gedi_multi,
    lat_col = lat_lon$lat,
    lon_col = lat_lon$lon,
    group_label = "beam",
    element_label = "footprint",
    convert_time = convert_time
  )
}

#' @rdname sl_read
#' @export
sl_read.sl_icesat2_search <- function(x, bbox = NULL, columns = NULL,
                                      convert_time = TRUE, ...) {
  search_bbox <- attr(x, "bbox")
  bbox <- bbox %||% search_bbox
  check_bbox_within(bbox, search_bbox)
  product <- attr(x, "product")
  geo_cols <- icesat2_lat_lon(product)

  read_product_multi(
    urls = x$url,
    product = product,
    bbox = bbox,
    columns = columns,
    rust_multi_fn = rust_read_icesat2_multi,
    lat_col = geo_cols$lat,
    lon_col = geo_cols$lon,
    group_label = "track",
    element_label = "element",
    convert_time = convert_time
  )
}

#' @rdname sl_read
#' @param product Character. Product level (e.g., `"L2A"`, `"ATL08"`).
#'   Required when `x` is a character vector and the product cannot be
#'   inferred from the file name.
#' @export
sl_read.character <- function(x, bbox, ..., product = NULL) {
  rlang::check_required(bbox)
  info <- detect_sensor(x[[1L]], product)
  read_urls(
    x,
    bbox = bbox,
    read_fn = info$read_fn,
    product = info$product,
    ...
  )
}

#' @export
sl_read.default <- function(x, bbox, ...) {
  cls <- paste(class(x), collapse = "/")
  cli::cli_abort(c(
    "{.fun sl_read} does not know how to handle an object of class {.cls {cls}}.",
    "i" = "Expected an {.cls sl_gedi_search}, {.cls sl_icesat2_search}, or {.cls character} URL vector."
  ))
}

#' Read multiple URLs and row-bind results.
#' @noRd
read_urls <- function(urls, bbox, read_fn, product, ...) {
  urls <- urls[!is.na(urls)]
  if (length(urls) == 0L) {
    cli::cli_inform("No URLs to read.")
    return(tibble::tibble())
  }

  results <- lapply(urls, function(u) {
    tryCatch(
      read_fn(url = u, product = product, bbox = bbox, ...),
      error = function(e) {
        cli::cli_warn(c(
          "!" = "Failed to read {.file {basename(u)}}.",
          "i" = conditionMessage(e)
        ))
        NULL
      }
    )
  })

  results <- Filter(Negate(is.null), results)
  if (length(results) == 0L) {
    return(tibble::tibble())
  }

  vctrs::vec_rbind(!!!results)
}

#' Detect sensor and product from a URL filename.
#' @noRd
detect_sensor <- function(url, product = NULL) {
  bn <- basename(url)

  if (grepl("^GEDI", bn, ignore.case = TRUE)) {
    read_fn <- read_gedi
    if (is.null(product)) {
      product <- if (grepl("GEDI01_B", bn)) {
        "L1B"
      } else if (grepl("GEDI02_A", bn)) {
        "L2A"
      } else if (grepl("GEDI02_B", bn)) {
        "L2B"
      } else if (grepl("GEDI_L4A|GEDI04_A", bn)) {
        "L4A"
      } else if (grepl("GEDI_L4C|GEDI04_C", bn)) {
        "L4C"
      } else {
        rlang::abort(c(
          "Cannot detect GEDI product from filename.",
          "i" = "Pass {.arg product} explicitly."
        ))
      }
    }
  } else if (grepl("^ATL", bn, ignore.case = TRUE)) {
    read_fn <- read_icesat2
    if (is.null(product)) {
      product <- if (grepl("ATL03", bn)) {
        "ATL03"
      } else if (grepl("ATL06", bn)) {
        "ATL06"
      } else if (grepl("ATL07", bn)) {
        "ATL07"
      } else if (grepl("ATL08", bn)) {
        "ATL08"
      } else if (grepl("ATL10", bn)) {
        "ATL10"
      } else if (grepl("ATL13", bn)) {
        "ATL13"
      } else if (grepl("ATL24", bn)) {
        "ATL24"
      } else {
        rlang::abort(c(
          "Cannot detect ICESat-2 product from filename.",
          "i" = "Pass {.arg product} explicitly."
        ))
      }
    }
  } else {
    if (is.null(product)) {
      rlang::abort(c(
        "Cannot detect sensor from URL filename {.val {bn}}.",
        "i" = "Pass {.arg product} explicitly."
      ))
    }
    # Infer sensor from the product string
    if (product %in% c("L1B", "L2A", "L2B", "L4A", "L4C")) {
      read_fn <- read_gedi
    } else if (product %in% c("ATL03", "ATL06", "ATL07", "ATL08", "ATL10", "ATL13", "ATL24")) {
      read_fn <- read_icesat2
    } else {
      rlang::abort("Unknown product {.val {product}}.")
    }
  }

  list(read_fn = read_fn, product = product)
}

# ---------------------------------------------------------------------------
# sl_bbox
# ---------------------------------------------------------------------------

#' Create a bounding box for spatial queries
#'
#' Wraps four corner coordinates into an `sl_bbox` vector used by
#' [sl_search()] and [sl_read()]. The main value is up-front validation:
#' arguments are checked for correct ordering (`xmin < xmax`, `ymin < ymax`)
#' and for coordinates falling within WGS84 bounds (latitude in
#' \[-90, 90\], longitude in \[-180, 180\]), so mistakes surface here
#' rather than as a silent empty search or a failed HTTP request.
#'
#' @param xmin Minimum longitude (western boundary).
#' @param ymin Minimum latitude (southern boundary).
#' @param xmax Maximum longitude (eastern boundary).
#' @param ymax Maximum latitude (northern boundary).
#' @returns A named double vector of class `sl_bbox`.
#' @examples
#' # Construct a bounding box over a Pacific Northwest forest site.
#' sl_bbox(-124.04, 41.39, -124.01, 41.42)
#'
#' # Validation catches common mistakes before they reach the search
#' # or reader. Wrap in try() so the example chunk keeps running.
#' try(sl_bbox(-124.01, 41.39, -124.04, 41.42))  # xmin >= xmax
#' try(sl_bbox(0, -100, 1, 1))                   # latitude out of range
#' @export
sl_bbox <- function(xmin, ymin, xmax, ymax) {
  rlang::check_required(xmin)
  rlang::check_required(ymin)
  rlang::check_required(xmax)
  rlang::check_required(ymax)

  if (xmin >= xmax) {
    cli::cli_abort("{.arg xmin} must be less than {.arg xmax}.")
  }
  if (ymin >= ymax) {
    cli::cli_abort("{.arg ymin} must be less than {.arg ymax}.")
  }
  if (ymin < -90 || ymax > 90) {
    cli::cli_abort("Latitude values must be between -90 and 90.")
  }
  if (xmin < -180 || xmax > 180) {
    cli::cli_abort("Longitude values must be between -180 and 180.")
  }

  vctrs::new_vctr(
    c(xmin = xmin, ymin = ymin, xmax = xmax, ymax = ymax),
    class = "sl_bbox"
  )
}

#' @export
format.sl_bbox <- function(x, ...) {
  sprintf(
    "(%.4f, %.4f) - (%.4f, %.4f)",
    x[["xmin"]],
    x[["ymin"]],
    x[["xmax"]],
    x[["ymax"]]
  )
}

#' @export
print.sl_bbox <- function(x, ...) {
  cli::cli_text("{.cls sl_bbox}: {format(x)}")
  invisible(x)
}

#' Check that one bbox is fully contained within another.
#'
#' Used by `sl_read.sl_*_search()` methods to ensure a user-supplied `bbox`
#' does not extend outside the bbox the search was performed with. Equality
#' on any edge is allowed.
#'
#' @param inner The candidate bbox (will be coerced via `validate_bbox`).
#' @param outer The reference bbox (already an `sl_bbox`).
#' @noRd
check_bbox_within <- function(inner, outer, call = rlang::caller_env()) {
  i <- unclass(validate_bbox(inner))
  o <- unclass(outer)
  if (
    i[["xmin"]] < o[["xmin"]] ||
      i[["ymin"]] < o[["ymin"]] ||
      i[["xmax"]] > o[["xmax"]] ||
      i[["ymax"]] > o[["ymax"]]
  ) {
    cli::cli_abort(
      c(
        "{.arg bbox} extends outside the search bbox.",
        "i" = "Search bbox: {format(outer)}",
        "x" = "Grab bbox:   {format(validate_bbox(inner))}",
        "i" = "Re-run {.fun sl_search} with a wider bbox to avoid silently missing data."
      ),
      call = call
    )
  }
  invisible(inner)
}

# ---------------------------------------------------------------------------
# Internal: shared product reader (single-URL and multi-URL)
# ---------------------------------------------------------------------------

#' Validate inputs and resolve columns, credentials, and pool specs.
#'
#' Returns a list consumed by `read_product` / `read_product_multi`.
#' @noRd
prepare_read_params <- function(product, bbox, columns, lat_col, lon_col,
                                needs_creds = TRUE) {
  bbox <- validate_bbox(bbox)
  columns <- validate_columns(columns, product)
  # Capture the user's requested column set (after default-resolution
  # but before lat/lon and pool-index augmentation) as short names, for
  # downstream column reordering. Anything in the output that's NOT in
  # this list was added implicitly by the reader.
  user_columns <- sub(".*/", "", columns)
  columns <- ensure_lat_lon(columns, lat_col, lon_col)
  # Split into: transposed 2D columns, pool columns, and regular scalars
  trans_split <- split_transposed_columns(columns, product)
  pool_split <- split_pool_columns(trans_split$scalar, product)
  scalar_cols <- ensure_pool_indices(pool_split$scalar, pool_split$pool_short, product)
  pool_specs <- build_pool_specs(pool_split$pool_short, pool_split$pool_paths, product)
  transposed_specs <- build_transposed_specs(trans_split$transposed)
  # Only resolve Earthdata credentials when the reader is actually going
  # to make authenticated HTTP calls. Local file:// or bare-path reads
  # (used by the synthetic fixture tests, and by anyone with a local
  # granule cache) route through DataSource::Local on the Rust side
  # and don't need an NASA account.
  creds <- if (needs_creds) sl_earthdata_creds() else NULL
  list(
    bbox = unclass(bbox),
    scalar_cols = scalar_cols,
    pool_specs = pool_specs,
    pool_short = pool_split$pool_short,
    pool_idx_map = product_pool_index_map(product),
    transposed_specs = transposed_specs,
    fill_values = product_fill_values(product),
    scale_factors = product_scale_factors(product),
    user_columns = user_columns,
    creds = creds
  )
}

#' Does this URL require NASA Earthdata credentials?
#'
#' TRUE for real HTTP(S) URLs — the reader will need to authenticate.
#' FALSE for file:// URLs and bare filesystem paths — those route to
#' DataSource::Local on the Rust side (see src/rust/src/ffi.rs:make_source).
#' @noRd
is_remote_url <- function(x) {
  grepl("^https?://", x, ignore.case = TRUE)
}

#' Is the cross-beam spatial-filter optimization enabled?
#'
#' Checks the `spacelaser.cross_beam_scan` option. The env var
#' `SPACELASER_CROSS_BEAM_SCAN` is the internal transport — the R
#' wrapper sets it from the option before calling Rust and restores
#' afterwards — and is not intended as a user-facing knob.
#' @noRd
sl_cross_beam_enabled <- function() {
  isTRUE(getOption("spacelaser.cross_beam_scan", default = FALSE))
}

#' Call the Rust FFI reader with the prepared parameters.
#'
#' `target` is either a single URL (for `rust_read_gedi`) or a character
#' vector of URLs (for `rust_read_gedi_multi`).
#' @noRd
call_rust_reader <- function(rust_fn, target, product, params) {
  rust_fn(
    target,
    product,
    params$bbox[["xmin"]],
    params$bbox[["ymin"]],
    params$bbox[["xmax"]],
    params$bbox[["ymax"]],
    params$scalar_cols,
    NULL,
    params$creds$username,
    params$creds$password,
    if (length(params$pool_specs) > 0L) params$pool_specs else NULL,
    if (length(params$transposed_specs) > 0L) params$transposed_specs else NULL
  )
}

#' Reference epoch for GEDI / ICESat-2 `delta_time`.
#'
#' Both missions publish `delta_time` as seconds since 2018-01-01 00:00:00 UTC:
#' GEDI exactly (the "GEDI reference epoch") and ICESat-2 within ~18 leap
#' seconds of the ATLAS SDP GPS epoch. We hardcode the nominal 2018-01-01 UTC
#' value to avoid a second HDF5 round-trip to read `/ancillary_data/\\
#' atlas_sdp_gps_epoch`. The ~18 s difference is immaterial for every
#' user-facing analysis I can think of; if sub-second precision ever matters
#' we can switch to reading the file-level value.
#' @noRd
.delta_time_epoch <- function() {
  as.POSIXct("2018-01-01 00:00:00", tz = "UTC")
}

#' Convert `delta_time` (numeric seconds since epoch) to `time` (POSIXct).
#'
#' Operates in-place on the column's position so the rest of the frame's
#' layout isn't disturbed. Silently returns `data` unchanged if the
#' reader didn't include `delta_time`.
#' @noRd
convert_delta_time <- function(data) {
  if (!"delta_time" %in% names(data)) return(data)
  idx <- which(names(data) == "delta_time")
  data[[idx]] <- .delta_time_epoch() + data[[idx]]
  names(data)[idx] <- "time"
  data
}

#' Reorder output columns into a stable, predictable layout.
#'
#' Hash-based iteration order on the Rust side means raw column order
#' is unstable across runs. This helper imposes a canonical layout so
#' the same `sl_read()` call always returns columns in the same order,
#' and the order matches the user's mental model:
#'
#' 1. Group identifier (`beam` / `track`)
#' 2. Row identifier (`shot_number`)
#' 3. Time (`time`, or `delta_time` when `convert_time = FALSE`)
#' 4. Coordinates (lat/lon, with subgroup prefix stripped)
#' 5. User-requested columns in their requested order, with 2D
#'    expansions kept adjacent to their base column. 2D suffixes are
#'    sorted numerically (rh0, rh1, ..., rh100). Transposed expansions
#'    follow the registry's label order (land, ocean, sea_ice, ...).
#' 6. Auto-added pool-related infrastructure (start / count indices like
#'    `rx_sample_start_index`, plus any declared `deps` such as
#'    `elevation_bin0` / `elevation_lastbin` for `rxwaveform`)
#'    immediately before `geometry` — kept visible so the provenance
#'    of pool list-columns can be audited, but pushed past the science
#'    columns so they don't clutter the head of the frame. Any of
#'    these that the user explicitly requested stays in their
#'    requested position.
#' 7. `geometry` last.
#' @noRd
reorder_output_columns <- function(data,
                                    user_columns,
                                    lat_col,
                                    lon_col,
                                    group_label,
                                    pool_idx_map,
                                    transposed_specs) {
  if (!is.data.frame(data) || nrow(data) == 0L || ncol(data) == 0L) {
    return(data)
  }
  out_names <- names(data)
  user_columns <- as.character(user_columns)
  lat_short <- sub(".*/", "", lat_col)
  lon_short <- sub(".*/", "", lon_col)

  # Marquee front matter: stable canonical order regardless of where
  # the user listed these (or whether they listed them at all).
  marquee_candidates <- c(
    group_label, "shot_number",
    "time", "delta_time",
    lat_short, lon_short
  )
  marquee <- intersect(marquee_candidates, out_names)

  # Pool-related infrastructure columns: the start / count indices used
  # to slice each pool list-column, plus any extra `deps` declared in
  # the pool index spec (e.g. L1B `rxwaveform` declares
  # elevation_bin0 / elevation_lastbin so sl_extract_waveforms() can
  # interpolate per-sample elevations).
  start_count_cols <- character(0)
  dep_cols <- character(0)
  if (length(pool_idx_map) > 0L) {
    starts <- vapply(pool_idx_map, function(s) s$start, character(1))
    counts <- vapply(pool_idx_map, function(s) s$count, character(1))
    deps   <- unlist(
      lapply(pool_idx_map, function(s) s$deps %||% character(0)),
      use.names = FALSE
    )
    start_count_cols <- unique(c(starts, counts))
    dep_cols <- unique(deps)
  }

  # Pool start/count index columns are consumed by the Rust pool slicer
  # and have no user-facing value. Strip them unless the user explicitly
  # requested them. Deps (elevation_bin0 etc.) are kept — users need
  # them for sl_extract_waveforms and similar downstream processing.
  drop_cols <- intersect(
    setdiff(start_count_cols, user_columns),
    out_names
  )
  auto_deps <- intersect(
    setdiff(dep_cols, user_columns),
    out_names
  )

  # Trailing: auto-added deps, then geometry.
  trailing <- intersect(c(auto_deps, "geometry"), out_names)

  # Map base name -> ordered expansion column names, derived from the
  # transposed_specs (encoded "path:label1,label2,..."). Used when a
  # user-requested column wasn't found verbatim in the output but was
  # expanded into per-category columns by the reader.
  trans_lookup <- list()
  for (spec in transposed_specs) {
    parts <- strsplit(spec, ":", fixed = TRUE)[[1]]
    if (length(parts) != 2L) next
    base <- sub(".*/", "", parts[[1]])
    labels <- strsplit(parts[[2]], ",", fixed = TRUE)[[1]]
    trans_lookup[[base]] <- paste0(base, "_", labels)
  }

  # Middle: user-requested columns in user order, expansions adjacent.
  taken <- c(marquee, trailing)
  middle <- character(0)
  for (nm in user_columns) {
    if (nm %in% taken) next
    if (nm %in% out_names) {
      middle <- c(middle, nm)
      taken <- c(taken, nm)
      next
    }
    # Transposed expansion (registry-ordered)
    if (nm %in% names(trans_lookup)) {
      cols <- intersect(trans_lookup[[nm]], setdiff(out_names, taken))
      if (length(cols) > 0L) {
        middle <- c(middle, cols)
        taken <- c(taken, cols)
        next
      }
    }
    # 2D numeric expansion (e.g. rh -> rh0..rh100, sorted by suffix)
    pat <- paste0("^", nm, "\\d+$")
    cols <- grep(pat, out_names, value = TRUE)
    cols <- setdiff(cols, taken)
    if (length(cols) > 0L) {
      suffixes <- as.integer(sub(paste0("^", nm), "", cols))
      cols <- cols[order(suffixes)]
      middle <- c(middle, cols)
      taken <- c(taken, cols)
    }
  }

  # Defensive: any column not yet placed (shouldn't happen, but never
  # silently drop) goes between the user middle and the trailing block.
  remaining <- setdiff(out_names, c(marquee, middle, trailing, drop_cols))

  final_order <- c(marquee, middle, remaining, trailing)
  data[, final_order, drop = FALSE]
}

#' Convert the Rust reader's raw output into a combined data frame.
#'
#' For each group (beam / track) builds a tibble (geometry attached,
#' pool columns sliced into per-shot list-cols, scale factors applied),
#' row-binds across groups, then post-processes the combined frame:
#'
#'   1. Bbox post-filter — a no-op for direct-scan products, but trims
#'      the few edge photons ATL03's segment-level filter can include.
#'   2. `delta_time` -> POSIXct `time` (when `convert_time = TRUE`).
#'   3. Canonical column reorder (group / id / time / coords / user /
#'      auto-added infrastructure / geometry).
#' @noRd
assemble_read_result <- function(
  raw_result,
  bbox,
  lat_col,
  lon_col,
  group_label,
  element_label,
  pool_short,
  pool_idx_map,
  fill_values = numeric(0),
  scale_factors = list(),
  user_columns = character(0),
  transposed_specs = character(0),
  convert_time = TRUE
) {
  if (length(raw_result) == 0L) {
    cli::cli_inform("No {element_label}s found within the bounding box.")
    return(tibble::tibble())
  }

  group_tbls <- lapply(raw_result, function(gd) {
    tbl <- build_tibble(
      gd,
      lat_col = lat_col,
      lon_col = lon_col,
      pool_short = pool_short,
      pool_index_map = pool_idx_map,
      fill_values = fill_values,
      scale_factors = scale_factors
    )
    tbl[[group_label]] <- gd$group_name
    tbl
  })

  result <- vctrs::vec_rbind(!!!group_tbls)

  # Post-filter: ensure all rows are within the bbox. This is a no-op
  # for products with direct lat/lon scanning (every row already
  # passes), but matters for ATL03 which uses segment-level spatial
  # filtering (~20m resolution). Edge segments may include a few
  # photons slightly outside the bbox.
  lat_short <- sub(".*/", "", lat_col)
  lon_short <- sub(".*/", "", lon_col)
  if (lat_short %in% names(result) && lon_short %in% names(result) && nrow(result) > 0) {
    b <- unclass(bbox)
    lat <- result[[lat_short]]
    lon <- result[[lon_short]]
    keep <- !is.na(lat) & !is.na(lon) &
      lat >= b[["ymin"]] & lat <= b[["ymax"]] &
      lon >= b[["xmin"]] & lon <= b[["xmax"]]
    if (!all(keep)) {
      result <- result[keep, ]
    }
  }

  if (convert_time) {
    result <- convert_delta_time(result)
  }

  result <- reorder_output_columns(
    result,
    user_columns = user_columns,
    lat_col = lat_col,
    lon_col = lon_col,
    group_label = group_label,
    pool_idx_map = pool_idx_map,
    transposed_specs = transposed_specs
  )

  n <- nrow(result)
  n_groups <- length(group_tbls)
  cli::cli_progress_done()
  # qty() overrides the pluraliser's quantity — without it the most
  # recent inline is the label string, which counts as 1 and defeats
  # {?s}.
  cli::cli_inform(c(
    "v" = "Read {n} {element_label}{cli::qty(n)}{?s} from \\
           {n_groups} {group_label}{cli::qty(n_groups)}{?s}."
  ))

  result
}

#' Read a single granule.
#' @noRd
read_product <- function(
  url,
  product,
  bbox,
  columns,
  rust_fn,
  lat_col,
  lon_col,
  group_label,
  element_label,
  convert_time = TRUE
) {
  params <- prepare_read_params(product, bbox, columns, lat_col, lon_col,
                                needs_creds = is_remote_url(url))
  cli::cli_progress_step("Reading {product} from {.url {basename(url)}}")
  raw_result <- call_rust_reader(rust_fn, url, product, params)
  assemble_read_result(
    raw_result, bbox, lat_col, lon_col, group_label, element_label,
    params$pool_short, params$pool_idx_map,
    params$fill_values, params$scale_factors,
    user_columns = params$user_columns,
    transposed_specs = params$transposed_specs,
    convert_time = convert_time
  )
}

#' Read multiple granules concurrently via Rust.
#'
#' All URLs are passed to a single Rust function that processes them in
#' parallel within one async runtime.
#' @noRd
read_product_multi <- function(
  urls,
  product,
  bbox,
  columns,
  rust_multi_fn,
  lat_col,
  lon_col,
  group_label,
  element_label,
  convert_time = TRUE
) {
  urls <- urls[!is.na(urls)]
  if (length(urls) == 0L) {
    cli::cli_inform("No URLs to read.")
    return(tibble::tibble())
  }

  params <- prepare_read_params(product, bbox, columns, lat_col, lon_col,
                                needs_creds = any(is_remote_url(urls)))

  # If the cross-beam optimization is enabled via option or env var,
  # set SPACELASER_CROSS_BEAM_SCAN=1 for the duration of the Rust call
  # and restore afterwards. The Rust side reads the env var on each
  # sl_read, so this scoping keeps the effect local and predictable.
  if (sl_cross_beam_enabled()) {
    prev <- Sys.getenv("SPACELASER_CROSS_BEAM_SCAN", unset = NA)
    Sys.setenv(SPACELASER_CROSS_BEAM_SCAN = "1")
    on.exit({
      if (is.na(prev)) {
        Sys.unsetenv("SPACELASER_CROSS_BEAM_SCAN")
      } else {
        Sys.setenv(SPACELASER_CROSS_BEAM_SCAN = prev)
      }
    }, add = TRUE)
  }

  cli::cli_progress_step("Reading {product} from {length(urls)} granule{?s}")
  raw_result <- call_rust_reader(rust_multi_fn, urls, product, params)
  assemble_read_result(
    raw_result, bbox, lat_col, lon_col, group_label, element_label,
    params$pool_short, params$pool_idx_map,
    params$fill_values, params$scale_factors,
    user_columns = params$user_columns,
    transposed_specs = params$transposed_specs,
    convert_time = convert_time
  )
}

# ---------------------------------------------------------------------------
# Internal: lat/lon column helpers (shared between read_* and sl_read)
# ---------------------------------------------------------------------------

#' @noRd
gedi_lat_lon <- function(product) {
  switch(
    product,
    "L2A" = list(lat = "lat_lowestmode", lon = "lon_lowestmode"),
    "L2B" = list(
      lat = "geolocation/lat_lowestmode",
      lon = "geolocation/lon_lowestmode"
    ),
    "L4A" = list(lat = "lat_lowestmode", lon = "lon_lowestmode"),
    "L4C" = list(lat = "lat_lowestmode", lon = "lon_lowestmode"),
    "L1B" = list(
      lat = "geolocation/latitude_bin0",
      lon = "geolocation/longitude_bin0"
    )
  )
}

#' @noRd
icesat2_lat_lon <- function(product) {
  switch(
    product,
    ATL03 = list(lat = "heights/lat_ph", lon = "heights/lon_ph"),
    ATL06 = list(
      lat = "land_ice_segments/latitude",
      lon = "land_ice_segments/longitude"
    ),
    ATL08 = list(
      lat = "land_segments/latitude",
      lon = "land_segments/longitude"
    ),
    ATL07 = list(
      lat = "sea_ice_segments/latitude",
      lon = "sea_ice_segments/longitude"
    ),
    ATL10 = list(
      lat = "freeboard_segment/latitude",
      lon = "freeboard_segment/longitude"
    ),
    ATL13 = list(lat = "segment_lat", lon = "segment_lon"),
    ATL24 = list(lat = "lat_ph", lon = "lon_ph")
  )
}

# ---------------------------------------------------------------------------
# Internal: column parsing and tibble construction
# ---------------------------------------------------------------------------

#' Convert raw bytes from Rust to an R vector based on HDF5 dtype.
#'
#' Most columns are returned from the Rust FFI already typed (Doubles /
#' Integers, with fill-values replaced). This raw-bytes path is used for:
#'
#'   - Pool columns (rxwaveform, txwaveform, pgap_theta_z): variable
#'     length per shot, sliced into list-cols on the R side from the
#'     flat byte buffer rather than typed up front.
#'   - `sl_hdf5_read()`: low-level dataset access where the user reads
#'     a single arbitrary HDF5 path with no per-product knowledge.
#'
#' The Rust side accompanies the bytes with a JSON string describing
#' the HDF5 datatype (e.g. `FixedPoint { size: 4, signed: true }`) and
#' element count; we use `readBin()` to reinterpret. uint64 is split
#' into hi/lo int32 pairs and combined arithmetically since R has no
#' native u64.
#'
#' @param raw_bytes Raw vector of bytes from the Rust reader.
#' @param info_json JSON string with element_size, num_elements, dtype fields.
#' @returns An appropriate R vector (double, integer, or raw).
#' @noRd
parse_column <- function(raw_bytes, info_json) {
  info <- parse_column_info(info_json)
  n <- info$num_elements
  dtype <- info$dtype
  elem_size <- info$element_size

  if (grepl("FloatingPoint", dtype, fixed = TRUE)) {
    readBin(
      raw_bytes,
      what = "double",
      n = n,
      size = elem_size,
      endian = "little"
    )
  } else if (grepl("FixedPoint", dtype, fixed = TRUE)) {
    signed <- grepl("signed: true", dtype, fixed = TRUE)
    if (elem_size <= 4 && signed) {
      readBin(raw_bytes, what = "integer", n = n, size = elem_size, endian = "little")
    } else if (elem_size == 1) {
      as.integer(raw_bytes)
    } else if (elem_size == 2 && !signed) {
      readBin(raw_bytes, what = "integer", n = n, size = 2L, endian = "little", signed = FALSE)
    } else if (elem_size <= 4) {
      # uint32: R integer is signed 32-bit (max 2.1e9). Unsigned values
      # above that wrap negative. Promote to double (exact for all u32
      # since max 4.3e9 << 2^53).
      x <- readBin(raw_bytes, what = "integer", n = n, size = elem_size, endian = "little")
      ifelse(x < 0L, as.double(x) + 4294967296, as.double(x))
    } else {
      # int64 / uint64 → bit64::integer64.
      # readBin as "double" gives us the raw 8 bytes as f64 bit patterns,
      # which is exactly the representation bit64 uses internally.
      x <- readBin(raw_bytes, what = "double", n = n, size = 8L, endian = "little")
      class(x) <- "integer64"
      x
    }
  } else {
    raw_bytes
  }
}

#' Extract element_size, num_elements, dtype from column info JSON.
#'
#' Uses simple regex extraction instead of a full JSON parser. The
#' shape is fixed and known (three flat keys, no nesting), so the
#' regex is unambiguous and we avoid pulling in jsonlite as a
#' dependency for what's only used by `parse_column()` and
#' `sl_hdf5_read()`.
#'
#' @noRd
parse_column_info <- function(json_str) {
  extract_int <- function(key) {
    m <- regmatches(
      json_str,
      regexpr(paste0('"', key, '":\\s*(\\d+)'), json_str)
    )
    as.integer(sub(paste0('"', key, '":\\s*'), "", m))
  }
  m <- regmatches(json_str, regexpr('"dtype":"([^"]*)"', json_str))
  dtype <- sub('"dtype":"([^"]*)"', "\\1", m)

  list(
    element_size = extract_int("element_size"),
    num_elements = extract_int("num_elements"),
    dtype = dtype
  )
}

#' Build a data frame from a Rust group data list.
#'
#' The Rust side returns pre-typed R vectors (doubles/integers) with
#' fill values already replaced by NA and column names already stripped
#' of HDF5 subgroup prefixes. This function just assembles them into
#' a data frame, applies scale factors, attaches geometry, and slices
#' pool columns into per-shot list columns.
#' @noRd
build_tibble <- function(
  group_data,
  lat_col,
  lon_col,
  pool_short = character(0),
  pool_index_map = list(),
  fill_values = numeric(0),
  scale_factors = list()
) {
  # Columns are already typed R vectors (Doubles/Integers) from Rust,
  # with fill values → NA and name prefixes stripped.
  cols <- as.list(group_data$columns)
  col_names <- names(cols)

  n <- group_data$n_elements %||% 0L
  if (n == 0L && length(cols) > 0L) {
    n <- length(cols[[1]])
  }

  tbl <- tibble::new_tibble(cols, nrow = n)

  # Apply per-column scale factors (R-side only: product-specific,
  # currently only L2B rh100 cm→m).
  if (length(scale_factors) > 0L) {
    tbl_names <- names(tbl)
    for (base_name in names(scale_factors)) {
      factor <- scale_factors[[base_name]]
      pattern <- paste0("^", base_name, "\\d*$")
      matching <- grep(pattern, tbl_names, value = TRUE)
      for (nm in matching) {
        if (is.numeric(tbl[[nm]])) {
          tbl[[nm]] <- tbl[[nm]] * factor
        }
      }
    }
  }

  # Strip subgroup prefix from lat/lon for geometry lookup
  lat_short <- sub(".*/", "", lat_col)
  lon_short <- sub(".*/", "", lon_col)
  if (lat_short %in% col_names && lon_short %in% col_names) {
    tbl[["geometry"]] <- wk::xy(
      x = tbl[[lon_short]],
      y = tbl[[lat_short]],
      crs = wk::wk_crs_longlat()
    )
  }

  # Pool columns: Rust returns raw bytes (not typed) because the R
  # side needs to slice per-shot. Parse, apply fill-value replacement,
  # then slice using count vectors.
  if (length(pool_short) > 0L) {
    pool_raw <- group_data$pool_columns %||% list()
    pool_info <- group_data$pool_col_info %||% list()
    for (pc in pool_short) {
      if (is.null(pool_raw[[pc]])) next
      flat <- parse_column(pool_raw[[pc]], pool_info[[pc]])
      if (length(fill_values) > 0L && is.numeric(flat)) {
        for (fv in fill_values) {
          flat[!is.na(flat) & flat == fv] <- NA
        }
      }
      spec <- pool_index_map[[pc]]
      if (is.null(spec)) next
      counts <- tbl[[spec$count]]
      if (is.null(counts)) next

      slices <- vector("list", n)
      pos <- 1L
      for (i in seq_len(n)) {
        k <- as.integer(counts[[i]])
        if (is.na(k) || k <= 0L) {
          slices[[i]] <- flat[integer(0)]
        } else {
          slices[[i]] <- flat[pos:(pos + k - 1L)]
          pos <- pos + k
        }
      }
      tbl[[pc]] <- slices
    }
  }

  tbl
}

#' @importFrom rlang `%||%`
NULL
