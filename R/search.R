#' Search NASA CMR for GEDI or ICESat-2 granules
#'
#' Searches NASA's Common Metadata Repository (CMR) for satellite lidar
#' granules that overlap a bounding box and optional date range. The sensor
#' (GEDI or ICESat-2) is determined automatically from `product`, since the
#' valid product strings do not overlap.
#'
#' @param bbox An `sl_bbox` object created by [sl_bbox()], or a numeric vector
#'   `c(xmin, ymin, xmax, ymax)`.
#' @param product Character. One of:
#'   * GEDI: `"L1B"`, `"L2A"`, `"L2B"`, `"L4A"`
#'   * ICESat-2: `"ATL03"`, `"ATL06"`, `"ATL08"`
#' @param date_start Character or POSIXct. Start of date range. Defaults to
#'   the start of the relevant mission (2019-03-25 for GEDI, 2018-10-14 for
#'   ICESat-2).
#' @param date_end Character or POSIXct. End of date range (default: today).
#'
#' @returns A classed data frame (`sl_gedi_search` or `sl_icesat2_search`)
#'   with columns:
#'   \describe{
#'     \item{id}{CMR granule identifier.}
#'     \item{time_start}{POSIXct start time of the granule.}
#'     \item{time_end}{POSIXct end time of the granule.}
#'     \item{url}{HTTPS data URL.}
#'     \item{geometry}{`wk_wkt` polygon of the granule swath footprint.}
#'   }
#'   The returned object carries `bbox` and `product` as attributes so
#'   [sl_read()] can dispatch without re-specifying them.
#'
#' @details
#' No authentication is needed for the search itself; Earthdata credentials
#' are only required when reading data via [sl_read()].
#'
#' The CMR search filters by bounding box on the server side. For finer
#' spatial filtering (e.g. against an irregular polygon), filter the returned
#' `geometry` column with your favourite spatial package.
#'
#' @seealso [sl_read()] to read data from the returned granules.
#' @importFrom rlang check_required arg_match
#' @export
sl_search <- function(
  bbox,
  product = c("L2A", "L2B", "L4A", "L4C", "L1B", "ATL08", "ATL03", "ATL06", "ATL13", "ATL24"),
  date_start = NULL,
  date_end = NULL
) {
  rlang::check_required(bbox)
  product <- rlang::arg_match(product)
  bbox <- validate_bbox(bbox)

  spec <- product_search_spec(product)

  result <- search_cmr(
    bbox = bbox,
    concept_id = spec$concept_id,
    date_start = date_start %||% spec$default_start,
    date_end = date_end,
    product_label = spec$label
  )

  new_sl_search(result, product = product, bbox = bbox, sensor = spec$sensor)
}

#' Per-product CMR search parameters.
#'
#' Maps a product string to its CMR concept ID, default mission start date,
#' sensor (used to pick the search-result class), and a human-readable label
#' for progress messages. Centralising the table here keeps `sl_search()`
#' free of branching.
#' @noRd
product_search_spec <- function(product) {
  switch(
    product,
    L1B   = list(sensor = "gedi",    concept_id = "C2142749196-LPCLOUD",  default_start = "2019-03-25", label = "GEDI L1B"),
    L2A   = list(sensor = "gedi",    concept_id = "C2142771958-LPCLOUD",  default_start = "2019-03-25", label = "GEDI L2A"),
    L2B   = list(sensor = "gedi",    concept_id = "C2142776747-LPCLOUD",  default_start = "2019-03-25", label = "GEDI L2B"),
    L4A   = list(sensor = "gedi",    concept_id = "C2237824918-ORNL_CLOUD", default_start = "2019-03-25", label = "GEDI L4A"),
    L4C   = list(sensor = "gedi",    concept_id = "C3049900163-ORNL_CLOUD", default_start = "2019-03-25", label = "GEDI L4C"),
    ATL03 = list(sensor = "icesat2", concept_id = "C3326974349-NSIDC_CPRD", default_start = "2018-10-14", label = "ICESat-2 ATL03"),
    ATL06 = list(sensor = "icesat2", concept_id = "C3564876127-NSIDC_CPRD", default_start = "2018-10-14", label = "ICESat-2 ATL06"),
    ATL08 = list(sensor = "icesat2", concept_id = "C3565574177-NSIDC_CPRD", default_start = "2018-10-14", label = "ICESat-2 ATL08"),
    ATL13 = list(sensor = "icesat2", concept_id = "C3565574351-NSIDC_CPRD", default_start = "2018-10-14", label = "ICESat-2 ATL13"),
    ATL24 = list(sensor = "icesat2", concept_id = "C3433822507-NSIDC_CPRD", default_start = "2018-10-14", label = "ICESat-2 ATL24")
  )
}

# ---------------------------------------------------------------------------
# Internal: CMR query engine
# ---------------------------------------------------------------------------

#' Query NASA CMR for granules matching a concept ID and bounding box.
#' @noRd
search_cmr <- function(bbox, concept_id, date_start, date_end, product_label) {
  bbox <- validate_bbox(bbox)
  b <- unclass(bbox)
  bbox_str <- paste(
    b[["xmin"]],
    b[["ymin"]],
    b[["xmax"]],
    b[["ymax"]],
    sep = ","
  )

  base_url <- paste0(
    "https://cmr.earthdata.nasa.gov/search/granules.json?",
    "page_size=2000",
    "&concept_id=",
    concept_id,
    "&bounding_box=",
    bbox_str
  )

  start_iso <- format_iso8601(date_start)
  end_iso <- format_iso8601(date_end %||% Sys.Date())
  base_url <- paste0(base_url, "&temporal=", start_iso, ",", end_iso)

  cli::cli_progress_step("Searching CMR for {product_label} granules")

  entries <- list()
  page <- 1L
  repeat {
    page_url <- paste0(base_url, "&pageNum=", page)
    json <- fetch_cmr_json(page_url)

    page_entries <- json$feed$entry
    if (length(page_entries) == 0L) {
      break
    }
    entries <- c(entries, page_entries)
    page <- page + 1L
  }

  cli::cli_progress_done()

  if (length(entries) == 0L) {
    cli::cli_inform("No {product_label} granules found.")
    return(empty_find_result())
  }

  result <- build_find_result(entries)
  cli::cli_inform(c(
    "v" = "Found {nrow(result)} {product_label} granule{?s}."
  ))

  # Attach class and product metadata for S3 dispatch in grab()
  result
}

# ---------------------------------------------------------------------------
# Internal: HTTP + JSON
# ---------------------------------------------------------------------------

#' Fetch JSON from a CMR URL.
#' @noRd
fetch_cmr_json <- function(request_url) {
  con <- url(request_url)
  on.exit(try(close(con), silent = TRUE), add = TRUE)

  lines <- tryCatch(
    readLines(con, warn = FALSE),
    error = function(e) {
      cli::cli_abort(c(
        "Failed to query NASA CMR.",
        "i" = conditionMessage(e)
      ))
    }
  )

  jsonlite::fromJSON(paste(lines, collapse = ""), simplifyVector = FALSE)
}

# ---------------------------------------------------------------------------
# Internal: result construction
# ---------------------------------------------------------------------------

#' Build a data frame from CMR entry list.
#' @noRd
build_find_result <- function(entries) {
  n <- length(entries)

  ids <- character(n)
  starts <- character(n)
  ends <- character(n)
  urls <- character(n)
  geom_wkts <- character(n)

  for (i in seq_len(n)) {
    entry <- entries[[i]]
    ids[i] <- entry$id %||% NA_character_
    starts[i] <- entry$time_start %||% NA_character_
    ends[i] <- entry$time_end %||% NA_character_

    # Pick the .h5 download link; fall back to the first link.
    url <- NA_character_
    for (link in entry$links) {
      if (grepl("\\.h5(5)?$", link$href, ignore.case = TRUE)) {
        url <- link$href
        break
      }
    }
    if (is.na(url) && length(entry$links) > 0L) {
      url <- entry$links[[1L]]$href
    }
    urls[i] <- url

    # Build WKT polygon from the CMR swath polygon.
    if (length(entry$polygons) > 0L) {
      geom_wkts[i] <- cmr_polygon_to_wkt(entry$polygons[[1L]][[1L]])
    } else {
      geom_wkts[i] <- NA_character_
    }
  }

  vctrs::new_data_frame(list(
    id = ids,
    time_start = as.POSIXct(starts, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC"),
    time_end = as.POSIXct(ends, format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC"),
    url = urls,
    geometry = wk::wkt(geom_wkts, crs = wk::wk_crs_longlat())
  ))
}

#' Convert CMR polygon coordinates to WKT.
#'
#' CMR polygons are space-separated `"lat1 lon1 lat2 lon2 ..."` strings
#' (note: latitude first).
#' @noRd
cmr_polygon_to_wkt <- function(coord_string) {
  nums <- as.numeric(strsplit(trimws(coord_string), "\\s+")[[1L]])
  if (length(nums) < 6L || length(nums) %% 2L != 0L) {
    return(NA_character_)
  }
  lats <- nums[seq(1L, length(nums), 2L)]
  lons <- nums[seq(2L, length(nums), 2L)]
  ring <- paste(sprintf("%.6f %.6f", lons, lats), collapse = ", ")
  sprintf("POLYGON ((%s))", ring)
}

#' Format a date as ISO 8601 for CMR temporal queries.
#' @noRd
format_iso8601 <- function(x) {
  if (inherits(x, "POSIXt")) {
    return(format(x, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))
  }
  if (inherits(x, "Date")) {
    return(format(as.POSIXct(x, tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))
  }
  if (is.character(x)) {
    dt <- as.POSIXct(x, tz = "UTC")
    if (is.na(dt)) {
      cli::cli_abort("Cannot parse date: {.val {x}}")
    }
    return(format(dt, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))
  }
  cli::cli_abort(
    "Date must be character, Date, or POSIXt, not {.cls {class(x)}}."
  )
}

#' Empty result for zero-match searches.
#' @noRd
empty_find_result <- function() {
  vctrs::new_data_frame(list(
    id = character(),
    time_start = as.POSIXct(character(), tz = "UTC"),
    time_end = as.POSIXct(character(), tz = "UTC"),
    url = character(),
    geometry = wk::wkt(character(), crs = wk::wk_crs_longlat())
  ))
}

# ---------------------------------------------------------------------------
# S3 class: sl_gedi_search / sl_icesat2_search
# ---------------------------------------------------------------------------

#' Construct an S3 search result.
#'
#' Wraps the data frame from `build_find_result()` in either
#' `sl_gedi_search` or `sl_icesat2_search`, carrying `product` and `bbox` as
#' attributes so `sl_read()` can dispatch without the user re-specifying them.
#'
#' @noRd
new_sl_search <- function(df, product, bbox, sensor = c("gedi", "icesat2")) {
  sensor <- match.arg(sensor)
  cls <- paste0("sl_", sensor, "_search")
  attr(df, "product") <- product
  attr(df, "bbox") <- bbox
  class(df) <- c(cls, class(df))
  df
}

#' @export
print.sl_gedi_search <- function(x, ...) {
  product <- attr(x, "product")
  bbox <- attr(x, "bbox")
  cli::cli_text(
    "{.cls sl_gedi_search} | GEDI {product} | {nrow(x)} granule{?s} | {.field {format(bbox)}}"
  )
  print_search_result(x, ...)
}

#' @export
print.sl_icesat2_search <- function(x, ...) {
  product <- attr(x, "product")
  bbox <- attr(x, "bbox")
  cli::cli_text(
    "{.cls sl_icesat2_search} | ICESat-2 {product} | {nrow(x)} granule{?s} | {.field {format(bbox)}}"
  )
  print_search_result(x, ...)
}

# Subset operators must preserve `product`, `bbox`, and class so that
# `granules[1:2, ]` still dispatches to the search-result methods of
# `sl_read()`. Base `[.data.frame` strips both the subclass and the
# attributes; we restore them.

#' @export
`[.sl_gedi_search` <- function(x, ...) {
  reattach_search_attrs(NextMethod(), x, "sl_gedi_search")
}

#' @export
`[.sl_icesat2_search` <- function(x, ...) {
  reattach_search_attrs(NextMethod(), x, "sl_icesat2_search")
}

#' Restore search-result class and attributes after a subset.
#'
#' Only attaches the class if the subset still resembles a data frame
#' (i.e. preserves the column structure). Column-wise drops to a single
#' vector fall through unchanged.
#' @noRd
reattach_search_attrs <- function(out, x, cls) {
  if (!is.data.frame(out)) {
    return(out)
  }
  attr(out, "product") <- attr(x, "product")
  attr(out, "bbox") <- attr(x, "bbox")
  if (!inherits(out, cls)) {
    class(out) <- c(cls, class(out))
  }
  out
}

#' Print the body of a search result (shared helper).
#' @noRd
print_search_result <- function(x, n = 10L, ...) {
  if (nrow(x) == 0L) {
    cli::cli_text("(no granules)")
    return(invisible(x))
  }
  plain <- strip_search_class(x)
  print(utils::head(plain, n), ...)
  if (nrow(x) > n) {
    cli::cli_text("# ... with {nrow(x) - n} more granule{?s}")
  }
  invisible(x)
}

#' Drop the sl_*_search class, returning a plain data frame.
#' @noRd
strip_search_class <- function(x) {
  cls <- class(x)
  class(x) <- cls[!grepl("^sl_.*_search$", cls)]
  x
}
