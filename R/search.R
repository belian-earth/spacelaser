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
#'   * GEDI: `"L1B"`, `"L2A"`, `"L2B"`, `"L4A"`, `"L4C"`
#'   * ICESat-2: `"ATL03"`, `"ATL06"`, `"ATL07"`, `"ATL08"`, `"ATL10"`,
#'     `"ATL13"`, `"ATL24"`
#' @param date_start,date_end Either a `Date` object or a character string in
#'   strict `"YYYY-MM-DD"` format (e.g. `"2020-06-01"`). Other character
#'   forms (e.g. `"01/06/2020"`, `"June 1 2020"`, `"2020-06"`) are rejected
#'   with an informative error to avoid ambiguity. `POSIXct` inputs are
#'   not accepted — format them explicitly with `format(x, "%Y-%m-%d")`.
#'
#'   Bounds are treated as UTC and inclusive of the end date: a range of
#'   `"2020-06-01"` to `"2020-06-30"` covers every granule whose start time
#'   falls on any of those 30 days.
#'
#'   `date_start` defaults to the mission start (2019-03-25 for GEDI,
#'   2018-10-14 for ICESat-2). `date_end` defaults to today.
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
#' @examplesIf interactive()
#' # GEDI L2A over a small Pacific Northwest forest bbox, summer 2020.
#' granules <- sl_search(
#'   sl_bbox(-124.04, 41.39, -124.01, 41.42),
#'   product    = "L2A",
#'   date_start = "2020-06-01",
#'   date_end   = "2020-09-01"
#' )
#' granules
#'
#' # ICESat-2 ATL08 (land + canopy segments) over the same bbox, 2020.
#' sl_search(
#'   sl_bbox(-124.10, 41.36, -124.00, 41.45),
#'   product    = "ATL08",
#'   date_start = "2020-01-01",
#'   date_end   = "2021-01-01"
#' )
#' @export
sl_search <- function(
  bbox,
  product = c(
    "L2A",
    "L2B",
    "L4A",
    "L4C",
    "L1B",
    "ATL03",
    "ATL06",
    "ATL07",
    "ATL08",
    "ATL10",
    "ATL13",
    "ATL24"
  ),
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
    L1B = list(
      sensor = "gedi",
      concept_id = "C2142749196-LPCLOUD",
      default_start = "2019-03-25",
      label = "GEDI L1B"
    ),
    L2A = list(
      sensor = "gedi",
      concept_id = "C2142771958-LPCLOUD",
      default_start = "2019-03-25",
      label = "GEDI L2A"
    ),
    L2B = list(
      sensor = "gedi",
      concept_id = "C2142776747-LPCLOUD",
      default_start = "2019-03-25",
      label = "GEDI L2B"
    ),
    L4A = list(
      sensor = "gedi",
      concept_id = "C2237824918-ORNL_CLOUD",
      default_start = "2019-03-25",
      label = "GEDI L4A"
    ),
    L4C = list(
      sensor = "gedi",
      concept_id = "C3049900163-ORNL_CLOUD",
      default_start = "2019-03-25",
      label = "GEDI L4C"
    ),
    ATL03 = list(
      sensor = "icesat2",
      concept_id = "C3326974349-NSIDC_CPRD",
      default_start = "2018-10-14",
      label = "ICESat-2 ATL03"
    ),
    ATL06 = list(
      sensor = "icesat2",
      concept_id = "C3564876127-NSIDC_CPRD",
      default_start = "2018-10-14",
      label = "ICESat-2 ATL06"
    ),
    ATL07 = list(
      sensor = "icesat2",
      concept_id = "C3564876395-NSIDC_CPRD",
      default_start = "2018-10-14",
      label = "ICESat-2 ATL07"
    ),
    ATL08 = list(
      sensor = "icesat2",
      concept_id = "C3565574177-NSIDC_CPRD",
      default_start = "2018-10-14",
      label = "ICESat-2 ATL08"
    ),
    ATL10 = list(
      sensor = "icesat2",
      concept_id = "C3565574246-NSIDC_CPRD",
      default_start = "2018-10-14",
      label = "ICESat-2 ATL10"
    ),
    ATL13 = list(
      sensor = "icesat2",
      concept_id = "C3565574351-NSIDC_CPRD",
      default_start = "2018-10-14",
      label = "ICESat-2 ATL13"
    ),
    ATL24 = list(
      sensor = "icesat2",
      concept_id = "C3433822507-NSIDC_CPRD",
      default_start = "2018-10-14",
      label = "ICESat-2 ATL24"
    )
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

  start_date <- parse_search_date(date_start, arg = "date_start")
  end_date <- parse_search_date(date_end %||% Sys.Date(), arg = "date_end")
  if (end_date < start_date) {
    cli::cli_abort(c(
      "{.arg date_end} ({.val {format(end_date)}}) must be on or after \\
       {.arg date_start} ({.val {format(start_date)}}).",
      "i" = "Check the order of your date arguments."
    ))
  }
  # Inclusive-end semantics: use start-of-day for date_start and end-of-day
  # for date_end so `date_end = \"2020-06-30\"` actually includes June 30.
  start_iso <- format(start_date, "%Y-%m-%dT00:00:00Z")
  end_iso   <- format(end_date,   "%Y-%m-%dT23:59:59Z")
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
  rl <- nrow(result)
  cli::cli_inform(c(
    "v" = "Found {rl} {product_label} {cli::qty(rl)} granule{?s}."
  ))

  # Caller (sl_search) wraps in new_sl_search() to attach the class and
  # bbox/product attributes that sl_read.sl_*_search dispatch on.
  result
}

# ---------------------------------------------------------------------------
# Internal: HTTP + JSON
# ---------------------------------------------------------------------------

#' Fetch JSON from a CMR URL with retry and timeout.
#'
#' Uses httr2 for connection reuse across pagination, exponential backoff
#' on transient failures (CMR occasionally returns 5xx during peak hours),
#' and structured HTTP error handling.
#' @noRd
fetch_cmr_json <- function(request_url) {
  req <- httr2::request(request_url) |>
    httr2::req_timeout(30) |>
    httr2::req_retry(
      max_tries = 3,
      backoff = function(i) 2^i
    ) |>
    httr2::req_user_agent("spacelaser (github.com/belian-earth/spacelaser)")

  resp <- tryCatch(
    httr2::req_perform(req),
    error = function(e) {
      cli::cli_abort(c(
        "Failed to query NASA CMR.",
        "i" = conditionMessage(e)
      ))
    }
  )

  httr2::resp_body_json(resp, simplifyVector = FALSE)
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

  tibble::new_tibble(list(
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

#' Parse a search date argument into a `Date`.
#'
#' Accepts strict `YYYY-MM-DD` character strings or `Date` objects. Rejects
#' `POSIXct` (sub-day precision isn't useful for granule search and the
#' time-zone handling is a foot-gun) and malformed strings, with clear
#' errors in each case so user typos get caught at the call site rather
#' than producing an empty result silently.
#' @noRd
parse_search_date <- function(x, arg) {
  if (inherits(x, "Date")) {
    if (length(x) != 1L || is.na(x)) {
      cli::cli_abort("{.arg {arg}} must be a single non-NA date, got {.val {x}}.")
    }
    return(x)
  }
  if (is.character(x)) {
    if (length(x) != 1L) {
      cli::cli_abort(
        "{.arg {arg}} must be length 1, got length {length(x)}."
      )
    }
    if (!grepl("^\\d{4}-\\d{2}-\\d{2}$", x)) {
      cli::cli_abort(c(
        "{.arg {arg}} must be a date string in {.val YYYY-MM-DD} format.",
        "x" = "Got {.val {x}}.",
        "i" = "Example: {.val 2020-06-01}"
      ))
    }
    d <- tryCatch(as.Date(x, format = "%Y-%m-%d"), error = function(e) NA)
    if (is.na(d)) {
      cli::cli_abort(c(
        "{.arg {arg}} does not name a real calendar date.",
        "x" = "Got {.val {x}}."
      ))
    }
    return(d)
  }
  if (inherits(x, "POSIXt")) {
    cli::cli_abort(c(
      "{.arg {arg}} must be a {.cls Date} or a {.val YYYY-MM-DD} string.",
      "x" = "Got a {.cls POSIXct} value.",
      "i" = "Pass {.code format(x, \"%Y-%m-%d\")} instead."
    ))
  }
  cli::cli_abort(c(
    "{.arg {arg}} must be a {.cls Date} or a {.val YYYY-MM-DD} string.",
    "x" = "Got {.cls {class(x)}}."
  ))
}

#' Empty result for zero-match searches.
#' @noRd
empty_find_result <- function() {
  tibble::new_tibble(list(
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
