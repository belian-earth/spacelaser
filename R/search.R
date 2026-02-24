#' Find GEDI granules for a spatial area
#'
#' Searches NASA's Common Metadata Repository (CMR) for GEDI granules that
#' overlap a bounding box and optional date range. Returns granule URLs that
#' can be passed directly to [grab_gedi()].
#'
#' @param bbox An `sl_bbox` object created by [sl_bbox()], or a numeric vector
#'   `c(xmin, ymin, xmax, ymax)`.
#' @param product Character. GEDI product level: `"L2A"`, `"L2B"`, `"L4A"`, or
#'   `"L1B"`.
#' @param date_start Character or POSIXct. Start of date range (default:
#'   `"2019-03-25"`, the start of GEDI operations).
#' @param date_end Character or POSIXct. End of date range (default: today).
#'
#' @returns A data frame with columns:
#'   \describe{
#'     \item{id}{CMR granule identifier.}
#'     \item{time_start}{POSIXct start time of the granule.}
#'     \item{time_end}{POSIXct end time of the granule.}
#'     \item{url}{HTTPS data URL — pass to [grab_gedi()].}
#'     \item{geometry}{`wk_wkt` polygon of the granule swath footprint.}
#'   }
#'
#' @details
#' No authentication is needed for the search itself; Earthdata credentials
#' are only required when reading data via [grab_gedi()].
#'
#' The CMR search filters by bounding box on the server side. For finer
#' spatial filtering (e.g. against an irregular polygon), filter the returned
#' `geometry` column with your favourite spatial package.
#'
#' @seealso [grab_gedi()] to read data from the returned URLs.
#' @importFrom rlang check_required arg_match
#' @export
find_gedi <- function(
  bbox,
  product = c("L2A", "L2B", "L4A", "L1B"),
  date_start = NULL,
  date_end = NULL
) {
  rlang::check_required(bbox)
  product <- rlang::arg_match(product)

  concept_id <- switch(
    product,
    "L1B" = "C2142749196-LPCLOUD",
    "L2A" = "C2142771958-LPCLOUD",
    "L2B" = "C2142776747-LPCLOUD",
    "L4A" = "C2237824918-ORNL_CLOUD"
  )

  result <- search_cmr(
    bbox = bbox,
    concept_id = concept_id,
    date_start = date_start %||% "2019-03-25",
    date_end = date_end,
    product_label = paste0("GEDI ", product)
  )

  new_sl_search(result, product = product, sensor = "gedi")
}

#' Find ICESat-2 granules for a spatial area
#'
#' Searches NASA's CMR for ICESat-2 granules that overlap a bounding box and
#' optional date range. Returns granule URLs that can be passed directly to
#' [grab_icesat2()].
#'
#' @inheritParams find_gedi
#' @param product Character. ICESat-2 product: `"ATL08"`, `"ATL03"`, or
#'   `"ATL06"`.
#'
#' @returns A data frame with the same structure as [find_gedi()].
#'
#' @seealso [grab_icesat2()] to read data from the returned URLs.
#' @importFrom rlang check_required arg_match
#' @export
find_icesat2 <- function(
  bbox,
  product = c("ATL08", "ATL03", "ATL06"),
  date_start = NULL,
  date_end = NULL
) {
  rlang::check_required(bbox)
  product <- rlang::arg_match(product)

  concept_id <- switch(
    product,
    "ATL03" = "C2596864127-NSIDC_CPRD",
    "ATL06" = "C2670138092-NSIDC_CPRD",
    "ATL08" = "C2613553260-NSIDC_CPRD"
  )

  result <- search_cmr(
    bbox = bbox,
    concept_id = concept_id,
    date_start = date_start %||% "2018-10-14",
    date_end = date_end,
    product_label = paste0("ICESat-2 ", product)
  )

  new_sl_search(result, product = product, sensor = "icesat2")
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
#' `sl_gedi_search` or `sl_icesat2_search`, carrying `product` as an
#' attribute so `grab()` can dispatch without the user re-specifying it.
#'
#' @noRd
new_sl_search <- function(df, product, sensor = c("gedi", "icesat2")) {
  sensor <- match.arg(sensor)
  cls <- paste0("sl_", sensor, "_search")
  attr(df, "product") <- product
  class(df) <- c(cls, class(df))
  df
}

#' @export
print.sl_gedi_search <- function(x, ...) {
  product <- attr(x, "product")
  cli::cli_text(
    "{.cls sl_gedi_search} | GEDI {product} | {nrow(x)} granule{?s}"
  )
  print_search_result(x, ...)
}

#' @export
print.sl_icesat2_search <- function(x, ...) {
  product <- attr(x, "product")
  cli::cli_text(
    "{.cls sl_icesat2_search} | ICESat-2 {product} | {nrow(x)} granule{?s}"
  )
  print_search_result(x, ...)
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
