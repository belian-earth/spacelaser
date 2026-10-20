# ---------------------------------------------------------------------------
# Internal: shared HTTP policy for NASA services (CMR, Earthdata Login)
# ---------------------------------------------------------------------------
#
# CMR and URS fail transiently: timeouts with no bytes, then 5xx, during
# outages that can last tens of minutes. httr2 retries neither curl
# failures nor 500/502/504 by default, so every idempotent request to a
# NASA API goes through `req_nasa_retry()`. Granule reads are made by the
# Rust reader and have their own retry logic.

#' HTTP statuses treated as transient for NASA APIs.
#' @noRd
nasa_transient_status <- c(429L, 500L, 502L, 503L, 504L)

#' Apply the NASA retry policy to an idempotent httr2 request.
#'
#' Retries curl failures (timeouts, refused or reset connections) and the
#' statuses in `nasa_transient_status`, with capped exponential backoff,
#' for up to `nasa_retry_max_seconds()` in total. A `Retry-After` header
#' on 429 or 503 takes precedence over the backoff, capped at 60 s: httr2
#' checks the budget before each wait, not after, so an uncapped header
#' (`Retry-After: 3600`) would overrun it.
#' @noRd
req_nasa_retry <- function(req) {
  max_seconds <- nasa_retry_max_seconds()
  if (max_seconds == 0) {
    return(req)
  }
  httr2::req_retry(
    req,
    max_seconds = max_seconds,
    retry_on_failure = TRUE,
    is_transient = function(resp) {
      httr2::resp_status(resp) %in% nasa_transient_status
    },
    backoff = nasa_retry_backoff,
    after = nasa_retry_after
  )
}

#' Seconds to wait before retry `i`: 2^i capped at 60, plus up to 1 s of
#' jitter so parallel workers do not retry in step.
#' @noRd
nasa_retry_backoff <- function(i) {
  min(60, 2^i) + sample.int(1000L, 1L) / 1000
}

#' Seconds to wait from a `Retry-After` header, capped at 60; `NA` (no
#' header) falls back to the backoff.
#' @noRd
nasa_retry_after <- function(resp) {
  after <- httr2::resp_retry_after(resp)
  if (is.na(after)) NA else min(after, 60)
}

#' Total retry budget in seconds, from the `spacelaser.cmr_max_seconds`
#' option (default 600). Zero disables retries.
#' @noRd
nasa_retry_max_seconds <- function() {
  x <- getOption("spacelaser.cmr_max_seconds", 600)
  if (!is.numeric(x) || length(x) != 1L || is.na(x) || x < 0) {
    cli::cli_abort(
      "Option {.code spacelaser.cmr_max_seconds} must be a single non-negative number of seconds."
    )
  }
  ceiling(x)
}

#' Per-request timeout in seconds for NASA API requests.
#' @noRd
nasa_request_timeout <- function() 30

#' Is an error from `httr2::req_perform()` a transient upstream failure?
#'
#' True for curl failures and for HTTP errors whose status is in
#' `nasa_transient_status`.
#' @noRd
is_transient_http_error <- function(e) {
  if (inherits(e, "httr2_failure")) {
    return(TRUE)
  }
  inherits(e, "httr2_http") &&
    !is.null(e$resp) &&
    httr2::resp_status(e$resp) %in% nasa_transient_status
}

#' Raise the error for a NASA service that is unavailable.
#'
#' Carries class `spacelaser_upstream_error` with fields `service`
#' ("cmr" or "earthdata"), `url` and `status` (the HTTP status, or `NA`
#' for a curl failure), so callers can tell an outage from a bad request.
#' @noRd
abort_upstream <- function(message, service, url, status = NA_integer_,
                           call = rlang::caller_env()) {
  cli::cli_abort(
    message,
    class = "spacelaser_upstream_error",
    service = service,
    url = url,
    status = status,
    call = call,
    .envir = parent.frame()
  )
}
