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
#'
#' With `retry_bad_request = TRUE` (CMR searches), a 400 is also retried
#' up to `length(nasa_bad_request_delays())` times, after the delays that
#' function gives. CMR has returned spurious 400s during incidents (on
#' 2026-10-20 a granule search got a 400 that the identical query did not
#' get 18 minutes later); a truly malformed query still fails within
#' seconds. Other 4xx, and 400 from other services, are never retried.
#' @noRd
req_nasa_retry <- function(req, retry_bad_request = FALSE) {
  max_seconds <- nasa_retry_max_seconds()
  if (max_seconds == 0) {
    return(req)
  }
  # Per-request count of 400s retried. A new request object is built for
  # every CMR call, so this state is never shared between calls.
  state <- new.env(parent = emptyenv())
  state$bad_requests <- 0L
  httr2::req_retry(
    req,
    max_seconds = max_seconds,
    retry_on_failure = TRUE,
    is_transient = function(resp) {
      status <- httr2::resp_status(resp)
      if (status %in% nasa_transient_status) {
        return(TRUE)
      }
      if (retry_bad_request && status == 400L &&
          state$bad_requests < length(nasa_bad_request_delays())) {
        state$bad_requests <- state$bad_requests + 1L
        return(TRUE)
      }
      FALSE
    },
    backoff = nasa_retry_backoff,
    after = function(resp) {
      if (httr2::resp_status(resp) == 400L) {
        return(nasa_bad_request_delays()[[state$bad_requests]])
      }
      nasa_retry_after(resp)
    }
  )
}

#' Seconds to wait before each retry of a CMR 400; its length is the
#' number of retries.
#' @noRd
nasa_bad_request_delays <- function() c(2, 5)

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
#' Further fields (such as `cmr_errors`) go in `...`.
#' @noRd
abort_upstream <- function(message, service, url, status = NA_integer_,
                           ..., call = rlang::caller_env()) {
  cli::cli_abort(
    message,
    class = "spacelaser_upstream_error",
    service = service,
    url = url,
    status = status,
    ...,
    call = call,
    .envir = parent.frame()
  )
}

#' Error text from a CMR error response.
#'
#' CMR error bodies are JSON such as
#' `{"errors":["An Internal Error has occurred."]}`. Returns the `errors`
#' entries as a character vector; for any other body, its whitespace-
#' collapsed text cut to 300 characters; for no response or an empty
#' body, `character()`.
#' @noRd
cmr_error_text <- function(resp) {
  if (is.null(resp) || !httr2::resp_has_body(resp)) {
    return(character())
  }
  body <- tryCatch(httr2::resp_body_string(resp), error = function(e) "")
  body <- trimws(gsub("\\s+", " ", body))
  if (!nzchar(body)) {
    return(character())
  }
  json <- tryCatch(
    httr2::resp_body_json(resp, check_type = FALSE, simplifyVector = FALSE),
    error = function(e) NULL
  )
  if (is.list(json) && !is.null(json$errors)) {
    errors <- as.character(unlist(json$errors, use.names = FALSE))
    errors <- errors[nzchar(errors)]
    if (length(errors) > 0L) {
      return(errors)
    }
  }
  if (nchar(body) > 300L) {
    body <- paste0(substr(body, 1L, 297L), "...")
  }
  body
}
