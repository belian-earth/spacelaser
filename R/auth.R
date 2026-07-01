#' Resolve a NASA Earthdata bearer token for data access.
#'
#' Returns the bearer token the Rust reader sends to NASA Earthdata. The token
#' goes as `Authorization: Bearer` to the DAAC host, which validates it and
#' redirects to a presigned CloudFront/S3 URL. This is the sole authentication
#' mechanism: no username/password or `.netrc` is used at read time.
#'
#' The token is read from the `EARTHDATA_TOKEN` environment variable on every
#' call, so a token updated mid-session (e.g. via [generate_ed_token()] or
#' `Sys.setenv()`) is picked up on the next read. Mint one at
#' <https://urs.earthdata.nasa.gov> (Generate Token), or with
#' [generate_ed_token()], then set it in `~/.Renviron` as
#' `EARTHDATA_TOKEN=<token>`.
#'
#' Earthdata tokens expire after 60 days. An expired token surfaces as a clear
#' error at read time telling you to mint a new one.
#'
#' @returns A list with a single element `token`.
#' @noRd
sl_earthdata_creds <- function() {
  token <- trimws(Sys.getenv("EARTHDATA_TOKEN", unset = ""))
  if (nzchar(token)) {
    return(list(token = token))
  }

  cli::cli_abort(c(
    "No NASA Earthdata token found.",
    "i" = "Set {.envvar EARTHDATA_TOKEN} to a bearer token.",
    "i" = "Mint one with {.code spacelaser::generate_ed_token()},",
    "i" = "or at {.url https://urs.earthdata.nasa.gov/} (Generate Token).",
    "i" = "Then add {.code EARTHDATA_TOKEN=<token>} to your {.file ~/.Renviron}."
  ))
}

#' Generate a NASA Earthdata bearer token
#'
#' Mints (or reuses) a NASA Earthdata Login (URS) bearer token via the URS user
#' token API, using your Earthdata username and password. URS allows at most two
#' active tokens per account. By default an existing token is reused; set
#' `new = TRUE` to force a fresh one.
#'
#' Tokens are valid for 60 days. Use this once at setup, then store the returned
#' token in `~/.Renviron` as `EARTHDATA_TOKEN=<token>` so it is picked up
#' automatically in future sessions. Re-run when the token expires.
#'
#' @param username NASA Earthdata username. Defaults to the `EARTHDATA_USERNAME`
#'   environment variable.
#' @param password NASA Earthdata password. Defaults to the `EARTHDATA_PASSWORD`
#'   environment variable.
#' @param new If `FALSE` (default), reuse an existing token when the account has
#'   one, only minting a new token if none exist. If `TRUE`, always mint a fresh
#'   token; because URS caps accounts at two, this errors when two already
#'   exist (revoke one first).
#' @param set_renviron If `TRUE`, append (or update) `EARTHDATA_TOKEN` in
#'   `~/.Renviron` and set it in the current session. Defaults to `FALSE`, in
#'   which case the token is only returned and set for the current session.
#'
#' @returns The bearer token, invisibly.
#' @examplesIf interactive()
#' # With EARTHDATA_USERNAME / EARTHDATA_PASSWORD set:
#' token <- generate_ed_token()
#' # Force a fresh token and persist it for future sessions:
#' generate_ed_token(new = TRUE, set_renviron = TRUE)
#' @export
generate_ed_token <- function(
  username = Sys.getenv("EARTHDATA_USERNAME", unset = ""),
  password = Sys.getenv("EARTHDATA_PASSWORD", unset = ""),
  new = FALSE,
  set_renviron = FALSE
) {
  if (!nzchar(username) || !nzchar(password)) {
    cli::cli_abort(c(
      "Earthdata username and password are required to mint a token.",
      "i" = "Pass {.arg username}/{.arg password}, or set {.envvar EARTHDATA_USERNAME} and {.envvar EARTHDATA_PASSWORD}.",
      "i" = "Register at {.url https://urs.earthdata.nasa.gov/}."
    ))
  }

  token <- ed_token_reuse_or_create(username, password, new = new)

  Sys.setenv(EARTHDATA_TOKEN = token)

  cli::cli_alert_success(
    "Retrieved a NASA Earthdata token and set {.envvar EARTHDATA_TOKEN} for this R session."
  )
  if (isTRUE(set_renviron)) {
    path <- write_renviron_token(token)
    cli::cli_alert_success(
      "Saved it to {.file {path}}; it will load automatically in new sessions."
    )
  } else {
    cli::cli_alert_info(
      "To persist it across sessions, either re-run with {.code set_renviron = TRUE}, or add this line to {.file ~/.Renviron}:"
    )
    # The token itself is not a cli-formatted field: print it plain so it
    # copy-pastes cleanly without markup or truncation.
    cli::cli_code(paste0("EARTHDATA_TOKEN=", token))
  }
  cli::cli_alert_info("The token is valid for 60 days; re-run {.fn generate_ed_token} when it expires.")

  invisible(token)
}

#' Reuse an existing URS token or create a new one.
#'
#' URS caps accounts at two tokens. When `new` is `FALSE`, reuse an existing
#' token if present, otherwise create one. When `new` is `TRUE`, always create
#' a fresh token, erroring if the account is already at the two-token cap.
#' @noRd
ed_token_reuse_or_create <- function(username, password, new = FALSE) {
  existing <- ed_token_list(username, password)

  if (!new && length(existing) > 0L) {
    # Reuse the longest-lived token so the caller gets the most days before
    # the next refresh. URS lists tokens oldest-first, so this is usually the
    # last entry, but selecting by expiry is robust to any ordering.
    return(longest_lived_token(existing))
  }

  if (length(existing) >= 2L) {
    cli::cli_abort(c(
      "Your NASA Earthdata account already has the maximum of two tokens.",
      "i" = "Revoke one at {.url https://urs.earthdata.nasa.gov/} (Generate Token page),",
      "i" = "or call {.code generate_ed_token(new = FALSE)} to reuse an existing token."
    ))
  }

  ed_token_create(username, password)
}

#' Pick the `token` string with the latest expiry from an `ed_token_list()`
#' result. Tokens with an unparseable expiry sort last; ties resolve to the
#' first such token.
#' @noRd
longest_lived_token <- function(tokens) {
  expiry_num <- vapply(tokens, function(t) {
    e <- t$expires
    if (length(e) != 1L || is.na(e)) -Inf else as.numeric(e)
  }, numeric(1))
  tokens[[which.max(expiry_num)]]$token
}

#' List existing URS tokens (GET /api/users/tokens).
#' @returns A list of records, each `list(token = <chr>, expires = <Date>)`.
#'   Entries without a usable `access_token` are dropped; the list is empty
#'   when the account has no tokens.
#' @noRd
ed_token_list <- function(username, password) {
  resp <- ed_urs_request("api/users/tokens", "GET", username, password)
  body <- httr2::resp_body_json(resp, simplifyVector = FALSE)
  records <- lapply(body, function(t) {
    list(
      token = t$access_token %||% NA_character_,
      expires = parse_urs_date(t$expiration_date %||% NA_character_)
    )
  })
  Filter(function(r) !is.na(r$token), records)
}

#' Parse a URS `expiration_date` ("MM/DD/YYYY") into a `Date`.
#' Returns `NA` (a `Date` `NA`) when the value is missing or unparseable.
#' @noRd
parse_urs_date <- function(x) {
  if (length(x) != 1L || is.na(x) || !nzchar(x)) {
    return(as.Date(NA_character_))
  }
  as.Date(x, format = "%m/%d/%Y")
}

#' Create a new URS token (POST /api/users/token).
#' @returns The new `access_token` string.
#' @noRd
ed_token_create <- function(username, password) {
  resp <- ed_urs_request("api/users/token", "POST", username, password)
  body <- httr2::resp_body_json(resp, simplifyVector = FALSE)
  tok <- body$access_token
  if (is.null(tok) || !nzchar(tok)) {
    cli::cli_abort("URS returned no access token.")
  }
  tok
}

#' Perform an authenticated request against the URS token API.
#'
#' Uses HTTP Basic auth with the Earthdata username/password. Surfaces URS's
#' JSON error bodies (e.g. account locked, bad credentials) as R errors.
#' @noRd
ed_urs_request <- function(path, method, username, password) {
  req <- httr2::request("https://urs.earthdata.nasa.gov") |>
    httr2::req_url_path(path) |>
    httr2::req_method(method) |>
    httr2::req_auth_basic(username, password) |>
    httr2::req_user_agent("spacelaser (github.com/belian-earth/spacelaser)") |>
    httr2::req_timeout(30) |>
    httr2::req_error(is_error = function(resp) FALSE)

  resp <- tryCatch(
    httr2::req_perform(req),
    error = function(e) {
      cli::cli_abort(c(
        "Failed to reach the NASA Earthdata token API.",
        "i" = conditionMessage(e)
      ))
    }
  )

  status <- httr2::resp_status(resp)
  if (status >= 400L) {
    detail <- ed_error_detail(resp)
    cli::cli_abort(c(
      "NASA Earthdata token request failed (HTTP {status}).",
      "i" = detail
    ))
  }
  resp
}

#' Extract a human-readable message from a URS JSON error body.
#' @noRd
ed_error_detail <- function(resp) {
  body <- tryCatch(
    httr2::resp_body_json(resp, simplifyVector = FALSE),
    error = function(e) NULL
  )
  if (is.null(body)) {
    return("The Earthdata server returned an error with no details.")
  }
  desc <- body$error_description %||% body$error %||% NULL
  if (is.null(desc)) {
    return("The Earthdata server returned an unrecognised error.")
  }
  desc
}

#' Append or update EARTHDATA_TOKEN in ~/.Renviron.
#' @noRd
write_renviron_token <- function(token) {
  path <- file.path(Sys.getenv("HOME", "~"), ".Renviron")
  line <- paste0("EARTHDATA_TOKEN=", token)

  existing <- if (file.exists(path)) readLines(path, warn = FALSE) else character()
  keep <- existing[!grepl("^\\s*EARTHDATA_TOKEN\\s*=", existing)]
  writeLines(c(keep, line), path)
  invisible(path)
}
