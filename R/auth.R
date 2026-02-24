# Package-level environment for caching credentials within a session.
.sl_env <- new.env(parent = emptyenv())

#' Resolve Earthdata credentials for data access.
#'
#' Returns username/password for the NASA Earthdata OAuth flow. Credentials
#' are resolved in this order:
#'
#' 1. `EARTHDATA_USERNAME` + `EARTHDATA_PASSWORD` environment variables
#' 2. A `.netrc` file (checks `GDAL_HTTP_NETRC_FILE` env var, then `~/.netrc`)
#'
#' Cached for the session after first successful resolution.
#'
#' @returns A list with `username` and `password`.
#' @noRd
sl_earthdata_creds <- function() {
  cached <- .sl_env$earthdata_creds
  if (!is.null(cached)) {
    return(cached)
  }

  creds <- earthdata_credentials()
  if (!is.null(creds)) {
    .sl_env$earthdata_creds <- creds
    return(creds)
  }

  cli::cli_abort(c(
    "No NASA Earthdata credentials found.",
    "i" = 'Run {.code earthdatalogin::edl_netrc()} to set up authentication,',
    "i" = "or set {.envvar EARTHDATA_USERNAME} and {.envvar EARTHDATA_PASSWORD}.",
    "i" = "Register at {.url https://urs.earthdata.nasa.gov/}."
  ))
}

#' Resolve Earthdata username/password from available sources.
#' @returns A list with `username` and `password`, or `NULL`.
#' @noRd
earthdata_credentials <- function() {
  # 1. Environment variables
  username <- Sys.getenv("EARTHDATA_USERNAME", unset = "")
  password <- Sys.getenv("EARTHDATA_PASSWORD", unset = "")
  if (nzchar(username) && nzchar(password)) {
    return(list(username = username, password = password))
  }

  # 2. Netrc file (earthdatalogin's path via GDAL env var, then ~/.netrc)
  netrc <- parse_netrc("urs.earthdata.nasa.gov")
  if (!is.null(netrc)) {
    return(netrc)
  }

  NULL
}

#' Parse a .netrc file for a specific machine entry.
#'
#' Checks `GDAL_HTTP_NETRC_FILE` first (set by earthdatalogin::edl_netrc()),
#' then falls back to `~/.netrc`.
#'
#' @param machine The machine hostname to look for.
#' @returns A list with `username` and `password`, or `NULL`.
#' @noRd
parse_netrc <- function(machine) {
  # Check earthdatalogin's netrc path first (set by edl_netrc())
  gdal_netrc <- Sys.getenv("GDAL_HTTP_NETRC_FILE", unset = "")
  home_netrc <- file.path(Sys.getenv("HOME", "~"), ".netrc")
  paths <- unique(c(
    if (nzchar(gdal_netrc)) gdal_netrc,
    home_netrc
  ))

  for (netrc_path in paths) {
    if (!file.exists(netrc_path)) next

    lines <- readLines(netrc_path, warn = FALSE)
    tokens <- unlist(strsplit(paste(lines, collapse = " "), "\\s+"))
    tokens <- tokens[nzchar(tokens)]

    i <- 1L
    while (i <= length(tokens)) {
      if (tokens[i] == "machine" && i + 1L <= length(tokens) &&
          tokens[i + 1L] == machine) {
        login <- NULL
        password <- NULL
        j <- i + 2L
        while (j <= length(tokens) && tokens[j] != "machine") {
          if (tokens[j] == "login" && j + 1L <= length(tokens)) {
            login <- tokens[j + 1L]
            j <- j + 2L
          } else if (tokens[j] == "password" && j + 1L <= length(tokens)) {
            password <- tokens[j + 1L]
            j <- j + 2L
          } else {
            j <- j + 1L
          }
        }
        if (!is.null(login) && !is.null(password)) {
          return(list(username = login, password = password))
        }
      }
      i <- i + 1L
    }
  }

  NULL
}
