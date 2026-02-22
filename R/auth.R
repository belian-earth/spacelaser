# Package-level environment for caching the bearer token within a session.
.sl_env <- new.env(parent = emptyenv())

#' Get a NASA Earthdata bearer token
#'
#' Retrieves a bearer token for authenticating with NASA Earthdata HTTPS
#' endpoints. Credentials are resolved in this order:
#'
#' 1. The `token` argument (pass-through)
#' 2. The `EARTHDATA_TOKEN` environment variable (a pre-existing bearer token)
#' 3. `EARTHDATA_USERNAME` + `EARTHDATA_PASSWORD` environment variables
#' 4. A `~/.netrc` entry for `urs.earthdata.nasa.gov`
#' 5. Interactive prompt (if running interactively)
#'
#' For options 3-5, the username/password are exchanged for a bearer token
#' via the Earthdata Login token API. The token is cached for the session.
#'
#' @param token Optional token string. If provided, returned as-is.
#' @returns A character string with the bearer token, or `NULL` if no
#'   credentials are found.
#' @export
sl_earthdata_token <- function(token = NULL) {
  if (!is.null(token)) {
    return(token)
  }

  # 1. EARTHDATA_TOKEN env var (pre-existing bearer token)
  env_token <- Sys.getenv("EARTHDATA_TOKEN", unset = "")
  if (nzchar(env_token)) {
    return(env_token)
  }

  # 2. Check session cache (from a previous call)
  cached <- .sl_env$earthdata_token
  if (!is.null(cached)) {
    return(cached)
  }

  # 3. Obtain credentials and exchange for a bearer token
  creds <- earthdata_credentials()
  if (!is.null(creds)) {
    token <- tryCatch(
      rust_earthdata_token(creds$username, creds$password),
      error = function(e) {
        cli::cli_warn(c(
          "!" = "Failed to obtain Earthdata bearer token.",
          "i" = "{conditionMessage(e)}"
        ))
        NULL
      }
    )
    if (!is.null(token) && nzchar(token)) {
      .sl_env$earthdata_token <- token
      cli::cli_inform(c(
        "i" = "Obtained NASA Earthdata bearer token (cached for session)."
      ))
      return(token)
    }
  }

  cli::cli_warn(c(
    "!" = "No NASA Earthdata credentials found.",
    "i" = "Set {.envvar EARTHDATA_TOKEN} or create a {.file ~/.netrc} file.",
    "i" = "Register at {.url https://urs.earthdata.nasa.gov/}."
  ))

  NULL
}

#' Resolve Earthdata username/password from available sources.
#' @returns A list with `username` and `password`, or `NULL`.
#' @noRd
earthdata_credentials <- function() {
  # a. Environment variables
  username <- Sys.getenv("EARTHDATA_USERNAME", unset = "")
  password <- Sys.getenv("EARTHDATA_PASSWORD", unset = "")
  if (nzchar(username) && nzchar(password)) {
    return(list(username = username, password = password))
  }

  # b. .netrc file
  netrc <- parse_netrc("urs.earthdata.nasa.gov")
  if (!is.null(netrc)) {
    return(netrc)
  }

  # c. Interactive prompt
  if (interactive()) {
    cli::cli_inform(c(
      "i" = "Enter your NASA Earthdata Login credentials.",
      "i" = "Register at {.url https://urs.earthdata.nasa.gov/} if needed."
    ))
    username <- readline("Earthdata username: ")
    password <- readline("Earthdata password: ")
    if (nzchar(username) && nzchar(password)) {
      return(list(username = username, password = password))
    }
  }

  NULL
}

#' Parse a .netrc file for a specific machine entry.
#' @param machine The machine hostname to look for.
#' @returns A list with `username` and `password`, or `NULL`.
#' @noRd
parse_netrc <- function(machine) {
  netrc_path <- file.path(Sys.getenv("HOME", "~"), ".netrc")
  if (!file.exists(netrc_path)) {
    return(NULL)
  }

  lines <- readLines(netrc_path, warn = FALSE)
  # Collapse into a single string and tokenize
  tokens <- unlist(strsplit(paste(lines, collapse = " "), "\\s+"))
  tokens <- tokens[nzchar(tokens)]

  i <- 1L
  while (i <= length(tokens)) {
    if (tokens[i] == "machine" && i + 1L <= length(tokens) &&
        tokens[i + 1L] == machine) {
      # Found our machine -- scan for login and password
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

  NULL
}
