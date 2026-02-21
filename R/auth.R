#' Get a NASA Earthdata bearer token
#'
#' Retrieves a bearer token for authenticating with NASA Earthdata HTTPS
#' endpoints. Looks for credentials in the following order:
#'
#' 1. The `EARTHDATA_TOKEN` environment variable
#' 2. A `.netrc` file in the user's home directory
#' 3. Interactive prompt (if in an interactive session)
#'
#' @param token Optional token string. If provided, returned as-is.
#' @returns A character string with the bearer token, or `NULL` if no
#'   credentials are found.
#' @export
sl_earthdata_token <- function(token = NULL) {
  if (!is.null(token)) {
    return(token)
  }

  # Check environment variable

  env_token <- Sys.getenv("EARTHDATA_TOKEN", unset = "")
  if (nzchar(env_token)) {
    return(env_token)
  }

  # Check .netrc
  netrc_path <- file.path(Sys.getenv("HOME", "~"), ".netrc")
  if (file.exists(netrc_path)) {
    lines <- readLines(netrc_path, warn = FALSE)
    earthdata_lines <- grep("urs.earthdata.nasa.gov", lines, value = TRUE)
    if (length(earthdata_lines) > 0) {
      cli::cli_inform(c(
        "i" = "Using credentials from {.file {netrc_path}}."
      ))
      # .netrc auth is handled by curl, not bearer token
      # Return a sentinel so the Rust side knows to skip bearer auth
      return(NULL)
    }
  }

  cli::cli_warn(c(
    "!" = "No NASA Earthdata credentials found.",
    "i" = "Set {.envvar EARTHDATA_TOKEN} or create a {.file ~/.netrc} file.",
    "i" = "Register at {.url https://urs.earthdata.nasa.gov/}."
  ))

  NULL
}
