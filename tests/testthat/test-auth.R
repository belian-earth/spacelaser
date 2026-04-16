# ---------------------------------------------------------------------------
# Earthdata credential resolution (auth.R)
# ---------------------------------------------------------------------------
#
# Exercises the three public states of the resolver:
#   1. EARTHDATA_USERNAME/PASSWORD env vars present
#   2. netrc file (via GDAL_HTTP_NETRC_FILE or ~/.netrc) contains the
#      urs.earthdata.nasa.gov machine entry
#   3. neither → sl_earthdata_creds() aborts with a helpful message
#
# All tests isolate env + HOME + the session-level cache so they run
# deterministically regardless of the developer's real credentials.

clear_creds_cache <- function() {
  if (exists("earthdata_creds", envir = spacelaser:::.sl_env, inherits = FALSE)) {
    rm("earthdata_creds", envir = spacelaser:::.sl_env)
  }
}

local_isolated_auth <- function(.local_envir = parent.frame()) {
  # Isolate env vars the resolver reads. HOME is redirected to a scratch
  # dir so any real ~/.netrc is invisible; callers can opt in by writing
  # to file.path(Sys.getenv("HOME"), ".netrc").
  scratch <- withr::local_tempdir(.local_envir = .local_envir)
  withr::local_envvar(
    EARTHDATA_USERNAME = "",
    EARTHDATA_PASSWORD = "",
    GDAL_HTTP_NETRC_FILE = "",
    HOME = scratch,
    .local_envir = .local_envir
  )
  clear_creds_cache()
  withr::defer(clear_creds_cache(), envir = .local_envir)
  scratch
}

test_that("env vars win when both env and netrc are present", {
  scratch <- local_isolated_auth()
  writeLines(
    c("machine urs.earthdata.nasa.gov login netuser password netpass"),
    file.path(scratch, ".netrc")
  )
  withr::local_envvar(
    EARTHDATA_USERNAME = "envuser",
    EARTHDATA_PASSWORD = "envpass"
  )

  creds <- spacelaser:::sl_earthdata_creds()
  expect_equal(creds$username, "envuser")
  expect_equal(creds$password, "envpass")
})

test_that("parse_netrc resolves ~/.netrc when no env vars are set", {
  scratch <- local_isolated_auth()
  writeLines(
    c("machine urs.earthdata.nasa.gov", "  login netuser", "  password netpass"),
    file.path(scratch, ".netrc")
  )

  creds <- spacelaser:::sl_earthdata_creds()
  expect_equal(creds$username, "netuser")
  expect_equal(creds$password, "netpass")
})

test_that("GDAL_HTTP_NETRC_FILE is checked before ~/.netrc", {
  scratch <- local_isolated_auth()
  # Home netrc points at a different login; GDAL path should win.
  writeLines(
    "machine urs.earthdata.nasa.gov login homeuser password homepass",
    file.path(scratch, ".netrc")
  )
  gdal_path <- file.path(scratch, "edl_netrc")
  writeLines(
    "machine urs.earthdata.nasa.gov login gdaluser password gdalpass",
    gdal_path
  )
  withr::local_envvar(GDAL_HTTP_NETRC_FILE = gdal_path)

  creds <- spacelaser:::sl_earthdata_creds()
  expect_equal(creds$username, "gdaluser")
})

test_that("parse_netrc skips unrelated machine entries", {
  scratch <- local_isolated_auth()
  writeLines(
    c(
      "machine other.example.com login wrong password wrong",
      "machine urs.earthdata.nasa.gov login right password right"
    ),
    file.path(scratch, ".netrc")
  )

  creds <- spacelaser:::sl_earthdata_creds()
  expect_equal(creds$username, "right")
  expect_equal(creds$password, "right")
})

test_that("missing creds abort with setup guidance", {
  local_isolated_auth()
  expect_error(
    spacelaser:::sl_earthdata_creds(),
    "No NASA Earthdata credentials found"
  )
})

test_that("creds are cached after first successful resolution", {
  scratch <- local_isolated_auth()
  withr::local_envvar(
    EARTHDATA_USERNAME = "one",
    EARTHDATA_PASSWORD = "two"
  )

  first <- spacelaser:::sl_earthdata_creds()
  # Flip the env vars — a fresh resolve would pick these up, but the
  # cache should stick to the original value.
  Sys.setenv(EARTHDATA_USERNAME = "changed", EARTHDATA_PASSWORD = "changed")
  second <- spacelaser:::sl_earthdata_creds()

  expect_equal(second$username, first$username)
  expect_equal(second$password, "two")
})

test_that("parse_netrc returns NULL when no file exists", {
  local_isolated_auth()
  expect_null(spacelaser:::parse_netrc("urs.earthdata.nasa.gov"))
})

test_that("parse_netrc returns NULL when entry lacks login or password", {
  scratch <- local_isolated_auth()
  writeLines(
    "machine urs.earthdata.nasa.gov login onlyuser",
    file.path(scratch, ".netrc")
  )
  expect_null(spacelaser:::parse_netrc("urs.earthdata.nasa.gov"))
})
