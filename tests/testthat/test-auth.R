# ---------------------------------------------------------------------------
# Earthdata bearer-token resolution (auth.R)
# ---------------------------------------------------------------------------
#
# Token-only auth. Exercises:
#   1. EARTHDATA_TOKEN present -> resolved and cached
#   2. EARTHDATA_TOKEN absent  -> sl_earthdata_creds() aborts with guidance
#   3. session cache + sl_reset_auth()
#   4. the ~/.Renviron writer used by generate_ed_token()
#
# All tests isolate env + the session-level cache so they run
# deterministically regardless of the developer's real credentials.

clear_creds_cache <- function() {
  if (exists("earthdata_creds", envir = spacelaser:::.sl_env, inherits = FALSE)) {
    rm("earthdata_creds", envir = spacelaser:::.sl_env)
  }
}

local_isolated_auth <- function(.local_envir = parent.frame()) {
  scratch <- withr::local_tempdir(.local_envir = .local_envir)
  withr::local_envvar(
    EARTHDATA_TOKEN = "",
    HOME = scratch,
    .local_envir = .local_envir
  )
  clear_creds_cache()
  withr::defer(clear_creds_cache(), envir = .local_envir)
  scratch
}

test_that("EARTHDATA_TOKEN resolves to a token credential", {
  local_isolated_auth()
  withr::local_envvar(EARTHDATA_TOKEN = "  abc.def.ghi  ")

  creds <- spacelaser:::sl_earthdata_creds()
  expect_equal(creds$token, "abc.def.ghi") # trimmed
})

test_that("missing token aborts with setup guidance", {
  local_isolated_auth()
  expect_error(
    spacelaser:::sl_earthdata_creds(),
    "No NASA Earthdata token found"
  )
})

test_that("token is cached, and sl_reset_auth() forces a re-read", {
  local_isolated_auth()
  withr::local_envvar(EARTHDATA_TOKEN = "first-token")

  first <- spacelaser:::sl_earthdata_creds()
  # Flip the env var — a fresh resolve would pick this up, but the cache
  # should stick to the original value until reset.
  Sys.setenv(EARTHDATA_TOKEN = "second-token")
  expect_equal(spacelaser:::sl_earthdata_creds()$token, first$token)

  sl_reset_auth()
  expect_equal(spacelaser:::sl_earthdata_creds()$token, "second-token")
})

test_that("generate_ed_token() requires username and password", {
  expect_error(
    generate_ed_token(username = "", password = ""),
    "username and password are required"
  )
})

rec <- function(token, expires) {
  list(token = token, expires = as.Date(expires))
}

test_that("ed_token_reuse_or_create reuses or mints per `new` and the 2-token cap", {
  created <- 0L
  fake_create <- function(username, password) {
    created <<- created + 1L
    "minted"
  }

  # new = FALSE: reuse an existing token, never mint.
  testthat::local_mocked_bindings(
    ed_token_list = function(username, password) {
      list(rec("existing-1", "2026-08-08"), rec("existing-2", "2026-08-30"))
    },
    ed_token_create = fake_create
  )
  expect_type(spacelaser:::ed_token_reuse_or_create("u", "p", new = FALSE), "character")
  expect_equal(created, 0L)

  # new = FALSE with no tokens: mint one.
  testthat::local_mocked_bindings(
    ed_token_list = function(username, password) list(),
    ed_token_create = fake_create
  )
  expect_equal(spacelaser:::ed_token_reuse_or_create("u", "p", new = FALSE), "minted")
  expect_equal(created, 1L)

  # new = TRUE with one existing token: mint a second.
  testthat::local_mocked_bindings(
    ed_token_list = function(username, password) list(rec("existing-1", "2026-08-08")),
    ed_token_create = fake_create
  )
  expect_equal(spacelaser:::ed_token_reuse_or_create("u", "p", new = TRUE), "minted")

  # new = TRUE at the two-token cap: error, do not mint.
  testthat::local_mocked_bindings(
    ed_token_list = function(username, password) {
      list(rec("existing-1", "2026-08-08"), rec("existing-2", "2026-08-30"))
    },
    ed_token_create = fake_create
  )
  expect_error(
    spacelaser:::ed_token_reuse_or_create("u", "p", new = TRUE),
    "maximum of two tokens"
  )
})

test_that("reuse prefers the longest-lived token regardless of list order", {
  testthat::local_mocked_bindings(
    ed_token_list = function(username, password) {
      # Deliberately oldest-last to prove selection is by expiry, not position.
      list(rec("longer", "2026-08-30"), rec("shorter", "2026-08-08"))
    },
    ed_token_create = function(username, password) stop("should not mint")
  )
  expect_equal(spacelaser:::ed_token_reuse_or_create("u", "p", new = FALSE), "longer")
})

test_that("longest_lived_token tolerates unparseable expiries", {
  # A dated token beats an NA-expiry one.
  toks <- list(rec("no-date", NA), rec("dated", "2026-08-30"))
  expect_equal(spacelaser:::longest_lived_token(toks), "dated")
  # All NA: fall back to the first entry.
  toks <- list(rec("first", NA), rec("second", NA))
  expect_equal(spacelaser:::longest_lived_token(toks), "first")
})

test_that("write_renviron_token adds, updates, and dedupes EARTHDATA_TOKEN", {
  scratch <- withr::local_tempdir()
  withr::local_envvar(HOME = scratch)
  rn <- file.path(scratch, ".Renviron")
  writer <- spacelaser:::write_renviron_token

  # Fresh file: token is appended.
  writer("tok1")
  expect_equal(readLines(rn), "EARTHDATA_TOKEN=tok1")

  # Existing unrelated lines preserved; stale token line replaced.
  writeLines(c("FOO=1", "EARTHDATA_TOKEN=stale", "BAR=2"), rn)
  writer("tok2")
  expect_equal(readLines(rn), c("FOO=1", "BAR=2", "EARTHDATA_TOKEN=tok2"))
})
