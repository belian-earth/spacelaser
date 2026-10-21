# ---------------------------------------------------------------------------
# NASA retry policy (http.R) against a local webfakes server
# ---------------------------------------------------------------------------
#
# httr2's mocked responses bypass its retry loop, so these tests serve real
# HTTP from local webfakes processes. Backoff and the CMR 400 delays are
# mocked to 0 and the retry budget set to 1 s, so no test sleeps for a
# backoff. The timeout test waits for one 0.5 s request timeout.

# A webfakes delay blocks the whole process, so the stalled response comes
# from a second server that nothing else uses.
hang_app <- function() {
  app <- webfakes::new_app()
  app$get("/hang", function(req, res) {
    if (is.null(res$locals$seen)) {
      res$locals$seen <- TRUE
      return(res$delay(5))
    }
    res$send_status(200L)
  })
  app
}

nasa_test_app <- function(hang_url) {
  app <- webfakes::new_app()
  app$locals$hits <- list()
  app$locals$hang_url <- hang_url
  hit <- function(req) {
    key <- req$path
    n <- (req$app$locals$hits[[key]] %||% 0L) + 1L
    req$app$locals$hits[[key]] <- n
    n
  }
  feed <- function(res) {
    res$set_type("application/json")$send('{"feed": {"entry": []}}')
  }

  # 504 on the first request, 200 after.
  app$get("/flaky", function(req, res) {
    if (hit(req) == 1L) res$send_status(504L) else feed(res)
  })
  # The first request is redirected to the hang server, which sends no
  # bytes before the client times out; 200 after.
  app$get("/stall", function(req, res) {
    if (hit(req) == 1L) {
      res$redirect(req$app$locals$hang_url, 307L)
    } else {
      feed(res)
    }
  })
  app$get("/down", function(req, res) {
    hit(req)
    res$send_status(500L)
  })
  # CMR-shaped error body on every request.
  app$get("/bad", function(req, res) {
    hit(req)
    res$set_status(400L)$set_type("application/json")$send(
      '{"errors":["An Internal Error has occurred."]}'
    )
  })
  # A spurious 400 on the first request, 200 after.
  app$get("/bad-once", function(req, res) {
    if (hit(req) == 1L) res$send_status(400L) else feed(res)
  })
  # URS-shaped endpoints for ed_urs_request().
  app$get("/api/users/tokens", function(req, res) {
    if (hit(req) == 1L) {
      res$send_status(503L)
    } else {
      res$set_type("application/json")$send("[]")
    }
  })
  app$get("/api/users/bad", function(req, res) {
    hit(req)
    res$send_status(400L)
  })
  app$post("/api/users/token", function(req, res) {
    hit(req)
    res$send_status(503L)
  })
  app$get("/hits", function(req, res) {
    res$send_json(as.list(req$app$locals$hits), auto_unbox = TRUE)
  })
  app
}

hits <- function(server, path) {
  h <- httr2::request(server$url("/hits")) |>
    httr2::req_perform() |>
    httr2::resp_body_json()
  h[[path]] %||% 0L
}

local_fast_retry <- function(.env = parent.frame()) {
  testthat::local_mocked_bindings(
    nasa_retry_backoff = function(i) 0,
    nasa_bad_request_delays = function() c(0, 0),
    .env = .env
  )
  withr::local_options(spacelaser.cmr_max_seconds = 1, .local_envir = .env)
}

test_that("backoff is exponential, capped at 60 s, with under 1 s of jitter", {
  b <- vapply(1:10, nasa_retry_backoff, numeric(1))
  expect_true(all(b - pmin(60, 2^(1:10)) > 0))
  expect_true(all(b - pmin(60, 2^(1:10)) <= 1))
})

test_that("the retry budget comes from spacelaser.cmr_max_seconds", {
  withr::local_options(spacelaser.cmr_max_seconds = NULL)
  expect_equal(nasa_retry_max_seconds(), 600)
  withr::local_options(spacelaser.cmr_max_seconds = 90)
  expect_equal(nasa_retry_max_seconds(), 90)
  withr::local_options(spacelaser.cmr_max_seconds = "ten")
  expect_error(nasa_retry_max_seconds(), "non-negative")
})

test_that("CMR error text comes from the errors array, else the trimmed body", {
  resp <- function(body, type = "application/json") {
    httr2::response(400L, headers = list(`Content-Type` = type),
                    body = charToRaw(body))
  }
  expect_equal(
    cmr_error_text(resp('{"errors":["Bad bounding box.", "Bad temporal."]}')),
    c("Bad bounding box.", "Bad temporal.")
  )
  expect_equal(
    cmr_error_text(resp("  <html>\n  Service   down </html> ", "text/html")),
    "<html> Service down </html>"
  )
  long <- cmr_error_text(resp(strrep("x", 1000), "text/plain"))
  expect_equal(nchar(long), 300L)
  expect_identical(cmr_error_text(httr2::response(400L)), character())
  expect_identical(cmr_error_text(NULL), character())
})

skip_if_not_installed("webfakes")
hang <- webfakes::local_app_process(hang_app())
server <- webfakes::local_app_process(nasa_test_app(hang$url("/hang")))

test_that("a 504 then a 200 succeeds", {
  local_fast_retry()
  json <- fetch_cmr_json(server$url("/flaky"))
  expect_equal(json$feed$entry, list())
  expect_equal(hits(server, "/flaky"), 2L)
})

test_that("a timeout then a 200 succeeds", {
  local_fast_retry()
  local_mocked_bindings(nasa_request_timeout = function() 0.5)
  json <- fetch_cmr_json(server$url("/stall"))
  expect_equal(json$feed$entry, list())
  expect_equal(hits(server, "/stall"), 2L)
})

test_that("a persistent 500 ends in spacelaser_upstream_error after the cap", {
  local_fast_retry()
  url <- server$url("/down")
  t0 <- Sys.time()
  err <- expect_error(fetch_cmr_json(url), class = "spacelaser_upstream_error")
  expect_gte(as.numeric(Sys.time() - t0, units = "secs"), 1)
  expect_gt(hits(server, "/down"), 1L)
  expect_equal(err$service, "cmr")
  expect_equal(err$url, url)
  expect_equal(err$status, 500L)
  expect_identical(err$cmr_errors, character())
  expect_match(conditionMessage(err), "Failed to query NASA CMR (127.0.0.1)", fixed = TRUE)
})

test_that("a CMR 400 then a 200 succeeds after one retry", {
  local_fast_retry()
  json <- fetch_cmr_json(server$url("/bad-once"))
  expect_equal(json$feed$entry, list())
  expect_equal(hits(server, "/bad-once"), 2L)
})

test_that("a persistent CMR 400 fails after 3 attempts as a 400 with CMR's error text", {
  local_fast_retry()
  url <- server$url("/bad")
  err <- expect_error(fetch_cmr_json(url), "Failed to query NASA CMR")
  expect_false(inherits(err, "spacelaser_upstream_error"))
  expect_equal(hits(server, "/bad"), 3L)
  expect_equal(err$status, 400L)
  expect_equal(err$service, "cmr")
  expect_equal(err$url, url)
  expect_equal(err$cmr_errors, "An Internal Error has occurred.")
  expect_match(conditionMessage(err), "HTTP 400", fixed = TRUE)
  expect_match(conditionMessage(err), "An Internal Error has occurred.", fixed = TRUE)
})

test_that("a 400 from the Earthdata token API is not retried", {
  local_fast_retry()
  base <- sub("/$", "", server$url("/"))
  expect_error(
    ed_urs_request("api/users/bad", "GET", "u", "p", base_url = base),
    "HTTP 400"
  )
  expect_equal(hits(server, "/api/users/bad"), 1L)
})

test_that("a curl failure without a response is an upstream error with status NA", {
  local_fast_retry()
  # A port nothing listens on: connection refused on every attempt.
  url <- "http://127.0.0.1:9/search"
  err <- expect_error(fetch_cmr_json(url), class = "spacelaser_upstream_error")
  expect_equal(err$service, "cmr")
  expect_identical(err$status, NA_integer_)
})

test_that("spacelaser.cmr_max_seconds = 0 disables retries", {
  withr::local_options(spacelaser.cmr_max_seconds = 0)
  before <- hits(server, "/down")
  expect_error(fetch_cmr_json(server$url("/down")), class = "spacelaser_upstream_error")
  expect_equal(hits(server, "/down"), before + 1L)
})

test_that("Earthdata token GETs retry; POSTs do not, and fail as upstream errors", {
  local_fast_retry()
  base <- sub("/$", "", server$url("/"))

  resp <- ed_urs_request("api/users/tokens", "GET", "u", "p", base_url = base)
  expect_equal(httr2::resp_status(resp), 200L)
  expect_equal(hits(server, "/api/users/tokens"), 2L)

  err <- expect_error(
    ed_urs_request("api/users/token", "POST", "u", "p", base_url = base),
    class = "spacelaser_upstream_error"
  )
  expect_equal(hits(server, "/api/users/token"), 1L)
  expect_equal(err$service, "earthdata")
  expect_equal(err$status, 503L)
  expect_match(conditionMessage(err), "HTTP 503")
})

test_that("Retry-After is capped at 60 s and absent means the backoff", {
  # httr2 checks the budget before each wait, so an uncapped header would
  # sleep past it.
  resp <- function(...) httr2::response(503L, headers = list(...))
  expect_equal(nasa_retry_after(resp(`Retry-After` = "3600")), 60)
  expect_equal(nasa_retry_after(resp(`Retry-After` = "5")), 5)
  expect_true(is.na(nasa_retry_after(resp())))
})
