# Generate a NASA Earthdata bearer token

Mints (or reuses) a NASA Earthdata Login (URS) bearer token via the URS
user token API, using your Earthdata username and password. URS allows
at most two active tokens per account. By default an existing token is
reused; set `new = TRUE` to force a fresh one.

## Usage

``` r
generate_ed_token(
  username = Sys.getenv("EARTHDATA_USERNAME", unset = ""),
  password = Sys.getenv("EARTHDATA_PASSWORD", unset = ""),
  new = FALSE,
  set_renviron = FALSE
)
```

## Arguments

- username:

  NASA Earthdata username. Defaults to the `EARTHDATA_USERNAME`
  environment variable.

- password:

  NASA Earthdata password. Defaults to the `EARTHDATA_PASSWORD`
  environment variable.

- new:

  If `FALSE` (default), reuse an existing token when the account has
  one, only minting a new token if none exist. If `TRUE`, always mint a
  fresh token; because URS caps accounts at two, this errors when two
  already exist (revoke one first).

- set_renviron:

  If `TRUE`, append (or update) `EARTHDATA_TOKEN` in `~/.Renviron` and
  set it in the current session. Defaults to `FALSE`, in which case the
  token is only returned and set for the current session.

## Value

The bearer token, invisibly.

## Details

Tokens are valid for 60 days. Use this once at setup, then store the
returned token in `~/.Renviron` as `EARTHDATA_TOKEN=<token>` so it is
picked up automatically in future sessions. Re-run when the token
expires.

## Examples

``` r
if (FALSE) { # interactive()
# With EARTHDATA_USERNAME / EARTHDATA_PASSWORD set:
token <- generate_ed_token()
# Force a fresh token and persist it for future sessions:
generate_ed_token(new = TRUE, set_renviron = TRUE)
}
```
