# ---------------------------------------------------------------------------
# Package startup: register default options so they show up in options()
# ---------------------------------------------------------------------------
#
# Respects the user's existing values (set via .Rprofile or
# `options()` before loading spacelaser). Defaults are only applied
# for options that haven't already been set.

.onLoad <- function(libname, pkgname) {
  op <- options()
  op_spacelaser <- list(
    # Enable the cross-beam spatial-filter optimization: one reference
    # beam scans an inflated lat band per granule, other beams
    # dense-read the resulting shot-index range. See `?sl_read` under
    # "Performance tuning" for the tradeoff.
    spacelaser.cross_beam_scan = FALSE
  )
  toset <- !(names(op_spacelaser) %in% names(op))
  if (any(toset)) options(op_spacelaser[toset])
  invisible()
}
