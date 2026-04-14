# ---------------------------------------------------------------------------
# Fixture-test helpers
# ---------------------------------------------------------------------------

#' Assert every column in a product's registry appears in a read result.
#'
#' Accepts four shapes per registry entry:
#'   - exact match: `nm %in% out_names`
#'   - 2D expansion: `nm0`, `nm1`, ... (e.g. `rh0..rh100`)
#'   - transposed 2D expansion: `nm_<label>` (e.g. `surface_type_land`)
#'   - the `delta_time` → `time` rename applied by `convert_time = TRUE`
#'
#' @noRd
expect_registry_roundtrip <- function(data, product) {
  registry_names <- names(sl_columns(product))
  out_names <- names(data)
  has_time <- "time" %in% out_names

  missing <- character(0)
  for (nm in registry_names) {
    if (nm %in% out_names) next
    if (any(grepl(paste0("^", nm, "\\d+$"), out_names))) next
    if (any(grepl(paste0("^", nm, "_[a-z_]+$"), out_names))) next
    if (nm == "delta_time" && has_time) next
    missing <- c(missing, nm)
  }
  testthat::expect_equal(missing, character(0))
}
