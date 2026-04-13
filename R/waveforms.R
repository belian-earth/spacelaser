#' Extract L1B waveforms to long form with elevation profile
#'
#' Converts GEDI L1B received waveform list columns into a long-form
#' data frame with one row per waveform sample. Each sample is assigned
#' an elevation by linearly interpolating between `elevation_bin0`
#' (waveform top) and `elevation_lastbin` (waveform bottom).
#'
#' This is the standard preprocessing step for waveform visualisation
#' and analysis. The output can be plotted directly with ggplot2:
#'
#' ```r
#' library(ggplot2)
#' wf <- sl_extract_waveforms(gedi_l1b)
#' ggplot(wf, aes(amplitude, elevation)) +
#'   geom_path() +
#'   facet_wrap(~shot_number)
#' ```
#'
#' @param x A data frame from [sl_read()] containing GEDI L1B data.
#'   Must include columns: `shot_number`, `elevation_bin0`,
#'   `elevation_lastbin`, `rx_sample_count`, and `rxwaveform` (list
#'   column). If `beam` is present, it is carried through to the output.
#'
#'   Tip: request these columns explicitly when reading:
#'   ```r
#'   d <- sl_read(granules, columns = c(
#'     "shot_number", "rx_energy", "rxwaveform"
#'   ))
#'   ```
#'   The `elevation_bin0`, `elevation_lastbin`, and `rx_sample_count`
#'   columns are auto-added when `rxwaveform` is requested.
#'
#' @returns A data frame with one row per waveform sample:
#'   \describe{
#'     \item{shot_number}{Shot identifier (repeated per sample).}
#'     \item{beam}{Beam name, if present in input.}
#'     \item{elevation}{Sample elevation in metres (WGS-84 ellipsoidal),
#'       interpolated from `elevation_bin0` and `elevation_lastbin`.}
#'     \item{amplitude}{Waveform return amplitude (raw float from HDF5).}
#'   }
#'
#' @details
#' The elevation for each sample is computed as:
#'
#' `elevation[i] = elevation_bin0 - i * (elevation_bin0 - elevation_lastbin) / rx_sample_count`
#'
#' where `i` runs from 1 to `rx_sample_count`. This places sample 1 one
#' step below `elevation_bin0` and sample `rx_sample_count` at
#' `elevation_lastbin`, matching the chewie convention.
#'
#' Shots with `NA` elevation or zero `rx_sample_count` are dropped.
#'
#' @seealso [sl_read()] with `columns = c("rxwaveform", ...)`
#' @export
sl_extract_waveforms <- function(x) {
  required <- c(
    "shot_number", "elevation_bin0", "elevation_lastbin",
    "rx_sample_count", "rxwaveform"
  )
  missing <- setdiff(required, names(x))
  if (length(missing) > 0L) {
    cli::cli_abort(c(
      "Input is missing required columns for waveform extraction.",
      "x" = "Missing: {.field {missing}}",
      "i" = "Use {.code sl_read(granules, columns = c(\"rxwaveform\", ...))}
             to read L1B data with waveforms."
    ))
  }

  # Filter to shots with valid elevation and non-zero waveforms
  counts <- as.integer(x$rx_sample_count)
  valid <- !is.na(x$elevation_bin0) &
    !is.na(x$elevation_lastbin) &
    !is.na(counts) &
    counts > 0L
  x <- x[valid, ]
  counts <- counts[valid]

  if (nrow(x) == 0L) {
    cols <- list(
      shot_number = numeric(0),
      elevation = numeric(0),
      amplitude = numeric(0)
    )
    if ("beam" %in% names(x)) cols$beam <- character(0)
    return(tibble::new_tibble(cols))
  }

  n_total <- sum(counts)

  # Vectorised expansion: no per-shot loop.
  # sequence(counts) produces 1,2,...,c[1], 1,2,...,c[2], ...
  sample_idx <- sequence(counts)

  # Per-shot elevation step, repeated for each sample
  step <- (x$elevation_bin0 - x$elevation_lastbin) / counts
  step_rep <- rep(step, counts)
  bin0_rep <- rep(x$elevation_bin0, counts)

  cols <- list(
    shot_number = rep(x$shot_number, counts),
    elevation = bin0_rep - sample_idx * step_rep,
    amplitude = unlist(x$rxwaveform, use.names = FALSE)
  )
  if ("beam" %in% names(cols) || "beam" %in% names(x)) {
    cols$beam <- rep(x$beam, counts)
  }

  tibble::new_tibble(cols, nrow = n_total)
}
