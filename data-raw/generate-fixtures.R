# ---------------------------------------------------------------------------
# Synthetic HDF5 fixture generator — orchestrator
# ---------------------------------------------------------------------------
#
# Generates tiny HDF5 files that mirror the real GEDI and ICESat-2
# schemas for offline parser / reader testing. Per-product generators
# live in data-raw/fixtures/*.R; this file just sources them and runs
# each in turn.
#
# Add a new product by dropping `data-raw/fixtures/<sensor>-<product>.R`
# beside the existing ones, defining a `make_<sensor>_<product>()`
# function, and wiring a call below.
#
# Run from the package root:
#   Rscript data-raw/generate-fixtures.R

suppressPackageStartupMessages({
  library(hdf5r)
  devtools::load_all(quiet = TRUE)
})

FIXTURE_DIR <- file.path("tests", "testthat", "fixtures")
dir.create(FIXTURE_DIR, recursive = TRUE, showWarnings = FALSE)

# Fix the RNG so regeneration is byte-stable. Changes to fixture files
# in git then reflect real schema/generator changes, not random noise.
set.seed(20260414L)

# Load helpers and per-product generators
source("data-raw/fixtures/_helpers.R")
source("data-raw/fixtures/gedi-l1b.R")
source("data-raw/fixtures/gedi-l2a.R")
source("data-raw/fixtures/gedi-l2b.R")
source("data-raw/fixtures/gedi-l4a.R")
source("data-raw/fixtures/gedi-l4c.R")
source("data-raw/fixtures/icesat2-atl08.R")

generate <- function(label, maker, filename) {
  message(sprintf("Generating synthetic %s fixture", label))
  path <- file.path(FIXTURE_DIR, filename)
  maker(path)
  report_size(path)
}

generate("GEDI L1B", make_gedi_l1b, "gedi-l1b.h5")
generate("GEDI L2A", make_gedi_l2a, "gedi-l2a.h5")
generate("GEDI L2B", make_gedi_l2b, "gedi-l2b.h5")
generate("GEDI L4A", make_gedi_l4a, "gedi-l4a.h5")
generate("GEDI L4C", make_gedi_l4c, "gedi-l4c.h5")
generate("ICESat-2 ATL08", make_icesat2_atl08, "icesat2-atl08.h5")

message("Done.")
