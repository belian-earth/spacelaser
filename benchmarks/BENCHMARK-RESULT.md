
<!-- BENCHMARK-RESULT.md is generated — do not edit by hand.
     Re-render with:
       Rscript -e 'rmarkdown::render("benchmarks/BENCHMARK-RESULT.Rmd")'
     Or run the full pipeline via benchmarks/run.sh
     Methodology, caveats, and reproduction instructions live in README.md. -->

# Benchmark result

GEDI **L2A**, bbox `9.32,0.55,9.35,0.58`, 2020-01-01 → 2021-12-31, 11
granules. Run at **2026-04-15T21:26:33Z**.

## Headline

| Pipeline | Wall time | Download | Read | Bytes on disk | Rows |
|:---|---:|---:|---:|---:|---:|
| Spacelaser (Rust, partial HTTP range reads) | 73.9 s | — | — | — | 1,246 |
| h5coro (Python, partial HTTP range reads) | 120.1 s | — | — | — | 1,246 |
| Status quo (curl + hdf5r, full download) | 1058.9 s | 1057.7 s | 1.3 s | 27.2 Gb | 1,246 |

**Spacelaser: 14.3×** faster than status quo.  
**h5coro: 8.8×** faster than status quo.  
Spacelaser is **1.63×** faster than h5coro.

Status-quo phase split: **1057.7 s downloading** vs **1.3 s reading** —
the bottleneck is data transfer, not HDF5 parsing.

## Equivalence

| Pair | Shared cols | All match | Notes |
|:---|---:|:---|:---|
| hdf5r vs spacelaser | 112 | ✓ |  |
| hdf5r vs h5coro | 112 | ✗ | quality_flag,sensitivity,elev_lowestmode,elev_highestreturn,rh0,rh1,rh2,rh3,rh4,rh5,rh6,rh7,rh8,rh9,rh10,rh11,rh12,rh13,rh14,rh15,rh16,rh17,rh18,rh19,rh20,rh21,rh22,rh23,rh24,rh25,rh26,rh27,rh28,rh29,rh30,rh31,rh32,rh33,rh34,rh35,rh36,rh37,rh38,rh39,rh40,rh41,rh42,rh43,rh44,rh45,rh46,rh47,rh48,rh49,rh50,rh51,rh52,rh53,rh54,rh55,rh56,rh57,rh58,rh59,rh60,rh61,rh62,rh63,rh64,rh65,rh66,rh67,rh68,rh69,rh70,rh71,rh72,rh73,rh74,rh75,rh76,rh77,rh78,rh79,rh80,rh81,rh82,rh83,rh84,rh85,rh86,rh87,rh88,rh89,rh90,rh91,rh92,rh93,rh94,rh95,rh96,rh97,rh98,rh99,rh100 |
| spacelaser vs h5coro | 112 | ✗ | quality_flag,sensitivity,elev_lowestmode,elev_highestreturn,rh0,rh1,rh2,rh3,rh4,rh5,rh6,rh7,rh8,rh9,rh10,rh11,rh12,rh13,rh14,rh15,rh16,rh17,rh18,rh19,rh20,rh21,rh22,rh23,rh24,rh25,rh26,rh27,rh28,rh29,rh30,rh31,rh32,rh33,rh34,rh35,rh36,rh37,rh38,rh39,rh40,rh41,rh42,rh43,rh44,rh45,rh46,rh47,rh48,rh49,rh50,rh51,rh52,rh53,rh54,rh55,rh56,rh57,rh58,rh59,rh60,rh61,rh62,rh63,rh64,rh65,rh66,rh67,rh68,rh69,rh70,rh71,rh72,rh73,rh74,rh75,rh76,rh77,rh78,rh79,rh80,rh81,rh82,rh83,rh84,rh85,rh86,rh87,rh88,rh89,rh90,rh91,rh92,rh93,rh94,rh95,rh96,rh97,rh98,rh99,rh100 |

spacelaser and hdf5r agree on every shared column. h5coro matches on row
count and coordinates but returns zeros on the science columns (rh0–100,
quality_flag, sensitivity, elev\_\*); pending investigation, likely a
hyperslice or fill-value handling issue in our h5coro driver rather than
h5coro itself. Timing comparison remains valid.

------------------------------------------------------------------------

See [`README.md`](README.md) for methodology, workload details, variance
discussion, and reproduction instructions.
