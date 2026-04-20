
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
| gedidb (Python, GFZ-hosted TileDB query) | 50.2 s | — | — | — | 241 |

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
h5coro itself. gedidb is excluded from the equivalence matrix — see the
next section for why. Timing comparison remains valid.

## gedidb: different architecture, not a drop-in competitor

gedidb is included as a timing reference point but solves a different
problem to the other three pipelines. A quick summary:

|  | spacelaser / h5coro / hdf5r | gedidb |
|----|----|----|
| Data source | NASA DAAC granules (live) | Pre-indexed TileDB on GFZ-Potsdam Ceph |
| Auth | NASA Earthdata required | None (public S3) |
| Currency | Always matches NASA’s latest processing | Depends on GFZ reprocessing cadence |
| Products | GEDI L1B/L2A/L2B/L4A/L4C + ICESat-2 | L2A/L2B/L4A/L4C only |
| Waveforms (rxwaveform / txwaveform) | Available (L1B) | Not ingested |
| 2D profiles (rh, pai_z, cover_z, pavd_z) | Available | Available |
| `elev_highestreturn` | Native column | Only `_a1`/`_a2` variants |
| Polygon queries | Bbox only | Shapely geometry supported |
| Push-down quality filters | Post-hoc in R | SQL-like kwargs |
| Output | `tibble` / DataFrame | `xarray.Dataset` or DataFrame |

gedidb’s timing represents “how fast can you query pre-indexed L2A
summary metrics from a European-hosted TileDB?” — not “how fast can you
range-read live NASA granules?” Where the products overlap
(L2A/L2B/L4A/L4C, non-polar, no waveforms), gedidb’s TileDB architecture
will generally win on pure latency because spacelaser still has to go
via CMR → DAAC. That’s the expected shape of the comparison, not a loss.

**Row-count comparison needs care.** The other three pipelines return
every shot in the bbox (spacelaser / hdf5r / h5coro all return 1,246
rows on our workload). gedidb returns 241 rows — the subset that
survived their ingest-time filter routine, which is broader than a
single quality flag. From gedidb’s own [filter
documentation](https://gedidb.readthedocs.io/en/latest/user/fundamentals.filters.html),
L2A ingest enforces all of: `quality_flag == 1`,
`0.5 ≤ sensitivity_a0 ≤ 1.0`, `0.7 < sensitivity_a2 ≤ 1.0`, excluded
`degrade_flag`, `surface_flag == 1`, and
`|elev_lowestmode - DEM| ≤ 150 m`. L2B adds
`landsat_water_persistence < 10` and `urban_proportion ≤ 50` — **water
bodies and urban surfaces are explicitly removed** and cannot be
recovered from gedidb queries. L4A and L4C inherit these filters via
inner-join on `shot_number`.

For a direct raw-vs-raw comparison, apply the same filter set to the
other pipelines. For hydrology, coastal, cryosphere, or urban work,
gedidb is the wrong tool — those shots were dropped at ingest and there
is no query flag to recover them.

Minor schema difference: gedidb’s `rh` expands to `rh0..rh101` (102
bins) vs NASA’s 101 (`rh0..rh100`). Origin unclear; spot-check against
L2A spec before relying on `rh101`.

Use gedidb when you want pre-cooked summary metrics with SQL-like
filters and no auth setup. Use spacelaser when you want L1B waveforms,
ICESat-2 breadth (ATL03/06/07/08/10/13/24 vs ATL08-only via icesat2db),
access to shots that fail GEDI’s built-in quality flag (non-vegetated
surfaces, water bodies, bare earth — filtered out by gedidb’s ingest),
or NASA-live granules.

------------------------------------------------------------------------

See [`README.md`](README.md) for methodology, workload details, variance
discussion, and reproduction instructions.
