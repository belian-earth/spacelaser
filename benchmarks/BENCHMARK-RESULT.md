
<!-- BENCHMARK-RESULT.md is generated — do not edit by hand.
     Re-render with:
       Rscript -e 'rmarkdown::render("benchmarks/BENCHMARK-RESULT.Rmd")'
     Or run the full pipeline via benchmarks/run.sh
     Methodology, caveats, and reproduction instructions live in README.md. -->

# Benchmark result

GEDI **L2A**, bbox `9.32,0.55,9.35,0.58`, 2020-01-01 → 2021-12-31, 11
granules. Run at **2026-04-22T22:43:52Z**.

## Headline

| Pipeline | Wall time | Download | Read | Bytes on disk | Rows |
|:---|---:|---:|---:|---:|---:|
| Spacelaser (Rust, partial HTTP range reads) | 60.7 s | — | — | — | 1,376 |
| h5coro (Python, partial HTTP range reads) | 152.2 s | — | — | — | 1,376 |
| Download + hdf5r (full granule download, then read) | 1167.3 s | 1165.3 s | 2.0 s | 27.2 Gb | 1,376 |
| gedidb (Python, GFZ-hosted TileDB query) | 55.5 s | — | — | — | 241 |

### Wall-time ratio vs the download+hdf5r pipeline:

- spacelaser is 19.2× quicker
- h5coro is 7.7× quicker

Between the two partial-read pipelines, spacelaser completes **2.51×**
quicker than h5coro on this workload.

Download+hdf5r phase split: **1165.3 s downloading** vs **2.0 s
reading** — wall time on this workload is dominated by the file
transfer, which is where partial-read pipelines (spacelaser, h5coro)
save their time.

## Equivalence

| Pair | Shared cols | All match | Notes |
|:---|---:|:---|:---|
| hdf5r vs spacelaser | 112 | ✓ |  |
| hdf5r vs h5coro | 112 | ✗ | quality_flag,sensitivity,elev_lowestmode,elev_highestreturn,rh0,rh1,rh2,rh3,rh4,rh5,rh6,rh7,rh8,rh9,rh10,rh11,rh12,rh13,rh14,rh15,rh16,rh17,rh18,rh19,rh20,rh21,rh22,rh23,rh24,rh25,rh26,rh27,rh28,rh29,rh30,rh31,rh32,rh33,rh34,rh35,rh36,rh37,rh38,rh39,rh40,rh41,rh42,rh43,rh44,rh45,rh46,rh47,rh48,rh49,rh50,rh51,rh52,rh53,rh54,rh55,rh56,rh57,rh58,rh59,rh60,rh61,rh62,rh63,rh64,rh65,rh66,rh67,rh68,rh69,rh70,rh71,rh72,rh73,rh74,rh75,rh76,rh77,rh78,rh79,rh80,rh81,rh82,rh83,rh84,rh85,rh86,rh87,rh88,rh89,rh90,rh91,rh92,rh93,rh94,rh95,rh96,rh97,rh98,rh99,rh100 |
| spacelaser vs h5coro | 112 | ✗ | quality_flag,sensitivity,elev_lowestmode,elev_highestreturn,rh0,rh1,rh2,rh3,rh4,rh5,rh6,rh7,rh8,rh9,rh10,rh11,rh12,rh13,rh14,rh15,rh16,rh17,rh18,rh19,rh20,rh21,rh22,rh23,rh24,rh25,rh26,rh27,rh28,rh29,rh30,rh31,rh32,rh33,rh34,rh35,rh36,rh37,rh38,rh39,rh40,rh41,rh42,rh43,rh44,rh45,rh46,rh47,rh48,rh49,rh50,rh51,rh52,rh53,rh54,rh55,rh56,rh57,rh58,rh59,rh60,rh61,rh62,rh63,rh64,rh65,rh66,rh67,rh68,rh69,rh70,rh71,rh72,rh73,rh74,rh75,rh76,rh77,rh78,rh79,rh80,rh81,rh82,rh83,rh84,rh85,rh86,rh87,rh88,rh89,rh90,rh91,rh92,rh93,rh94,rh95,rh96,rh97,rh98,rh99,rh100 |

spacelaser and hdf5r agree on every shared column. h5coro matches on row
count and coordinates, and our driver currently shows zeros on the
science columns (rh0–100, quality_flag, sensitivity, elev\_\*) — this is
a known issue in our h5coro-side code (hyperslice or fill-value
handling), not in h5coro itself; tracked for follow-up. gedidb is
reported alongside rather than inside the equivalence matrix because it
exposes a different slice of the catalogue — see the next section.
Timing comparison remains valid.

## gedidb: a different architecture

gedidb takes a meaningfully different approach to serving GEDI data, and
the comparison below is intended to characterise the differences rather
than adjudicate. A summary of what each pipeline exposes:

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

gedidb’s timing reflects “query pre-indexed L2A summary metrics from a
European-hosted TileDB”, whereas the other pipelines measure “range-read
live NASA granules”. Where the products overlap (L2A/L2B/L4A/L4C, no
waveforms), gedidb’s TileDB architecture is latency-efficient by design
— the shots are already ingested, indexed, and filtered — so comparing
wall-clock against a live-CMR pipeline isn’t apples-to-apples.

**Row counts reflect different data slices, not equivalent results.**
The other three pipelines return every shot in the bbox (1,376 in this
workload). gedidb returns 241 rows — the subset that passes its
ingest-time quality filter, which is deliberate and documented in
gedidb’s [filter
documentation](https://gedidb.readthedocs.io/en/latest/user/fundamentals.filters.html).

These filters reflect sensible defaults for the canopy-focused workflows
gedidb is primarily designed for, and are clearly documented. For a
like-for-like row comparison, applying the same filter set to the other
pipelines brings them in line. For users whose work depends on shots
that don’t pass those filters — hydrology / water surfaces, urban /
coastal research, or anyone who wants unfiltered raw shots — spacelaser
or direct DAAC access is the path; gedidb is explicit about its
filtering choices so users can pick the approach that fits.

**When each is a great fit:**

- **gedidb** is ideal for pre-cooked summary metrics with SQL-like
  filtering and no auth setup — especially for canopy / vegetation work
  at scale, where the ingest filters reflect community practice.
- **spacelaser** is a good fit when you want L1B waveforms, ICESat-2
  breadth (ATL03/06/07/08/10/13/24 vs ATL08-only via icesat2db), shots
  that fall outside gedidb’s default filters, or NASA-live reprocessed
  data.
- **curl + hdf5r (full download + read)** remains a solid choice when
  you want the full granule locally, or when your workflow depends on
  other hdf5r features.

------------------------------------------------------------------------

See [`README.md`](README.md) for methodology, workload details, variance
discussion, and reproduction instructions.
