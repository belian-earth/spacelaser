#!/usr/bin/env python3
"""
gedidb pipeline for the spacelaser benchmark.

Self-contained: reads the workload spec from benchmarks/workload.json
and queries the public GFZ-hosted gedidb TileDB database (no auth
required) for the same bbox / date range / columns the other
pipelines use, then writes:

  benchmarks/results/latest/gedidb-timing.parquet  (1 row, tracked)
  benchmarks/results/latest/gedidb-data.parquet    (full data, gitignored)

Architectural notes:
  - Unlike the other three pipelines, gedidb does NOT read NASA
    granules directly. It queries a pre-indexed TileDB database
    hosted on GFZ-Potsdam's Ceph S3 object store
    (https://s3.gfz-potsdam.de, bucket dog.gedidb.gedi-l2-l4-v002).
    The database is publicly readable; no NASA Earthdata credentials
    or AWS keys are needed.
  - Column name differences between gedidb and NASA:
      * gedidb has no bare `elev_highestreturn` — only algorithm-
        specific variants `elev_highestreturn_a1`/`_a2`. We request
        `_a1` as the closest analogue (same algorithm the default
        `selected_algorithm` usually picks).
      * 2D `rh` expands to `rh_0..rh_100` (with underscore) in
        gedidb output, vs `rh0..rh100` (no underscore) from
        spacelaser / h5coro / hdf5r. We rename to the spacelaser
        convention so cross-pipeline equivalence checks can line up.
      * gedidb flattens HDF5 subgroups (land_cover_data/ etc.) into
        bare variable names, matching spacelaser's user-facing
        convention.

Usage (from package root):
    uv run --project benchmarks/python-gedidb \\
        benchmarks/python-gedidb/bench_gedidb.py
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import gedidb as gdb
import geopandas as gpd
import pandas as pd
from shapely.geometry import box

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PKG_ROOT      = Path(__file__).resolve().parents[2]
WORKLOAD_PATH = PKG_ROOT / "benchmarks" / "workload.json"
OUT_TIMING    = PKG_ROOT / "benchmarks" / "results" / "latest" / "gedidb-timing.parquet"
OUT_DATA      = PKG_ROOT / "benchmarks" / "results" / "latest" / "gedidb-data.parquet"

# ---------------------------------------------------------------------------
# Public GFZ TileDB endpoint
# ---------------------------------------------------------------------------

GFZ_URL       = "https://s3.gfz-potsdam.de"
GFZ_BUCKET    = "dog.gedidb.gedi-l2-l4-v002"
GFZ_REGION    = "eu-central-1"


# ---------------------------------------------------------------------------
# Column-name translation: workload short names → gedidb variable names
# ---------------------------------------------------------------------------

def to_gedidb_variables(short_names: list[str]) -> list[str]:
    """Map the workload's short column names to gedidb variable names.

    gedidb stores a curated subset of L2A/L2B/L4A/L4C variables with a
    flat namespace (no HDF5 subgroups) and some algorithm-specific
    splits. See the module docstring for the differences.
    """
    remap = {
        # gedidb has no bare elev_highestreturn; _a1 is the closest equivalent
        "elev_highestreturn": "elev_highestreturn_a1",
    }
    return [remap.get(name, name) for name in short_names]


def rename_to_spacelaser_convention(df: pd.DataFrame) -> pd.DataFrame:
    """Rename gedidb output columns to match the spacelaser pipeline.

    - rh_0..rh_100  → rh0..rh100 (drop underscore)
    - elev_highestreturn_a1 → elev_highestreturn (undo the outbound remap)
    """
    new_cols: dict[str, str] = {}
    for col in df.columns:
        if col.startswith("rh_") and col[3:].isdigit():
            new_cols[col] = f"rh{col[3:]}"
        elif col == "elev_highestreturn_a1":
            new_cols[col] = "elev_highestreturn"
    return df.rename(columns=new_cols) if new_cols else df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    workload = json.loads(WORKLOAD_PATH.read_text())
    bbox = workload["bbox"]

    if workload["product"] != "L2A":
        sys.exit(
            f"ERROR: gedidb benchmark only supports L2A (workload has "
            f"product={workload['product']}). L1B waveforms and higher "
            "ICESat-2 products are not in the gedidb TileDB."
        )

    # gedidb requires a GeoDataFrame (not a bare Shapely geometry) for
    # bounding_box queries. WGS84 / EPSG:4326 matches the source data's
    # native CRS.
    geometry = gpd.GeoDataFrame(
        geometry=[box(bbox["xmin"], bbox["ymin"], bbox["xmax"], bbox["ymax"])],
        crs="EPSG:4326",
    )
    variables = to_gedidb_variables(workload["columns"])

    print(f"gedidb: querying public GFZ TileDB ({GFZ_URL})")
    print(f"        bbox={bbox}, {workload['date_start']} -> {workload['date_end']}")
    print(f"        variables: {variables}")

    provider = gdb.GEDIProvider(
        storage_type="s3",
        s3_bucket=GFZ_BUCKET,
        url=GFZ_URL,
        region=GFZ_REGION,
    )

    t0 = time.perf_counter()
    df = provider.get_data(
        variables=variables,
        query_type="bounding_box",
        geometry=geometry,
        start_time=workload["date_start"],
        end_time=workload["date_end"],
        return_type="dataframe",
    )
    elapsed = time.perf_counter() - t0

    if df is None:
        df = pd.DataFrame()
    df = rename_to_spacelaser_convention(df)

    OUT_DATA.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_DATA, index=False)

    bbox_str = ",".join(str(bbox[k]) for k in ("xmin", "ymin", "xmax", "ymax"))
    timing = pd.DataFrame([{
        "pipeline":         "gedidb",
        "timestamp":        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bbox":             bbox_str,
        "date_start":       workload["date_start"],
        "date_end":         workload["date_end"],
        "product":          workload["product"],
        # n_granules is not a meaningful concept for gedidb (single
        # TileDB query, no per-granule fan-out), so we leave it NA.
        "n_granules":       float("nan"),
        "n_rows":           len(df),
        "seconds_total":    elapsed,
        "seconds_download": float("nan"),
        "seconds_read":     float("nan"),
        "bytes_downloaded": float("nan"),
        "notes":            "GFZ TileDB (public, no auth)",
    }])
    timing.to_parquet(OUT_TIMING, index=False)

    print(f"gedidb: {elapsed:.1f}s, {len(df)} rows -> {OUT_TIMING}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
