#!/usr/bin/env python3
"""
h5coro pipeline for the spacelaser benchmark.

Self-contained: reads the workload spec from benchmarks/workload.json,
the granule list from benchmarks/results/latest/granules.parquet
(written by benchmarks/search.R), runs h5coro against each granule
with the same per-beam concurrency and column set the R pipelines
use, and writes:

  benchmarks/results/latest/h5coro-timing.parquet  (1 row, tracked)
  benchmarks/results/latest/h5coro-data.parquet    (full data, gitignored)

For each granule:
  1. Open with H5Coro + HTTPDriver (EDL bearer token).
  2. Per beam, concurrently:
     a. Read full lat_lowestmode + lon_lowestmode.
     b. np.where(in bbox) -> indices.
     c. Read each science column over [min_idx, max_idx+1] hyperslice.
     d. Numpy-index into the returned slab to get the in-bbox rows.
  3. 2D datasets (e.g. rh) expand to rh0..rh100 columns.

Auth: requires `EARTHDATA_TOKEN` env var (EDL user token from
https://urs.earthdata.nasa.gov/user_tokens). h5coro's HTTPDriver
sends it as `Authorization: Bearer <token>` on every range request.

Usage (from package root):
    uv run --project benchmarks/python benchmarks/python/bench_h5coro.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from h5coro import h5coro, webdriver

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
# All paths resolved relative to the package root, regardless of where the
# script is invoked from. uv run preserves the caller's CWD.

PKG_ROOT      = Path(__file__).resolve().parents[2]
WORKLOAD_PATH = PKG_ROOT / "benchmarks" / "workload.json"
GRANULES_PATH = PKG_ROOT / "benchmarks" / "results" / "latest" / "granules.parquet"
OUT_TIMING    = PKG_ROOT / "benchmarks" / "results" / "latest" / "h5coro-timing.parquet"
OUT_DATA      = PKG_ROOT / "benchmarks" / "results" / "latest" / "h5coro-data.parquet"

# Match spacelaser's multi-granule buffer_unordered level so concurrency
# is comparable across pipelines.
MAX_CONCURRENT_GRANULES = 8

GEDI_BEAMS = [
    "BEAM0000", "BEAM0001", "BEAM0010", "BEAM0011",
    "BEAM0100", "BEAM0101", "BEAM0110", "BEAM1011",
]


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_token() -> str:
    token = os.environ.get("EARTHDATA_TOKEN", "").strip()
    if not token:
        sys.exit(
            "ERROR: EARTHDATA_TOKEN not set. Generate an EDL user token at "
            "https://urs.earthdata.nasa.gov/user_tokens and export it as "
            "EARTHDATA_TOKEN."
        )
    # Surface expired tokens up-front rather than letting them rain 401s.
    # JWT payload is the middle dot-separated chunk, base64url-encoded.
    parts = token.split(".")
    if len(parts) == 3:
        try:
            import base64
            pad = "=" * (-len(parts[1]) % 4)
            payload = json.loads(
                base64.urlsafe_b64decode(parts[1] + pad).decode("utf-8")
            )
            exp = payload.get("exp")
            if exp and exp < time.time():
                expired_on = datetime.fromtimestamp(exp, tz=timezone.utc)
                sys.exit(
                    f"ERROR: EARTHDATA_TOKEN expired on {expired_on:%Y-%m-%d}. "
                    "Generate a new one at "
                    "https://urs.earthdata.nasa.gov/user_tokens."
                )
        except Exception:
            pass  # Not a JWT or malformed — let h5coro surface the issue.
    return token


# ---------------------------------------------------------------------------
# Per-granule / per-beam read
# ---------------------------------------------------------------------------

def read_one_beam(h5, beam, bbox, column_paths):
    """Spatial filter via lat/lon, then targeted science-column reads."""
    lat_path = f"{beam}/lat_lowestmode"
    lon_path = f"{beam}/lon_lowestmode"

    # Phase 1: full lat/lon read (h5coro's per-dataset thread pool runs
    # these concurrently within the call).
    promise = h5.readDatasets(
        datasets=[
            {"dataset": lat_path, "hyperslice": []},
            {"dataset": lon_path, "hyperslice": []},
        ],
        block=True,
    )
    lat_raw = promise[lat_path]
    lon_raw = promise[lon_path]
    if lat_raw is None or lon_raw is None:
        return None
    lat = np.asarray(lat_raw)
    lon = np.asarray(lon_raw)

    mask = (
        (lat >= bbox["ymin"]) & (lat <= bbox["ymax"]) &
        (lon >= bbox["xmin"]) & (lon <= bbox["xmax"])
    )
    idx = np.flatnonzero(mask)
    if idx.size == 0:
        return None

    # Phase 2: contiguous-range science-column reads. h5coro's hyperslice
    # is a list of [start, end) pairs, one per dimension. For 2D datasets
    # we only constrain dim 0 (rows) and let h5coro default dim 1 to full.
    row_lo = int(idx.min())
    row_hi = int(idx.max()) + 1
    requests = [
        {"dataset": f"{beam}/{path}", "hyperslice": [[row_lo, row_hi]]}
        for path in column_paths.values()
    ]
    promise = h5.readDatasets(datasets=requests, block=True)

    rel_idx = idx - row_lo
    out: dict = {
        "beam": np.full(idx.size, beam, dtype=object),
        "lat_lowestmode": lat[idx],
        "lon_lowestmode": lon[idx],
    }
    for short, path in column_paths.items():
        raw = promise[f"{beam}/{path}"]
        if raw is None:
            continue
        arr = np.asarray(raw)
        if arr.ndim == 1:
            out[short] = arr[rel_idx]
        else:
            # 2D [n_shots, n_bins] -> {short}0..{short}{n-1}
            slab = arr[rel_idx, :]
            for j in range(slab.shape[1]):
                out[f"{short}{j}"] = slab[:, j]
    return pd.DataFrame(out)


def read_one_granule(url, token, bbox, column_paths):
    h5 = h5coro.H5Coro(url, webdriver.HTTPDriver, credentials=token)
    beam_frames = []
    with ThreadPoolExecutor(max_workers=len(GEDI_BEAMS)) as pool:
        futs = {
            pool.submit(read_one_beam, h5, beam, bbox, column_paths): beam
            for beam in GEDI_BEAMS
        }
        for fut in as_completed(futs):
            try:
                df = fut.result()
            except Exception as e:
                print(
                    f"[warn] beam {futs[fut]} of {Path(url).name}: {e}",
                    file=sys.stderr,
                )
                continue
            if df is not None and len(df) > 0:
                beam_frames.append(df)
    if not beam_frames:
        return pd.DataFrame()
    return pd.concat(beam_frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    workload = json.loads(WORKLOAD_PATH.read_text())
    bbox = workload["bbox"]
    column_paths = workload["column_paths"]

    if not GRANULES_PATH.exists():
        sys.exit(
            f"ERROR: {GRANULES_PATH} not found. Run benchmarks/search.R "
            "first (or the full benchmarks/run.sh)."
        )
    urls = pq.read_table(GRANULES_PATH, columns=["url"])["url"].to_pylist()

    token = get_token()

    print(f"h5coro: reading {len(urls)} granules ({MAX_CONCURRENT_GRANULES} concurrent)")
    t0 = time.perf_counter()
    dfs = []
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_GRANULES) as pool:
        futs = {
            pool.submit(read_one_granule, url, token, bbox, column_paths): url
            for url in urls
        }
        for fut in as_completed(futs):
            try:
                df = fut.result()
            except Exception as e:
                print(f"[warn] {Path(futs[fut]).name}: {e}", file=sys.stderr)
                continue
            if len(df) > 0:
                dfs.append(df)
    elapsed = time.perf_counter() - t0

    combined = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    OUT_DATA.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(OUT_DATA, index=False)

    bbox_str = ",".join(str(bbox[k]) for k in ("xmin", "ymin", "xmax", "ymax"))
    timing = pd.DataFrame([{
        "pipeline":         "h5coro",
        "timestamp":        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bbox":             bbox_str,
        "date_start":       workload["date_start"],
        "date_end":         workload["date_end"],
        "product":          workload["product"],
        "n_granules":       len(urls),
        "n_rows":           len(combined),
        "seconds_total":    elapsed,
        "seconds_download": float("nan"),
        "seconds_read":     float("nan"),
        "bytes_downloaded": float("nan"),
        "notes":            None,
    }])
    timing.to_parquet(OUT_TIMING, index=False)

    print(f"h5coro: {elapsed:.1f}s, {len(combined)} rows -> {OUT_TIMING}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
