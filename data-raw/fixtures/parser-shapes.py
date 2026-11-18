# ---------------------------------------------------------------------------
# Parser-shape fixtures
# ---------------------------------------------------------------------------
#
# The per-product fixtures (generate-fixtures.R) are written by hdf5r from
# the column registry, so they share one HDF5 shape: old-format object
# headers, symbol-table groups, single-level chunk B-trees, float64 data
# with no fill values. Real granules differ on every one of those points.
# The test columns sit in a subgroup `t` of each beam, so sl_read() takes
# them as raw HDF5 paths ("t/row") without a registry entry.
# These two files cover the shapes the product fixtures cannot:
#
#   parser-v1.h5  old-format file (what GEDI and ICESat-2 granules use)
#     /BEAM0000   1,002,000 rows, chunked: a two-level chunk B-tree, large
#                 enough to switch on row pruning. Only the last rows fall
#                 in the test bbox.
#     /BEAM0001   one column per datatype, each with fill sentinels in the
#                 data: declared by a `_FillValue` attribute, or GEDI's
#                 undeclared -9999. Also a fixed-length string, an enum,
#                 a dataset at a different rate, a 3-D dataset, a 2-D
#                 dataset chunked across its columns, and a dataset
#                 whose chunks are mostly unallocated.
#     /gt1l       the ATLAS sentinels (largest finite float32 / float64,
#                 largest int32) with no attribute, read as ATL13.
#
#   parser-v2.h5  new-format file (libver = v108)
#     /BEAM0000   version 2 object headers, version 3 attribute messages,
#                 version 2 filter pipeline with Fletcher32.
#     /dense      600 links: dense (fractal heap) link storage, with links
#                 that end at the very edge of a heap block.
#
# hdf5r cannot select the file format version, hence Python. Run from the
# package root:
#   uv run --with h5py --with numpy data-raw/fixtures/parser-shapes.py

import os

import h5py
import numpy as np

OUT = os.path.join("tests", "testthat", "fixtures")

# Rows inside the test bbox sl_bbox(-124.04, 41.39, -124.01, 41.42).
IN_LAT, IN_LON = 41.40, -124.02
OUT_LAT, OUT_LON = 10.0, 10.0


def coords(group, n, inside, lat="lat_lowestmode", lon="lon_lowestmode", **kw):
    la = np.full(n, OUT_LAT)
    lo = np.full(n, OUT_LON)
    la[inside] = IN_LAT
    lo[inside] = IN_LON
    group.create_dataset(lat, data=la, **kw)
    group.create_dataset(lon, data=lo, **kw)


def with_fill(group, name, values, fill):
    ds = group.create_dataset(name, data=values)
    ds.attrs.create("_FillValue", np.array(fill, dtype=values.dtype))
    return ds


def make_v1(path):
    with h5py.File(path, "w", libver="earliest") as f:
        # -- Two-level chunk B-tree --------------------------------------
        n = 1_002_000
        b0 = f.create_group("BEAM0000")
        inside = np.zeros(n, dtype=bool)
        # Only the last chunk is selected: pruning then has to keep the
        # B-tree's final child. With a row count that is a whole number
        # of chunks, the library writes that child's upper key as the
        # last chunk's own offset, so the key is not an upper bound.
        inside[-7:] = True
        kw = dict(chunks=(2000,), compression="gzip", shuffle=True)
        coords(b0, n, inside, **kw)
        t = b0.create_group("t")
        t.create_dataset("row", data=np.arange(n, dtype=np.int32), **kw)
        # A column with a larger chunk, so its last chunk starts earlier.
        t.create_dataset(
            "row_wide",
            data=np.arange(n, dtype=np.int32),
            chunks=(3000,), compression="gzip", shuffle=True,
        )

        # -- One column per datatype, with fills -------------------------
        n = 100
        b1 = f.create_group("BEAM0001")
        coords(b1, n, np.ones(n, dtype=bool))
        b1 = b1.create_group("t")
        fill_rows = np.arange(n) % 4 == 0  # 25 of 100 rows

        def col(dtype, fill):
            v = (np.arange(n) % 50).astype(dtype)
            v[fill_rows] = fill
            return v

        with_fill(b1, "f32_declared", col("f4", np.finfo("f4").max), np.finfo("f4").max)
        with_fill(b1, "f64_declared", col("f8", np.finfo("f8").max), np.finfo("f8").max)
        with_fill(b1, "i8_declared", col("i1", 127), 127)
        with_fill(b1, "i16_declared", col("i2", 32767), 32767)
        with_fill(b1, "i32_declared", col("i4", 2**31 - 1), 2**31 - 1)
        with_fill(b1, "i64_declared", col("i8", 2**63 - 1), 2**63 - 1)
        with_fill(b1, "u8_declared", col("u1", 255), 255)
        with_fill(b1, "u16_declared", col("u2", 65535), 65535)
        with_fill(b1, "u32_declared", col("u4", 2**32 - 1), 2**32 - 1)
        with_fill(b1, "i8_minus_one", col("i1", -1), -1)
        # 127 and 255 are data here: nothing declares them as fill.
        b1.create_dataset("i8_undeclared", data=col("i1", 127))
        b1.create_dataset("u8_undeclared", data=col("u1", 255))
        # GEDI's sentinel, undeclared, in float32, float64 and int16.
        b1.create_dataset("f32_gedi", data=col("f4", -9999.0))
        b1.create_dataset("f64_gedi", data=col("f8", -999999.0))
        b1.create_dataset("i16_gedi", data=col("i2", -9999))
        # A 2-D column with a declared fill: expands to three columns.
        m = np.tile(np.arange(3, dtype="f4"), (n, 1))
        m[fill_rows, 1] = np.finfo("f4").max
        with_fill(b1, "matrix", m, np.finfo("f4").max)

        # Chunked across columns as well as rows, as GEDI version 3
        # stores `rh`: each row is assembled from three chunks, the last
        # of which is wider than the dataset.
        wide = (np.arange(n)[:, None] * 100 + np.arange(7)[None, :]).astype("i4")
        b1.create_dataset("split", data=wide, chunks=(32, 3), compression="gzip")
        # Only the first chunk is ever written: the rest of the dataset
        # is unallocated and reads as the fill value.
        sparse = b1.create_dataset("sparse", shape=(n,), dtype="i2", chunks=(10,), fillvalue=-7)
        sparse[0:10] = np.arange(10)

        strata = np.array([b"ENT_NAm", b"DBT_Eu", b"GSW"])[np.arange(n) % 3]
        b1.create_dataset("stratum", data=strata.astype("S8"))
        flag = h5py.enum_dtype({"exclude": 0, "include": 1}, basetype="i1")
        b1.create_dataset("enum_flag", data=(np.arange(n) % 2).astype("i1"), dtype=flag)
        b1.create_dataset("other_rate", data=np.arange(10, dtype="f8"))
        b1.create_dataset("cube", data=np.zeros((n, 2, 2), dtype="f4"))

        # -- ATLAS sentinels without an attribute ------------------------
        g = f.create_group("gt1l")
        coords(g, n, np.ones(n, dtype=bool), lat="segment_lat", lon="segment_lon")
        g = g.create_group("t")
        g.create_dataset("f32_atlas", data=col("f4", np.finfo("f4").max))
        g.create_dataset("f64_atlas", data=col("f8", np.finfo("f8").max))
        g.create_dataset("i32_atlas", data=col("i4", 2**31 - 1))
        g.create_dataset("i8_atlas", data=col("i1", 127))


def make_v2(path):
    # v108 rather than latest: version 2 object headers, but the chunk
    # index is still the version 1 B-tree. The chunk indexes HDF5 1.10
    # added (layout message version 4) are not read by the parser.
    with h5py.File(path, "w", libver=("v108", "v108")) as f:
        n = 300
        b0 = f.create_group("BEAM0000")
        inside = np.arange(n) < 150
        kw = dict(chunks=(100,), compression="gzip", shuffle=True, fletcher32=True)
        coords(b0, n, inside, **kw)
        b0 = b0.create_group("t")
        v = np.arange(n, dtype="f4")
        v[::5] = np.finfo("f4").max
        ds = b0.create_dataset("value", data=v, **kw)
        ds.attrs.create("_FillValue", np.array(np.finfo("f4").max, dtype="f4"))
        ds.attrs["units"] = "m"
        b0.create_dataset("row", data=np.arange(n, dtype="i4"), chunks=(100,), fletcher32=True)

        # Names of varying length, so that some links end within the
        # last four bytes of a heap block (eight of these 600 do).
        dense = f.create_group("dense")
        for i in range(600):
            dense.create_group(f"d{i:03d}" + "x" * (i % 7))


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    make_v1(os.path.join(OUT, "parser-v1.h5"))
    make_v2(os.path.join(OUT, "parser-v2.h5"))
    for name in ("parser-v1.h5", "parser-v2.h5"):
        print(name, os.path.getsize(os.path.join(OUT, name)), "bytes")
