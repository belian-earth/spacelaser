use crate::hdf5::btree::ChunkInfo;
use crate::hdf5::types::*;
use crate::io::reader::Reader;

/// Determine which chunks overlap a given row range for a 1-D or 2-D dataset.
///
/// For GEDI/ICESat-2 data, the typical access pattern is:
/// 1. Read the full lat/lon arrays (or their relevant chunks)
/// 2. Determine which row indices fall within the bounding box
/// 3. Read only the chunks of other datasets that contain those rows
///
/// This function takes a list of chunks and a set of row ranges, and returns
/// only the chunks that overlap those ranges.
pub fn chunks_for_row_ranges<'a>(
    chunks: &'a [ChunkInfo],
    row_ranges: &[(u64, u64)],
    chunk_dims: &[u32],
) -> Vec<&'a ChunkInfo> {
    if chunk_dims.is_empty() || chunks.is_empty() {
        return Vec::new();
    }

    let chunk_size_dim0 = chunk_dims[0] as u64;

    chunks
        .iter()
        .filter(|chunk| {
            let chunk_start = chunk.offsets.first().copied().unwrap_or(0);
            let chunk_end = chunk_start + chunk_size_dim0;

            row_ranges
                .iter()
                .any(|&(range_start, range_end)| chunk_start < range_end && chunk_end > range_start)
        })
        .collect()
}

/// Read and decompress a single chunk's data from the file.
///
/// Decompression (deflate, unshuffle) is offloaded to a blocking thread pool
/// via `spawn_blocking` so that CPU-bound work doesn't starve the async I/O
/// runtime.
pub async fn read_chunk(
    reader: &Reader,
    chunk: &ChunkInfo,
    filters: &[Filter],
    element_size: usize,
) -> Result<Vec<u8>, Hdf5Error> {
    // Sanity cap to avoid allocating gigabytes on a corrupted size
    // field. Real GEDI/ICESat-2 chunks run a few MB compressed.
    const MAX_CHUNK_BYTES: u32 = 256 * 1024 * 1024;
    if chunk.size > MAX_CHUNK_BYTES {
        return Err(Hdf5Error::InvalidStructure(format!(
            "Chunk at 0x{:x} claims {} bytes; exceeds sanity cap",
            chunk.address, chunk.size
        )));
    }
    let raw = reader.read(chunk.address, chunk.size as usize).await?;

    if filters.is_empty() || chunk.filter_mask == u32::MAX {
        // No filters applied
        Ok(raw)
    } else {
        let filters = filters.to_vec();
        let filter_mask = chunk.filter_mask;
        tokio::task::spawn_blocking(move || {
            crate::filters::apply_filters(&raw, &filters, filter_mask, element_size)
        })
        .await
        .map_err(|e| Hdf5Error::Decompression(format!("spawn_blocking join: {}", e)))?
    }
}

/// Read contiguous dataset data (no chunking).
///
/// `row_size` is the number of bytes per row.  For 1-D datasets this equals
/// the scalar element size; for 2-D datasets (e.g. `rh` [N×101] of f32)
/// this is `101 × 4 = 404`.
pub async fn read_contiguous(
    reader: &Reader,
    address: u64,
    total_size: u64,
    row_range: Option<(u64, u64)>,
    row_size: usize,
) -> Result<Vec<u8>, Hdf5Error> {
    match row_range {
        Some((start, end)) => {
            let byte_start = start * row_size as u64;
            let byte_length = (end - start) * row_size as u64;
            let data = reader
                .read(address + byte_start, byte_length as usize)
                .await?;
            Ok(data)
        }
        None => {
            let data = reader.read(address, total_size as usize).await?;
            Ok(data)
        }
    }
}

/// Assemble data from multiple chunks for specific row ranges.
///
/// Returns exactly one dataset row (`dataset_dims[1..]` elements) per
/// requested row, in order. Rows are assembled from every chunk that
/// covers them: a 2-D dataset may be chunked across its columns as well
/// as its rows (`[N, 101]` in chunks of `[1024, 16]`), in which case
/// several chunks share a row offset and each supplies part of the row.
///
/// Storage that was never allocated holds the dataset's fill value
/// (`fill`, one element; zeros when the file defines none), as it does
/// when read through the HDF5 library.
///
/// `row_ranges` must be sorted and non-overlapping.
pub fn extract_rows_from_chunks(
    chunks: &[(ChunkInfo, Vec<u8>)],
    row_ranges: &[(u64, u64)],
    chunk_dims: &[u32],
    dataset_dims: &[u64],
    element_size: usize,
    fill: Option<&[u8]>,
) -> Result<Vec<u8>, Hdf5Error> {
    let rank = chunk_dims.len();
    if rank == 0 || dataset_dims.len() != rank || element_size == 0 {
        return Err(Hdf5Error::InvalidStructure(format!(
            "chunk dimensions {:?} do not match dataset dimensions {:?}",
            chunk_dims, dataset_dims
        )));
    }
    // Beyond two dimensions only whole-row chunks are assembled.
    if rank > 2
        && chunk_dims[1..]
            .iter()
            .zip(&dataset_dims[1..])
            .any(|(&c, &d)| c as u64 != d)
    {
        return Err(Hdf5Error::UnsupportedChunkShape(format!(
            "{}-D dataset {:?} chunked as {:?}",
            rank, dataset_dims, chunk_dims
        )));
    }

    let chunk_rows = chunk_dims[0] as u64;
    // Elements per row: of the dataset, and of one chunk.
    let (dst_cols, chunk_cols) = if rank == 2 {
        (dataset_dims[1] as usize, chunk_dims[1] as usize)
    } else {
        let n: usize = dataset_dims[1..].iter().map(|&d| d as usize).product();
        (n, n)
    };
    let dst_row_bytes = dst_cols * element_size;
    let chunk_row_bytes = chunk_cols * element_size;

    let total_rows: u64 = row_ranges.iter().map(|(s, e)| e - s).sum();
    let total_bytes = total_rows as usize * dst_row_bytes;
    let mut result = match fill {
        Some(f) if f.len() == element_size && f.iter().any(|&b| b != 0) => {
            f.iter().copied().cycle().take(total_bytes).collect()
        }
        _ => vec![0u8; total_bytes],
    };

    // Output row index at which each range starts.
    let mut out_base = Vec::with_capacity(row_ranges.len());
    let mut acc = 0u64;
    for &(s, e) in row_ranges {
        out_base.push(acc);
        acc += e - s;
    }

    for (ci, data) in chunks {
        let r0 = ci.offsets.first().copied().unwrap_or(0);
        let r1 = r0 + chunk_rows;
        let c0 = if rank == 2 {
            ci.offsets.get(1).copied().unwrap_or(0) as usize
        } else {
            0
        };
        if c0 >= dst_cols {
            continue;
        }
        let copy_bytes = chunk_cols.min(dst_cols - c0) * element_size;

        // Ranges are sorted: start at the first one ending after r0.
        let first = row_ranges.partition_point(|&(_, e)| e <= r0);
        for (k, &(s, e)) in row_ranges.iter().enumerate().skip(first) {
            if s >= r1 {
                break;
            }
            for row in s.max(r0)..e.min(r1) {
                let src = (row - r0) as usize * chunk_row_bytes;
                let chunk_row = data.get(src..src + copy_bytes).ok_or_else(|| {
                    Hdf5Error::InvalidStructure(format!(
                        "chunk at row {} holds {} bytes, too few for its {:?} shape",
                        r0,
                        data.len(),
                        chunk_dims
                    ))
                })?;
                let dst = (out_base[k] + (row - s)) as usize * dst_row_bytes + c0 * element_size;
                result[dst..dst + copy_bytes].copy_from_slice(chunk_row);
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunks_for_row_ranges() {
        let chunks = vec![
            ChunkInfo {
                size: 4000,
                filter_mask: 0,
                offsets: vec![0],
                address: 1000,
            },
            ChunkInfo {
                size: 4000,
                filter_mask: 0,
                offsets: vec![1000],
                address: 5000,
            },
            ChunkInfo {
                size: 4000,
                filter_mask: 0,
                offsets: vec![2000],
                address: 9000,
            },
        ];

        let chunk_dims = vec![1000u32];

        // Range spanning chunks 0 and 1
        let ranges = vec![(500, 1500)];
        let result = chunks_for_row_ranges(&chunks, &ranges, &chunk_dims);
        assert_eq!(result.len(), 2);

        // Range fully within chunk 2
        let ranges = vec![(2100, 2200)];
        let result = chunks_for_row_ranges(&chunks, &ranges, &chunk_dims);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].offsets[0], 2000);
    }

    fn chunk(offsets: &[u64], data: Vec<u8>) -> (ChunkInfo, Vec<u8>) {
        (
            ChunkInfo { size: data.len() as u32, filter_mask: 0, offsets: offsets.to_vec(), address: 0 },
            data,
        )
    }

    #[test]
    fn rows_from_one_dimensional_chunks() {
        // 10 rows of u8 in chunks of 4; the last chunk is padded.
        let chunks = vec![
            chunk(&[0], vec![0, 1, 2, 3]),
            chunk(&[4], vec![4, 5, 6, 7]),
            chunk(&[8], vec![8, 9, 0, 0]),
        ];
        let out = extract_rows_from_chunks(&chunks, &[(1, 3), (7, 10)], &[4], &[10], 1, None).unwrap();
        assert_eq!(out, vec![1, 2, 7, 8, 9]);
    }

    #[test]
    fn rows_from_column_split_chunks() {
        // [4, 5] dataset, value = 10 * row + col, chunked [2, 3]: the
        // second column chunk is one column wider than the dataset.
        let cell = |r: u8, c: u8| if c < 5 { 10 * r + c } else { 0 };
        let mut chunks = Vec::new();
        for r0 in [0u8, 2] {
            for c0 in [0u8, 3] {
                let mut d = Vec::new();
                for r in r0..r0 + 2 {
                    for c in c0..c0 + 3 {
                        d.push(cell(r, c));
                    }
                }
                chunks.push(chunk(&[r0 as u64, c0 as u64], d));
            }
        }
        let out = extract_rows_from_chunks(&chunks, &[(1, 4)], &[2, 3], &[4, 5], 1, None).unwrap();
        assert_eq!(
            out,
            vec![10, 11, 12, 13, 14, 20, 21, 22, 23, 24, 30, 31, 32, 33, 34]
        );
    }

    #[test]
    fn unallocated_chunks_hold_the_fill_value() {
        let chunks = vec![chunk(&[0], vec![1, 0, 2, 0])];
        // Rows 2..4 have no chunk: u16 fill 0xFFFE.
        let out =
            extract_rows_from_chunks(&chunks, &[(1, 4)], &[2], &[4], 2, Some(&[0xFE, 0xFF])).unwrap();
        assert_eq!(out, vec![2, 0, 0xFE, 0xFF, 0xFE, 0xFF]);
        let zeros = extract_rows_from_chunks(&chunks, &[(1, 4)], &[2], &[4], 2, None).unwrap();
        assert_eq!(zeros, vec![2, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn short_chunk_is_an_error() {
        let chunks = vec![chunk(&[0], vec![1, 2])];
        assert!(extract_rows_from_chunks(&chunks, &[(0, 4)], &[4], &[4], 1, None).is_err());
    }
}
