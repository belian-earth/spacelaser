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
pub async fn read_chunk(
    reader: &Reader,
    chunk: &ChunkInfo,
    filters: &[Filter],
    element_size: usize,
) -> Result<Vec<u8>, Hdf5Error> {
    let raw = reader.read(chunk.address, chunk.size as usize).await?;

    if filters.is_empty() || chunk.filter_mask == u32::MAX {
        // No filters applied
        Ok(raw)
    } else {
        crate::filters::apply_filters(&raw, filters, chunk.filter_mask, element_size)
    }
}

/// Read contiguous dataset data (no chunking).
pub async fn read_contiguous(
    reader: &Reader,
    address: u64,
    total_size: u64,
    row_range: Option<(u64, u64)>,
    element_size: usize,
) -> Result<Vec<u8>, Hdf5Error> {
    match row_range {
        Some((start, end)) => {
            let byte_start = start * element_size as u64;
            let byte_length = (end - start) * element_size as u64;
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
/// Given a set of decompressed chunks and the row ranges of interest,
/// extracts just the rows needed and concatenates them in order.
pub fn extract_rows_from_chunks(
    chunks: &[(ChunkInfo, Vec<u8>)],
    row_ranges: &[(u64, u64)],
    chunk_dims: &[u32],
    element_size: usize,
    ndims: usize,
) -> Vec<u8> {
    let chunk_size_dim0 = chunk_dims[0] as u64;
    let row_size = if ndims > 1 {
        chunk_dims[1..].iter().map(|d| *d as usize).product::<usize>() * element_size
    } else {
        element_size
    };

    let total_rows: u64 = row_ranges.iter().map(|(s, e)| e - s).sum();
    let mut result = Vec::with_capacity(total_rows as usize * row_size);

    for &(range_start, range_end) in row_ranges {
        for row in range_start..range_end {
            // Find the chunk containing this row
            for (chunk_info, chunk_data) in chunks {
                let chunk_start = chunk_info.offsets.first().copied().unwrap_or(0);
                let chunk_end = chunk_start + chunk_size_dim0;

                if row >= chunk_start && row < chunk_end {
                    let local_row = (row - chunk_start) as usize;
                    let byte_offset = local_row * row_size;
                    let byte_end = byte_offset + row_size;

                    if byte_end <= chunk_data.len() {
                        result.extend_from_slice(&chunk_data[byte_offset..byte_end]);
                    }
                    break;
                }
            }
        }
    }

    result
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
}
