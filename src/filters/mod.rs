use crate::hdf5::types::{Filter, Hdf5Error, FILTER_DEFLATE, FILTER_FLETCHER32, FILTER_SHUFFLE};
use flate2::read::ZlibDecoder;
use std::io::Read;

/// Apply the filter pipeline in reverse to decompress chunk data.
///
/// HDF5 filters are applied in order when writing; they must be reversed when
/// reading. For typical GEDI/ICESat-2 data, the pipeline is:
///   write: shuffle → deflate
///   read:  deflate → shuffle (reverse order)
pub fn apply_filters(
    data: &[u8],
    filters: &[Filter],
    filter_mask: u32,
    element_size: usize,
) -> Result<Vec<u8>, Hdf5Error> {
    let mut buf = data.to_vec();

    // Apply filters in reverse order (last applied during write = first to undo)
    for (i, filter) in filters.iter().enumerate().rev() {
        // Check if this filter was skipped (bit set in filter_mask)
        if filter_mask & (1 << i) != 0 {
            continue;
        }

        buf = apply_single_filter(filter, &buf, element_size)?;
    }

    Ok(buf)
}

fn apply_single_filter(
    filter: &Filter,
    data: &[u8],
    element_size: usize,
) -> Result<Vec<u8>, Hdf5Error> {
    match filter.id {
        FILTER_DEFLATE => decompress_deflate(data),
        FILTER_SHUFFLE => unshuffle(data, element_size),
        FILTER_FLETCHER32 => {
            // Fletcher32 is a checksum -- just strip the last 4 bytes
            if data.len() >= 4 {
                Ok(data[..data.len() - 4].to_vec())
            } else {
                Ok(data.to_vec())
            }
        }
        id => Err(Hdf5Error::UnsupportedFilter(id)),
    }
}

/// Decompress zlib/deflate compressed data.
fn decompress_deflate(data: &[u8]) -> Result<Vec<u8>, Hdf5Error> {
    let mut decoder = ZlibDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| Hdf5Error::Decompression(format!("deflate: {}", e)))?;
    Ok(decompressed)
}

/// Reverse the HDF5 shuffle filter.
///
/// The shuffle filter transposes the byte order of elements to group
/// corresponding bytes together (all byte-0s, then all byte-1s, etc.),
/// which dramatically improves deflate compression ratios for numeric data.
///
/// To unshuffle: interleave bytes from each group back into elements.
fn unshuffle(data: &[u8], element_size: usize) -> Result<Vec<u8>, Hdf5Error> {
    if element_size <= 1 {
        return Ok(data.to_vec());
    }

    let num_elements = data.len() / element_size;
    if num_elements == 0 {
        return Ok(data.to_vec());
    }

    let mut output = vec![0u8; data.len()];

    for i in 0..num_elements {
        for j in 0..element_size {
            output[i * element_size + j] = data[j * num_elements + i];
        }
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unshuffle_4byte() {
        // 3 elements of 4 bytes each
        // Shuffled: [a0,b0,c0, a1,b1,c1, a2,b2,c2, a3,b3,c3]
        // where element a = [a0,a1,a2,a3], etc.
        let shuffled = vec![
            1, 4, 7, // byte 0 of elements 0,1,2
            2, 5, 8, // byte 1 of elements 0,1,2
            3, 6, 9, // byte 2 of elements 0,1,2
            0, 0, 0, // byte 3 of elements 0,1,2
        ];
        let result = unshuffle(&shuffled, 4).unwrap();
        assert_eq!(
            result,
            vec![
                1, 2, 3, 0, // element 0
                4, 5, 6, 0, // element 1
                7, 8, 9, 0, // element 2
            ]
        );
    }

    #[test]
    fn test_deflate_roundtrip() {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        use std::io::Write;

        let original = b"Hello, HDF5 world! This is test data for compression.";
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        let decompressed = decompress_deflate(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }
}
