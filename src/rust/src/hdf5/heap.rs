use crate::hdf5::superblock::{read_length, read_offset};
use crate::hdf5::types::Hdf5Error;
use crate::io::reader::Reader;

/// Fractal heap (v2 groups use these to store link messages).
///
/// This is a simplified reader that handles the common cases for GEDI/ICESat-2
/// files where fractal heaps store link names and link message data.
///
/// The fractal heap is a complex structure; we implement enough to navigate
/// v2 groups in GEDI/ICESat-2 files.

/// Fractal heap header signature.
const FRHP_SIGNATURE: [u8; 4] = [b'F', b'R', b'H', b'P'];

/// Read objects from a fractal heap by walking its direct blocks.
///
/// Returns a list of (heap_id_offset, data) pairs where data is the raw bytes
/// at each managed object location.
pub async fn read_fractal_heap_objects(
    reader: &Reader,
    heap_address: u64,
    offset_size: u8,
    length_size: u8,
) -> Result<Vec<(u64, Vec<u8>)>, Hdf5Error> {
    let header = reader.read(heap_address, 256).await?;

    if header[0..4] != FRHP_SIGNATURE {
        return Err(Hdf5Error::InvalidStructure(format!(
            "Expected FRHP signature at 0x{:x}",
            heap_address
        )));
    }

    let _version = header[4];
    let _heap_id_len = u16::from_le_bytes([header[5], header[6]]) as usize;
    let io_filter_len = u16::from_le_bytes([header[7], header[8]]) as usize;
    let _flags = header[9];

    let mut pos = 10;

    // Max size of managed objects
    let _max_managed_obj_size = u32::from_le_bytes([
        header[pos],
        header[pos + 1],
        header[pos + 2],
        header[pos + 3],
    ]);
    pos += 4;

    // Next huge object ID
    let _next_huge_id = read_length(&header, &mut pos, length_size);
    // Huge objects B-tree v2 address
    let _huge_bt_address = read_offset(&header, &mut pos, offset_size);
    // Amount of free space in managed blocks
    let _free_space = read_length(&header, &mut pos, length_size);
    // Address of free-space manager
    let _free_space_manager_addr = read_offset(&header, &mut pos, offset_size);
    // Amount of managed space
    let _managed_space = read_length(&header, &mut pos, length_size);
    // Amount of allocated managed space
    let _alloc_managed_space = read_length(&header, &mut pos, length_size);
    // Offset of direct block allocation iterator
    let _iter_offset = read_length(&header, &mut pos, length_size);
    // Number of managed objects
    let _num_managed_objects = read_length(&header, &mut pos, length_size);
    // Size of huge objects
    let _huge_objects_size = read_length(&header, &mut pos, length_size);
    // Number of huge objects
    let _num_huge_objects = read_length(&header, &mut pos, length_size);
    // Size of tiny objects
    let _tiny_objects_size = read_length(&header, &mut pos, length_size);
    // Number of tiny objects
    let _num_tiny_objects = read_length(&header, &mut pos, length_size);

    // Table width
    let table_width = u16::from_le_bytes([header[pos], header[pos + 1]]) as usize;
    pos += 2;

    // Starting block size
    let starting_block_size = read_length(&header, &mut pos, length_size) as usize;
    // Max direct block size
    let _max_direct_block_size = read_length(&header, &mut pos, length_size);
    // Max heap size (in bits)
    let max_heap_size = u16::from_le_bytes([header[pos], header[pos + 1]]);
    pos += 2;

    // Starting # of rows in root indirect block
    let _starting_rows = u16::from_le_bytes([header[pos], header[pos + 1]]);
    pos += 2;

    // Root block address
    let root_block_address = read_offset(&header, &mut pos, offset_size);

    // Current # of rows in root indirect block
    let current_rows = u16::from_le_bytes([header[pos], header[pos + 1]]) as usize;
    let _pos = pos + 2;

    // I/O filter info is skipped (consumed above already in the read)

    if current_rows == 0 {
        // Root block is a direct block
        let objects = read_direct_block(
            reader,
            root_block_address,
            starting_block_size,
            max_heap_size,
            offset_size,
        )
        .await?;
        return Ok(objects);
    }

    // Root block is an indirect block -- read direct blocks from it
    let mut all_objects = Vec::new();
    let indirect_data = reader.read(root_block_address, 4096).await?;

    // Indirect block header: FHIB signature(4) + version(1) + heap header addr(offset_size) + block offset(ceil(max_heap_size/8))
    let block_offset_size = ((max_heap_size as usize) + 7) / 8;
    let mut ipos = 4 + 1 + offset_size as usize + block_offset_size;

    // Direct block entries
    let _max_direct_rows = current_rows.min(table_width * 2);
    let mut block_size = starting_block_size;

    for row in 0..current_rows {
        let entries_in_row = table_width;
        if row > 1 {
            block_size = starting_block_size * (1 << (row - 1));
        }

        for _ in 0..entries_in_row {
            if ipos + offset_size as usize > indirect_data.len() {
                break;
            }
            let mut tmp_pos = ipos;
            let block_addr = read_offset(&indirect_data, &mut tmp_pos, offset_size);
            ipos += offset_size as usize;

            // Skip filtered size + filter mask if I/O filters are used
            if io_filter_len > 0 {
                ipos += length_size as usize + 4;
            }

            if block_addr == 0 || block_addr == u64::MAX {
                continue;
            }

            let objects = read_direct_block(
                reader,
                block_addr,
                block_size,
                max_heap_size,
                offset_size,
            )
            .await?;
            all_objects.extend(objects);
        }
    }

    Ok(all_objects)
}

/// Read managed objects from a direct block.
async fn read_direct_block(
    reader: &Reader,
    address: u64,
    block_size: usize,
    max_heap_size: u16,
    offset_size: u8,
) -> Result<Vec<(u64, Vec<u8>)>, Hdf5Error> {
    let data = reader.read(address, block_size).await?;

    // Direct block header: FHDB signature(4) + version(1) + heap_header_addr(offset_size) + block_offset(ceil(max_heap_size/8))
    let block_offset_size = ((max_heap_size as usize) + 7) / 8;
    let header_size = 4 + 1 + offset_size as usize + block_offset_size;

    // Checksum at the end (4 bytes)
    let data_end = if data.len() >= 4 {
        data.len() - 4
    } else {
        data.len()
    };

    // The remaining bytes after the header contain managed objects
    // These are stored sequentially; each object's boundaries depend on
    // the heap ID used to reference them
    let objects = vec![(address, data[header_size..data_end].to_vec())];
    Ok(objects)
}
