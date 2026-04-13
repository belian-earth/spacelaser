use crate::hdf5::superblock::{read_length, read_offset};
use crate::hdf5::types::Hdf5Error;
use crate::io::reader::Reader;

/// B-tree v1 signature: "TREE"
const BTREE_SIGNATURE: [u8; 4] = [b'T', b'R', b'E', b'E'];
/// Symbol table node signature: "SNOD"
const SNOD_SIGNATURE: [u8; 4] = [b'S', b'N', b'O', b'D'];

/// B-tree v1 node types.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BtreeNodeType {
    /// Type 0: group nodes (keys are symbol table entries).
    Group = 0,
    /// Type 1: raw data chunks (keys are chunk coordinates).
    RawData = 1,
}

/// A symbol table entry mapping a name to an object header address.
#[derive(Debug, Clone)]
pub struct SymbolTableEntry {
    /// Offset of the name in the local heap.
    pub name_offset: u64,
    /// Address of the object header.
    pub object_header_address: u64,
    /// Cache type (0 = none, 1 = group info in scratch pad).
    pub cache_type: u32,
    /// For cache_type 1: B-tree address from scratch pad.
    pub scratch_btree_address: Option<u64>,
    /// For cache_type 1: Name heap address from scratch pad.
    pub scratch_heap_address: Option<u64>,
}

/// Information about a single chunk's location, from a B-tree v1 raw data node.
#[derive(Debug, Clone)]
pub struct ChunkInfo {
    /// Chunk size in bytes (stored in the file, may be compressed size).
    pub size: u32,
    /// Filter mask (which filters were applied; 0 = all filters applied).
    pub filter_mask: u32,
    /// Chunk offset coordinates in the dataset's dataspace (scaled by chunk dims).
    pub offsets: Vec<u64>,
    /// File address where this chunk's data is stored.
    pub address: u64,
}

/// Walk a B-tree v1 for group nodes and collect all symbol table entries.
///
/// This is used to enumerate the members of an HDF5 group.
pub async fn read_group_btree(
    reader: &Reader,
    btree_address: u64,
    offset_size: u8,
    length_size: u8,
) -> Result<Vec<SymbolTableEntry>, Hdf5Error> {
    // Read the B-tree node header
    let header = reader.read(btree_address, 512).await?;

    if header[0..4] != BTREE_SIGNATURE {
        return Err(Hdf5Error::InvalidStructure(format!(
            "Expected TREE signature at 0x{:x}, found {:?}",
            btree_address,
            &header[0..4]
        )));
    }

    let node_type = header[4];
    let node_level = header[5];
    let entries_used = u16::from_le_bytes([header[6], header[7]]) as usize;
    // let _left_sibling = read_offset_at(&header, 8, offset_size);
    // let _right_sibling = read_offset_at(&header, 8 + offset_size as usize, offset_size);

    if node_type != BtreeNodeType::Group as u8 {
        return Err(Hdf5Error::InvalidStructure(format!(
            "Expected group B-tree (type 0), found type {}",
            node_type
        )));
    }

    let keys_start = 8 + 2 * offset_size as usize; // after left+right siblings

    if node_level > 0 {
        // Internal node: keys and child pointers
        // Each key is length_size bytes, each child pointer is offset_size bytes
        // Layout: key0 child0 key1 child1 ... keyN childN keyN+1
        let mut entries = Vec::new();
        let mut pos = keys_start;

        for i in 0..entries_used {
            // key_i (skip)
            let _key = read_length(&header, &mut pos, length_size);
            // child pointer
            let child_addr = read_offset(&header, &mut pos, offset_size);

            // Skip key_i+1 on last iteration handled by the loop structure
            if i == entries_used - 1 {
                // Read the trailing key
                let _key = read_length(&header, &mut pos, length_size);
            }

            // Recursively read the child node
            let child_entries = Box::pin(read_group_btree(
                reader,
                child_addr,
                offset_size,
                length_size,
            ))
            .await?;
            entries.extend(child_entries);
        }

        Ok(entries)
    } else {
        // Leaf node: child pointers are symbol table node (SNOD) addresses
        let mut snod_addresses = Vec::new();
        let mut pos = keys_start;

        for i in 0..entries_used {
            // key
            let _key = read_length(&header, &mut pos, length_size);
            // child pointer (SNOD address)
            let snod_addr = read_offset(&header, &mut pos, offset_size);
            snod_addresses.push(snod_addr);

            if i == entries_used - 1 {
                let _key = read_length(&header, &mut pos, length_size);
            }
        }

        // Read each symbol table node
        let mut entries = Vec::new();
        for snod_addr in snod_addresses {
            let snod_entries =
                read_symbol_table_node(reader, snod_addr, offset_size).await?;
            entries.extend(snod_entries);
        }

        Ok(entries)
    }
}

/// Read a symbol table node (SNOD) and return its entries.
async fn read_symbol_table_node(
    reader: &Reader,
    address: u64,
    offset_size: u8,
) -> Result<Vec<SymbolTableEntry>, Hdf5Error> {
    // SNOD can be quite large; read enough for typical cases
    let data = reader.read(address, 2048).await?;

    if data[0..4] != SNOD_SIGNATURE {
        return Err(Hdf5Error::InvalidStructure(format!(
            "Expected SNOD signature at 0x{:x}",
            address
        )));
    }

    let _version = data[4];
    // byte 5: reserved
    let num_symbols = u16::from_le_bytes([data[6], data[7]]) as usize;

    let mut entries = Vec::new();
    let mut pos = 8;

    // Each symbol table entry is:
    //   name_offset (offset_size bytes)
    //   object_header_address (offset_size bytes)
    //   cache_type (4 bytes)
    //   reserved (4 bytes)
    //   scratch pad (16 bytes)
    // Total: 2*offset_size + 24 bytes per entry
    let entry_size = 2 * offset_size as usize + 24;

    for _ in 0..num_symbols {
        if pos + entry_size > data.len() {
            // Need more data
            let extra = reader.read(address + pos as u64, entry_size).await?;
            let name_offset = read_offset(&extra, &mut 0, offset_size);
            let oh_addr = read_offset(&extra, &mut (offset_size as usize), offset_size);
            let ct_off = 2 * offset_size as usize;
            let cache_type = u32::from_le_bytes([
                extra[ct_off],
                extra[ct_off + 1],
                extra[ct_off + 2],
                extra[ct_off + 3],
            ]);

            let (sbt, shp) = if cache_type == 1 {
                let sp_off = ct_off + 8;
                let mut sp_pos = sp_off;
                let bt = read_offset(&extra, &mut sp_pos, offset_size);
                let hp = read_offset(&extra, &mut sp_pos, offset_size);
                (Some(bt), Some(hp))
            } else {
                (None, None)
            };

            entries.push(SymbolTableEntry {
                name_offset,
                object_header_address: oh_addr,
                cache_type,
                scratch_btree_address: sbt,
                scratch_heap_address: shp,
            });
            pos += entry_size;
            continue;
        }

        let name_offset = read_offset(&data, &mut pos, offset_size);
        let object_header_address = read_offset(&data, &mut pos, offset_size);
        let cache_type = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        pos += 4; // reserved

        // Scratch pad space (16 bytes)
        let (scratch_btree_address, scratch_heap_address) = if cache_type == 1 {
            let mut sp_pos = pos;
            let bt = read_offset(&data, &mut sp_pos, offset_size);
            let hp = read_offset(&data, &mut sp_pos, offset_size);
            (Some(bt), Some(hp))
        } else {
            (None, None)
        };
        pos += 16; // always skip 16 bytes of scratch pad

        entries.push(SymbolTableEntry {
            name_offset,
            object_header_address,
            cache_type,
            scratch_btree_address,
            scratch_heap_address,
        });
    }

    Ok(entries)
}

/// Walk a B-tree v1 for raw data chunks and collect all chunk info.
///
/// Read chunk info from a B-tree v1 for raw data chunks.
///
/// When `row_bounds` is `None`, reads all chunks (full tree scan). When
/// `Some((start, end))`, only navigates to subtrees and collects chunks
/// whose first-dimension offset overlaps `[start, end)`. This turns a
/// full tree scan (hundreds of HTTP reads for large datasets) into a
/// targeted lookup (3-5 reads). `chunk_row_dim` is the chunk size in
/// the first dimension (needed at leaf level to compute chunk coverage).
pub async fn read_chunk_btree(
    reader: &Reader,
    btree_address: u64,
    offset_size: u8,
    ndims: usize,
    row_bounds: Option<(u64, u64)>,
    chunk_row_dim: u64,
) -> Result<Vec<ChunkInfo>, Hdf5Error> {
    let header = reader.read(btree_address, 4096).await?;

    if header[0..4] != BTREE_SIGNATURE {
        return Err(Hdf5Error::InvalidStructure(format!(
            "Expected TREE signature at 0x{:x} for chunk B-tree",
            btree_address
        )));
    }

    let node_type = header[4];
    let node_level = header[5];
    let entries_used = u16::from_le_bytes([header[6], header[7]]) as usize;

    if node_type != BtreeNodeType::RawData as u8 {
        return Err(Hdf5Error::InvalidStructure(format!(
            "Expected raw data B-tree (type 1), found type {}",
            node_type
        )));
    }

    let keys_start = 8 + 2 * offset_size as usize;
    let key_size = 4 + 4 + (ndims + 1) * 8;

    // Helper: extract the first-dimension offset from a key at `kpos`.
    let key_row_offset = |kpos: usize| -> u64 {
        let off_pos = kpos + 4 + 4; // skip chunk_size + filter_mask
        u64::from_le_bytes([
            header[off_pos],
            header[off_pos + 1],
            header[off_pos + 2],
            header[off_pos + 3],
            header[off_pos + 4],
            header[off_pos + 5],
            header[off_pos + 6],
            header[off_pos + 7],
        ])
    };

    if node_level > 0 {
        // Internal node: parse all keys and child pointers first, then
        // only recurse into children whose key range overlaps row_bounds.
        let mut key_positions = Vec::with_capacity(entries_used + 1);
        let mut child_addrs = Vec::with_capacity(entries_used);
        let mut pos = keys_start;

        for _i in 0..entries_used {
            key_positions.push(pos);
            pos += key_size;
            child_addrs.push(read_offset(&header, &mut pos, offset_size));
        }
        key_positions.push(pos); // trailing key

        let mut chunks = Vec::new();
        for i in 0..entries_used {
            // Child[i] contains chunks with offsets in [key[i], key[i+1])
            if let Some((range_start, range_end)) = row_bounds {
                let child_min = key_row_offset(key_positions[i]);
                let child_max = key_row_offset(key_positions[i + 1]);
                // Skip if no overlap
                if child_max <= range_start || child_min >= range_end {
                    continue;
                }
            }

            let child_chunks = Box::pin(read_chunk_btree(
                reader,
                child_addrs[i],
                offset_size,
                ndims,
                row_bounds,
                chunk_row_dim,
            ))
            .await?;
            chunks.extend(child_chunks);
        }

        Ok(chunks)
    } else {
        // Leaf node: parse chunks and optionally filter by row_bounds.
        let mut chunks = Vec::new();
        let mut pos = keys_start;

        for i in 0..entries_used {
            let chunk_size = u32::from_le_bytes([
                header[pos],
                header[pos + 1],
                header[pos + 2],
                header[pos + 3],
            ]);
            pos += 4;
            let filter_mask = u32::from_le_bytes([
                header[pos],
                header[pos + 1],
                header[pos + 2],
                header[pos + 3],
            ]);
            pos += 4;

            let mut offsets = Vec::with_capacity(ndims);
            for _ in 0..=ndims {
                let off = u64::from_le_bytes([
                    header[pos],
                    header[pos + 1],
                    header[pos + 2],
                    header[pos + 3],
                    header[pos + 4],
                    header[pos + 5],
                    header[pos + 6],
                    header[pos + 7],
                ]);
                offsets.push(off);
                pos += 8;
            }
            offsets.truncate(ndims);

            let address = read_offset(&header, &mut pos, offset_size);

            if i == entries_used - 1 {
                pos += key_size; // trailing key
            }

            // Optionally filter by row bounds
            if let Some((range_start, range_end)) = row_bounds {
                let chunk_start = offsets[0];
                let chunk_end = chunk_start + chunk_row_dim;
                if chunk_end <= range_start || chunk_start >= range_end {
                    continue;
                }
            }

            chunks.push(ChunkInfo {
                size: chunk_size,
                filter_mask,
                offsets,
                address,
            });
        }

        Ok(chunks)
    }
}

/// Read a local heap and return its data segment.
///
/// Local heaps store the names for symbol table entries in v0/v1 groups.
pub async fn read_local_heap(
    reader: &Reader,
    address: u64,
    offset_size: u8,
    length_size: u8,
) -> Result<Vec<u8>, Hdf5Error> {
    // Local heap header:
    //   0-3: signature "HEAP"
    //   4:   version
    //   5-7: reserved
    //   8:   data segment size (length_size bytes)
    //   then: offset to free list head (length_size bytes)
    //   then: data segment address (offset_size bytes)
    let header_size = 8 + length_size as usize * 2 + offset_size as usize;
    let header = reader.read(address, header_size).await?;

    if &header[0..4] != b"HEAP" {
        return Err(Hdf5Error::InvalidStructure(format!(
            "Expected HEAP signature at 0x{:x}",
            address
        )));
    }

    let version = header[4];
    if version != 0 {
        return Err(Hdf5Error::UnsupportedLocalHeapVersion(version));
    }

    let mut pos = 8;
    let data_size = read_length(&header, &mut pos, length_size) as usize;
    let _free_list_offset = read_length(&header, &mut pos, length_size);
    let data_address = read_offset(&header, &mut pos, offset_size);

    // Read the heap data segment
    let data = reader.read(data_address, data_size).await?;
    Ok(data)
}

/// Look up a null-terminated string in heap data at the given offset.
pub fn heap_string(heap_data: &[u8], offset: u64) -> String {
    let start = offset as usize;
    if start >= heap_data.len() {
        return String::new();
    }
    let end = heap_data[start..]
        .iter()
        .position(|&b| b == 0)
        .map(|p| start + p)
        .unwrap_or(heap_data.len());
    String::from_utf8_lossy(&heap_data[start..end]).to_string()
}
