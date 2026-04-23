use crate::hdf5::types::Hdf5Error;
use crate::io::reader::Reader;

/// HDF5 file signature: \x89HDF\r\n\x1a\n
const HDF5_SIGNATURE: [u8; 8] = [0x89, 0x48, 0x44, 0x46, 0x0d, 0x0a, 0x1a, 0x0a];

/// Parsed superblock containing the essential file metadata.
#[derive(Debug, Clone)]
pub struct Superblock {
    /// Superblock version (0, 1, 2, or 3).
    pub version: u8,
    /// Size of file offsets in bytes (typically 8).
    pub offset_size: u8,
    /// Size of lengths in bytes (typically 8).
    pub length_size: u8,
    /// File address of the root group object header.
    pub root_group_address: u64,
    /// For v0/v1: address of root group symbol table entry's B-tree.
    pub root_btree_address: Option<u64>,
    /// For v0/v1: address of root group's local heap.
    pub root_heap_address: Option<u64>,
    /// Base address of the file (usually 0).
    pub base_address: u64,
}

impl Superblock {
    /// Parse the superblock from the beginning of an HDF5 file.
    pub async fn read(reader: &Reader) -> Result<Self, Hdf5Error> {
        // Search for signature at offsets 0, 512, 1024, 2048, ...
        let sig_offset = Self::find_signature(reader).await?;

        // Read a generous chunk starting at the signature
        let header = reader.read(sig_offset, 256).await?;
        if header.len() < 16 {
            return Err(Hdf5Error::InvalidStructure(format!(
                "Superblock at 0x{:x} truncated ({} bytes)",
                sig_offset,
                header.len()
            )));
        }

        // Bytes 8: superblock version
        let version = header[8];

        match version {
            0 | 1 => Self::parse_v0_v1(&header, sig_offset),
            2 | 3 => Self::parse_v2_v3(&header, sig_offset),
            _ => Err(Hdf5Error::UnsupportedSuperblockVersion(version)),
        }
    }

    /// Search for the HDF5 signature at the standard offsets.
    async fn find_signature(reader: &Reader) -> Result<u64, Hdf5Error> {
        // Try offset 0 first (most common)
        let buf = reader.read(0, 8).await?;
        if buf.len() >= 8 && buf[..8] == HDF5_SIGNATURE {
            return Ok(0);
        }

        // Try powers of 2: 512, 1024, 2048, ...
        for power in 9..20 {
            let offset = 1u64 << power;
            let buf = reader.read(offset, 8).await?;
            if buf.len() >= 8 && buf[..8] == HDF5_SIGNATURE {
                return Ok(offset);
            }
        }

        Err(Hdf5Error::InvalidSignature(0))
    }

    /// Parse superblock version 0 or 1.
    ///
    /// Layout (v0):
    ///   0-7:   signature
    ///   8:     superblock version (0)
    ///   9:     free-space storage version
    ///   10:    root group symbol table entry version
    ///   11:    reserved
    ///   12:    shared header message format version
    ///   13:    size of offsets
    ///   14:    size of lengths
    ///   15:    reserved
    ///   16-17: group leaf node K
    ///   18-19: group internal node K
    ///   20-23: consistency flags
    ///   -- if version 1: 24-25: indexed storage internal node K, 26-27: reserved
    ///   then:  base address (offset_size bytes)
    ///          free-space info address (offset_size bytes)
    ///          end-of-file address (offset_size bytes)
    ///          driver info block address (offset_size bytes)
    ///          root group symbol table entry (variable size)
    fn parse_v0_v1(data: &[u8], _sig_offset: u64) -> Result<Self, Hdf5Error> {
        let version = data[8];
        let offset_size = data[13];
        let length_size = data[14];

        let fixed_header_size: usize = if version == 0 { 24 } else { 28 };
        let mut pos = fixed_header_size;

        let base_address = read_offset(data, &mut pos, offset_size);
        let _freespace_address = read_offset(data, &mut pos, offset_size);
        let _eof_address = read_offset(data, &mut pos, offset_size);
        let _driver_info_address = read_offset(data, &mut pos, offset_size);

        // Root group symbol table entry:
        //   link name offset (offset_size)
        //   object header address (offset_size)
        //   cache type (4 bytes)
        //   reserved (4 bytes)
        //   scratch pad (16 bytes) -- for cache type 1: B-tree address + name heap address
        let _link_name_offset = read_offset(data, &mut pos, offset_size);
        let root_group_address = read_offset(data, &mut pos, offset_size);
        let cache_type = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let _reserved = &data[pos..pos + 4];
        pos += 4;

        // Scratch pad space (16 bytes)
        let (root_btree_address, root_heap_address) = if cache_type == 1 {
            let btree_addr = read_offset(data, &mut pos, offset_size);
            let heap_addr = read_offset(data, &mut pos, offset_size);
            (Some(btree_addr), Some(heap_addr))
        } else {
            (None, None)
        };

        Ok(Superblock {
            version,
            offset_size,
            length_size,
            root_group_address,
            root_btree_address,
            root_heap_address,
            base_address,
        })
    }

    /// Parse superblock version 2 or 3.
    ///
    /// Layout (v2/v3):
    ///   0-7:   signature
    ///   8:     superblock version
    ///   9:     size of offsets
    ///   10:    size of lengths
    ///   11:    file consistency flags
    ///   12-*:  base address (offset_size bytes)
    ///          superblock extension address (offset_size bytes)
    ///          end of file address (offset_size bytes)
    ///          root group object header address (offset_size bytes)
    ///   then:  superblock checksum (4 bytes)
    fn parse_v2_v3(data: &[u8], _sig_offset: u64) -> Result<Self, Hdf5Error> {
        let version = data[8];
        let offset_size = data[9];
        let length_size = data[10];
        // let consistency_flags = data[11];

        let mut pos = 12;
        let base_address = read_offset(data, &mut pos, offset_size);
        let _ext_address = read_offset(data, &mut pos, offset_size);
        let _eof_address = read_offset(data, &mut pos, offset_size);
        let root_group_address = read_offset(data, &mut pos, offset_size);

        Ok(Superblock {
            version,
            offset_size,
            length_size,
            root_group_address,
            root_btree_address: None,
            root_heap_address: None,
            base_address,
        })
    }
}

/// Read an offset value (variable-size, little-endian) from a byte buffer.
pub fn read_offset(data: &[u8], pos: &mut usize, size: u8) -> u64 {
    let val = read_n_bytes_le(data, *pos, size as usize);
    *pos += size as usize;
    val
}

/// Read a length value (variable-size, little-endian) from a byte buffer.
pub fn read_length(data: &[u8], pos: &mut usize, size: u8) -> u64 {
    let val = read_n_bytes_le(data, *pos, size as usize);
    *pos += size as usize;
    val
}

/// Read n bytes as a little-endian unsigned integer. Returns 0 if the
/// buffer is too short — callers are expected to validate lengths up
/// front; this guard is a belt-and-braces against malformed inputs
/// slipping past a missed check.
fn read_n_bytes_le(data: &[u8], offset: usize, n: usize) -> u64 {
    if offset.saturating_add(n) > data.len() {
        return 0;
    }
    let mut val: u64 = 0;
    for i in 0..n {
        val |= (data[offset + i] as u64) << (8 * i);
    }
    val
}

/// The "undefined address" sentinel in HDF5 (all 1-bits for the address size).
pub fn is_undefined_address(addr: u64, offset_size: u8) -> bool {
    // offset_size must be in [1, 8] for a valid HDF5 file. Anything
    // else is treated as "not undefined" so a malformed value can't
    // make real addresses look like sentinels.
    if offset_size == 0 || offset_size > 8 {
        return false;
    }
    let mask = if offset_size == 8 {
        u64::MAX
    } else {
        (1u64 << (offset_size as u64 * 8)) - 1
    };
    addr == mask
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_n_bytes_le() {
        let data = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        assert_eq!(read_n_bytes_le(&data, 0, 4), 0x04030201);
        assert_eq!(read_n_bytes_le(&data, 0, 8), 0x0807060504030201);
        assert_eq!(read_n_bytes_le(&data, 0, 2), 0x0201);
    }

    #[test]
    fn test_undefined_address() {
        assert!(is_undefined_address(u64::MAX, 8));
        assert!(is_undefined_address(0xFFFFFFFF, 4));
        assert!(!is_undefined_address(0, 8));
        assert!(!is_undefined_address(1024, 8));
    }
}
