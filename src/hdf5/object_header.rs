use crate::hdf5::superblock::{is_undefined_address, read_length, read_offset};
use crate::hdf5::types::*;
use crate::io::reader::Reader;

/// Object header message types.
const MSG_DATASPACE: u16 = 0x0001;
const MSG_LINK_INFO: u16 = 0x0002;
const MSG_DATATYPE: u16 = 0x0003;
const MSG_FILL_VALUE_OLD: u16 = 0x0004;
const MSG_FILL_VALUE: u16 = 0x0005;
const MSG_LINK: u16 = 0x0006;
const MSG_DATA_LAYOUT: u16 = 0x0008;
const MSG_FILTER_PIPELINE: u16 = 0x000B;
#[allow(dead_code)]
const MSG_ATTRIBUTE: u16 = 0x000C;
const MSG_HEADER_CONTINUATION: u16 = 0x0010;
const MSG_SYMBOL_TABLE: u16 = 0x0011;
#[allow(dead_code)]
const MSG_BTREE_K_VALUES: u16 = 0x0013;

/// A parsed object header message.
#[derive(Debug, Clone)]
pub enum HeaderMessage {
    Dataspace(Dataspace),
    Datatype(Datatype),
    FillValue(Option<Vec<u8>>),
    DataLayout(DataLayout),
    FilterPipeline(Vec<Filter>),
    SymbolTable {
        btree_address: u64,
        heap_address: u64,
    },
    Link {
        name: String,
        target_address: u64,
    },
    LinkInfo {
        btree_address: Option<u64>,
        heap_address: Option<u64>,
    },
    Unknown {
        msg_type: u16,
    },
}

/// Parsed object header containing all its messages.
#[derive(Debug)]
pub struct ObjectHeader {
    pub messages: Vec<HeaderMessage>,
}

impl ObjectHeader {
    /// Read and parse an object header at the given address.
    pub async fn read(
        reader: &Reader,
        address: u64,
        offset_size: u8,
        length_size: u8,
    ) -> Result<Self, Hdf5Error> {
        // Read first bytes to determine version
        let prefix = reader.read(address, 16).await?;

        if &prefix[0..4] == b"OHDR" {
            // Version 2 object header
            Self::read_v2(reader, address, offset_size, length_size).await
        } else {
            // Version 1 object header (no signature)
            Self::read_v1(reader, address, offset_size, length_size).await
        }
    }

    /// Parse a version 1 object header.
    ///
    /// Layout:
    ///   0: version (1)
    ///   1: reserved
    ///   2-3: total number of header messages
    ///   4-7: object reference count
    ///   8-11: header data size
    ///   then: messages (each: type(2) + size(2) + flags(1) + reserved(3) + data)
    async fn read_v1(
        reader: &Reader,
        address: u64,
        offset_size: u8,
        length_size: u8,
    ) -> Result<Self, Hdf5Error> {
        // Read a generous initial chunk
        let data = reader.read(address, 4096).await?;
        let version = data[0];

        if version != 1 {
            return Err(Hdf5Error::UnsupportedObjectHeaderVersion(version));
        }

        let num_messages = u16::from_le_bytes([data[2], data[3]]) as usize;
        let _ref_count = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        let header_size = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;

        let mut messages = Vec::new();
        let mut pos = 12; // after the fixed header
        let header_end = 12 + header_size;

        // Parse messages from the current chunk, then follow continuations
        let mut remaining_data = data;
        let mut data_base_address = address;

        for _ in 0..num_messages.max(256) {
            // Align to 8-byte boundary
            pos = (pos + 7) & !7;

            if pos + 8 > remaining_data.len() || pos >= header_end {
                break;
            }

            let msg_type = u16::from_le_bytes([remaining_data[pos], remaining_data[pos + 1]]);
            let msg_size =
                u16::from_le_bytes([remaining_data[pos + 2], remaining_data[pos + 3]]) as usize;
            let _flags = remaining_data[pos + 4];
            pos += 8; // type(2) + size(2) + flags(1) + reserved(3)

            if msg_size == 0 && msg_type == 0 {
                // Null message (padding)
                continue;
            }

            if pos + msg_size > remaining_data.len() {
                // Need more data
                let extra = reader
                    .read(data_base_address + pos as u64, msg_size)
                    .await?;
                let msg = parse_message(msg_type, &extra, offset_size, length_size)?;
                messages.push(msg);
            } else {
                let msg_data = &remaining_data[pos..pos + msg_size];
                let msg = parse_message(msg_type, msg_data, offset_size, length_size)?;

                // Handle continuation messages
                if let HeaderMessage::Unknown { msg_type: t } = &msg {
                    if *t == MSG_HEADER_CONTINUATION {
                        let mut cpos = 0;
                        let cont_offset = read_offset(msg_data, &mut cpos, offset_size);
                        let cont_length = read_length(msg_data, &mut cpos, length_size);

                        if !is_undefined_address(cont_offset, offset_size) && cont_length > 0 {
                            let cont_data =
                                reader.read(cont_offset, cont_length as usize).await?;
                            // Recursively parse continuation block messages
                            remaining_data = cont_data;
                            data_base_address = cont_offset;
                            pos = 0;
                            continue;
                        }
                    }
                }

                messages.push(msg);
            }

            pos += msg_size;
        }

        Ok(ObjectHeader { messages })
    }

    /// Parse a version 2 object header.
    ///
    /// Layout:
    ///   0-3: "OHDR" signature
    ///   4: version (2)
    ///   5: flags
    ///   then (if flags & 0x04): access time, modification time, change time, birth time (4 bytes each)
    ///   then (if flags & 0x02): max compact attrs(2), min dense attrs(2)
    ///   then: chunk#0 size (1/2/4/8 bytes depending on flags & 0x03)
    ///   then: messages
    async fn read_v2(
        reader: &Reader,
        address: u64,
        offset_size: u8,
        length_size: u8,
    ) -> Result<Self, Hdf5Error> {
        let data = reader.read(address, 8192).await?;

        let _version = data[4]; // should be 2
        let flags = data[5];

        let mut pos = 6;

        // Timestamps
        if flags & 0x04 != 0 {
            pos += 16; // 4 timestamps x 4 bytes
        }

        // Attribute storage phase change values
        if flags & 0x02 != 0 {
            pos += 4; // max_compact(2) + min_dense(2)
        }

        // Chunk#0 size
        let size_field_size = 1 << (flags & 0x03);
        let chunk0_size = match size_field_size {
            1 => data[pos] as usize,
            2 => u16::from_le_bytes([data[pos], data[pos + 1]]) as usize,
            4 => u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                as usize,
            8 => u64::from_le_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]) as usize,
            _ => unreachable!(),
        };
        pos += size_field_size;

        let mut messages = Vec::new();
        let chunk_end = pos + chunk0_size;

        while pos + 4 <= chunk_end && pos + 4 <= data.len() {
            let msg_type = u16::from_le_bytes([data[pos], data[pos + 1]]);
            let msg_size = u16::from_le_bytes([data[pos + 2], data[pos + 3]]) as usize;
            let msg_flags = data[pos + 4];
            pos += 5;

            // v2 has creation order if object header flags & 0x04
            if flags & 0x04 != 0 {
                pos += 2; // creation order (2 bytes)
            }

            if msg_type == 0 && msg_size == 0 {
                continue;
            }

            if pos + msg_size > data.len() {
                break;
            }

            let msg_data = &data[pos..pos + msg_size];

            if msg_type == MSG_HEADER_CONTINUATION {
                let mut cpos = 0;
                let cont_offset = read_offset(msg_data, &mut cpos, offset_size);
                let cont_length = read_length(msg_data, &mut cpos, length_size);

                if !is_undefined_address(cont_offset, offset_size) && cont_length > 0 {
                    let cont_messages = Self::read_v2_continuation(
                        reader,
                        cont_offset,
                        cont_length as usize,
                        offset_size,
                        length_size,
                        flags,
                    )
                    .await?;
                    messages.extend(cont_messages);
                }
            } else {
                let msg = parse_message(msg_type, msg_data, offset_size, length_size)?;
                // Check for creation index on attributes
                let _ = msg_flags;
                messages.push(msg);
            }

            pos += msg_size;
        }

        Ok(ObjectHeader { messages })
    }

    /// Read messages from a v2 continuation block (OCHK).
    async fn read_v2_continuation(
        reader: &Reader,
        address: u64,
        length: usize,
        offset_size: u8,
        length_size: u8,
        oh_flags: u8,
    ) -> Result<Vec<HeaderMessage>, Hdf5Error> {
        let data = reader.read(address, length).await?;
        let mut messages = Vec::new();

        // OCHK signature (4 bytes)
        let mut pos = if &data[0..4] == b"OCHK" { 4 } else { 0 };

        // Last 4 bytes are checksum
        let data_end = data.len().saturating_sub(4);

        while pos + 4 < data_end {
            let msg_type = u16::from_le_bytes([data[pos], data[pos + 1]]);
            let msg_size = u16::from_le_bytes([data[pos + 2], data[pos + 3]]) as usize;
            let _msg_flags = data[pos + 4];
            pos += 5;

            if oh_flags & 0x04 != 0 {
                pos += 2;
            }

            if msg_type == 0 && msg_size == 0 {
                continue;
            }

            if pos + msg_size > data.len() {
                break;
            }

            let msg_data = &data[pos..pos + msg_size];

            if msg_type == MSG_HEADER_CONTINUATION {
                let mut cpos = 0;
                let cont_offset = read_offset(msg_data, &mut cpos, offset_size);
                let cont_length = read_length(msg_data, &mut cpos, length_size);

                if !is_undefined_address(cont_offset, offset_size) && cont_length > 0 {
                    let cont_messages = Box::pin(Self::read_v2_continuation(
                        reader,
                        cont_offset,
                        cont_length as usize,
                        offset_size,
                        length_size,
                        oh_flags,
                    ))
                    .await?;
                    messages.extend(cont_messages);
                }
            } else {
                let msg = parse_message(msg_type, msg_data, offset_size, length_size)?;
                messages.push(msg);
            }

            pos += msg_size;
        }

        Ok(messages)
    }

    /// Find the first message of a given type.
    pub fn find<F, T>(&self, f: F) -> Option<T>
    where
        F: Fn(&HeaderMessage) -> Option<T>,
    {
        self.messages.iter().find_map(|m| f(m))
    }

    /// Get the dataspace message, if present.
    pub fn dataspace(&self) -> Option<&Dataspace> {
        self.messages.iter().find_map(|m| match m {
            HeaderMessage::Dataspace(ds) => Some(ds),
            _ => None,
        })
    }

    /// Get the datatype message, if present.
    pub fn datatype(&self) -> Option<&Datatype> {
        self.messages.iter().find_map(|m| match m {
            HeaderMessage::Datatype(dt) => Some(dt),
            _ => None,
        })
    }

    /// Get the data layout message, if present.
    pub fn layout(&self) -> Option<&DataLayout> {
        self.messages.iter().find_map(|m| match m {
            HeaderMessage::DataLayout(dl) => Some(dl),
            _ => None,
        })
    }

    /// Get the filter pipeline, if present.
    pub fn filters(&self) -> Vec<Filter> {
        self.messages
            .iter()
            .find_map(|m| match m {
                HeaderMessage::FilterPipeline(f) => Some(f.clone()),
                _ => None,
            })
            .unwrap_or_default()
    }

    /// Get the symbol table info (B-tree + heap addresses), if present.
    pub fn symbol_table(&self) -> Option<(u64, u64)> {
        self.messages.iter().find_map(|m| match m {
            HeaderMessage::SymbolTable {
                btree_address,
                heap_address,
            } => Some((*btree_address, *heap_address)),
            _ => None,
        })
    }

    /// Get all link messages (v2 groups).
    pub fn links(&self) -> Vec<(&str, u64)> {
        self.messages
            .iter()
            .filter_map(|m| match m {
                HeaderMessage::Link {
                    name,
                    target_address,
                } => Some((name.as_str(), *target_address)),
                _ => None,
            })
            .collect()
    }
}

/// Parse a single object header message.
fn parse_message(
    msg_type: u16,
    data: &[u8],
    offset_size: u8,
    length_size: u8,
) -> Result<HeaderMessage, Hdf5Error> {
    match msg_type {
        MSG_DATASPACE => parse_dataspace(data),
        MSG_DATATYPE => parse_datatype(data).map(HeaderMessage::Datatype),
        MSG_FILL_VALUE_OLD | MSG_FILL_VALUE => parse_fill_value(data),
        MSG_DATA_LAYOUT => parse_data_layout(data, offset_size, length_size),
        MSG_FILTER_PIPELINE => parse_filter_pipeline(data),
        MSG_SYMBOL_TABLE => parse_symbol_table(data, offset_size),
        MSG_LINK => parse_link_message(data, offset_size),
        MSG_LINK_INFO => parse_link_info(data, offset_size),
        MSG_HEADER_CONTINUATION => Ok(HeaderMessage::Unknown {
            msg_type: MSG_HEADER_CONTINUATION,
        }),
        _ => Ok(HeaderMessage::Unknown { msg_type }),
    }
}

/// Parse a dataspace message.
fn parse_dataspace(data: &[u8]) -> Result<HeaderMessage, Hdf5Error> {
    let version = data[0];
    let rank = data[1];
    let flags = data[2];

    let mut pos = match version {
        1 => 8,  // v1: 4 bytes reserved after flags
        2 => 4,  // v2: flags + 1 reserved byte
        _ => 4,
    };

    let mut dims = Vec::with_capacity(rank as usize);
    for _ in 0..rank {
        let dim = u64::from_le_bytes([
            data[pos],
            data[pos + 1],
            data[pos + 2],
            data[pos + 3],
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
        ]);
        dims.push(dim);
        pos += 8;
    }

    let max_dims = if (version == 1 && flags & 0x01 != 0) || (version == 2 && flags & 0x01 != 0) {
        let mut md = Vec::with_capacity(rank as usize);
        for _ in 0..rank {
            let dim = u64::from_le_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]);
            md.push(dim);
            pos += 8;
        }
        Some(md)
    } else {
        None
    };

    Ok(HeaderMessage::Dataspace(Dataspace {
        rank,
        dims,
        max_dims,
    }))
}

/// Parse a datatype message.
pub fn parse_datatype(data: &[u8]) -> Result<Datatype, Hdf5Error> {
    let class_and_version = data[0];
    let class = class_and_version & 0x0F;
    let _version = (class_and_version >> 4) & 0x0F;
    let class_bits = u32::from_le_bytes([data[1], data[2], data[3], 0]) & 0x00FFFFFF;
    let size = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;

    match class {
        // Fixed-point (integer)
        0 => {
            let byte_order = if class_bits & 0x01 != 0 {
                ByteOrder::BigEndian
            } else {
                ByteOrder::LittleEndian
            };
            let signed = class_bits & 0x08 != 0;
            Ok(Datatype::FixedPoint {
                size,
                signed,
                byte_order,
            })
        }
        // Floating-point
        1 => {
            let byte_order = match class_bits & 0x41 {
                0x00 => ByteOrder::LittleEndian,
                0x01 => ByteOrder::BigEndian,
                _ => ByteOrder::LittleEndian,
            };
            Ok(Datatype::FloatingPoint { size, byte_order })
        }
        // String
        3 => Ok(Datatype::String { size }),
        // Variable-length
        9 => {
            // Base type follows at offset 8 (after the 8-byte datatype header portion)
            if data.len() > 12 {
                let base = parse_datatype(&data[8..])?;
                Ok(Datatype::VariableLength {
                    base_type: Box::new(base),
                })
            } else {
                Ok(Datatype::VariableLength {
                    base_type: Box::new(Datatype::FixedPoint {
                        size: 1,
                        signed: false,
                        byte_order: ByteOrder::LittleEndian,
                    }),
                })
            }
        }
        _ => Err(Hdf5Error::UnsupportedDatatypeClass(class)),
    }
}

/// Parse a fill value message.
fn parse_fill_value(data: &[u8]) -> Result<HeaderMessage, Hdf5Error> {
    if data.is_empty() {
        return Ok(HeaderMessage::FillValue(None));
    }

    let version = data[0];
    match version {
        1 | 2 => {
            // Version 1/2: allocation time (1), fill write time (1), fill defined (1)
            // then: size (4) + data
            if data.len() < 7 {
                return Ok(HeaderMessage::FillValue(None));
            }
            let fill_defined = data[4];
            if fill_defined != 0 && data.len() >= 9 {
                let size =
                    u32::from_le_bytes([data[5], data[6], data[7], data[8]]) as usize;
                if size > 0 && data.len() >= 9 + size {
                    return Ok(HeaderMessage::FillValue(Some(data[9..9 + size].to_vec())));
                }
            }
            Ok(HeaderMessage::FillValue(None))
        }
        3 => {
            // Version 3: flags (1 byte), then optional size(4) + data
            if data.len() < 2 {
                return Ok(HeaderMessage::FillValue(None));
            }
            let flags = data[1];
            let fill_defined = flags & 0x20 != 0;
            if fill_defined && data.len() >= 6 {
                let size =
                    u32::from_le_bytes([data[2], data[3], data[4], data[5]]) as usize;
                if size > 0 && data.len() >= 6 + size {
                    return Ok(HeaderMessage::FillValue(Some(data[6..6 + size].to_vec())));
                }
            }
            Ok(HeaderMessage::FillValue(None))
        }
        _ => Ok(HeaderMessage::FillValue(None)),
    }
}

/// Parse a data storage layout message.
fn parse_data_layout(
    data: &[u8],
    offset_size: u8,
    length_size: u8,
) -> Result<HeaderMessage, Hdf5Error> {
    let version = data[0];

    match version {
        3 => {
            let layout_class = data[1];
            let mut pos = 2;

            match layout_class {
                0 => {
                    // Compact storage
                    let size = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                    pos += 2;
                    let compact_data = data[pos..pos + size].to_vec();
                    Ok(HeaderMessage::DataLayout(DataLayout::Compact {
                        data: compact_data,
                    }))
                }
                1 => {
                    // Contiguous storage
                    let address = read_offset(data, &mut pos, offset_size);
                    let size = read_length(data, &mut pos, length_size);
                    Ok(HeaderMessage::DataLayout(DataLayout::Contiguous {
                        address,
                        size,
                    }))
                }
                2 => {
                    // Chunked storage (v3 layout message)
                    let ndims = data[pos] as usize; // dimensionality (incl. element size dim)
                    pos += 1;
                    let btree_address = read_offset(data, &mut pos, offset_size);

                    // Chunk dimensions: ndims x 4-byte values
                    // The last dimension is the element size
                    let mut chunk_dims = Vec::with_capacity(ndims);
                    for _ in 0..ndims {
                        let dim = u32::from_le_bytes([
                            data[pos],
                            data[pos + 1],
                            data[pos + 2],
                            data[pos + 3],
                        ]);
                        chunk_dims.push(dim);
                        pos += 4;
                    }

                    // Last dimension value is the element size
                    let element_size = chunk_dims.pop().unwrap_or(1);

                    Ok(HeaderMessage::DataLayout(DataLayout::Chunked {
                        btree_address,
                        chunk_dims,
                        element_size,
                    }))
                }
                _ => Err(Hdf5Error::UnsupportedLayoutClass(layout_class)),
            }
        }
        // Version 1 and 2 layout messages
        1 | 2 => {
            let ndims = data[1] as usize;
            let layout_class = data[2];
            // reserved bytes: data[3..8]
            let mut pos = 8;

            match layout_class {
                0 => {
                    // Compact (v1/v2) -- data is inline after dimensions
                    let size_offset = pos + ndims * 4;
                    let size = u32::from_le_bytes([
                        data[size_offset],
                        data[size_offset + 1],
                        data[size_offset + 2],
                        data[size_offset + 3],
                    ]) as usize;
                    let data_start = size_offset + 4;
                    let compact_data = data[data_start..data_start + size].to_vec();
                    Ok(HeaderMessage::DataLayout(DataLayout::Compact {
                        data: compact_data,
                    }))
                }
                1 => {
                    // Contiguous
                    let address = read_offset(data, &mut pos, offset_size);
                    // dimensions follow but for contiguous, there's a length
                    let mut total_size = 1u64;
                    for _ in 0..ndims.saturating_sub(1) {
                        let dim = u32::from_le_bytes([
                            data[pos],
                            data[pos + 1],
                            data[pos + 2],
                            data[pos + 3],
                        ]) as u64;
                        total_size *= dim;
                        pos += 4;
                    }
                    Ok(HeaderMessage::DataLayout(DataLayout::Contiguous {
                        address,
                        size: total_size,
                    }))
                }
                2 => {
                    // Chunked
                    let address = read_offset(data, &mut pos, offset_size);
                    let mut chunk_dims = Vec::with_capacity(ndims);
                    for _ in 0..ndims {
                        let dim = u32::from_le_bytes([
                            data[pos],
                            data[pos + 1],
                            data[pos + 2],
                            data[pos + 3],
                        ]);
                        chunk_dims.push(dim);
                        pos += 4;
                    }
                    let element_size = chunk_dims.pop().unwrap_or(1);

                    Ok(HeaderMessage::DataLayout(DataLayout::Chunked {
                        btree_address: address,
                        chunk_dims,
                        element_size,
                    }))
                }
                _ => Err(Hdf5Error::UnsupportedLayoutClass(layout_class)),
            }
        }
        _ => Err(Hdf5Error::UnsupportedObjectHeaderVersion(version)),
    }
}

/// Parse a filter pipeline message.
fn parse_filter_pipeline(data: &[u8]) -> Result<HeaderMessage, Hdf5Error> {
    let version = data[0];
    let num_filters = data[1] as usize;

    let mut filters = Vec::with_capacity(num_filters);
    let mut pos = match version {
        1 => 8, // 6 reserved bytes after version + nfilters
        2 => 2, // just version + nfilters
        _ => 2,
    };

    for _ in 0..num_filters {
        if pos + 8 > data.len() {
            break;
        }

        let filter_id = u16::from_le_bytes([data[pos], data[pos + 1]]);
        pos += 2;

        let name_length = if version == 1 || filter_id >= 256 {
            let nl = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            nl
        } else {
            0
        };

        let _flags = u16::from_le_bytes([data[pos], data[pos + 1]]);
        pos += 2;
        let num_client_data = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        let name = if name_length > 0 {
            let name_end = pos + name_length;
            let n = String::from_utf8_lossy(&data[pos..name_end])
                .trim_end_matches('\0')
                .to_string();
            // Pad to multiple of 8 for v1
            pos = if version == 1 {
                (name_end + 7) & !7
            } else {
                name_end
            };
            Some(n)
        } else {
            None
        };

        let mut client_data = Vec::with_capacity(num_client_data);
        for _ in 0..num_client_data {
            if pos + 4 <= data.len() {
                let val = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                client_data.push(val);
                pos += 4;
            }
        }

        // Pad to multiple of 8 in v1 if odd number of client data values
        if version == 1 && num_client_data % 2 != 0 {
            pos += 4;
        }

        filters.push(Filter {
            id: filter_id,
            name,
            client_data,
        });
    }

    Ok(HeaderMessage::FilterPipeline(filters))
}

/// Parse a symbol table message (v1 groups).
fn parse_symbol_table(data: &[u8], offset_size: u8) -> Result<HeaderMessage, Hdf5Error> {
    let mut pos = 0;
    let btree_address = read_offset(data, &mut pos, offset_size);
    let heap_address = read_offset(data, &mut pos, offset_size);
    Ok(HeaderMessage::SymbolTable {
        btree_address,
        heap_address,
    })
}

/// Parse a link message (v2 groups).
fn parse_link_message(data: &[u8], offset_size: u8) -> Result<HeaderMessage, Hdf5Error> {
    let _version = data[0];
    let flags = data[1];
    let mut pos = 2;

    // Optional link type
    let link_type = if flags & 0x08 != 0 {
        let lt = data[pos];
        pos += 1;
        lt
    } else {
        0 // hard link
    };

    // Optional creation order
    if flags & 0x04 != 0 {
        pos += 8; // 8-byte creation order
    }

    // Optional link name character set
    if flags & 0x10 != 0 {
        pos += 1;
    }

    // Link name length
    let name_size_field = (flags & 0x03) as usize;
    let name_length = match name_size_field {
        0 => {
            let v = data[pos] as usize;
            pos += 1;
            v
        }
        1 => {
            let v = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;
            v
        }
        2 => {
            let v = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                as usize;
            pos += 4;
            v
        }
        3 => {
            let v = u64::from_le_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]) as usize;
            pos += 8;
            v
        }
        _ => unreachable!(),
    };

    let name = String::from_utf8_lossy(&data[pos..pos + name_length]).to_string();
    pos += name_length;

    // Link value depends on link type
    let target_address = if link_type == 0 {
        // Hard link: target is an object header address
        read_offset(data, &mut pos, offset_size)
    } else {
        // Soft link, external link, etc. -- not supported for navigation
        0
    };

    Ok(HeaderMessage::Link {
        name,
        target_address,
    })
}

/// Parse a link info message (v2 groups with indexed links).
fn parse_link_info(data: &[u8], offset_size: u8) -> Result<HeaderMessage, Hdf5Error> {
    let _version = data[0];
    let flags = data[1];
    let mut pos = 2;

    // Optional max creation index
    if flags & 0x01 != 0 {
        pos += 8;
    }

    let fractal_heap_address = read_offset(data, &mut pos, offset_size);
    let btree_address = read_offset(data, &mut pos, offset_size);

    let fh = if is_undefined_address(fractal_heap_address, offset_size) {
        None
    } else {
        Some(fractal_heap_address)
    };

    let bt = if is_undefined_address(btree_address, offset_size) {
        None
    } else {
        Some(btree_address)
    };

    Ok(HeaderMessage::LinkInfo {
        btree_address: bt,
        heap_address: fh,
    })
}
