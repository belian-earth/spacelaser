use crate::hdf5::btree;
use crate::hdf5::dataset::Dataset;
use crate::hdf5::heap;
use crate::hdf5::object_header::{HeaderMessage, ObjectHeader};
use crate::hdf5::superblock::{read_offset, Superblock};
use crate::hdf5::types::*;
use crate::io::reader::{Reader, ReaderConfig};
use crate::io::source::DataSource;
use std::collections::HashMap;
use std::sync::Mutex;

/// Maximum recursion depth for soft-link resolution. Well-formed HDF5
/// files shouldn't have cycles, but this guards against malformed files.
const MAX_LINK_DEPTH: usize = 8;

/// A high-level handle to a remote or local HDF5 file.
///
/// This is the main entry point for reading HDF5 data. It lazily navigates
/// the file structure via HTTP range requests, caching metadata as it goes.
///
/// All methods take `&self` (not `&mut self`) so that multiple concurrent
/// reads can proceed in parallel on the same file handle.  The only mutable
/// state is the path-resolution cache, which is protected by a `Mutex`.
pub struct Hdf5File {
    reader: Reader,
    superblock: Superblock,
    /// Cache of resolved paths → object header addresses.
    path_cache: Mutex<HashMap<String, u64>>,
}

impl Hdf5File {
    /// Open an HDF5 file from a data source.
    ///
    /// Reads the superblock and validates the file signature.
    pub async fn open(source: DataSource) -> Result<Self, Hdf5Error> {
        Self::open_with_config(source, ReaderConfig::default()).await
    }

    /// Open an HDF5 file with custom reader configuration.
    pub async fn open_with_config(
        source: DataSource,
        config: ReaderConfig,
    ) -> Result<Self, Hdf5Error> {
        let reader = Reader::new(source, config);
        let superblock = Superblock::read(&reader).await?;

        Ok(Hdf5File {
            reader,
            superblock,
            path_cache: Mutex::new(HashMap::new()),
        })
    }

    /// List the members of a group at the given path.
    ///
    /// Returns a list of (name, object_header_address) pairs.
    pub async fn list_group(&self, path: &str) -> Result<Vec<(String, u64)>, Hdf5Error> {
        let (_oh_address, oh) = self.resolve_group(path).await?;

        // Check for v1 symbol table (B-tree + local heap)
        if let Some((btree_addr, heap_addr)) = oh.symbol_table() {
            return self.list_group_v1(btree_addr, heap_addr).await;
        }

        // Check for direct link messages (small v2 groups)
        let links = oh.links();
        if !links.is_empty() {
            return Ok(links
                .into_iter()
                .map(|(name, addr)| (name.to_string(), addr))
                .collect());
        }

        // Check superblock scratch pad for root group
        if path == "/"
            && self.superblock.root_btree_address.is_some()
            && self.superblock.root_heap_address.is_some()
        {
            let btree_addr = self.superblock.root_btree_address.unwrap();
            let heap_addr = self.superblock.root_heap_address.unwrap();
            return self.list_group_v1(btree_addr, heap_addr).await;
        }

        // Check for v2 link info (fractal heap + B-tree v2)
        for msg in &oh.messages {
            if let HeaderMessage::LinkInfo {
                heap_address: Some(heap_addr),
                ..
            } = msg
            {
                return self.read_links_from_fractal_heap(*heap_addr).await;
            }
        }

        Ok(Vec::new())
    }

    /// List a v1 group using its B-tree and local heap.
    async fn list_group_v1(
        &self,
        btree_addr: u64,
        heap_addr: u64,
    ) -> Result<Vec<(String, u64)>, Hdf5Error> {
        let entries = btree::read_group_btree(
            &self.reader,
            btree_addr,
            self.superblock.offset_size,
            self.superblock.length_size,
        )
        .await?;

        let heap_data = btree::read_local_heap(
            &self.reader,
            heap_addr,
            self.superblock.offset_size,
            self.superblock.length_size,
        )
        .await?;

        let mut members = Vec::new();
        for entry in &entries {
            let name = btree::heap_string(&heap_data, entry.name_offset);
            if name.is_empty() {
                continue;
            }
            members.push((name, entry.object_header_address));
        }

        Ok(members)
    }

    /// Open a dataset at the given path.
    pub async fn dataset(&self, path: &str) -> Result<Dataset, Hdf5Error> {
        let addr = self.resolve_path(path).await?;
        log::debug!("dataset '{}' resolved to address 0x{:x}", path, addr);
        let oh = ObjectHeader::read(
            &self.reader,
            addr,
            self.superblock.offset_size,
            self.superblock.length_size,
        )
        .await?;
        Dataset::from_object_header(&oh)
    }

    /// Read an entire dataset's data as raw bytes.
    pub async fn read_dataset(&self, path: &str) -> Result<(DatasetMeta, Vec<u8>), Hdf5Error> {
        let ds = self.dataset(path).await?;
        let data = ds.read_all(&self.reader, self.superblock.offset_size).await?;
        Ok((ds.meta, data))
    }

    /// Read specific rows from a dataset.
    ///
    /// `row_ranges` is a list of (start, end) pairs (exclusive end).
    pub async fn read_dataset_rows(
        &self,
        path: &str,
        row_ranges: &[(u64, u64)],
    ) -> Result<(DatasetMeta, Vec<u8>), Hdf5Error> {
        let ds = self.dataset(path).await?;
        let data = ds
            .read_rows(&self.reader, self.superblock.offset_size, row_ranges)
            .await?;
        Ok((ds.meta, data))
    }

    /// Resolve a path to an object header address, navigating through groups.
    async fn resolve_path(&self, path: &str) -> Result<u64, Hdf5Error> {
        self.resolve_path_with_depth(path, 0).await
    }

    /// Inner resolver that tracks recursion depth to prevent soft-link cycles.
    async fn resolve_path_with_depth(
        &self,
        path: &str,
        depth: usize,
    ) -> Result<u64, Hdf5Error> {
        if depth > MAX_LINK_DEPTH {
            return Err(Hdf5Error::InvalidStructure(format!(
                "Soft-link chain exceeds max depth {} at path {}",
                MAX_LINK_DEPTH, path
            )));
        }

        {
            let cache = self.path_cache.lock().unwrap();
            if let Some(&addr) = cache.get(path) {
                return Ok(addr);
            }
        }

        let clean_path = path.trim_matches('/');
        if clean_path.is_empty() {
            return Ok(self.superblock.root_group_address);
        }

        let parts: Vec<&str> = clean_path.split('/').collect();
        let mut current_addr = self.superblock.root_group_address;

        for (i, part) in parts.iter().enumerate() {
            let partial_path = if i == 0 {
                format!("/{}", part)
            } else {
                format!("/{}", parts[..=i].join("/"))
            };

            {
                let cache = self.path_cache.lock().unwrap();
                if let Some(&cached_addr) = cache.get(&partial_path) {
                    current_addr = cached_addr;
                    continue;
                }
            }

            // Read the current group's object header
            let oh = ObjectHeader::read(
                &self.reader,
                current_addr,
                self.superblock.offset_size,
                self.superblock.length_size,
            )
            .await?;

            // Look for the child by name. The parent path is needed so
            // soft links with relative targets can be resolved.
            let parent_path = if i == 0 {
                "/".to_string()
            } else {
                format!("/{}", parts[..i].join("/"))
            };
            let child_addr = Box::pin(self.find_child(&oh, part, &parent_path, depth))
                .await?;

            {
                let mut cache = self.path_cache.lock().unwrap();
                cache.insert(partial_path, child_addr);
            }
            current_addr = child_addr;
        }

        {
            let mut cache = self.path_cache.lock().unwrap();
            cache.insert(path.to_string(), current_addr);
        }
        Ok(current_addr)
    }

    /// Find a child object within a group by name.
    ///
    /// Transparently resolves soft links (HDF5 cache_type 2) by looking
    /// up the link target in the group's local heap and recursively
    /// resolving that path. `parent_path` is the absolute path to the
    /// group being searched; it's needed to resolve relative link
    /// targets. `depth` is the current recursion depth (for cycle prevention).
    async fn find_child(
        &self,
        oh: &ObjectHeader,
        name: &str,
        parent_path: &str,
        depth: usize,
    ) -> Result<u64, Hdf5Error> {
        // Try v1 symbol table first
        if let Some((btree_addr, heap_addr)) = oh.symbol_table() {
            let entries = btree::read_group_btree(
                &self.reader,
                btree_addr,
                self.superblock.offset_size,
                self.superblock.length_size,
            )
            .await?;

            let heap_data = btree::read_local_heap(
                &self.reader,
                heap_addr,
                self.superblock.offset_size,
                self.superblock.length_size,
            )
            .await?;

            for entry in &entries {
                let entry_name = btree::heap_string(&heap_data, entry.name_offset);
                if entry_name == name {
                    if entry.cache_type == 2 {
                        // Soft link: the scratch pad holds an offset into
                        // the group's local heap where the target path
                        // string is stored. Read it and recursively
                        // resolve.
                        let link_off = entry
                            .scratch_link_name_offset
                            .ok_or_else(|| Hdf5Error::InvalidStructure(format!(
                                "Soft link '{}' is missing scratch link name offset",
                                name
                            )))?;
                        let target = btree::heap_string(&heap_data, link_off as u64);
                        if target.is_empty() {
                            return Err(Hdf5Error::InvalidStructure(format!(
                                "Soft link '{}' has empty target",
                                name
                            )));
                        }
                        let resolved_path = if target.starts_with('/') {
                            // Absolute path
                            target
                        } else {
                            // Relative to the group containing the link
                            if parent_path == "/" {
                                format!("/{}", target)
                            } else {
                                format!("{}/{}", parent_path, target)
                            }
                        };
                        log::debug!(
                            "soft link '{}/{}' → '{}'",
                            parent_path.trim_end_matches('/'),
                            name,
                            resolved_path,
                        );
                        return Box::pin(
                            self.resolve_path_with_depth(&resolved_path, depth + 1),
                        )
                        .await;
                    }
                    return Ok(entry.object_header_address);
                }
            }
        }

        // Try v2 link messages (inline)
        for msg in &oh.messages {
            if let HeaderMessage::Link {
                name: link_name,
                target_address,
            } = msg
            {
                if link_name == name {
                    return Ok(*target_address);
                }
            }
        }

        // Try v2 link info (fractal heap + B-tree v2)
        for msg in &oh.messages {
            if let HeaderMessage::LinkInfo {
                heap_address: Some(heap_addr),
                ..
            } = msg
            {
                let links = self.read_links_from_fractal_heap(*heap_addr).await?;
                for (link_name, link_addr) in &links {
                    if link_name == name {
                        return Ok(*link_addr);
                    }
                }
            }
        }

        Err(Hdf5Error::PathNotFound(name.to_string()))
    }

    /// Resolve a path to a group's object header.
    async fn resolve_group(&self, path: &str) -> Result<(u64, ObjectHeader), Hdf5Error> {
        let addr = self.resolve_path(path).await?;
        let oh = ObjectHeader::read(
            &self.reader,
            addr,
            self.superblock.offset_size,
            self.superblock.length_size,
        )
        .await?;
        Ok((addr, oh))
    }

    /// Read link messages from a v2 group's fractal heap.
    ///
    /// Used by `find_child` and `list_group` when the group stores its
    /// children via a `LinkInfo` message (common in ICESat-2 v007 files)
    /// rather than a v1 symbol table or inline Link messages.
    ///
    /// Returns `(name, object_header_address)` pairs for hard links.
    async fn read_links_from_fractal_heap(
        &self,
        heap_addr: u64,
    ) -> Result<Vec<(String, u64)>, Hdf5Error> {
        let blocks = heap::read_fractal_heap_objects(
            &self.reader,
            heap_addr,
            self.superblock.offset_size,
            self.superblock.length_size,
        )
        .await?;

        let mut links = Vec::new();
        for (block_addr, block_data) in &blocks {
            log::debug!(
                "  heap block at 0x{:x}: {} bytes, first 32: {:02x?}",
                block_addr,
                block_data.len(),
                &block_data[..block_data.len().min(32)],
            );
            parse_links_from_heap_block(
                block_data,
                self.superblock.offset_size,
                &mut links,
            );
        }

        log::debug!(
            "fractal heap at 0x{:x}: found {} links: {:?}",
            heap_addr,
            links.len(),
            links.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        );

        Ok(links)
    }

    /// Get a reference to the superblock.
    pub fn superblock(&self) -> &Superblock {
        &self.superblock
    }

    /// Get a reference to the underlying reader.
    pub fn reader(&self) -> &Reader {
        &self.reader
    }
}

// ---------------------------------------------------------------------------
// Fractal heap link parsing (outside impl block)
// ---------------------------------------------------------------------------

/// Parse link messages from the data portion of a fractal heap direct block.
///
/// In v2 groups, each managed object in the heap is a serialised link
/// message (same format as header message type 0x0006). We parse them
/// sequentially until the data is exhausted or we hit invalid data.
fn parse_links_from_heap_block(
    data: &[u8],
    offset_size: u8,
    out: &mut Vec<(String, u64)>,
) {
    let mut pos = 0;
    while pos < data.len() {
        // Zero byte signals end of object data (free space / padding).
        if data[pos] == 0 {
            break;
        }
        match parse_single_link(&data[pos..], offset_size) {
            Some((name, addr, consumed)) => {
                if !name.is_empty() {
                    out.push((name, addr));
                }
                pos += consumed;
            }
            None => break,
        }
    }
}

/// Parse one link message from a byte slice, returning
/// `(link_name, target_address, bytes_consumed)`.
///
/// Returns `None` if the bytes don't look like a valid link message.
fn parse_single_link(data: &[u8], offset_size: u8) -> Option<(String, u64, usize)> {
    if data.len() < 4 {
        return None;
    }

    let version = data[0];
    if version != 1 {
        return None;
    }

    let flags = data[1];
    let mut pos: usize = 2;

    // Optional link type (bit 3)
    let link_type = if flags & 0x08 != 0 {
        let lt = *data.get(pos)?;
        pos += 1;
        lt
    } else {
        0 // hard link
    };

    // Optional creation order (bit 2)
    if flags & 0x04 != 0 {
        pos += 8;
    }

    // Optional character set (bit 4)
    if flags & 0x10 != 0 {
        pos += 1;
    }

    // Link name length (bits 0-1 determine encoding size)
    let name_len = match flags & 0x03 {
        0 => {
            let v = *data.get(pos)? as usize;
            pos += 1;
            v
        }
        1 => {
            let v = u16::from_le_bytes([*data.get(pos)?, *data.get(pos + 1)?]) as usize;
            pos += 2;
            v
        }
        2 => {
            let v = u32::from_le_bytes([
                *data.get(pos)?,
                *data.get(pos + 1)?,
                *data.get(pos + 2)?,
                *data.get(pos + 3)?,
            ]) as usize;
            pos += 4;
            v
        }
        _ => {
            return None;
        }
    };

    if pos + name_len > data.len() {
        return None;
    }
    let name = String::from_utf8_lossy(&data[pos..pos + name_len]).to_string();
    pos += name_len;

    // Link value
    if link_type == 0 {
        // Hard link: object header address
        if pos + offset_size as usize > data.len() {
            return None;
        }
        let mut rpos = pos;
        let addr = read_offset(data, &mut rpos, offset_size);
        pos = rpos;
        Some((name, addr, pos))
    } else if link_type == 1 {
        // Soft link: skip value-length (2 bytes) + value string
        if pos + 2 > data.len() {
            return None;
        }
        let val_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2 + val_len;
        Some((name, u64::MAX, pos)) // soft link, undefined address
    } else {
        None // external or unknown link type
    }
}
