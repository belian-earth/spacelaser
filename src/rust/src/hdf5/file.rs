use crate::hdf5::btree;
use crate::hdf5::dataset::Dataset;
use crate::hdf5::object_header::{HeaderMessage, ObjectHeader};
use crate::hdf5::superblock::Superblock;
use crate::hdf5::types::*;
use crate::io::reader::{Reader, ReaderConfig};
use crate::io::source::DataSource;
use std::collections::HashMap;
use std::sync::Mutex;

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

            // Look for the child by name
            let child_addr = self.find_child(&oh, part).await?;

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
    async fn find_child(&self, oh: &ObjectHeader, name: &str) -> Result<u64, Hdf5Error> {
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
                    // Cache type 2 means the entry is a soft (symbolic)
                    // link. Per the HDF5 spec, `object_header_address` is
                    // *undefined* for such entries (GEDI L2B files set it
                    // to the all-ones sentinel), so we must not feed it to
                    // `ObjectHeader::read` or the parser will panic on an
                    // empty slice. Transparent soft-link resolution is a
                    // TODO; for now, return a clean error telling the user
                    // which path is affected and what to do about it.
                    if entry.cache_type == 2 {
                        return Err(Hdf5Error::SoftLinkNotSupported(name.to_string()));
                    }
                    return Ok(entry.object_header_address);
                }
            }
        }

        // Try v2 link messages
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

    /// Get a reference to the superblock.
    pub fn superblock(&self) -> &Superblock {
        &self.superblock
    }

    /// Get a reference to the underlying reader.
    pub fn reader(&self) -> &Reader {
        &self.reader
    }
}
