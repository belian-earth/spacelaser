use bytes::Bytes;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::ops::Range;

/// Block-level LRU cache for byte ranges read from remote files.
///
/// Reads from HDF5 files are aligned to fixed-size blocks and cached so that
/// nearby metadata reads (which are very common during B-tree traversal) hit
/// the cache instead of issuing new HTTP requests.
pub struct BlockCache {
    /// Block size in bytes. All reads are aligned to this boundary.
    block_size: usize,
    /// LRU cache mapping block index → block data (zero-copy reference-counted).
    blocks: LruCache<u64, Bytes>,
}

impl BlockCache {
    /// Create a new block cache.
    ///
    /// - `block_size`: Size of each cached block in bytes (e.g., 256 KiB).
    /// - `max_blocks`: Maximum number of blocks to keep in cache.
    pub fn new(block_size: usize, max_blocks: usize) -> Self {
        Self {
            block_size,
            blocks: LruCache::new(NonZeroUsize::new(max_blocks).unwrap()),
        }
    }

    /// The block size used by this cache.
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Number of blocks currently in the cache.
    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    /// Determine which blocks are needed to cover a byte range. Cached
    /// blocks are returned as `(idx, bytes)` pairs with the data taken
    /// by Bytes clone (cheap Arc bump) so the caller owns them for the
    /// rest of the read — this prevents a concurrent reader from
    /// evicting a block between our query and extract phases.
    ///
    /// Returns `(cached_blocks, missing_block_indices)`.
    pub fn query(&mut self, offset: u64, length: usize) -> (Vec<(u64, Bytes)>, Vec<u64>) {
        let start_block = offset / self.block_size as u64;
        let end_block = (offset + length as u64).saturating_sub(1) / self.block_size as u64;

        let mut cached = Vec::new();
        let mut missing = Vec::new();

        for block_idx in start_block..=end_block {
            if let Some(data) = self.blocks.get(&block_idx) {
                cached.push((block_idx, data.clone()));
            } else {
                missing.push(block_idx);
            }
        }

        (cached, missing)
    }

    /// Get a cached block's data by index (cheap Arc clone).
    pub fn get(&mut self, block_index: &u64) -> Option<Bytes> {
        self.blocks.get(block_index).cloned()
    }

    /// Insert a fetched block into the cache.
    pub fn insert(&mut self, block_index: u64, data: Bytes) {
        self.blocks.put(block_index, data);
    }

    /// Compute the byte range for a given block index.
    pub fn block_range(&self, block_index: u64) -> Range<u64> {
        let start = block_index * self.block_size as u64;
        let end = start + self.block_size as u64;
        start..end
    }

    /// Extract the requested bytes from block-aligned cached data.
    ///
    /// `offset` and `length` are the original request parameters.
    /// `blocks` is a sorted list of (block_index, block_data) covering the range.
    pub fn extract_range(
        &self,
        offset: u64,
        length: usize,
        blocks: &[(u64, Bytes)],
    ) -> Vec<u8> {
        let mut result = Vec::with_capacity(length);
        let end = offset + length as u64;

        for (block_idx, data) in blocks {
            let block_start = block_idx * self.block_size as u64;
            let block_end = block_start + data.len() as u64;

            // Overlap between [offset, end) and [block_start, block_end)
            let overlap_start = offset.max(block_start);
            let overlap_end = end.min(block_end);

            if overlap_start < overlap_end {
                let local_start = (overlap_start - block_start) as usize;
                let local_end = (overlap_end - block_start) as usize;
                result.extend_from_slice(&data[local_start..local_end]);
            }
        }

        result
    }

    /// Coalesce a list of block indices into minimal byte ranges for fetching.
    ///
    /// Blocks are merged into a single range request when the gap between them
    /// is at most `max_gap_blocks`. This reduces HTTP round trips at the cost
    /// of fetching small amounts of unneeded data in the gaps.
    ///
    /// A `max_gap_blocks` of 0 merges only strictly adjacent blocks (original
    /// behavior). A value of 4 with a 256 KiB block size means gaps up to 1 MiB
    /// are bridged.
    pub fn coalesce_ranges(
        &self,
        mut block_indices: Vec<u64>,
        max_gap_blocks: u64,
    ) -> Vec<Range<u64>> {
        if block_indices.is_empty() {
            return Vec::new();
        }
        block_indices.sort_unstable();
        block_indices.dedup();

        let mut ranges = Vec::new();
        let mut current_start = block_indices[0];
        let mut current_end = block_indices[0];

        for &idx in &block_indices[1..] {
            if idx <= current_end + 1 + max_gap_blocks {
                current_end = idx;
            } else {
                ranges.push(self.block_range(current_start).start..self.block_range(current_end).end);
                current_start = idx;
                current_end = idx;
            }
        }
        ranges.push(self.block_range(current_start).start..self.block_range(current_end).end);
        ranges
    }
}

/// Coalesce arbitrary byte ranges into merged ranges. Two ranges are
/// merged when the gap between them is ≤ `max_gap` bytes; overlapping
/// ranges always merge. Used to batch many small chunk fetches across
/// parallel column reads into a few large HTTP range requests.
pub fn coalesce_byte_ranges(mut ranges: Vec<Range<u64>>, max_gap: u64) -> Vec<Range<u64>> {
    if ranges.is_empty() {
        return Vec::new();
    }
    ranges.sort_by_key(|r| r.start);
    let mut out = Vec::with_capacity(ranges.len());
    let mut current = ranges[0].clone();
    for r in ranges.into_iter().skip(1) {
        if r.start <= current.end.saturating_add(max_gap) {
            if r.end > current.end {
                current.end = r.end;
            }
        } else {
            out.push(current);
            current = r;
        }
    }
    out.push(current);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_range_calculation() {
        let cache = BlockCache::new(1024, 10);
        assert_eq!(cache.block_range(0), 0..1024);
        assert_eq!(cache.block_range(1), 1024..2048);
        assert_eq!(cache.block_range(5), 5120..6144);
    }

    #[test]
    fn test_coalesce_adjacent_blocks() {
        let cache = BlockCache::new(1024, 10);
        let ranges = cache.coalesce_ranges(vec![0, 1, 2, 5, 6, 10], 0);
        assert_eq!(ranges, vec![0..3072, 5120..7168, 10240..11264]);
    }

    #[test]
    fn test_coalesce_with_gap() {
        let cache = BlockCache::new(1024, 10);
        // Gap of 2 blocks between block 2 and 5 -- bridged with max_gap_blocks=2
        // Gap of 3 blocks between block 6 and 10 -- NOT bridged (3 > 2)
        let ranges = cache.coalesce_ranges(vec![0, 1, 2, 5, 6, 10], 2);
        assert_eq!(ranges, vec![0..7168, 10240..11264]);
    }

    #[test]
    fn test_coalesce_all_within_gap() {
        let cache = BlockCache::new(1024, 10);
        // All blocks within gap tolerance
        let ranges = cache.coalesce_ranges(vec![0, 1, 2, 5, 6, 10], 10);
        assert_eq!(ranges, vec![0..11264]);
    }

    #[test]
    fn test_insert_and_query() {
        let mut cache = BlockCache::new(1024, 10);
        cache.insert(0, Bytes::from(vec![0u8; 1024]));
        cache.insert(1, Bytes::from(vec![1u8; 1024]));

        let (cached, missing) = cache.query(0, 2048);
        assert_eq!(cached.len(), 2);
        assert!(missing.is_empty());
        assert_eq!(cached[0].0, 0);
        assert_eq!(cached[0].1[0], 0);
        assert_eq!(cached[1].0, 1);
        assert_eq!(cached[1].1[0], 1);

        let (cached, missing) = cache.query(0, 3072);
        assert_eq!(cached.len(), 2);
        assert_eq!(missing, vec![2]);
    }

    /// Covers the fix for a data-loss race: a block reported "cached"
    /// by query() must be returned by value so a subsequent eviction
    /// can't truncate the caller's read.
    #[test]
    fn test_query_survives_eviction() {
        let mut cache = BlockCache::new(1024, 2);
        cache.insert(0, Bytes::from(vec![7u8; 1024]));
        cache.insert(1, Bytes::from(vec![8u8; 1024]));

        let (cached, _missing) = cache.query(0, 1024);
        // Evict block 0 by pushing two fresh blocks
        cache.insert(2, Bytes::from(vec![9u8; 1024]));
        cache.insert(3, Bytes::from(vec![9u8; 1024]));

        // Block 0 is gone from the cache, but our captured clone still
        // holds the data.
        assert!(cache.get(&0).is_none());
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].1[0], 7);
    }

    #[test]
    fn test_extract_range() {
        let cache = BlockCache::new(4, 10);
        // Block 0: [0,1,2,3], Block 1: [4,5,6,7]
        let blocks: Vec<(u64, Bytes)> = vec![
            (0, Bytes::from(vec![0u8, 1, 2, 3])),
            (1, Bytes::from(vec![4u8, 5, 6, 7])),
        ];
        // Read bytes 2..6
        let result = cache.extract_range(2, 4, &blocks);
        assert_eq!(result, vec![2, 3, 4, 5]);
    }

    #[test]
    fn test_coalesce_byte_ranges_merges_with_gap() {
        // gap of 50 bytes between 0..100 and 150..200 → merged at max_gap=100
        let r = coalesce_byte_ranges(vec![0..100, 150..200, 1000..1100], 100);
        assert_eq!(r, vec![0..200, 1000..1100]);
    }

    #[test]
    fn test_coalesce_byte_ranges_handles_overlap_and_sorting() {
        // unsorted + overlapping; max_gap=0 still merges overlaps
        let r = coalesce_byte_ranges(vec![200..300, 50..250, 0..60], 0);
        assert_eq!(r, vec![0..300]);
    }

    #[test]
    fn test_coalesce_byte_ranges_empty() {
        assert!(coalesce_byte_ranges(Vec::new(), 1024).is_empty());
    }

    #[test]
    fn test_len() {
        let mut cache = BlockCache::new(1024, 10);
        assert_eq!(cache.len(), 0);
        cache.insert(0, Bytes::from(vec![0u8; 1024]));
        assert_eq!(cache.len(), 1);
    }
}
