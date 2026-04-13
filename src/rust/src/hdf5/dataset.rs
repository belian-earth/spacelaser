use crate::hdf5::btree;
use crate::hdf5::chunk;
use crate::hdf5::object_header::ObjectHeader;
use crate::hdf5::types::*;
use crate::io::reader::Reader;
use futures::future::try_join_all;

/// A handle to an HDF5 dataset, with all metadata parsed and ready for reading.
pub struct Dataset {
    pub meta: DatasetMeta,
}

impl Dataset {
    /// Create a Dataset from a parsed object header.
    pub fn from_object_header(oh: &ObjectHeader) -> Result<Self, Hdf5Error> {
        let datatype = oh.datatype().cloned().ok_or_else(|| {
            let msg_types: Vec<String> = oh
                .messages
                .iter()
                .map(|m| match m {
                    crate::hdf5::object_header::HeaderMessage::Dataspace(_) => "Dataspace".into(),
                    crate::hdf5::object_header::HeaderMessage::Datatype(_) => "Datatype".into(),
                    crate::hdf5::object_header::HeaderMessage::FillValue(_) => "FillValue".into(),
                    crate::hdf5::object_header::HeaderMessage::DataLayout(_) => "DataLayout".into(),
                    crate::hdf5::object_header::HeaderMessage::FilterPipeline(_) => {
                        "FilterPipeline".into()
                    }
                    crate::hdf5::object_header::HeaderMessage::SymbolTable { .. } => {
                        "SymbolTable".into()
                    }
                    crate::hdf5::object_header::HeaderMessage::Link { .. } => "Link".into(),
                    crate::hdf5::object_header::HeaderMessage::LinkInfo { .. } => "LinkInfo".into(),
                    crate::hdf5::object_header::HeaderMessage::Unknown { msg_type } => {
                        format!("Unknown(0x{:04x})", msg_type)
                    }
                })
                .collect();
            Hdf5Error::InvalidStructure(format!(
                "Dataset missing datatype at 0x{:x} (found {} messages: [{}], prefix={:02x?})",
                oh.debug_address,
                oh.messages.len(),
                msg_types.join(", "),
                &oh.debug_prefix[..16.min(oh.debug_prefix.len())]
            ))
        })?;
        let dataspace = oh
            .dataspace()
            .cloned()
            .ok_or_else(|| Hdf5Error::InvalidStructure("Dataset missing dataspace".into()))?;
        let layout = oh
            .layout()
            .cloned()
            .ok_or_else(|| Hdf5Error::InvalidStructure("Dataset missing layout".into()))?;
        let filters = oh.filters();
        let fill_value = oh.find(|m| match m {
            crate::hdf5::object_header::HeaderMessage::FillValue(fv) => fv.clone(),
            _ => None,
        });

        Ok(Dataset {
            meta: DatasetMeta {
                datatype,
                dataspace,
                layout,
                filters,
                fill_value,
            },
        })
    }

    /// Read the entire dataset.
    pub async fn read_all(&self, reader: &Reader, offset_size: u8) -> Result<Vec<u8>, Hdf5Error> {
        let element_size = self.meta.datatype.size();

        match &self.meta.layout {
            DataLayout::Compact { data } => Ok(data.clone()),

            DataLayout::Contiguous { address, size } => {
                chunk::read_contiguous(reader, *address, *size, None, element_size).await
            }

            DataLayout::Chunked {
                btree_address,
                chunk_dims,
                element_size: elem_sz,
            } => {
                let ndims = chunk_dims.len();
                let chunk_row_dim = if ndims > 0 { chunk_dims[0] as u64 } else { 1 };
                let chunks =
                    btree::read_chunk_btree(reader, *btree_address, offset_size, ndims, None, chunk_row_dim).await?;

                // Read all chunks concurrently
                let filters = &self.meta.filters;
                let elem_sz_val = *elem_sz as usize;
                let chunk_futs: Vec<_> = chunks.iter().map(|ci| async move {
                    let data = chunk::read_chunk(reader, ci, filters, elem_sz_val).await?;
                    Ok::<_, Hdf5Error>((ci.clone(), data))
                }).collect();
                let all_data = try_join_all(chunk_futs).await?;

                // Sort by offset and concatenate
                let mut all_data_sorted = all_data;
                all_data_sorted.sort_by_key(|(ci, _)| ci.offsets.first().copied().unwrap_or(0));

                let total_elements = self.meta.dataspace.num_elements() as usize;
                let total_bytes = total_elements * element_size;
                let mut result = vec![0u8; total_bytes];

                let row_size = if ndims > 1 {
                    chunk_dims[1..].iter().map(|d| *d as usize).product::<usize>()
                        * (*elem_sz as usize)
                } else {
                    element_size
                };

                for (ci, data) in &all_data_sorted {
                    let start_row = ci.offsets.first().copied().unwrap_or(0) as usize;
                    let dst_offset = start_row * row_size;
                    let copy_len = data.len().min(result.len().saturating_sub(dst_offset));
                    if copy_len > 0 {
                        result[dst_offset..dst_offset + copy_len]
                            .copy_from_slice(&data[..copy_len]);
                    }
                }

                Ok(result)
            }
        }
    }

    /// Bytes per row, accounting for multi-dimensional datasets.
    ///
    /// For a 1-D dataset this equals the scalar element size.
    /// For a 2-D dataset like `rh` `[N, 101]` of f32, this is `101 × 4 = 404`.
    pub fn row_size(&self) -> usize {
        let element_size = self.meta.datatype.size();
        let dims = &self.meta.dataspace.dims;
        if dims.len() > 1 {
            dims[1..].iter().product::<u64>() as usize * element_size
        } else {
            element_size
        }
    }

    /// Read specific row ranges from a dataset.
    ///
    /// This is the key method for spatial subsetting: after determining which
    /// rows match a spatial query, read only those rows from each dataset.
    pub async fn read_rows(
        &self,
        reader: &Reader,
        offset_size: u8,
        row_ranges: &[(u64, u64)],
    ) -> Result<Vec<u8>, Hdf5Error> {
        let element_size = self.meta.datatype.size();
        let row_size = self.row_size();

        match &self.meta.layout {
            DataLayout::Compact { data } => {
                let mut result = Vec::new();
                for &(start, end) in row_ranges {
                    let byte_start = start as usize * row_size;
                    let byte_end = end as usize * row_size;
                    if byte_end <= data.len() {
                        result.extend_from_slice(&data[byte_start..byte_end]);
                    }
                }
                Ok(result)
            }

            DataLayout::Contiguous { address, size } => {
                let mut result = Vec::new();
                for &(start, end) in row_ranges {
                    let data = chunk::read_contiguous(
                        reader,
                        *address,
                        *size,
                        Some((start, end)),
                        row_size,
                    )
                    .await?;
                    result.extend(data);
                }
                Ok(result)
            }

            DataLayout::Chunked {
                btree_address,
                chunk_dims,
                element_size: elem_sz,
            } => {
                let ndims = chunk_dims.len();
                let chunk_row_dim = if ndims > 0 { chunk_dims[0] as u64 } else { 1 };
                // Targeted B-tree navigation: only for 1D datasets where
                // dim 0 IS the row dimension. For 2D datasets (e.g. rh
                // [N,101], surface_type [5,N]), the first B-tree key
                // dimension does not correspond to our row ranges and
                // pruning would incorrectly exclude chunks. 2D datasets
                // have small B-trees anyway so full scan is fine.
                //
                // Additionally, only apply when the dataset is large
                // enough to benefit. Small datasets (< 1M elements) have
                // B-trees that fit in a few nodes — full scan is fast
                // and avoids edge cases in targeted navigation.
                let total_elements = self.meta.dataspace.num_elements();
                let row_bounds = if ndims == 1 && total_elements > 1_000_000 && !row_ranges.is_empty() {
                    let min_row = row_ranges.iter().map(|(s, _)| *s).min().unwrap();
                    let max_row = row_ranges.iter().map(|(_, e)| *e).max().unwrap();
                    Some((min_row, max_row))
                } else {
                    None
                };
                let all_chunks =
                    btree::read_chunk_btree(reader, *btree_address, offset_size, ndims, row_bounds, chunk_row_dim).await?;

                // Find only the chunks that overlap our row ranges
                let needed_chunks =
                    chunk::chunks_for_row_ranges(&all_chunks, row_ranges, chunk_dims);

                // Read and decompress needed chunks concurrently
                let filters = &self.meta.filters;
                let elem_sz_val = *elem_sz as usize;
                let chunk_futs: Vec<_> = needed_chunks.into_iter().map(|ci| async move {
                    let data = chunk::read_chunk(reader, ci, filters, elem_sz_val).await?;
                    Ok::<_, Hdf5Error>((ci.clone(), data))
                }).collect();
                let chunk_data = try_join_all(chunk_futs).await?;

                // Extract the specific rows
                Ok(chunk::extract_rows_from_chunks(
                    &chunk_data,
                    row_ranges,
                    chunk_dims,
                    element_size,
                    ndims,
                ))
            }
        }
    }

    /// Get the total number of elements in this dataset.
    pub fn num_elements(&self) -> u64 {
        self.meta.dataspace.num_elements()
    }

    /// Get the shape (dimensions) of this dataset.
    pub fn shape(&self) -> &[u64] {
        &self.meta.dataspace.dims
    }

    /// Get the element size in bytes.
    pub fn element_size(&self) -> usize {
        self.meta.datatype.size()
    }
}
