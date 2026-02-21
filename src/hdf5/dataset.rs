use crate::hdf5::btree;
use crate::hdf5::chunk;
use crate::hdf5::object_header::ObjectHeader;
use crate::hdf5::types::*;
use crate::io::reader::Reader;

/// A handle to an HDF5 dataset, with all metadata parsed and ready for reading.
pub struct Dataset {
    pub meta: DatasetMeta,
}

impl Dataset {
    /// Create a Dataset from a parsed object header.
    pub fn from_object_header(oh: &ObjectHeader) -> Result<Self, Hdf5Error> {
        let datatype = oh
            .datatype()
            .cloned()
            .ok_or_else(|| Hdf5Error::InvalidStructure("Dataset missing datatype".into()))?;
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
                let chunks =
                    btree::read_chunk_btree(reader, *btree_address, offset_size, ndims).await?;

                let mut all_data = Vec::new();
                for ci in &chunks {
                    let data = chunk::read_chunk(
                        reader,
                        ci,
                        &self.meta.filters,
                        *elem_sz as usize,
                    )
                    .await?;
                    all_data.push((ci.clone(), data));
                }

                // For a 1D dataset, sort by offset and concatenate
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

        match &self.meta.layout {
            DataLayout::Compact { data } => {
                // Extract requested rows from compact data
                let mut result = Vec::new();
                for &(start, end) in row_ranges {
                    let byte_start = start as usize * element_size;
                    let byte_end = end as usize * element_size;
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
                        element_size,
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
                let all_chunks =
                    btree::read_chunk_btree(reader, *btree_address, offset_size, ndims).await?;

                // Find only the chunks that overlap our row ranges
                let needed_chunks =
                    chunk::chunks_for_row_ranges(&all_chunks, row_ranges, chunk_dims);

                // Read and decompress needed chunks
                let mut chunk_data = Vec::new();
                for ci in needed_chunks {
                    let data = chunk::read_chunk(
                        reader,
                        ci,
                        &self.meta.filters,
                        *elem_sz as usize,
                    )
                    .await?;
                    chunk_data.push((ci.clone(), data));
                }

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
