use thiserror::Error;

#[derive(Error, Debug)]
pub enum Hdf5Error {
    #[error("I/O error: {0}")]
    Io(#[from] crate::io::reader::IoError),

    #[error("Invalid HDF5 signature at offset {0}")]
    InvalidSignature(u64),

    #[error("Unsupported superblock version: {0}")]
    UnsupportedSuperblockVersion(u8),

    #[error("Unsupported B-tree version: {0}")]
    UnsupportedBtreeVersion(u8),

    #[error("Unsupported object header version: {0}")]
    UnsupportedObjectHeaderVersion(u8),

    #[error("Unsupported datatype class: {0}")]
    UnsupportedDatatypeClass(u8),

    #[error("Unsupported data layout class: {0}")]
    UnsupportedLayoutClass(u8),

    #[error("Unsupported filter: {0}")]
    UnsupportedFilter(u16),

    #[error("Dataset not found: {0}")]
    DatasetNotFound(String),

    #[error("Group not found: {0}")]
    GroupNotFound(String),

    #[error("Path not found: {0}")]
    PathNotFound(String),

    #[error(
        "Soft link encountered at '{0}' but transparent soft-link resolution \
         is not yet implemented. Use the direct HDF5 path that the link \
         points to (for GEDI L2B, `geolocation/<name>` instead of `<name>`)."
    )]
    SoftLinkNotSupported(String),

    #[error("Invalid HDF5 structure: {0}")]
    InvalidStructure(String),

    #[error("Decompression error: {0}")]
    Decompression(String),

    #[error("Unsupported local heap version: {0}")]
    UnsupportedLocalHeapVersion(u8),
}

/// HDF5 datatype information.
#[derive(Debug, Clone)]
pub enum Datatype {
    FixedPoint {
        size: usize,
        signed: bool,
        byte_order: ByteOrder,
    },
    FloatingPoint {
        size: usize,
        byte_order: ByteOrder,
    },
    String {
        size: usize,
    },
    /// Variable-length sequence type.
    VariableLength {
        base_type: Box<Datatype>,
    },
}

impl Datatype {
    /// Size of one element in bytes.
    pub fn size(&self) -> usize {
        match self {
            Datatype::FixedPoint { size, .. } => *size,
            Datatype::FloatingPoint { size, .. } => *size,
            Datatype::String { size } => *size,
            Datatype::VariableLength { .. } => 16, // hvl_t: pointer + length
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ByteOrder {
    LittleEndian,
    BigEndian,
}

/// Describes how a dataset's raw data is stored.
#[derive(Debug, Clone)]
pub enum DataLayout {
    /// Data stored in a single contiguous block.
    Contiguous {
        address: u64,
        size: u64,
    },
    /// Data divided into independently stored chunks.
    Chunked {
        /// Address of the B-tree for chunk indexing.
        btree_address: u64,
        /// Dimensions of each chunk (in elements).
        chunk_dims: Vec<u32>,
        /// Size of each element in bytes.
        element_size: u32,
    },
    /// Data stored directly in the object header (small datasets).
    Compact {
        data: Vec<u8>,
    },
}

/// Filter in the data pipeline (e.g., deflate, shuffle).
#[derive(Debug, Clone)]
pub struct Filter {
    pub id: u16,
    pub name: Option<String>,
    pub client_data: Vec<u32>,
}

/// Well-known filter IDs.
pub const FILTER_DEFLATE: u16 = 1;
pub const FILTER_SHUFFLE: u16 = 2;
pub const FILTER_FLETCHER32: u16 = 3;
pub const FILTER_SZIP: u16 = 4;

/// Dataspace: describes the dimensions of a dataset.
#[derive(Debug, Clone)]
pub struct Dataspace {
    pub rank: u8,
    pub dims: Vec<u64>,
    pub max_dims: Option<Vec<u64>>,
}

impl Dataspace {
    /// Total number of elements in the dataspace.
    pub fn num_elements(&self) -> u64 {
        self.dims.iter().product()
    }
}

/// A parsed dataset's metadata -- everything needed to read its data.
#[derive(Debug, Clone)]
pub struct DatasetMeta {
    pub datatype: Datatype,
    pub dataspace: Dataspace,
    pub layout: DataLayout,
    pub filters: Vec<Filter>,
    pub fill_value: Option<Vec<u8>>,
}
