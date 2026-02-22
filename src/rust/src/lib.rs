//! spacelaser: Cloud-optimized partial HDF5 reader for GEDI and ICESat-2 data.
//!
//! This library implements an h5coro-style pure-Rust HDF5 reader that fetches
//! only the bytes needed via HTTP Range requests. No libhdf5 dependency.

pub mod auth;
pub mod filters;
pub mod hdf5;
pub mod io;
pub mod products;

#[cfg(feature = "r-ffi")]
mod ffi;

pub use hdf5::file::Hdf5File;
pub use hdf5::types::Datatype;
pub use io::source::DataSource;
pub use products::gedi::BBox;
