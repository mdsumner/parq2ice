//! `parq2ice` — convert kerchunk parquet reference stores to icechunk virtual stores.
//!
//! # Usage
//!
//! ```rust,no_run
//! # tokio_test::block_on(async {
//! use parq2ice::convert::build_icechunk_store;
//! use std::path::Path;
//!
//! let snapshot = build_icechunk_store(
//!     Path::new("../virtualized/ocean_salt_2023.parq"),
//!     Path::new("/tmp/ocean_salt_2023.icechunk"),
//! ).await.unwrap();
//!
//! println!("Created icechunk store, snapshot: {snapshot:?}");
//! # });
//! ```

pub mod convert;
pub mod error;
pub mod kerchunk;
pub mod zmetadata;
