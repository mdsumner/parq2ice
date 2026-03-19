//! Convert a kerchunk parquet store to an icechunk virtual store.
//!
//! The entry point is [`build_icechunk_store`].
//!
//! # Layout expected on disk
//!
//! ```text
//! <store_root>/
//!   .zmetadata          ← consolidated zarr V2 metadata
//!   <var>/
//!     refs.0.parq       ← chunk references for <var>
//!     refs.1.parq       ← (optional additional shards)
//!     ...
//! ```
//!
//! # Architecture
//!
//! The correct Rust-level API in icechunk 0.3.x is:
//!
//! ```text
//! Session  (writable)
//!   └─ Store::from_session(Arc<RwLock<Session>>)
//!        ├─ store.set("zarr/key", bytes)        ← writes group/.zarray/.zattrs metadata
//!        ├─ store.set_virtual_ref(key, vref, _) ← single chunk ref (by zarr key string)
//!        └─ store.set_virtual_refs(path, _, refs) ← batch refs (preferred)
//!        └─ session.write().await.commit(…)
//! ```
//!
//! `set_virtual_ref` is the Rust API name (not `set_chunk_ref`).
//! Zarr array metadata is written as raw bytes via `store.set()` against the
//! appropriate zarr key (e.g. `"salt/.zarray"`), which keeps us on Zarr V2
//! strings without having to construct internal icechunk format types.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use icechunk::format::manifest::{VirtualChunkLocation, VirtualChunkRef};
use icechunk::format::{ByteRange, ChunkIndices};
use icechunk::store::Store;
use icechunk::{Repository, RepositoryConfig};
use tokio::sync::RwLock;

use crate::error::{ParqIceError, Result};
use crate::kerchunk::read_all_refs;
use crate::zmetadata::{parse_chunk_key, parse_zmetadata};

/// Build an icechunk repository from a kerchunk parquet store.
///
/// `parq_store_root` — directory containing `.zmetadata` and per-variable
///   subdirectories with `refs.*.parq` files.
///
/// `ice_store_path` — directory where the new icechunk repository will be
///   written.  Must not already exist as an icechunk repo.
///
/// Returns the snapshot ID of the initial commit.
pub async fn build_icechunk_store(
    parq_store_root: &Path,
    ice_store_path: &Path,
) -> Result<icechunk::format::SnapshotId> {
    // ── 1. Parse .zmetadata ──────────────────────────────────────────────────
    let metas = parse_zmetadata(parq_store_root)?;

    // ── 2. Create icechunk repository + Store ────────────────────────────────
    let storage = icechunk::new_local_filesystem_storage(ice_store_path).await?;
    let config = RepositoryConfig::default();
    let repo = Repository::create(Some(config), storage, HashMap::new()).await?;

    let session = repo.writable_session("main").await?;
    let session = Arc::new(RwLock::new(session));
    let store = Store::from_session(session.clone()).await;

    // ── 3. Write root group metadata ─────────────────────────────────────────
    // icechunk's Store speaks zarr keys/values directly.
    // A zarr V2 group is signalled by a ".zgroup" key at the relevant path.
    store
        .set(
            "zarr.json",
            Bytes::from(r#"{"zarr_format":3,"node_type":"group"}"#),
        )
        .await
        .map_err(|e| ParqIceError::ZarrPath(e.to_string()))?;

    // ── 4. Write per-variable metadata and virtual chunk refs ─────────────────
    for meta in &metas {
        let var = &meta.var_name;
        let var_dir = parq_store_root.join(var);
        if !var_dir.is_dir() {
            eprintln!("Warning: no refs directory for variable '{var}', skipping");
            continue;
        }

        // Write array zarr.json (V3 metadata translated from .zarray V2).
        // icechunk requires Zarr V3 internally; we produce a minimal V3 blob.
        let zarr_json = v2_zarray_to_v3_zarr_json(&meta.zarray, &meta.zattrs);
        let array_meta_key = format!("{var}/zarr.json");
        store
            .set(
                &array_meta_key,
                Bytes::from(serde_json::to_vec(&zarr_json)?),
            )
            .await
            .map_err(|e| ParqIceError::ZarrPath(e.to_string()))?;

        // Read all chunk refs from parquet files.
        let chunk_refs = read_all_refs(&var_dir)?;

        let array_path = icechunk::format::Path::try_from(format!("/{var}"))
            .map_err(|e| ParqIceError::ZarrPath(e.to_string()))?;

        // Build (ChunkIndices, VirtualChunkRef) pairs.
        let refs: Vec<(ChunkIndices, VirtualChunkRef)> = chunk_refs
            .iter()
            .filter_map(|cr| {
                let indices = parse_chunk_key(&cr.key)?;
                let indices = ChunkIndices(indices.into_iter().map(|v| v as u32).collect());

                let location = VirtualChunkLocation::from_absolute_path(&cr.path)
                    .map_err(|e| {
                        eprintln!("Skipping '{}': bad path — {e}", cr.key);
                    })
                    .ok()?;

                Some((
                    indices,
                    VirtualChunkRef {
                        location,
                        offset: cr.offset,
                        length: cr.length,
                        checksum: None,
                    },
                ))
            })
            .collect();

        // Bulk-set all virtual refs for this array (more efficient than one-by-one).
        store
            .set_virtual_refs(&array_path, false, refs)
            .await
            .map_err(|e| ParqIceError::ZarrPath(e.to_string()))?;
    }

    // ── 5. Commit ─────────────────────────────────────────────────────────────
    let snapshot_id = session
        .write()
        .await
        .commit("Import kerchunk parquet refs", None)
        .await?;

    Ok(snapshot_id)
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Translate a zarr V2 `.zarray` blob + `.zattrs` blob into a zarr V3
/// `zarr.json` object.
///
/// icechunk requires Zarr V3 internally.  We produce a minimal V3 blob that
/// preserves dtype, shape, chunks, fill_value, and compressor information.
/// The codecs list is a best-effort translation of common V2 compressors.
fn v2_zarray_to_v3_zarr_json(
    zarray: &serde_json::Value,
    _zattrs: &serde_json::Value,
) -> serde_json::Value {
    let shape: Vec<u64> = zarray
        .get("shape")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_u64()).collect())
        .unwrap_or_default();

    let chunks: Vec<u64> = zarray
        .get("chunks")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_u64()).collect())
        .unwrap_or_else(|| shape.clone());

    let dtype = zarray
        .get("dtype")
        .and_then(|v| v.as_str())
        .unwrap_or("<f4");

    let fill_value = zarray
        .get("fill_value")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    let data_type = v2_dtype_to_v3(dtype);

    // Translate compressor to a V3 bytes codec.
    let codecs = build_codecs(zarray, dtype);

    serde_json::json!({
        "zarr_format": 3,
        "node_type": "array",
        "shape": shape,
        "data_type": data_type,
        "chunk_grid": {
            "name": "regular",
            "configuration": { "chunk_shape": chunks }
        },
        "chunk_key_encoding": {
            "name": "default",
            "separator": "/"
        },
        "fill_value": fill_value,
        "codecs": codecs,
        "attributes": _zattrs
    })
}

/// Map a zarr V2 dtype string to a zarr V3 data_type string.
fn v2_dtype_to_v3(dtype: &str) -> &'static str {
    match dtype.trim_start_matches(['<', '>', '|', '=']) {
        "f4" => "float32",
        "f8" => "float64",
        "f2" => "float16",
        "i1" => "int8",
        "i2" => "int16",
        "i4" => "int32",
        "i8" => "int64",
        "u1" => "uint8",
        "u2" => "uint16",
        "u4" => "uint32",
        "u8" => "uint64",
        "b1" => "bool",
        _ => "float32",
    }
}

/// Build a zarr V3 codecs list from a V2 `.zarray` compressor and order.
fn build_codecs(zarray: &serde_json::Value, dtype: &str) -> serde_json::Value {
    let order = zarray
        .get("order")
        .and_then(|v| v.as_str())
        .unwrap_or("C");

    let mut codecs = vec![];

    // Transpose codec for Fortran order.
    if order == "F" {
        let ndim = zarray
            .get("shape")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        let order_vec: Vec<usize> = (0..ndim).rev().collect();
        codecs.push(serde_json::json!({
            "name": "transpose",
            "configuration": { "order": order_vec }
        }));
    }

    // bytes codec (endian).
    let endian = if dtype.starts_with('<') {
        "little"
    } else if dtype.starts_with('>') {
        "big"
    } else {
        "little" // native / not-applicable types
    };
    codecs.push(serde_json::json!({
        "name": "bytes",
        "configuration": { "endian": endian }
    }));

    // Compression codec.
    if let Some(compressor) = zarray.get("compressor").filter(|v| !v.is_null()) {
        if let Some(id) = compressor.get("id").and_then(|v| v.as_str()) {
            let codec = match id {
                "blosc" => {
                    let cname = compressor
                        .get("cname")
                        .and_then(|v| v.as_str())
                        .unwrap_or("lz4");
                    let clevel = compressor
                        .get("clevel")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(5);
                    let shuffle = compressor
                        .get("shuffle")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(1);
                    Some(serde_json::json!({
                        "name": "blosc",
                        "configuration": {
                            "cname": cname,
                            "clevel": clevel,
                            "shuffle": shuffle
                        }
                    }))
                }
                "zlib" | "gzip" => {
                    let level = compressor
                        .get("level")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(1);
                    Some(serde_json::json!({
                        "name": "gzip",
                        "configuration": { "level": level }
                    }))
                }
                "zstd" => {
                    let level = compressor
                        .get("level")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(3);
                    Some(serde_json::json!({
                        "name": "zstd",
                        "configuration": { "level": level }
                    }))
                }
                _ => None,
            };
            if let Some(c) = codec {
                codecs.push(c);
            }
        }
    }

    serde_json::Value::Array(codecs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_v2_dtype_to_v3() {
        assert_eq!(v2_dtype_to_v3("<f4"), "float32");
        assert_eq!(v2_dtype_to_v3(">f8"), "float64");
        assert_eq!(v2_dtype_to_v3("<i4"), "int32");
        assert_eq!(v2_dtype_to_v3("|u1"), "uint8");
    }

    #[test]
    fn test_v2_zarray_to_v3_minimal() {
        let zarray = serde_json::json!({
            "shape": [12, 180, 360],
            "chunks": [1, 90, 180],
            "dtype": "<f4",
            "fill_value": 0.0,
            "order": "C",
            "compressor": {"id": "blosc", "cname": "lz4", "clevel": 5, "shuffle": 1}
        });
        let zattrs = serde_json::json!({"units": "PSU"});
        let v3 = v2_zarray_to_v3_zarr_json(&zarray, &zattrs);

        assert_eq!(v3["zarr_format"], 3);
        assert_eq!(v3["data_type"], "float32");
        assert_eq!(v3["shape"], serde_json::json!([12, 180, 360]));
        let codecs = v3["codecs"].as_array().unwrap();
        // C-order: no transpose; bytes + blosc
        assert_eq!(codecs.len(), 2);
        assert_eq!(codecs[0]["name"], "bytes");
        assert_eq!(codecs[1]["name"], "blosc");
    }

    #[test]
    fn test_build_codecs_fortran_order() {
        let zarray = serde_json::json!({
            "shape": [10, 20],
            "order": "F",
            "dtype": "<f4",
            "compressor": null
        });
        let codecs = build_codecs(&zarray, "<f4");
        let arr = codecs.as_array().unwrap();
        assert_eq!(arr[0]["name"], "transpose");
        assert_eq!(arr[1]["name"], "bytes");
    }

    #[test]
    fn test_parse_chunk_key_roundtrip() {
        use crate::zmetadata::parse_chunk_key;
        assert_eq!(parse_chunk_key("0.1.2"), Some(vec![0, 1, 2]));
        assert_eq!(parse_chunk_key("11.0.3"), Some(vec![11, 0, 3]));
    }
}
