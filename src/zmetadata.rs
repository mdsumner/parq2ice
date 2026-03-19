//! Parse the consolidated Zarr V2 metadata from `.zmetadata`.
//!
//! The `.zmetadata` file contains a JSON object with a `metadata` key whose
//! value is a flat map of `"<path>/.zarray"` and `"<path>/.zattrs"` entries.
//!
//! Example structure:
//! ```json
//! {
//!   "metadata": {
//!     ".zattrs": {...},
//!     ".zgroup": {"zarr_format": 2},
//!     "salt/.zattrs": {...},
//!     "salt/.zarray": { "shape": [...], "chunks": [...], "dtype": "..." }
//!   }
//! }
//! ```

use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

use crate::error::{ParqIceError, Result};

/// The array metadata we care about for building the icechunk store.
#[derive(Debug, Clone)]
pub struct ZarrArrayMeta {
    pub var_name: String,
    /// Raw `.zarray` JSON blob — passed through to icechunk as-is.
    pub zarray: Value,
    /// Raw `.zattrs` JSON blob (may be empty object).
    pub zattrs: Value,
}

/// Top-level structure of `.zmetadata`.
#[derive(Debug, Deserialize)]
struct ZMetadata {
    metadata: HashMap<String, Value>,
}

/// Parse `.zmetadata` from `store_root/.zmetadata` and return one
/// [`ZarrArrayMeta`] per variable found.
pub fn parse_zmetadata(store_root: &Path) -> Result<Vec<ZarrArrayMeta>> {
    let path = store_root.join(".zmetadata");
    let text = std::fs::read_to_string(&path)?;
    let zm: ZMetadata = serde_json::from_str(&text)?;

    let mut arrays: HashMap<String, ZarrArrayMeta> = HashMap::new();

    for (key, val) in &zm.metadata {
        if let Some(var) = key.strip_suffix("/.zarray") {
            let entry = arrays.entry(var.to_string()).or_insert_with(|| ZarrArrayMeta {
                var_name: var.to_string(),
                zarray: Value::Null,
                zattrs: Value::Object(Default::default()),
            });
            entry.zarray = val.clone();
        } else if let Some(var) = key.strip_suffix("/.zattrs") {
            let entry = arrays.entry(var.to_string()).or_insert_with(|| ZarrArrayMeta {
                var_name: var.to_string(),
                zarray: Value::Null,
                zattrs: Value::Object(Default::default()),
            });
            entry.zattrs = val.clone();
        }
    }

    // Discard incomplete entries (no .zarray = not an array).
    let mut result: Vec<ZarrArrayMeta> = arrays
        .into_values()
        .filter(|a| !a.zarray.is_null())
        .collect();
    result.sort_by(|a, b| a.var_name.cmp(&b.var_name));

    Ok(result)
}

/// Parse the chunk grid from `.zarray` — returns chunk shape as Vec<u64>.
pub fn chunk_shape(zarray: &Value) -> Option<Vec<u64>> {
    zarray
        .get("chunks")?
        .as_array()?
        .iter()
        .map(|v| v.as_u64())
        .collect()
}

/// Parse array shape from `.zarray`.
pub fn array_shape(zarray: &Value) -> Option<Vec<u64>> {
    zarray
        .get("shape")?
        .as_array()?
        .iter()
        .map(|v| v.as_u64())
        .collect()
}

/// Convert a zarr V2 dtype string to something icechunk/zarr can use.
/// Returns the dtype string unchanged — icechunk accepts zarr V2 dtype strings
/// when the array metadata is set via the zarr key-value store interface.
pub fn dtype_str(zarray: &Value) -> Option<&str> {
    zarray.get("dtype")?.as_str()
}

/// Parse a chunk key string like "0.1.2" into a Vec<u64> of chunk indices.
///
/// Zarr V2 uses `.` as the separator; zarr V3 uses `/` — kerchunk
/// produced from V2 files uses `.`.
pub fn parse_chunk_key(key: &str) -> Option<Vec<u64>> {
    key.split('.')
        .map(|s| s.parse::<u64>().ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture_store() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("ocean_salt_2023.parq")
    }

    #[test]
    fn test_parse_zmetadata_finds_arrays() {
        let metas = parse_zmetadata(&fixture_store()).expect("parse should succeed");
        assert!(!metas.is_empty(), "should find at least one array variable");
    }

    #[test]
    fn test_parse_zmetadata_salt_present() {
        let metas = parse_zmetadata(&fixture_store()).unwrap();
        let salt = metas.iter().find(|m| m.var_name == "salt");
        assert!(salt.is_some(), "salt variable should be present");
        let salt = salt.unwrap();
        assert!(!salt.zarray.is_null(), ".zarray should be populated");
    }

    #[test]
    fn test_chunk_shape_parses() {
        let metas = parse_zmetadata(&fixture_store()).unwrap();
        let salt = metas.iter().find(|m| m.var_name == "salt").unwrap();
        let cs = chunk_shape(&salt.zarray);
        assert!(cs.is_some(), "chunk_shape should parse");
        assert!(!cs.unwrap().is_empty());
    }

    #[test]
    fn test_parse_chunk_key() {
        assert_eq!(parse_chunk_key("0.1.2"), Some(vec![0, 1, 2]));
        assert_eq!(parse_chunk_key("0"), Some(vec![0]));
        assert_eq!(parse_chunk_key("bad.key.X"), None);
    }
}
