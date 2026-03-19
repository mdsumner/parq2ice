//! Read kerchunk-style parquet reference files.
//!
//! Schema produced by kerchunk / VirtualiZarr parquet stores:
//!   key     : UTF8   – zarr chunk key, e.g. "0.1.2"
//!   path    : UTF8   – URI/path to the source file
//!   offset  : INT64  – byte offset into source file
//!   length  : INT64  – byte length of the chunk  (column is called "length" in VirtualiZarr,
//!                      "size" in older kerchunk — we try both)
//!
//! Each variable's refs are split across one or more parquet files named
//! `refs.0.parq`, `refs.1.parq`, … under `<store>/<var>/`.

use std::path::{Path, PathBuf};

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::reader::RowIter;
use parquet::record::Row;

use crate::error::{ParqIceError, Result};

/// A single resolved chunk reference (path + byte range).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRef {
    /// Zarr chunk key, e.g. "0.1.2" (period-separated chunk indices).
    pub key: String,
    /// Absolute path or URI to the source file that contains the chunk bytes.
    pub path: String,
    /// Byte offset within the source file.
    pub offset: u64,
    /// Byte length of the chunk.
    pub length: u64,
}

/// Collect all `refs.*.parq` files under `var_dir` in lexicographic order.
pub fn ref_parquet_files(var_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(var_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("refs.") && (n.ends_with(".parq") || n.ends_with(".parquet")))
                .unwrap_or(false)
        })
        .collect();
    files.sort();
    Ok(files)
}

/// Read all chunk refs from a single parquet file.
///
/// Handles both the older kerchunk column name `size` and the VirtualiZarr
/// name `length` for the byte-count column.
pub fn read_refs_parquet(path: &Path) -> Result<Vec<ChunkRef>> {
    let file = std::fs::File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let schema = reader.metadata().file_metadata().schema_descr();

    // Detect which column name is used for byte length.
    let length_col_name = detect_length_column(schema)?;

    let mut refs = Vec::new();

    // Row-level iteration via parquet's record API — simple and allocation-cheap
    // at this scale (reference tables are metadata, not bulk data).
    let row_iter: RowIter = reader.get_row_iter(None)?;

    for (row_idx, row_result) in row_iter.enumerate() {
        let row: Row = row_result?;
        let cr = parse_row(&row, row_idx, &length_col_name)?;
        refs.push(cr);
    }

    Ok(refs)
}

/// Read all chunk refs for a variable directory (across all ref parquet files).
pub fn read_all_refs(var_dir: &Path) -> Result<Vec<ChunkRef>> {
    let files = ref_parquet_files(var_dir)?;
    let mut all = Vec::new();
    for f in &files {
        let mut part = read_refs_parquet(f)?;
        all.append(&mut part);
    }
    Ok(all)
}

// ── internal helpers ──────────────────────────────────────────────────────────

fn detect_length_column(
    schema: &parquet::schema::types::SchemaDescriptor,
) -> Result<String> {
    for i in 0..schema.num_columns() {
        let col = schema.column(i);
        let name = col.name();
        if name == "length" || name == "size" {
            return Ok(name.to_string());
        }
    }
    Err(ParqIceError::MissingColumn(
        "length or size".to_string(),
    ))
}

fn get_string_field<'a>(row: &'a Row, field: &str, row_idx: usize) -> Result<String> {
    use parquet::record::Field;
    for (name, field_val) in row.get_column_iter() {
        if name == field {
            return match field_val {
                Field::Str(s) => Ok(s.clone()),
                Field::Bytes(b) => Ok(String::from_utf8_lossy(b.data()).into_owned()),
                Field::Null => Err(ParqIceError::NullInRequiredColumn(
                    field.to_string(),
                    row_idx,
                )),
                other => Ok(format!("{other:?}")),
            };
        }
    }
    Err(ParqIceError::MissingColumn(field.to_string()))
}

fn get_i64_field(row: &Row, field: &str, row_idx: usize) -> Result<i64> {
    use parquet::record::Field;
    for (name, field_val) in row.get_column_iter() {
        if name == field {
            return match field_val {
                Field::Long(v) => Ok(*v),
                Field::Int(v) => Ok(*v as i64),
                Field::Null => Err(ParqIceError::NullInRequiredColumn(
                    field.to_string(),
                    row_idx,
                )),
                other => Err(ParqIceError::MissingColumn(format!(
                    "{field}: unexpected type {other:?}"
                ))),
            };
        }
    }
    Err(ParqIceError::MissingColumn(field.to_string()))
}

fn parse_row(row: &Row, row_idx: usize, length_col: &str) -> Result<ChunkRef> {
    let key = get_string_field(row, "key", row_idx)?;
    let path = get_string_field(row, "path", row_idx)?;
    let offset = get_i64_field(row, "offset", row_idx)? as u64;
    let length = get_i64_field(row, length_col, row_idx)? as u64;

    Ok(ChunkRef {
        key,
        path,
        offset,
        length,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Returns path to `tests/fixtures/<name>` relative to the crate root.
    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join(name)
    }

    #[test]
    fn test_ref_parquet_files_lists_correctly() {
        // The fixture dir has refs.0.parq and refs.1.parq.
        let dir = fixture("salt_var");
        let files = ref_parquet_files(&dir).expect("should list ref files");
        assert_eq!(files.len(), 2, "expected 2 ref parquet files");
        assert!(files[0].file_name().unwrap().to_str().unwrap().starts_with("refs.0"));
        assert!(files[1].file_name().unwrap().to_str().unwrap().starts_with("refs.1"));
    }

    #[test]
    fn test_read_refs_parquet_basic() {
        let path = fixture("salt_var/refs.0.parq");
        let refs = read_refs_parquet(&path).expect("should read parquet refs");
        assert!(!refs.is_empty(), "expected at least one ref");
        let r = &refs[0];
        assert!(!r.key.is_empty());
        assert!(!r.path.is_empty());
        assert!(r.length > 0, "chunk length must be positive");
    }

    #[test]
    fn test_read_all_refs_concatenates() {
        let dir = fixture("salt_var");
        let all = read_all_refs(&dir).expect("should read all refs");
        // Total must be at least what's in refs.0.parq alone.
        let part = read_refs_parquet(&dir.join("refs.0.parq")).unwrap();
        assert!(all.len() >= part.len());
    }
}
