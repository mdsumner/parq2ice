# parq2ice

Convert a kerchunk-style parquet virtual reference store to an
[icechunk](https://icechunk.io) transactional Zarr store — an experiment in Rust. 

See [hypertidy/zaro](https://github.com/hypertidy/zaro) for an alternative in R+Arrow, or [virtualized](https://github.com/mdsumner/virtualized?tab=readme-ov-file#example) for some examples using xarray, or GDAL (multidim + vsikerchunk) the traditional ways. 

## tldr

> (16.7M chunks, 89-byte URLs) genuinely exceeds icechunk 0.3.24's flatbuffers limit regardless of session/commit strategy. 

for testing 

```
# Delete the previous test store first
rm -rf target/test-icechunk

# Run integration tests in release mode with output visible
cargo test --release --test integration_test -- --nocapture

Compiling parq2ice v0.1.0 parq2ice)
    Finished `release` profile [optimized] target(s) in 9.34s
     Running tests/integration_test.rs (target/release/deps/integration_test-ab2f29883ac8c41b)

running 3 tests
Building icechunk store at: parq2ice/target/test-icechunk/ocean_salt_2023
Time: 0 chunk refs
Time_bnds: 5479 chunk refs
Time_bnds batch 1/1: committed EDSAG05CHJH9AMB529C0
average_DT: 5479 chunk refs
average_DT batch 1/1: committed WDJ1YSF00QZJFJT8RVTG
average_T1: 5479 chunk refs
average_T1 batch 1/1: committed J9FXS0E4FNF4Y7YN8WV0
average_T2: 5479 chunk refs
average_T2 batch 1/1: committed YPJQ4XEVFCD1C5W856M0
nv: 0 chunk refs
salt: 16765740 chunk refs
salt batch 1/17: committed 76XED0BNJ3YCWG2RBGEG
salt batch 2/17: committed XQE7H4R2AW4JT5TNB4G0
salt batch 3/17: committed Q276WBVKY1D30RTT8W60
test test_all_vars_imported has been running for over 60 seconds
test test_build_icechunk_store_creates_repository has been running for over 60 seconds
test test_virtual_refs_are_stored has been running for over 60 seconds
salt batch 4/17: committed 5QC03NDGZS81WZGAE3Z0
salt batch 5/17: committed TSKPHWRH7VGBF6742X60
salt batch 6/17: committed FF2X4N6Z5Z2W9C3K9DXG
salt batch 7/17: committed PFYKDVN0VV1BYX9GZBM0
salt batch 8/17: committed NY1V7ZF3GWFPKDY99AB0
salt batch 9/17: committed 7JW0ZHX6X96KN9TH2ZT0
salt batch 10/17: committed 3J4DGCRRBVD5AADN9R90
salt batch 11/17: committed QBEQGSBZ8JDPH0RCH9XG
salt batch 12/17: committed E2Y1KAMZSJWFS6J56VWG
salt batch 13/17: committed 59XJCW28WHA1ERZB05M0
salt batch 14/17: committed 607ES92RB6Q2XPZE0TSG

thread 'test_build_icechunk_store_creates_repository' (1537393) panicked at ~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/flatbuffers-25.12.19/src/endian_scalar.rs:163:19:
range start index 18446744071562067984 out of range for slice of length 2147483988
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test test_build_icechunk_store_creates_repository ... FAILED

```

## What this does

Kerchunk parquet stores describe large archival datasets (NetCDF, HDF5, etc.)
as a collection of chunk references: for each Zarr chunk, a `(path, offset,
size)` triple pointing into the original file. The metadata is tiny — a 200 MB
parquet store can index 16.7 million chunks pointing at petabyte-scale data.

This crate reads that parquet store and writes the references into an icechunk
repository, so the dataset can be accessed via the icechunk transactional store
interface (versioning, branching, atomic updates).

## Layout expected on disk

```
<store_root>/
  .zmetadata              ← consolidated Zarr V2 metadata (kerchunk format)
  <var>/
    refs.0.parq           ← chunk references for <var>
    refs.1.parq           ← (optional additional shards)
    ...
```

## Parquet schema

The actual schema produced by kerchunk / VirtualiZarr (no `key` column):

| column | type  | notes                                      |
|--------|-------|--------------------------------------------|
| path   | UTF8  | URI to source file (https://, s3://, ...) |
| offset | INT64 | byte offset into source file               |
| size   | INT64 | byte length (`length` in VirtualiZarr)     |
| raw    | BYTES | nullable; non-null rows are skipped        |

Chunk keys are **implicit**: row 0 across the sorted parquet files is flat
C-order chunk index 0, row 1 is index 1, and so on. This crate converts flat
indices to ND chunk indices using the array's chunk grid from `.zmetadata`.

## Usage

```rust
use parq2ice::convert::build_icechunk_store;
use std::path::Path;

let snapshot = build_icechunk_store(
    Path::new("../virtualized/ocean_salt_2023.parq"),
    Path::new("/tmp/ocean_salt_2023.icechunk"),
).await?;
```

The function commits one snapshot per variable so progress is visible and
memory stays bounded.

## Crate structure

| module        | purpose                                               |
|---------------|-------------------------------------------------------|
| `kerchunk`    | Read parquet ref files; derive flat chunk indices     |
| `zmetadata`   | Parse `.zmetadata`; translate Zarr V2 → V3 metadata  |
| `convert`     | Build icechunk store: metadata + virtual refs         |
| `error`       | `ParqIceError` enum                                   |

## Key implementation notes

**No `key` column.** Kerchunk parquet stores do not store chunk keys
explicitly. Keys are row positions across lexicographically sorted parquet
files, mapped to ND indices via `flat_to_nd()`.

**Zarr V2 → V3 translation.** Icechunk requires Zarr V3 internally.
`.zarray` metadata (dtype, shape, chunks, compressor/filters) is translated
to a `zarr.json` V3 blob and written via `Store::set()`. The codec translation
covers blosc, gzip/zlib, zstd, and C/Fortran order.

**NaN in `.zmetadata`.** Python's zarr/numpy writes bare `NaN`, `Infinity`,
`-Infinity` as JSON values, which are not valid JSON. The `sanitise_nan()`
function quotes them before parsing so `serde_json` can handle the file.

**URL handling.** Source paths are HTTPS URLs in this store
(`https://thredds.nci.org.au/...`). Bare absolute paths are converted to
`file://` URIs. Both are passed to `VirtualChunkLocation::from_absolute_path`.

**One commit per variable.** Icechunk serialises manifests using flatbuffers,
which has a ~2 GB in-memory buffer limit. The `salt` array alone has 16.7
million chunks (~800 MB uncompressed manifest), which overflows a single
transaction. Committing one variable at a time keeps each manifest within
bounds. This is a known icechunk limitation for bulk virtual reference import
at this scale; upstream manifest splitting (`rewrite_manifests`) is the
longer-term solution.

## On scale and format choice

The kerchunk parquet store for BRAN2023 ocean salt is **200 MB** for 16.7
million chunk references. Parquet handles this efficiently: the `path` column
compresses almost to nothing (one repeated URL), and row group statistics
enable spatial predicate pushdown. For a static archive read path, the
parquet store plus fsspec/VirtualiZarr is already an excellent format.

Icechunk adds value when you need:
- Incremental appends (new daily fields as they are produced)
- Coordinated writes from multiple processes
- Versioning, branching, and time travel over the collection

For a static archive, this crate provides the migration path into icechunk if
those semantics become useful later.

## Running tests

```sh
# Unit tests only (fast, ~40s for kerchunk tests against real parquet):
cargo test --lib

# Integration tests (slow on first run — builds the icechunk store):
cargo test --test integration_test -- --nocapture

# The icechunk store is written to target/test-icechunk/ocean_salt_2023/
# and reused on subsequent runs. To rebuild from scratch:
rm -rf target/test-icechunk && cargo test --test integration_test -- --nocapture
```

## Dependencies

| crate        | why                                              |
|--------------|--------------------------------------------------|
| `icechunk`   | Transactional Zarr store                         |
| `parquet`    | Read kerchunk parquet ref files                  |
| `serde_json` | Parse `.zmetadata`                               |
| `bytes`      | `Bytes` for icechunk Store::set()                |
| `tokio`      | Async runtime (required by icechunk)             |
| `thiserror`  | Error type                                       |
| `walkdir`    | Enumerate variable subdirectories                |
| `tempfile`   | Temporary stores in tests                        |

The `parquet` crate is pinned to `"54"` to share a compatible `zstd-sys`
version with icechunk (parquet 53 requires `zstd-sys < 2.0.14` which
conflicts with icechunk's `zstd-sys 2.0.16`).
