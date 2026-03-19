use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParqIceError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Wraps icechunk `SessionError` (which is a type alias for a boxed error).
    /// We convert to String at the call site to avoid the orphan rule.
    #[error("Icechunk session error: {0}")]
    Session(String),

    #[error("Missing column '{0}' in parquet refs file")]
    MissingColumn(String),

    #[error("Unexpected null in required column '{0}' row {1}")]
    NullInRequiredColumn(String, usize),

    #[error("Bad zarr path / icechunk error: {0}")]
    ZarrPath(String),
}

impl From<icechunk::session::SessionError> for ParqIceError {
    fn from(e: icechunk::session::SessionError) -> Self {
        ParqIceError::Session(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, ParqIceError>;
