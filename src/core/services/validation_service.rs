use async_trait::async_trait;
use thiserror::Error;

use crate::{file_entry::FileEntry, rd_index::RDIndex};

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Hash verification failed")]
    HashMismatch,
}

#[async_trait]
pub trait ValidationService: Send + Sync {
    /// Will validate a given file with its `FileEntry`
    ///
    /// # Params
    /// - `entry`
    /// - `path`: path of the file to validate
    ///
    async fn validate_file(&self, entry: &FileEntry, path: &str) -> Result<bool, ValidationError>;

    /// Will validate all files of a `RDIndex`
    ///
    /// # Params
    /// - `index`: rd-index
    /// - `base_path`: directory of the files
    ///
    async fn validate_index(
        &self,
        index: &RDIndex,
        base_path: &str,
    ) -> Result<bool, ValidationError>;
}
