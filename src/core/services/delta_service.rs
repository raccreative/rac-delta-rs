use async_trait::async_trait;
use std::{path::Path, time::SystemTimeError};
use thiserror::Error;

use crate::{HasherError, delta_plan::DeltaPlan, file_entry::FileEntry, rd_index::RDIndex};

#[derive(Error, Debug)]
pub enum DeltaError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Hashing failed: {0}")]
    HashError(#[from] HasherError),

    #[error("System time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),

    #[error("Index creation failed: {0}")]
    IndexError(String),

    #[error("Delta comparison failed")]
    CompareError,
}

#[async_trait]
pub trait AsyncChunkStream: Send + Sync {
    async fn next_chunk(&mut self) -> Option<Vec<u8>>;

    async fn reset(&mut self) {}

    async fn close(&mut self) {}
}

#[async_trait]
pub trait DeltaService: Send + Sync {
    ///
    /// Creates an RDIndex for a given directory.
    ///
    /// This process scans all files recursively, splits them into chunks,
    /// hashes each chunk (using HasherService.hashStream), and builds
    /// the rd-index.json structure.
    ///
    /// # Params
    /// - `rootPath` the path of the directory
    /// - `chunkSize` The size (in bytes) chunks will have, recommended is 1024.
    ///
    async fn create_index_from_directory(
        &self,
        root_path: &Path,
        chunk_size: u64,
        concurrency: Option<usize>,
        ignore_patterns: Option<Vec<String>>,
    ) -> Result<RDIndex, DeltaError>;

    ///
    /// Creates a FileEntry from a readable data stream.
    ///
    /// This method is used when the data source is remote or does not
    /// exist as a physical file in the local filesystem.
    ///
    /// It reads the stream in chunks (chunk size must be defined by source), hashes each one (using HasherService.hashStream),
    /// and produces a `FileEntry` compatible with RDIndex.
    ///
    /// # Params
    /// - `stream` An async stream providing file chunks.
    /// - `path` Relative path of the source file
    ///
    async fn create_file_entry_from_stream(
        &self,
        stream: &mut (dyn AsyncChunkStream + Send),
        path: &str,
    ) -> Result<FileEntry, DeltaError>;

    ///
    /// Compare two rd-index.json and generate a DeltaPlan.
    ///
    /// # Params
    /// - `source` The rd-index from source to compare (Example: local)
    /// - `target` The rd-index from target to compare (Example: remote server)
    ///
    /// Local -> Remote = upload comparison.
    ///
    /// Remote -> Local = download comparison.
    ///
    /// DeltaPlan Explanation:
    ///  - **missing chunks:** chunks that exist in `source` but are missing in `target`
    ///                   (i.e. need to be transferred from source -> target).
    ///  - **reused chunks:** chunks present in target that can be reused.
    ///
    ///  - **obsolete chunks:** chunks that no longer exists in source and needs to be removed from target
    ///
    fn compare(&self, source: &RDIndex, target: Option<&RDIndex>) -> Result<DeltaPlan, DeltaError>;

    ///
    /// Merges 2 `DeltaPlan`
    ///
    /// # Params
    /// - `base` base `DeltaPlan`
    /// - `updates` `DeltaPlan` to merge with base
    ///
    fn merge_plans(&self, base: &DeltaPlan, updates: &DeltaPlan) -> Result<DeltaPlan, DeltaError>;

    ///
    /// This wrapper will compare rd-indexes ready for upload update
    ///
    /// # Params
    /// - `localIndex` the local rd-index
    /// - `remoteIndex` the remote rd-index (null to upload everything)
    ///
    async fn compare_for_upload(
        &self,
        local_index: &RDIndex,
        remote_index: Option<&RDIndex>,
    ) -> Result<DeltaPlan, DeltaError>;

    ///
    /// This wrapper will compare rd-indexes ready for download update
    ///
    /// # Params
    /// - `localIndex` the local rd-index (null to download everything)
    /// - `remoteIndex` the remote rd-index
    ///
    async fn compare_for_download(
        &self,
        local_index: Option<&RDIndex>,
        remote_index: &RDIndex,
    ) -> Result<DeltaPlan, DeltaError>;
}
