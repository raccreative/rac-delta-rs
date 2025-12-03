use async_trait::async_trait;
use futures::stream::BoxStream;
use std::{collections::HashMap, path::Path, pin::Pin, sync::Arc};
use thiserror::Error;
use tokio::io::AsyncRead;

use crate::{delta_plan::DeltaPlan, file_entry::FileEntry};

/// Default in-place reconstruction threshold (400 MB)
pub const DEFAULT_IN_PLACE_RECONSTRUCTION_THRESHOLD: u64 = 400 * 1024 * 1024;

pub type FileProgressCallback = Arc<dyn Fn(f64, usize, Option<usize>) + Send + Sync>;

pub struct StreamChunksOptions {
    /// Number of parallel fetches (default 8)
    pub concurrency: Option<usize>,
    /// Whether to yield in input order (default true)
    pub preserve_order: Option<bool>,
}

pub struct GetChunksOptions {
    pub concurrency: Option<usize>,
}

pub struct ChunkData {
    pub hash: String,
    pub data: Box<dyn AsyncRead + Send + Unpin + 'static>,
}

#[derive(Default, Clone)]
pub struct ReconstructionOptions {
    /// Force to rebuild even if hash file matches.
    pub force_rebuild: Option<bool>,

    /// Verifies the reconstructed file hash after finishing.
    /// If hash does not match, an error is thrown.
    pub verify_after_rebuild: Option<bool>,

    /// Minimum file size (in bytes) required to perform an in-place reconstruction instead of using a temporary file.
    ///
    /// Default: `400 * 1024 * 1024` (400MB)
    ///
    /// **In-place reconstruction:**
    /// The existing file is opened and updated directly by overwriting only the modified or missing chunks.
    ///  
    /// **.tmp reconstruction:**
    /// The file is fully rebuilt in a temporary .tmp location using all chunks (new and existing), then replaced over the original file.
    ///
    /// **When to use:**
    /// In-place reconstruction is recommended for **large files**, as it avoids rewriting the entire file and significantly reduces disk space usage.
    /// However, it may be **unsafe for certain formats** (e.g., ZIP archives or databases) that are sensitive to partial writes or corruption.
    ///
    /// To disable in-place reconstruction entirely, set this value to 0.
    pub in_place_reconstruction_threshold: Option<u64>,

    /// How many files will reconstruct concurrently (default value is 5)
    pub file_concurrency: Option<usize>,

    /// Callback that returns disk usage and optional network speed (only for storage chunk sources via streaming download-reconstruction)
    ///
    /// # Params
    /// - `reconstruct_progress` current reconstruction progress
    /// - `disk_speed` speed of disk write in bytes per second
    /// - `network_progress` current network progress if any
    /// - `network_speed` download speed in bytes per second
    pub on_progress: Option<Arc<dyn Fn(f64, usize, Option<f64>, Option<usize>) + Send + Sync>>,
}

#[derive(Error, Debug)]
pub enum ChunkError {
    #[error("Chunk '{0}' not found")]
    NotFound(String),

    #[error("Chunk '{0}' could not be read")]
    ReadError(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Error, Debug)]
pub enum ReconstructionError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Chunk '{0}' not found")]
    ChunkNotFound(String),

    #[error("Hash mismatch for file '{0}'")]
    HashMismatch(String),

    #[error("Failed to read chunk '{0}'")]
    ChunkReadError(String),

    #[error("Reconstruction failed: {0}")]
    Other(String),
}

/// Abstract chunk source
#[async_trait]
pub trait ChunkSource: Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;

    /// Gets a chunk from the source.
    async fn get_chunk(&self, hash: &str) -> Result<Vec<u8>, ChunkError>;

    /// Retrieves multiple chunks concurrently (optional)
    async fn get_chunks(
        &self,
        hashes: &[String],
        options: Option<GetChunksOptions>,
    ) -> Result<HashMap<String, Vec<u8>>, ChunkError>;

    /// Streams file chunks from storage concurrently. (optional)
    /// Can preserve original order or emit as workers complete.
    ///
    async fn stream_chunks(
        &self,
        _hashes: &[String],
        _options: Option<StreamChunksOptions>,
    ) -> Option<BoxStream<'static, Result<ChunkData, ChunkError>>> {
        None
    }
}

#[async_trait]
pub trait ReconstructionService: Send + Sync {
    /// Reconstructs a file in disk.
    /// Able to reconstruct a new file or an existing file.
    ///
    /// # Parameters
    /// - `entry` The `FileEntry` containing the list of chunks and path of the file
    /// - `output_path` The path where the file will be reconstructed.
    /// - `chunk_source` the source implementations of the chunks
    /// - `options` optional parameters for the reconstruction
    async fn reconstruct_file(
        self: Arc<Self>,
        entry: &FileEntry,
        output_path: &Path,
        chunk_source: &dyn ChunkSource,
        options: Option<&ReconstructionOptions>,
        cb: Option<FileProgressCallback>,
    ) -> Result<(), ReconstructionError>;

    /// Reconstructs all files from a DeltaPlan in disk.
    ///
    /// # Parameters
    /// - `plan` The DeltaPlan containing the list of files and chunks.
    /// - `output_dir` The dir where the files will be reconstructed.
    /// - `chunk_source` the source implementations of the chunks
    /// - `options` optional parameters for the reconstruction
    async fn reconstruct_all(
        self: Arc<Self>,
        plan: &DeltaPlan,
        output_dir: &Path,
        chunk_source: Arc<dyn ChunkSource>,
        options: Option<&ReconstructionOptions>,
    ) -> Result<(), ReconstructionError>;

    /// Reconstructs a file to stream.
    ///
    /// # Parameters
    /// - `entry` The FileEntry containing the list of chunks of the file
    /// - `chunk_source` the source implementations of the chunks
    async fn reconstruct_to_stream(
        self: Arc<Self>,
        entry: FileEntry,
        chunk_source: Arc<dyn ChunkSource + Send + Sync>,
    ) -> Result<Pin<Box<dyn AsyncRead + Send + Sync>>, ReconstructionError>;
}
