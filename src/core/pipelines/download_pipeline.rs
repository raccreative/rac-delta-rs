use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::core::services::{ChunkSource, DeltaService, ReconstructionService, ValidationService};
use crate::{
    ChunkUrlInfo, DeltaPlan, HashStorageAdapter, RDIndex, RacDeltaConfig, UrlStorageAdapter,
};

pub type DownloadResult<T> = std::result::Result<T, DownloadError>;

#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Delta error: {0}")]
    Delta(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Index error: {0}")]
    Index(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Reconstruction error: {0}")]
    Reconstruction(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Operation aborted")]
    Aborted,

    #[error("Other error: {0}")]
    Other(String),
}

/// Determines how chunks are downloaded and reconstructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateStrategy {
    /// Downloads every chunk before reconstruction and save chunks in memory.
    /// Perfect for fast connection and offline reconstruction.
    ///
    /// **NOTE:** For large updates this is not recommended, as could use a lot of memory.
    DownloadAllFirstToMemory,

    /// Downloads chunks on demand while reconstructing (streaming).
    /// Useful for limited resourced environments or progressive streaming.
    StreamFromNetwork,

    /// Downloads every chunk before reconstruction and save chunks in disk to given path.
    /// Perfect for fast connection, fast disks and offline reconstruction.
    DownloadAllFirstToDisk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadState {
    Downloading,
    Reconstructing,
    Cleaning,
    Scanning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadPhase {
    Download,
    Reconstructing,
    Deleting,
}

/// Options controlling download and reconstruction behavior.
#[derive(Clone)]
pub struct DownloadOptions {
    /// If true, downloads everything; otherwise only new/modified chunks.
    pub force: Option<bool>,

    /// Directory where chunks will be saved if using the disk strategy.
    pub chunks_save_path: Option<PathBuf>,

    /// If true, uses existing rd-index.json in local dir (not recommended).
    pub use_existing_index: Option<bool>,

    /// How many files to reconstruct concurrently (default: 5).
    pub file_reconstruction_concurrency: Option<usize>,

    /// Minimum file size (in bytes) required to perform an **in-place reconstruction** instead of using a temporary file.
    /// Default: `400 * 1024 * 1024` (400MB)
    ///
    /// **In-place reconstruction:**
    /// The ecisting file is opened and updated directly by overwriting only the modified or missing chunks.
    ///
    /// **.tmp reconstruction:**
    /// The file is fully rebuilt in a temporary `.tmp` location using all chunks (new and existing ones), then replaced over the original file.
    ///
    /// **When to use:**
    /// In-place reconstruction is recommended for **large files**, as it avoids rewriting the entire file and significantly reduces disk space usage.
    /// However, it may be **unsafe for certain formats** (e.g., ZIP archives or databases) that are sensitive to partial writes or corruption.
    /// To disable in-place reconstruction entirely, set this value to `0`
    ///
    pub in_place_reconstruction_threshold: Option<u64>,

    /// Optional callback to inform progress.
    ///
    /// Signature: (phase, progress, optional disk_usage, optional speed)
    pub on_progress:
        Option<Arc<dyn Fn(DownloadPhase, f64, Option<f64>, Option<f64>) + Send + Sync>>,

    /// Optional callback for state changes.
    pub on_state_change: Option<Arc<dyn Fn(DownloadState) + Send + Sync>>,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            force: None,
            chunks_save_path: None,
            use_existing_index: None,
            file_reconstruction_concurrency: None,
            in_place_reconstruction_threshold: None,
            on_progress: None,
            on_state_change: None,
        }
    }
}

fn update_progress(
    value: f64,
    phase: DownloadPhase,
    disk_usage: Option<f64>,
    speed: Option<f64>,
    options: Option<&DownloadOptions>,
) {
    if let Some(opts) = options {
        if let Some(cb) = &opts.on_progress {
            cb(phase, value, disk_usage, speed);
        }
    }
}

fn change_state(state: DownloadState, options: Option<&DownloadOptions>) {
    if let Some(opts) = options {
        if let Some(cb) = &opts.on_state_change {
            cb(state);
        }
    }
}

#[async_trait]
pub trait HashDownloadPipeline: Send + Sync {
    fn new(
        storage: Arc<dyn HashStorageAdapter>,
        delta: Arc<dyn DeltaService>,
        reconstruction: Arc<dyn ReconstructionService>,
        validation: Arc<dyn ValidationService>,
        config: Arc<RacDeltaConfig>,
    ) -> Self
    where
        Self: Sized;

    async fn execute(
        &self,
        local_dir: &Path,
        strategy: UpdateStrategy,
        remote_index: Option<RDIndex>,
        options: Option<DownloadOptions>,
    ) -> DownloadResult<()>;

    /// Creates an rd-index.json for a given directory by scanning files.
    async fn load_local_index(&self, dir: &Path) -> DownloadResult<RDIndex>;

    /// Saves a local index to the given directory.
    async fn save_local_index(&self, local_dir: &Path, index: &RDIndex) -> DownloadResult<()>;

    /// Finds an existing rd-index.json in the given folder.
    async fn find_local_index(&self, local_dir: &Path) -> DownloadResult<Option<RDIndex>>;

    /// This method will download first all needed chunks for download, and save them temporary on disk or memory.
    ///
    /// Will return `ChunkSource`, ChunkSources will be needed to reconstruct files, this method will ONLY return
    /// memory or disk chunk sources for offline reconstruction, if you use a storage like S3, you can omit this
    /// and use directly the StorageChunkSource with `reconstruction.reconstructAll()` if you prefer.
    ///
    /// (Using StorageChunkSource will download chunks and reconstruct file at the same time, concurrently)
    async fn download_all_missing_chunks(
        &self,
        plan: &DeltaPlan,
        target: DownloadTarget,
        options: Option<DownloadOptions>,
    ) -> DownloadResult<Arc<dyn ChunkSource>>;

    /// Verifies reconstructed files and deletes obsolete chunks if needed.
    async fn verify_and_delete_obsolete_chunks(
        &self,
        plan: &DeltaPlan,
        local_dir: &Path,
        remote_index: &RDIndex,
        chunk_source: Arc<dyn ChunkSource>,
        options: Option<DownloadOptions>,
    ) -> DownloadResult<FileVerificationResult>;

    fn update_progress(
        value: f64,
        phase: DownloadPhase,
        disk_usage: Option<f64>,
        speed: Option<f64>,
        options: Option<&DownloadOptions>,
    ) where
        Self: Sized,
    {
        update_progress(value, phase, disk_usage, speed, options);
    }

    fn change_state(state: DownloadState, options: Option<&DownloadOptions>)
    where
        Self: Sized,
    {
        change_state(state, options);
    }
}

/// Defines where chunks will be temporarily saved during download.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadTarget {
    Memory,
    Disk,
}

/// Represents the result of a file verification and cleanup step.
#[derive(Debug, Clone)]
pub struct FileVerificationResult {
    pub deleted_files: Vec<String>,
    pub verified_files: Vec<String>,
    pub rebuilt_files: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DownloadUrls {
    pub download_urls: HashMap<String, ChunkUrlInfo>,
    pub index_url: String,
}

#[async_trait]
pub trait UrlDownloadPipeline: Send + Sync {
    fn new(
        storage: Arc<dyn UrlStorageAdapter>,
        reconstruction: Arc<dyn ReconstructionService>,
        validation: Arc<dyn ValidationService>,
        delta: Arc<dyn DeltaService>,
        config: Arc<RacDeltaConfig>,
    ) -> Self
    where
        Self: Sized;

    /// Executes the download pipeline using provided URLs.
    async fn execute(
        &self,
        local_dir: &Path,
        urls: DownloadUrls,
        strategy: UpdateStrategy,
        plan: Option<DeltaPlan>,
        options: Option<DownloadOptions>,
    ) -> DownloadResult<()>;

    /// Saves local index to given directory.
    async fn save_local_index(&self, local_dir: &Path, index: &RDIndex) -> DownloadResult<()>;

    /// Creates an rd-index.json for a given directory by scanning files.
    async fn load_local_index(&self, dir: &Path) -> DownloadResult<RDIndex>;

    /// Finds an existing rd-index.json in the given folder.
    async fn find_local_index(&self, local_dir: &Path) -> DownloadResult<Option<RDIndex>>;

    /// Downloads all missing chunks using signed URLs.
    async fn download_all_missing_chunks(
        &self,
        download_urls: HashMap<String, ChunkUrlInfo>,
        target: DownloadTarget,
        options: Option<DownloadOptions>,
    ) -> DownloadResult<Arc<dyn ChunkSource>>;

    /// Verifies reconstructed files and deletes obsolete chunks.
    async fn verify_and_delete_obsolete_chunks(
        &self,
        plan: &DeltaPlan,
        local_dir: &Path,
        remote_index: &RDIndex,
        chunk_source: Arc<dyn ChunkSource>,
        options: Option<DownloadOptions>,
    ) -> DownloadResult<FileVerificationResult>;

    fn update_progress(
        value: f64,
        phase: DownloadPhase,
        disk_usage: Option<f64>,
        speed: Option<f64>,
        options: Option<&DownloadOptions>,
    ) where
        Self: Sized,
    {
        update_progress(value, phase, disk_usage, speed, options);
    }

    fn change_state(state: DownloadState, options: Option<&DownloadOptions>)
    where
        Self: Sized,
    {
        change_state(state, options);
    }
}
