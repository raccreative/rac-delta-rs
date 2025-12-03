use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::core::services::DeltaService;
use crate::{
    ChunkUrlInfo, DeltaPlan, HashStorageAdapter, RDIndex, RacDeltaConfig, UrlStorageAdapter,
};

pub type UploadResult<T> = std::result::Result<T, PipelineError>;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Delta error: {0}")]
    Delta(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Index error: {0}")]
    Index(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Operation aborted")]
    Aborted,

    #[error("Other error: {0}")]
    Other(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadState {
    Uploading,
    Comparing,
    Cleaning,
    Finalizing,
    Scanning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadPhase {
    Upload,
    Deleting,
}

#[derive(Clone)]
pub struct UploadOptions {
    /// If true, forces complete upload even if remote index exists.
    /// If false, only new and modified chunks will be uploaded.
    pub force: Option<bool>,

    /// If true and no remote index found, abort upload.
    /// If false (default), uploads everything if no remote index found.
    pub require_remote_index: Option<bool>,

    /// Files or directories that must be ignored when creating the rd-index.json.
    /// Example: "*.ts", "/folder/*", "ignorefile.txt"...
    pub ignore_patterns: Option<Vec<String>>,

    /// Optional callback to inform progress.
    ///
    /// Signature: (phase, progress, optional speed)
    /// - `phase`: UploadPhase (Upload or Deleting)
    /// - `progress`: progress as number
    /// - `speed`: optional speed in bytes/sec
    pub on_progress: Option<Arc<dyn Fn(UploadPhase, f64, Option<f64>) + Send + Sync>>,

    /// Optional callback for state changes.
    pub on_state_change: Option<Arc<dyn Fn(UploadState) + Send + Sync>>,
}

impl Default for UploadOptions {
    fn default() -> Self {
        Self {
            force: None,
            require_remote_index: None,
            ignore_patterns: None,
            on_progress: None,
            on_state_change: None,
        }
    }
}

fn update_progress(
    value: f64,
    phase: UploadPhase,
    speed: Option<f64>,
    options: Option<&UploadOptions>,
) {
    if let Some(opts) = options {
        if let Some(cb) = &opts.on_progress {
            cb(phase, value, speed);
        }
    }
}

fn change_state(state: UploadState, options: Option<&UploadOptions>) {
    if let Some(opts) = options {
        if let Some(cb) = &opts.on_state_change {
            cb(state);
        }
    }
}

#[derive(Debug, Clone)]
pub struct UploadUrls {
    pub upload_urls: HashMap<String, ChunkUrlInfo>,
    pub delete_urls: Option<Vec<String>>,
    pub index_url: String,
}

#[async_trait]
pub trait HashUploadPipeline: Send + Sync {
    fn new(
        storage: Arc<dyn HashStorageAdapter>,
        delta: Arc<dyn DeltaService>,
        config: Arc<RacDeltaConfig>,
    ) -> Self
    where
        Self: Sized;

    /// Execute the upload pipeline for a local directory.
    ///
    /// - `directory`: path to scan and upload
    /// - `remote_index`: optional remote RDIndex to compare against
    /// - `options`: optional upload options
    async fn execute(
        &self,
        directory: &Path,
        remote_index: Option<RDIndex>,
        options: Option<UploadOptions>,
    ) -> UploadResult<RDIndex>;

    /// Scans a directory and produces an RDIndex (used by execute).
    async fn scan_directory(
        &self,
        dir: &Path,
        ignore_patterns: Option<Vec<String>>,
    ) -> UploadResult<RDIndex>;

    /// Upload missing chunks according to the DeltaPlan.
    ///
    /// - `plan`: delta plan with missing/obsolete chunks
    /// - `base_dir`: local base dir for reading files
    /// - `force`: force full upload of files even if some chunks exist
    /// - `options`: optional callbacks / flags
    async fn upload_missing_chunks(
        &self,
        plan: &DeltaPlan,
        base_dir: &Path,
        force: bool,
        options: Option<UploadOptions>,
    ) -> UploadResult<()>;

    /// Upload the rd-index.json to remote.
    async fn upload_index(&self, index: &RDIndex) -> UploadResult<()>;

    /// Delete obsolete chunks from remote according to the DeltaPlan.
    async fn delete_obsolete_chunks(
        &self,
        plan: &DeltaPlan,
        options: Option<UploadOptions>,
    ) -> UploadResult<()>;

    fn update_progress(
        value: f64,
        phase: UploadPhase,
        speed: Option<f64>,
        options: Option<&UploadOptions>,
    ) where
        Self: Sized,
    {
        update_progress(value, phase, speed, options);
    }

    fn change_state(state: UploadState, options: Option<&UploadOptions>)
    where
        Self: Sized,
    {
        change_state(state, options);
    }
}

#[async_trait]
pub trait UrlUploadPipeline: Send + Sync {
    fn new(storage: Arc<dyn UrlStorageAdapter>, config: Arc<RacDeltaConfig>) -> Self
    where
        Self: Sized;

    /// Execute the upload pipeline using signed/upload URLs.
    ///
    /// - `local_index`: local rd-index
    /// - `urls`: object containing uploadUrls, optional deleteUrls and indexUrl
    /// - `options`: optional upload options
    async fn execute(
        &self,
        local_index: RDIndex,
        urls: UploadUrls,
        options: Option<UploadOptions>,
    ) -> UploadResult<RDIndex>;

    /// Upload missing chunks using provided signed URLs.
    async fn upload_missing_chunks(
        &self,
        upload_urls: HashMap<String, ChunkUrlInfo>,
        options: Option<UploadOptions>,
    ) -> UploadResult<()>;

    /// Upload the RDIndex to the provided signed URL.
    async fn upload_index(&self, index: &RDIndex, upload_url: &str) -> UploadResult<()>;

    /// Delete obsolete chunks by given signed URLs.
    async fn delete_obsolete_chunks(
        &self,
        delete_urls: Vec<String>,
        options: Option<UploadOptions>,
    ) -> UploadResult<()>;

    fn update_progress(
        value: f64,
        phase: UploadPhase,
        speed: Option<f64>,
        options: Option<&UploadOptions>,
    ) where
        Self: Sized,
    {
        update_progress(value, phase, speed, options);
    }

    fn change_state(state: UploadState, options: Option<&UploadOptions>)
    where
        Self: Sized,
    {
        change_state(state, options);
    }
}
