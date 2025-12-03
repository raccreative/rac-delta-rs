use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::AsyncRead;

use crate::RDIndex;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Chunk not found: {0}")]
    NotFound(String),

    #[error("Chunk already exists: {0}")]
    AlreadyExists(String),

    #[error("Serialization error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    /// Errors for feature-specific cases (SSH, HTTP, S3, etc.)
    #[error("Other error: {0}")]
    Other(String),
}

pub struct BlobInfo {
    pub hash: String,
    pub size: u64,
    pub modified: Option<u64>,
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

pub struct PutChunkOptions {
    /// Whether to overwrite chunk if exists or not (depends on the implementation)
    pub overwrite: Option<bool>,
    /// ContentLength of the chunk, sometimes needed for some adapters
    pub size: Option<u64>,
}

#[derive(Clone)]
pub enum StorageAdapterEnum {
    Hash(Arc<dyn HashStorageAdapter + Send + Sync>),
    Url(Arc<dyn UrlStorageAdapter + Send + Sync>),
}

#[async_trait]
pub trait StorageAdapter: Send + Sync {
    /// Dispose resources if needed
    async fn dispose(&self);
}

/// Hash storage adapter
#[async_trait]
pub trait HashStorageAdapter: StorageAdapter {
    /// Returns a readable stream of the chunk if exists, None otherwise
    async fn get_chunk(
        &self,
        hash: &str,
    ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin + 'static>>, StorageError>;

    /// Upload a chunk from a readable stream
    async fn put_chunk(
        &self,
        hash: &str,
        data: Box<dyn AsyncRead + Send + Unpin + 'static>,
        opts: Option<PutChunkOptions>,
    ) -> Result<(), StorageError>;

    /// Check if chunk exists in storage
    async fn chunk_exists(&self, hash: &str) -> Result<bool, StorageError>;

    /// Delete a chunk identified by hash
    async fn delete_chunk(&self, hash: &str) -> Result<(), StorageError>;

    /// List available chunks (optional)
    ///
    /// Chunks are always under given-prefix/chunks/
    async fn list_chunks(&self) -> Result<Option<Vec<String>>, StorageError> {
        Ok(None)
    }

    /// Returns chunk info (optional)
    async fn get_chunk_info(&self, _hash: &str) -> Result<Option<BlobInfo>, StorageError> {
        Ok(None)
    }

    /// Helper to get rd-index.json
    /// Should return null if not found.
    ///
    /// rd-index.json is always on given-prefix/rd-index.json (root)
    async fn get_remote_index(&self) -> Result<Option<RDIndex>, StorageError>;

    /// Helper to upload rd-index.json
    ///
    /// rd-index.json is always on given-prefix/rd-index.json (root)
    async fn put_remote_index(&self, index: RDIndex) -> Result<(), StorageError>;
}

#[async_trait]
pub trait UrlStorageAdapter: StorageAdapter {
    /// Returns a readable stream of the chunk by URL, None otherwise
    async fn get_chunk_by_url(
        &self,
        url: &str,
    ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin + 'static>>, StorageError>;

    /// Upload a chunk by URL
    async fn put_chunk_by_url(
        &self,
        url: &str,
        data: Box<dyn AsyncRead + Send + Unpin + 'static>,
    ) -> Result<(), StorageError>;

    /// Delete a chunk by URL
    async fn delete_chunk_by_url(&self, url: &str) -> Result<(), StorageError>;

    /// Check if chunk exists by URL
    async fn chunk_exists_by_url(&self, url: &str) -> Result<bool, StorageError>;

    /// List available chunks by URL (optional)
    async fn list_chunks_by_url(&self, _url: &str) -> Result<Option<Vec<String>>, StorageError> {
        Ok(None)
    }

    /// Returns chunk info by URL (optional)
    async fn get_chunk_info_by_url(
        &self,
        _hash: &str,
        _url: &str,
    ) -> Result<Option<BlobInfo>, StorageError> {
        Ok(None)
    }

    /// Get remote RDIndex by URL
    async fn get_remote_index_by_url(&self, url: &str) -> Result<Option<RDIndex>, StorageError>;

    /// Upload RDIndex by URL
    async fn put_remote_index_by_url(&self, url: &str, index: RDIndex) -> Result<(), StorageError>;
}
