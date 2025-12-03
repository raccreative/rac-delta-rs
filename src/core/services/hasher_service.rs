use async_trait::async_trait;
use thiserror::Error;

use crate::{
    AsyncChunkStream,
    {chunk::Chunk, file_entry::FileEntry},
};

#[derive(Debug, Error)]
pub enum HasherError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Hashing failed: {0}")]
    Hash(String),

    #[error("Unexpected error: {0}")]
    Other(String),
}

pub trait StreamingHasher: Send {
    fn update(&mut self, data: &[u8]);

    /// Hex only for now
    fn digest(&self) -> String;
}

#[async_trait]
pub trait HasherService: Send + Sync {
    /// Will return a `FileEntry` of given file, calculating its hash and chunk hashes.
    ///
    /// # Params
    /// - `file_path`: the relative path of the file respect to root dir
    /// - `root_dir`: the root dir of the index
    /// - `chunk_size`: the size in bytes chunks will have, recommended is 1MB (1024 * 1024)
    ///
    /// **IMPORTANT NOTE:** selected chunk_size must be the same in all operations of rac-delta
    async fn hash_file(
        &self,
        file_path: &str,
        root_dir: &str,
        chunk_size: u64,
    ) -> Result<FileEntry, HasherError>;

    /// Will process a stream of chunks and return an array of hashed chunks
    ///
    /// # Params
    /// - `stream`
    /// - `on_chunk`: optional callback that returns the processed bytes
    ///
    async fn hash_stream(
        &self,
        stream: &mut (dyn AsyncChunkStream + Send),
        on_chunk: Option<Box<dyn Fn(Vec<u8>) + Send + Sync>>,
    ) -> Result<Vec<Chunk>, HasherError>;

    /// Returns a hash of a buffer
    async fn hash_buffer(&self, data: &[u8]) -> Result<String, HasherError>;

    /// Verifies that a chunk has the expected hash
    async fn verify_chunk(&self, data: &[u8], expected_hash: &str) -> Result<bool, HasherError>;

    /// Verifies that a file has the expected hash
    async fn verify_file(&self, path: &str, expected_hash: &str) -> Result<bool, HasherError>;

    /// Creates an incremental hasher (streaming)
    async fn create_streaming_hasher(&self) -> Box<dyn StreamingHasher + Send>;
}
