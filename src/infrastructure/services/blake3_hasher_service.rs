use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use blake3;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::{AsyncChunkStream, Chunk, FileEntry, HasherError, HasherService, StreamingHasher};

pub struct Blake3StreamingHasher {
    hasher: blake3::Hasher,
}

impl Blake3StreamingHasher {
    pub fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
        }
    }
}

impl StreamingHasher for Blake3StreamingHasher {
    fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    fn digest(&self) -> String {
        self.hasher.finalize().to_hex().to_string()
    }
}

pub struct Blake3HasherService;

impl Blake3HasherService {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl HasherService for Blake3HasherService {
    async fn hash_file(
        &self,
        file_path: &str,
        root_dir: &str,
        chunk_size: u64,
    ) -> Result<FileEntry, HasherError> {
        let root_path_buf = Path::new(root_dir);
        let full_root_path: PathBuf = if root_path_buf.is_absolute() {
            root_path_buf.to_path_buf()
        } else {
            std::env::current_dir()?.join(root_path_buf)
        };

        let full_path = full_root_path.join(file_path);

        let mut file = File::open(&full_path).await?;
        let metadata = file.metadata().await?;

        let mut file_hasher = blake3::Hasher::new();
        let mut chunks: Vec<Chunk> = Vec::new();
        let mut buffer = vec![0u8; chunk_size as usize];
        let mut offset = 0;

        loop {
            let bytes_read = file.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }

            let chunk_data = &buffer[..bytes_read];
            let mut chunk_hasher = blake3::Hasher::new();
            chunk_hasher.update(chunk_data);
            let chunk_hash = chunk_hasher.finalize().to_hex().to_string();

            file_hasher.update(chunk_data);

            chunks.push(Chunk {
                hash: chunk_hash,
                offset,
                size: bytes_read as u64,
            });

            offset += bytes_read as u64;
        }

        let file_hash = file_hasher.finalize().to_hex().to_string();

        let modified_at = metadata
            .modified()
            .unwrap_or(SystemTime::now())
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Ok(FileEntry {
            path: file_path.to_string(),
            size: metadata.len(),
            modified_at,
            hash: file_hash,
            chunks,
        })
    }

    async fn hash_stream(
        &self,
        stream: &mut (dyn AsyncChunkStream + Send),
        on_chunk: Option<Box<dyn Fn(Vec<u8>) + Send + Sync>>,
    ) -> Result<Vec<Chunk>, HasherError> {
        let mut chunks = Vec::new();
        let mut offset = 0;

        while let Some(chunk) = stream.next_chunk().await {
            let mut chunk_hasher = blake3::Hasher::new();
            chunk_hasher.update(&chunk);
            let chunk_hash = chunk_hasher.finalize().to_hex().to_string();

            if let Some(ref callback) = on_chunk {
                callback(chunk.clone());
            }

            chunks.push(Chunk {
                hash: chunk_hash,
                offset,
                size: chunk.len() as u64,
            });

            offset += chunk.len() as u64;
        }

        Ok(chunks)
    }

    async fn hash_buffer(&self, data: &[u8]) -> Result<String, HasherError> {
        Ok(blake3::hash(data).to_hex().to_string())
    }

    async fn verify_chunk(&self, data: &[u8], expected_hash: &str) -> Result<bool, HasherError> {
        let actual_hash = blake3::hash(data).to_hex().to_string();
        Ok(actual_hash == expected_hash)
    }

    async fn verify_file(&self, path: &str, expected_hash: &str) -> Result<bool, HasherError> {
        let path_buf = Path::new(path);
        let full_path: PathBuf = if path_buf.is_absolute() {
            path_buf.to_path_buf()
        } else {
            std::env::current_dir()?.join(path_buf)
        };

        let mut file = File::open(full_path).await?;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = [0u8; 8192];

        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }

        Ok(hasher.finalize().to_hex().to_string() == expected_hash)
    }

    async fn create_streaming_hasher(&self) -> Box<dyn StreamingHasher + Send> {
        Box::new(Blake3StreamingHasher::new())
    }
}

#[cfg(test)]
mod blake3_hasher_service_tests {
    use super::*;
    use async_trait::async_trait;
    use std::fs;
    use std::io::Write;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    struct MockChunkStream {
        data: Vec<Vec<u8>>,
        index: usize,
    }

    #[async_trait]
    impl AsyncChunkStream for MockChunkStream {
        async fn next_chunk(&mut self) -> Option<Vec<u8>> {
            if self.index >= self.data.len() {
                None
            } else {
                let chunk = self.data[self.index].clone();
                self.index += 1;
                Some(chunk)
            }
        }
    }

    #[tokio::test]
    async fn hash_buffer_consistency() {
        let service = Blake3HasherService::new();
        let content = b"The quick brown fox jumps over the lazy dog";

        let hash1 = service.hash_buffer(content).await.unwrap();
        let hash2 = service.hash_buffer(content).await.unwrap();

        assert_eq!(hash1, hash2, "hashes should be consistent");
    }

    #[tokio::test]
    async fn verify_chunk_correctly() {
        let service = Blake3HasherService::new();
        let data = b"The quick brown fox jumps over the lazy dog";
        let hash = service.hash_buffer(data).await.unwrap();

        let valid = service.verify_chunk(data, &hash).await.unwrap();
        let invalid = service.verify_chunk(data, "wronghash").await.unwrap();

        assert!(valid);
        assert!(!invalid);
    }

    #[tokio::test]
    async fn hash_file_and_metadata() {
        let service = Blake3HasherService::new();

        let tmp_dir = Path::new("tmp-hash-tests");
        let file_path = tmp_dir.join("test.txt");
        let content = b"The quick brown fox jumps over the lazy dog";

        let _ = fs::remove_dir_all(&tmp_dir);
        fs::create_dir_all(&tmp_dir).unwrap();

        {
            let mut f = fs::File::create(&file_path).unwrap();
            f.write_all(content).unwrap();
            f.flush().unwrap();
        }

        let entry = service
            .hash_file("test.txt", tmp_dir.to_str().unwrap(), 10)
            .await
            .unwrap();

        assert_eq!(entry.path, "test.txt");
        assert_eq!(entry.size, content.len() as u64);
        assert!(!entry.hash.is_empty());
        assert!(!entry.chunks.is_empty());

        for (i, chunk) in entry.chunks.iter().enumerate() {
            assert_eq!(chunk.offset, (i as u64) * 10);
            assert!(chunk.size <= 10);
        }

        fs::remove_dir_all(&tmp_dir).unwrap();
    }

    #[tokio::test]
    async fn verify_file_correctly() {
        let service = Blake3HasherService::new();

        let tmp_dir = Path::new("tmp-hash-tests");
        let file_path = tmp_dir.join("verify.txt");
        let content = b"The quick brown fox jumps over the lazy dog";

        let _ = std::fs::remove_dir_all(&tmp_dir);
        std::fs::create_dir_all(&tmp_dir).unwrap();
        {
            let mut f = std::fs::File::create(&file_path).unwrap();
            use std::io::Write;
            f.write_all(content).unwrap();
            f.flush().unwrap();
        }

        let file_hash = service.hash_buffer(content).await.unwrap();

        let abs_path = std::fs::canonicalize(&file_path).unwrap();
        let abs_str = abs_path.to_str().unwrap();

        let valid = service.verify_file(abs_str, &file_hash).await.unwrap();
        let invalid = service.verify_file(abs_str, "deadbeef").await.unwrap();

        assert!(valid);
        assert!(!invalid);

        std::fs::remove_dir_all(&tmp_dir).unwrap();
    }

    #[tokio::test]
    async fn hash_stream_and_trigger_callback() {
        let service = Blake3HasherService::new();
        let data = b"abcdef1234567890".to_vec();

        let seen = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let seen_clone = seen.clone();

        let mut stream = MockChunkStream {
            data: vec![data.clone()],
            index: 0,
        };

        let chunks = service
            .hash_stream(
                &mut stream,
                Some(Box::new(move |chunk| {
                    seen_clone.lock().unwrap().push(chunk);
                })),
            )
            .await
            .unwrap();

        let seen = seen.lock().unwrap();

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].size, data.len() as u64);
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0], data);
    }

    #[tokio::test]
    async fn streaming_hasher_consistent_output() {
        let service = Blake3HasherService::new();

        let mut hasher1 = service.create_streaming_hasher().await;
        hasher1.update(b"abc");
        let digest1 = hasher1.digest();

        let mut hasher2 = service.create_streaming_hasher().await;
        hasher2.update(b"abc");
        let digest2 = hasher2.digest();

        assert_eq!(digest1, digest2);
    }
}
