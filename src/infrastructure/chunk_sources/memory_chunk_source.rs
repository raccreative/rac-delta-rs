use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{ChunkError, ChunkSource, GetChunksOptions};

pub struct MemoryChunkSource {
    cache: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl MemoryChunkSource {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn set_chunk(&self, hash: String, data: Vec<u8>) {
        let mut cache = self.cache.write().await;
        cache.insert(hash, data);
    }

    pub async fn has_chunk(&self, hash: &str) -> bool {
        let cache = self.cache.read().await;
        cache.contains_key(hash)
    }
}

#[async_trait]
impl ChunkSource for MemoryChunkSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn get_chunk(&self, hash: &str) -> Result<Vec<u8>, ChunkError> {
        let cache = self.cache.read().await;
        cache
            .get(hash)
            .cloned()
            .ok_or_else(|| ChunkError::NotFound(hash.to_string()))
    }

    async fn get_chunks(
        &self,
        hashes: &[String],
        _options: Option<GetChunksOptions>,
    ) -> Result<HashMap<String, Vec<u8>>, ChunkError> {
        let mut results = HashMap::new();

        for hash in hashes {
            let data = self.get_chunk(hash).await?;
            results.insert(hash.clone(), data);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod memory_chunk_source_tests {
    use super::*;

    #[tokio::test]
    async fn stores_and_retrieves_single_chunk() {
        let source = MemoryChunkSource::new();
        let hash = "abc123".to_string();
        let data = b"hello world".to_vec();

        source.set_chunk(hash.clone(), data.clone()).await;
        let retrieved = source.get_chunk(&hash).await.unwrap();

        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn throws_when_requesting_missing_chunk() {
        let source = MemoryChunkSource::new();

        let err = source.get_chunk("missing").await.unwrap_err();
        match err {
            ChunkError::NotFound(h) => assert_eq!(h, "missing"),
            _ => panic!("Expected ChunkError::NotFound"),
        }
    }

    #[tokio::test]
    async fn checks_existence_correctly() {
        let source = MemoryChunkSource::new();

        source
            .set_chunk("exists".to_string(), b"data".to_vec())
            .await;

        assert!(source.has_chunk("exists").await);
        assert!(!source.has_chunk("nope").await);
    }

    #[tokio::test]
    async fn retrieves_multiple_chunks() {
        let source = MemoryChunkSource::new();

        let chunks = vec![
            ("a".to_string(), b"one".to_vec()),
            ("b".to_string(), b"two".to_vec()),
            ("c".to_string(), b"three".to_vec()),
        ];

        for (hash, data) in &chunks {
            source.set_chunk(hash.clone(), data.clone()).await;
        }

        let hashes: Vec<String> = chunks.iter().map(|(h, _)| h.clone()).collect();
        let result = source.get_chunks(&hashes, None).await.unwrap();

        assert_eq!(result.len(), 3);
        for (hash, data) in chunks {
            assert_eq!(result.get(&hash).unwrap(), &data);
        }
    }

    #[tokio::test]
    async fn throws_if_one_requested_chunk_missing() {
        let source = MemoryChunkSource::new();

        source.set_chunk("a".to_string(), b"data".to_vec()).await;

        let hashes = vec!["a".to_string(), "b".to_string()];
        let err = source.get_chunks(&hashes, None).await.unwrap_err();

        match err {
            ChunkError::NotFound(h) => assert_eq!(h, "b"),
            _ => panic!("Expected ChunkError::NotFound"),
        }
    }

    #[tokio::test]
    async fn overwrites_existing_chunk() {
        let source = MemoryChunkSource::new();

        let hash = "same".to_string();
        let first = b"old".to_vec();
        let second = b"new".to_vec();

        source.set_chunk(hash.clone(), first).await;
        source.set_chunk(hash.clone(), second.clone()).await;

        let retrieved = source.get_chunk(&hash).await.unwrap();
        assert_eq!(retrieved, second);
    }
}
