use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::fs;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

use crate::{ChunkError, ChunkSource, GetChunksOptions};

pub struct DiskChunkSource {
    cache_dir: PathBuf,
}

impl DiskChunkSource {
    pub fn new<P: AsRef<Path>>(cache_dir: P) -> Self {
        Self {
            cache_dir: cache_dir.as_ref().to_path_buf(),
        }
    }

    fn chunk_path(&self, hash: &str) -> PathBuf {
        self.cache_dir.join(hash)
    }

    pub async fn has_chunk(&self, hash: &str) -> bool {
        fs::metadata(self.chunk_path(hash)).await.is_ok()
    }

    pub async fn set_chunk_bytes(&self, hash: &str, data: &[u8]) -> io::Result<()> {
        let file_path = self.chunk_path(hash);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(file_path, data).await
    }

    pub async fn set_chunk_reader<R>(&self, hash: &str, mut reader: R) -> io::Result<()>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let file_path = self.chunk_path(hash);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut file = fs::File::create(file_path).await?;
        let mut buf = vec![0u8; 8192];

        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            file.write_all(&buf[..n]).await?;
        }

        Ok(())
    }

    pub async fn clear(&self) -> io::Result<()> {
        if fs::metadata(&self.cache_dir).await.is_ok() {
            fs::remove_dir_all(&self.cache_dir).await?;
        }

        fs::create_dir_all(&self.cache_dir).await?;

        Ok(())
    }
}

#[async_trait]
impl ChunkSource for DiskChunkSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn get_chunk(&self, hash: &str) -> Result<Vec<u8>, ChunkError> {
        let file_path = self.chunk_path(hash);

        match fs::metadata(&file_path).await {
            Ok(metadata) if metadata.is_file() => match fs::read(&file_path).await {
                Ok(data) => Ok(data),
                Err(_) => Err(ChunkError::ReadError(hash.to_string())),
            },
            _ => Err(ChunkError::NotFound(hash.to_string())),
        }
    }

    async fn get_chunks(
        &self,
        hashes: &[String],
        _options: Option<GetChunksOptions>,
    ) -> Result<HashMap<String, Vec<u8>>, ChunkError> {
        let mut result = HashMap::new();

        for hash in hashes {
            let data = self.get_chunk(hash).await?;

            result.insert(hash.clone(), data);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod disk_chunk_source_tests {
    use super::*;
    use std::fs;
    use std::io::Cursor;

    fn tmp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("disk_source_test_{name}"))
    }

    async fn setup(name: &str) -> (DiskChunkSource, PathBuf) {
        let dir = tmp_dir(name);

        if dir.exists() {
            let _ = fs::remove_dir_all(&dir);
        }
        fs::create_dir_all(&dir).unwrap();

        (DiskChunkSource::new(&dir), dir)
    }

    #[tokio::test]
    async fn stores_and_retrieves_chunk_as_bytes() {
        let (source, _dir) = setup("stores_and_retrieves_chunk_as_bytes").await;

        let hash = "chunk1";
        let data = b"hello world".to_vec();

        source.set_chunk_bytes(hash, &data).await.unwrap();
        let retrieved = source.get_chunk(hash).await.unwrap();

        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn stores_chunk_from_reader() {
        let (source, _dir) = setup("stores_chunk_from_reader").await;

        let hash = "streamed";
        let content = "streamed data".as_bytes().to_vec();
        let reader = Cursor::new(content.clone());

        source.set_chunk_reader(hash, reader).await.unwrap();
        let retrieved = source.get_chunk(hash).await.unwrap();

        assert_eq!(retrieved, content);
    }

    #[tokio::test]
    async fn get_chunk_not_found() {
        let (source, _dir) = setup("get_chunk_not_found").await;

        let err = source.get_chunk("missing").await.unwrap_err();
        match err {
            ChunkError::NotFound(_) => {}
            _ => panic!("Expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn exists_true() {
        let (source, _dir) = setup("exists_true").await;

        source
            .set_chunk_bytes("exists", b"exists data")
            .await
            .unwrap();

        assert!(source.has_chunk("exists").await);
    }

    #[tokio::test]
    async fn exists_false() {
        let (source, _dir) = setup("exists_false").await;

        assert!(!source.has_chunk("not_there").await);
    }

    #[tokio::test]
    async fn get_multiple_chunks() {
        let (source, _dir) = setup("get_multiple_chunks").await;

        let chunks = vec![
            ("a".to_string(), b"aaa".to_vec()),
            ("b".to_string(), b"bbb".to_vec()),
            ("c".to_string(), b"ccc".to_vec()),
        ];

        for (hash, data) in &chunks {
            source.set_chunk_bytes(hash, data).await.unwrap();
        }

        let hashes: Vec<String> = chunks.iter().map(|(h, _)| h.clone()).collect();
        let result = source.get_chunks(&hashes, None).await.unwrap();

        assert_eq!(result.len(), 3);
        for (hash, data) in chunks {
            assert_eq!(result.get(&hash).unwrap(), &data);
        }
    }

    #[tokio::test]
    async fn get_chunks_missing_one() {
        let (source, _dir) = setup("get_chunks_missing_one").await;

        source.set_chunk_bytes("a", b"data").await.unwrap();

        let hashes = vec!["a".to_string(), "missing".to_string()];
        let err = source.get_chunks(&hashes, None).await.unwrap_err();

        match err {
            ChunkError::NotFound(_) => {}
            _ => panic!("Expected NotFound"),
        }
    }

    #[tokio::test]
    async fn clear_removes_all_chunks() {
        let (source, dir) = setup("clear_removes_all_chunks").await;

        source.set_chunk_bytes("to-delete", b"data").await.unwrap();

        let files_before = fs::read_dir(&dir).unwrap().count();
        assert!(files_before > 0);

        source.clear().await.unwrap();

        let files_after = fs::read_dir(dir).unwrap().count();
        assert_eq!(files_after, 0);
    }

    #[tokio::test]
    async fn creates_nested_directories() {
        let (source, dir) = setup("creates_nested_directories").await;

        let nested_hash = "nested/dir/chunk";
        let data = b"nested data".to_vec();

        source.set_chunk_bytes(nested_hash, &data).await.unwrap();

        let nested_path = dir.join(nested_hash);
        let retrieved = fs::read(nested_path).unwrap();

        assert_eq!(retrieved, data);
    }
}
