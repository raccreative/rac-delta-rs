use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncRead, AsyncWriteExt};

use crate::{
    BlobInfo, HashStorageAdapter, LocalStorageConfig, PutChunkOptions, RDIndex, StorageAdapter,
    StorageError,
};

pub struct LocalStorageAdapter {
    config: LocalStorageConfig,
}

impl LocalStorageAdapter {
    pub fn new(config: LocalStorageConfig) -> Self {
        Self { config }
    }

    fn resolve_chunk_path(&self, hash: &str) -> PathBuf {
        let prefix = self.config.base.path_prefix.as_deref().unwrap_or("");
        Path::new(&self.config.base_path)
            .join(prefix)
            .join("chunks")
            .join(hash)
    }

    fn resolve_index_path(&self) -> PathBuf {
        let prefix = self.config.base.path_prefix.as_deref().unwrap_or("");
        Path::new(&self.config.base_path)
            .join(prefix)
            .join("rd-index.json")
    }
}

#[async_trait]
impl StorageAdapter for LocalStorageAdapter {
    async fn dispose(&self) {
        // Nothing to do for local storage
    }
}

#[async_trait]
impl HashStorageAdapter for LocalStorageAdapter {
    async fn get_chunk(
        &self,
        hash: &str,
    ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin + 'static>>, StorageError> {
        let path = self.resolve_chunk_path(hash);

        if path.exists() {
            let file = fs::File::open(path).await?;
            Ok(Some(Box::new(file)))
        } else {
            Ok(None)
        }
    }

    async fn put_chunk(
        &self,
        hash: &str,
        mut data: Box<dyn AsyncRead + Send + Unpin + 'static>,
        opts: Option<PutChunkOptions>,
    ) -> Result<(), StorageError> {
        let path = self.resolve_chunk_path(hash);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        if !matches!(opts.and_then(|o| o.overwrite), Some(true)) {
            if fs::metadata(&path).await.is_ok() {
                return Err(StorageError::AlreadyExists(hash.to_string()));
            }
        }

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await?;

        tokio::io::copy(&mut data, &mut file).await?;

        Ok(())
    }

    async fn chunk_exists(&self, hash: &str) -> Result<bool, StorageError> {
        Ok(self.resolve_chunk_path(hash).exists())
    }

    async fn delete_chunk(&self, hash: &str) -> Result<(), StorageError> {
        let _ = fs::remove_file(self.resolve_chunk_path(hash)).await;
        Ok(())
    }

    async fn list_chunks(&self) -> Result<Option<Vec<String>>, StorageError> {
        let chunks_dir = Path::new(&self.config.base_path)
            .join(self.config.base.path_prefix.as_deref().unwrap_or(""))
            .join("chunks");

        let mut hashes = Vec::new();

        if let Ok(mut entries) = fs::read_dir(chunks_dir).await {
            while let Some(entry) = entries.next_entry().await? {
                if entry.file_type().await?.is_file() {
                    if let Some(name) = entry.file_name().to_str() {
                        hashes.push(name.to_string());
                    }
                }
            }
        }

        Ok(Some(hashes))
    }

    async fn get_chunk_info(&self, hash: &str) -> Result<Option<BlobInfo>, StorageError> {
        let path = self.resolve_chunk_path(hash);

        if let Ok(meta) = fs::metadata(&path).await {
            let modified = meta
                .modified()
                .ok()
                .and_then(|t| t.elapsed().ok())
                .map(|d| d.as_secs());

            Ok(Some(BlobInfo {
                hash: hash.to_string(),
                size: meta.len(),
                modified,
                metadata: None,
            }))
        } else {
            Ok(None)
        }
    }

    async fn get_remote_index(&self) -> Result<Option<RDIndex>, StorageError> {
        let path = self.resolve_index_path();

        if let Ok(bytes) = fs::read(&path).await {
            Ok(Some(serde_json::from_slice(&bytes)?))
        } else {
            Ok(None)
        }
    }

    async fn put_remote_index(&self, index: RDIndex) -> Result<(), StorageError> {
        let path = self.resolve_index_path();

        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent).await;
        }

        let bytes = serde_json::to_vec_pretty(&index)?;

        let mut file = fs::File::create(path).await?;
        file.write_all(&bytes).await?;

        Ok(())
    }
}

#[cfg(test)]
mod local_storage_adapter_tests {
    use super::*;
    use std::fs;
    use std::io::Cursor;
    use std::path::PathBuf;
    use tokio::io::AsyncRead;
    use tokio::io::AsyncReadExt;

    use crate::BaseStorageConfig;

    fn tmp_dir(test_name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("local_storage_tests_{test_name}"))
    }

    fn buffer_to_box(data: Vec<u8>) -> Box<dyn AsyncRead + Send + Unpin> {
        Box::new(Cursor::new(data))
    }

    #[tokio::test]
    async fn puts_and_gets_chunk() -> Result<(), StorageError> {
        let dir = tmp_dir("puts_and_gets_chunk");
        let _ = fs::remove_dir_all(&dir);

        let base = BaseStorageConfig { path_prefix: None };

        let config = LocalStorageConfig {
            base_path: dir.clone(),
            base,
        };

        let adapter = LocalStorageAdapter::new(config);

        let hash = "abc123";
        let data = b"HelloWorld".to_vec();

        let options = PutChunkOptions {
            overwrite: Some(true),
            size: None,
        };

        adapter
            .put_chunk(hash, buffer_to_box(data.clone()), Some(options))
            .await?;
        let mut stream = adapter.get_chunk(hash).await?.expect("Chunk should exist");

        let mut result = Vec::new();
        stream.read_to_end(&mut result).await?;

        assert_eq!(result, data);

        Ok(())
    }

    #[tokio::test]
    async fn does_not_overwrite_by_default() -> Result<(), StorageError> {
        let dir = tmp_dir("does_not_overwrite_by_default");
        let _ = fs::remove_dir_all(&dir);

        let base = BaseStorageConfig { path_prefix: None };

        let config = LocalStorageConfig {
            base_path: dir.clone(),
            base,
        };

        let adapter = LocalStorageAdapter::new(config);
        let hash = "abc123";

        adapter
            .put_chunk(hash, buffer_to_box(b"Hello".to_vec()), None)
            .await?;

        let res = adapter
            .put_chunk(hash, buffer_to_box(b"World".to_vec()), None)
            .await;

        match res {
            Err(StorageError::AlreadyExists(h)) => assert_eq!(h, hash),
            _ => panic!("Expected AlreadyExists error"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn overwrites_when_specified() -> Result<(), StorageError> {
        let dir = tmp_dir("overwrites_when_specified");
        let _ = fs::remove_dir_all(&dir);

        let base = BaseStorageConfig { path_prefix: None };

        let config = LocalStorageConfig {
            base_path: dir.clone(),
            base,
        };

        let adapter = LocalStorageAdapter::new(config);
        let hash = "abc123";

        adapter
            .put_chunk(hash, buffer_to_box(b"Hello".to_vec()), None)
            .await?;
        adapter
            .put_chunk(
                hash,
                buffer_to_box(b"World".to_vec()),
                Some(PutChunkOptions {
                    overwrite: Some(true),
                    size: None,
                }),
            )
            .await?;

        let mut stream = adapter.get_chunk(hash).await?.expect("Chunk should exist");
        let mut result = Vec::new();
        stream.read_to_end(&mut result).await?;

        assert_eq!(result, b"World");

        Ok(())
    }

    #[tokio::test]
    async fn deletes_chunk() -> Result<(), StorageError> {
        let dir = tmp_dir("deletes_chunk");
        let _ = fs::remove_dir_all(&dir);

        let base = BaseStorageConfig { path_prefix: None };

        let config = LocalStorageConfig {
            base_path: dir.clone(),
            base,
        };

        let adapter = LocalStorageAdapter::new(config);
        let hash = "abc123";

        adapter
            .put_chunk(hash, buffer_to_box(b"DeleteMe".to_vec()), None)
            .await?;
        adapter.delete_chunk(hash).await?;

        assert!(!adapter.chunk_exists(hash).await?);

        Ok(())
    }

    #[tokio::test]
    async fn lists_chunks() -> Result<(), StorageError> {
        let dir = tmp_dir("lists_chunks");
        let _ = fs::remove_dir_all(&dir);

        let base = BaseStorageConfig { path_prefix: None };

        let config = LocalStorageConfig {
            base_path: dir.clone(),
            base,
        };

        let adapter = LocalStorageAdapter::new(config);

        let chunks = ["a", "b", "c"];
        for c in chunks.iter() {
            adapter
                .put_chunk(c, buffer_to_box(c.as_bytes().to_vec()), None)
                .await?;
        }

        let mut list = adapter.list_chunks().await?.unwrap();
        list.sort();
        let mut expected = chunks.to_vec();
        expected.sort();

        assert_eq!(list, expected);

        Ok(())
    }

    #[tokio::test]
    async fn puts_and_gets_remote_index() -> Result<(), StorageError> {
        let dir = tmp_dir("puts_and_gets_remote_index");
        let _ = fs::remove_dir_all(&dir);

        let base = BaseStorageConfig { path_prefix: None };

        let config = LocalStorageConfig {
            base_path: dir.clone(),
            base,
        };

        let adapter = LocalStorageAdapter::new(config);

        let index = RDIndex {
            chunk_size: 10,
            created_at: 0,
            version: 1,
            files: vec![],
        };

        adapter.put_remote_index(index.clone()).await?;
        let retrieved = adapter.get_remote_index().await?;

        assert_eq!(retrieved.unwrap(), index);

        Ok(())
    }

    #[tokio::test]
    async fn get_remote_index_returns_none_if_missing() -> Result<(), StorageError> {
        let dir = tmp_dir("get_remote_index_returns_none_if_missing");
        let _ = fs::remove_dir_all(&dir);

        let base = BaseStorageConfig { path_prefix: None };

        let config = LocalStorageConfig {
            base_path: dir.clone(),
            base,
        };

        let adapter = LocalStorageAdapter::new(config);
        let result = adapter.get_remote_index().await?;

        assert!(result.is_none());

        Ok(())
    }
}
