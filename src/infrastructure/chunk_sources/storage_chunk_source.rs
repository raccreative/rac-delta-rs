use async_trait::async_trait;

use futures::stream::BoxStream;
use futures::{StreamExt, stream};

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Mutex;

use crate::StorageAdapterEnum;
use crate::{ChunkData, ChunkError, ChunkSource, GetChunksOptions, StreamChunksOptions};

#[derive(Clone)]
pub struct StorageChunkSource {
    storage: Arc<StorageAdapterEnum>,
    urls_map: Option<Arc<HashMap<String, String>>>,
}

impl StorageChunkSource {
    pub fn new(storage: StorageAdapterEnum, urls_map: Option<HashMap<String, String>>) -> Self {
        Self {
            storage: Arc::new(storage),
            urls_map: urls_map.map(Arc::new),
        }
    }
}

#[async_trait]
impl ChunkSource for StorageChunkSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn get_chunk(&self, hash: &str) -> Result<Vec<u8>, ChunkError> {
        match &*self.storage {
            StorageAdapterEnum::Hash(adapter) => {
                let opt_reader = adapter
                    .get_chunk(hash)
                    .await
                    .map_err(|e| ChunkError::ReadError(e.to_string()))?;

                let mut reader = opt_reader.ok_or(ChunkError::NotFound(hash.to_string()))?;

                let mut buf: Vec<u8> = Vec::new();
                tokio::io::AsyncReadExt::read_to_end(&mut *reader, &mut buf)
                    .await
                    .map_err(ChunkError::Io)?;
                Ok(buf)
            }

            StorageAdapterEnum::Url(adapter) => {
                let url: String = self
                    .urls_map
                    .as_ref()
                    .and_then(|m| m.get(hash))
                    .ok_or(ChunkError::NotFound(format!(
                        "URL not found for hash {hash}"
                    )))?
                    .clone();

                let opt_reader = adapter
                    .get_chunk_by_url(&url)
                    .await
                    .map_err(|e| ChunkError::ReadError(e.to_string()))?;

                let mut reader = opt_reader.ok_or(ChunkError::NotFound(hash.to_string()))?;
                let mut buf: Vec<u8> = Vec::new();
                tokio::io::AsyncReadExt::read_to_end(&mut *reader, &mut buf)
                    .await
                    .map_err(ChunkError::Io)?;
                Ok(buf)
            }
        }
    }

    async fn get_chunks(
        &self,
        hashes: &[String],
        options: Option<GetChunksOptions>,
    ) -> Result<HashMap<String, Vec<u8>>, ChunkError> {
        let concurrency: usize = options.and_then(|o| o.concurrency).unwrap_or(8);

        let queue = Arc::new(Mutex::new(VecDeque::from(hashes.to_vec())));
        let results = Arc::new(Mutex::new(HashMap::new()));

        let self_arc: Arc<Self> = Arc::new(self.clone());

        let mut handles = Vec::new();
        for _ in 0..concurrency {
            let queue = Arc::clone(&queue);
            let results = Arc::clone(&results);
            let worker_self = Arc::clone(&self_arc);

            let handle = tokio::spawn(async move {
                loop {
                    let hash_opt = {
                        let mut q = queue.lock().await;
                        q.pop_front()
                    };

                    let hash = match hash_opt {
                        Some(h) => h,
                        None => break,
                    };

                    match worker_self.get_chunk(&hash).await {
                        Ok(data) => {
                            let mut res = results.lock().await;
                            res.insert(hash, data);
                        }
                        Err(err) => {
                            return Err(ChunkError::ReadError(format!(
                                "Failed to get chunk {hash}: {err:?}"
                            )));
                        }
                    }
                }
                Ok::<(), ChunkError>(())
            });

            handles.push(handle);
        }

        for handle in futures::future::join_all(handles).await {
            match handle {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(join_err) => {
                    return Err(ChunkError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Worker panicked: {join_err:?}"),
                    )));
                }
            }
        }

        let results = std::mem::take(&mut *results.lock().await);
        Ok(results)
    }

    async fn stream_chunks(
        &self,
        hashes: &[String],
        options: Option<StreamChunksOptions>,
    ) -> Option<BoxStream<'static, Result<ChunkData, ChunkError>>> {
        let concurrency = options.as_ref().and_then(|o| o.concurrency).unwrap_or(4);
        let preserve_order = options
            .as_ref()
            .and_then(|o| o.preserve_order)
            .unwrap_or(true);

        if hashes.is_empty() {
            return Some(stream::empty::<Result<ChunkData, ChunkError>>().boxed());
        }

        let worker_self = Arc::new(self.clone());
        let abort_flag = Arc::new(AtomicBool::new(false));
        let abort_flag_for_stream = Arc::clone(&abort_flag);

        let fut_stream = stream::iter(hashes.to_vec()).map(move |hash| {
            let worker = Arc::clone(&worker_self);
            let abort_flag = Arc::clone(&abort_flag);
            async move {
                if abort_flag.load(Ordering::Acquire) {
                    return Err(ChunkError::ReadError("aborted".into()));
                }

                let opt_reader = match &*worker.storage {
                    StorageAdapterEnum::Hash(adapter) => {
                        adapter.get_chunk(&hash).await.map_err(|e| {
                            ChunkError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e.to_string(),
                            ))
                        })?
                    }
                    StorageAdapterEnum::Url(adapter) => {
                        let url = worker
                            .urls_map
                            .as_ref()
                            .and_then(|m| m.get(&hash))
                            .cloned()
                            .ok_or_else(|| ChunkError::NotFound(hash.clone()))?;

                        adapter.get_chunk_by_url(&url).await.map_err(|e| {
                            ChunkError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e.to_string(),
                            ))
                        })?
                    }
                };

                let reader = opt_reader.ok_or_else(|| ChunkError::NotFound(hash.clone()))?;

                Ok::<ChunkData, ChunkError>(ChunkData { hash, data: reader })
            }
        });

        let stream = if preserve_order {
            fut_stream.buffered(concurrency).boxed()
        } else {
            fut_stream.buffer_unordered(concurrency).boxed()
        }
        .take_while({
            let abort_flag = Arc::clone(&abort_flag_for_stream);
            move |res| {
                if res.is_err() {
                    abort_flag.store(true, Ordering::Release);
                }
                futures::future::ready(!abort_flag.load(Ordering::Acquire))
            }
        });

        Some(stream.boxed())
    }
}

#[cfg(test)]
mod storage_chunk_source_tests {
    use std::{io::Cursor, path::PathBuf};
    use tokio::io::AsyncReadExt;

    use crate::{BaseStorageConfig, LocalStorageAdapter, LocalStorageConfig};

    use super::*;

    fn tmp_dir(test_name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("local_storage_tests_{test_name}"))
    }

    fn cleanup_tmp_dir(test_name: &str) {
        let dir = tmp_dir(test_name);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[tokio::test]
    async fn retrieves_a_chunk_successfully() {
        let test_name = "retrieves_a_chunk_successfully";
        cleanup_tmp_dir(test_name);
        let base_path = tmp_dir(test_name);
        tokio::fs::create_dir_all(&base_path).await.unwrap();

        let adapter = LocalStorageAdapter::new(LocalStorageConfig {
            base_path: base_path.clone(),
            base: BaseStorageConfig { path_prefix: None },
        });
        let storage_enum = StorageAdapterEnum::Hash(Arc::new(adapter));
        let source = StorageChunkSource::new(storage_enum, None);

        let hash = "abc123";
        let data = b"hello world".to_vec();

        let adapter_ref = match &*source.clone().storage {
            StorageAdapterEnum::Hash(a) => a.clone(),
            _ => unreachable!(),
        };
        adapter_ref
            .put_chunk(hash, Box::new(Cursor::new(data.clone())), None)
            .await
            .unwrap();

        let result = source.get_chunk(hash).await.unwrap();
        assert_eq!(result, data);

        cleanup_tmp_dir(test_name);
    }

    #[tokio::test]
    async fn throws_if_chunk_does_not_exist() {
        let test_name = "throws_if_chunk_does_not_exist";
        cleanup_tmp_dir(test_name);
        let base_path = tmp_dir(test_name);
        tokio::fs::create_dir_all(&base_path).await.unwrap();

        let adapter = LocalStorageAdapter::new(LocalStorageConfig {
            base_path: base_path,
            base: BaseStorageConfig { path_prefix: None },
        });
        let storage_enum = StorageAdapterEnum::Hash(Arc::new(adapter));
        let source = StorageChunkSource::new(storage_enum, None);

        let hash = "doesnotexist";
        let err = source.get_chunk(hash).await.unwrap_err();
        matches!(err, ChunkError::NotFound(_));

        cleanup_tmp_dir(test_name);
    }

    #[tokio::test]
    async fn gets_multiple_chunks_with_get_chunks() {
        let test_name = "gets_multiple_chunks_with_get_chunks";
        cleanup_tmp_dir(test_name);
        let base_path = tmp_dir(test_name);
        tokio::fs::create_dir_all(&base_path).await.unwrap();

        let adapter = LocalStorageAdapter::new(LocalStorageConfig {
            base_path: base_path,
            base: BaseStorageConfig { path_prefix: None },
        });
        let storage_enum = StorageAdapterEnum::Hash(Arc::new(adapter));
        let source = StorageChunkSource::new(storage_enum, None);

        let hashes = vec!["a", "b", "c"];
        let adapter_ref = match &*source.clone().storage {
            StorageAdapterEnum::Hash(a) => a.clone(),
            _ => unreachable!(),
        };

        for hash in &hashes {
            let data = hash.as_bytes().to_vec();
            adapter_ref
                .put_chunk(hash, Box::new(Cursor::new(data)), None)
                .await
                .unwrap();
        }

        let result_map = source
            .get_chunks(
                &hashes.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                Some(GetChunksOptions {
                    concurrency: Some(4),
                }),
            )
            .await
            .unwrap();

        assert_eq!(String::from_utf8_lossy(&result_map["a"]), "a");
        assert_eq!(String::from_utf8_lossy(&result_map["b"]), "b");
        assert_eq!(String::from_utf8_lossy(&result_map["c"]), "c");

        cleanup_tmp_dir(test_name);
    }

    #[tokio::test]
    async fn streams_chunks_in_order() {
        let test_name = "streams_chunks_in_order";
        cleanup_tmp_dir(test_name);
        let base_path = tmp_dir(test_name);
        tokio::fs::create_dir_all(&base_path).await.unwrap();

        let adapter = LocalStorageAdapter::new(LocalStorageConfig {
            base_path: base_path,
            base: BaseStorageConfig { path_prefix: None },
        });
        let storage_enum = StorageAdapterEnum::Hash(Arc::new(adapter));
        let source = StorageChunkSource::new(storage_enum, None);

        let adapter_ref = match &*source.clone().storage {
            StorageAdapterEnum::Hash(a) => a.clone(),
            _ => unreachable!(),
        };

        adapter_ref
            .put_chunk("x", Box::new(Cursor::new(b"X".to_vec())), None)
            .await
            .unwrap();
        adapter_ref
            .put_chunk("y", Box::new(Cursor::new(b"Y".to_vec())), None)
            .await
            .unwrap();

        let hashes = vec!["x".to_string(), "y".to_string()];
        let mut result: Vec<String> = Vec::new();

        let stream_opt = source
            .stream_chunks(
                &hashes,
                Some(StreamChunksOptions {
                    preserve_order: Some(true),
                    concurrency: Some(2),
                }),
            )
            .await;

        let mut stream = stream_opt.unwrap();
        while let Some(item) = stream.next().await {
            let chunk_data = item.unwrap();
            let mut buf = Vec::new();
            let mut reader = chunk_data.data;
            reader.read_to_end(&mut buf).await.unwrap();
            result.push(String::from_utf8_lossy(&buf).to_string());
        }

        assert_eq!(result, vec!["X", "Y"]);

        cleanup_tmp_dir(test_name);
    }
}
