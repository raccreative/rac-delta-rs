#[cfg(feature = "gcs")]
pub mod gcs_adapter {
    use async_trait::async_trait;

    use std::sync::Arc;

    use bytes::Bytes;

    use google_cloud_auth::credentials::service_account;
    use google_cloud_storage::client::{Storage, StorageControl};
    use google_cloud_storage::streaming_source::{Payload, StreamingSource};

    use futures::{StreamExt, TryStreamExt};
    use tokio::io::{AsyncRead, AsyncReadExt};
    use tokio::sync::Mutex;
    use tokio_util::io::StreamReader;

    use crate::{
        BlobInfo, GCSStorageConfig, HashStorageAdapter, PutChunkOptions, RDIndex, StorageAdapter,
        StorageError,
    };

    pub struct AsyncReadSource {
        reader: Arc<Mutex<dyn AsyncRead + Send + Unpin>>,
        buf_size: usize,
    }

    impl AsyncReadSource {
        pub fn new(reader: Box<dyn AsyncRead + Send + Unpin>, buf_size: usize) -> Self {
            Self {
                reader: Arc::new(Mutex::new(tokio::io::BufReader::new(reader))),
                buf_size,
            }
        }
    }

    #[allow(refining_impl_trait)]
    impl StreamingSource for AsyncReadSource {
        type Error = StorageError;

        fn next(&mut self) -> impl Future<Output = Option<Result<Bytes, Self::Error>>> + Send + '_ {
            let reader = Arc::clone(&self.reader);
            let buf_size = self.buf_size;

            async move {
                let mut buf = vec![0u8; buf_size];
                let mut guard = reader.lock().await;

                match guard.read(&mut buf).await {
                    Ok(0) => None,
                    Ok(n) => {
                        buf.truncate(n);
                        Some(Ok(Bytes::from(buf)))
                    }
                    Err(e) => Some(Err(StorageError::Other(format!(
                        "io error reading source: {}",
                        e
                    )))),
                }
            }
        }
    }

    pub struct GCSStorageAdapter {
        client: Storage,
        control: StorageControl,
        config: GCSStorageConfig,
    }

    impl GCSStorageAdapter {
        pub async fn new(config: GCSStorageConfig) -> Result<Self, StorageError> {
            let service_account_key = serde_json::json!({
                "client_email": config.credentials.client_email,
                "private_key": config.credentials.private_key,
                "project_id": config.credentials.project_id,
            });

            let credentials = service_account::Builder::new(service_account_key)
                .build()
                .map_err(|e| StorageError::Other(e.to_string()))?;

            let mut storage_builder = Storage::builder().with_credentials(credentials.clone());
            let mut storage_control_builder =
                StorageControl::builder().with_credentials(credentials);

            if let Some(endpoint) = &config.api_endpoint {
                storage_builder = storage_builder.with_endpoint(endpoint);
                storage_control_builder = storage_control_builder.with_endpoint(endpoint);
            }

            let client = storage_builder
                .build()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            let control = storage_control_builder
                .build()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            Ok(Self {
                client,
                config,
                control,
            })
        }

        fn get_chunk_path(&self, hash: &str) -> String {
            self.config.base.path_prefix.as_ref().map_or_else(
                || format!("chunks/{}", hash),
                |prefix| {
                    let p = prefix.trim_end_matches('/');
                    if p.is_empty() {
                        format!("chunks/{}", hash)
                    } else {
                        format!("{}/chunks/{}", p, hash)
                    }
                },
            )
        }

        fn get_index_path(&self) -> String {
            self.config.base.path_prefix.as_ref().map_or_else(
                || "rd-index.json".to_string(),
                |p| format!("{}/rd-index.json", p),
            )
        }
    }

    #[async_trait]
    impl StorageAdapter for GCSStorageAdapter {
        async fn dispose(&self) {}
    }

    #[async_trait]
    impl HashStorageAdapter for GCSStorageAdapter {
        async fn get_chunk(
            &self,
            hash: &str,
        ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, StorageError> {
            let chunk_path = self.get_chunk_path(hash);

            let result = self
                .client
                .read_object(&self.config.bucket, &chunk_path)
                .send()
                .await;

            let response = match result {
                Ok(r) => r,
                Err(err) => {
                    if let Some(404) = google_cloud_storage::Error::http_status_code(&err) {
                        return Ok(None);
                    } else {
                        return Err(StorageError::Other(err.to_string()));
                    }
                }
            };

            let byte_stream = response
                .into_stream()
                .map(|res| res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));

            let reader = StreamReader::new(byte_stream);

            Ok(Some(Box::new(reader)))
        }

        async fn put_chunk(
            &self,
            hash: &str,
            data: Box<dyn AsyncRead + Send + Unpin>,
            _opts: Option<PutChunkOptions>,
        ) -> Result<(), StorageError> {
            let chunk_path = self.get_chunk_path(hash);

            let stream_source = AsyncReadSource::new(data, 64 * 1024);

            let payload: Payload<AsyncReadSource> = Payload::from_stream(stream_source);
            let bucket = self.config.bucket.clone();
            let object = chunk_path.clone();

            self.client
                .write_object::<_, _, Payload<AsyncReadSource>, AsyncReadSource>(
                    &bucket, &object, payload,
                )
                .send_buffered()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            Ok(())
        }

        async fn chunk_exists(&self, hash: &str) -> Result<bool, StorageError> {
            let chunk_path = self.get_chunk_path(hash);

            match self
                .client
                .read_object(&self.config.bucket, &chunk_path)
                .send()
                .await
            {
                Ok(_) => Ok(true),
                Err(e) => {
                    if let Some(404) = google_cloud_storage::Error::http_status_code(&e) {
                        Ok(false)
                    } else {
                        Err(StorageError::Other(e.to_string()))
                    }
                }
            }
        }

        async fn delete_chunk(&self, hash: &str) -> Result<(), StorageError> {
            let chunk_path = self.get_chunk_path(hash);

            let _ = self
                .control
                .delete_object()
                .set_bucket(&self.config.bucket)
                .set_object(&chunk_path)
                .send()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            Ok(())
        }

        async fn list_chunks(&self) -> Result<Option<Vec<String>>, StorageError> {
            let prefix = self
                .config
                .base
                .path_prefix
                .clone()
                .map(|p| format!("{}/chunks/", p))
                .unwrap_or_else(|| "chunks/".to_string());

            let mut all_names = Vec::new();
            let mut next_page_token: Option<String> = None;

            loop {
                let mut request = self
                    .control
                    .list_objects()
                    .set_parent(&self.config.bucket)
                    .set_prefix(&prefix);

                if let Some(token) = &next_page_token {
                    request = request.set_page_token(token);
                }

                let response = request
                    .send()
                    .await
                    .map_err(|e| StorageError::Other(e.to_string()))?;

                let mut names: Vec<String> = response
                    .objects
                    .into_iter()
                    .map(|obj| {
                        obj.name
                            .strip_prefix(&prefix)
                            .unwrap_or(&obj.name)
                            .to_string()
                    })
                    .collect();

                all_names.append(&mut names);

                if response.next_page_token.is_empty() {
                    break;
                } else {
                    next_page_token = Some(response.next_page_token);
                }
            }

            Ok(Some(all_names))
        }

        async fn get_chunk_info(&self, hash: &str) -> Result<Option<BlobInfo>, StorageError> {
            let chunk_path = self.get_chunk_path(hash);

            match self
                .client
                .read_object(&self.config.bucket, &chunk_path)
                .send()
                .await
            {
                Ok(resp) => {
                    let obj = resp.object(); // ObjectHighlights
                    Ok(Some(BlobInfo {
                        hash: hash.to_string(),
                        size: obj.size as u64,
                        metadata: None,
                        modified: Some(resp.object().generation as u64),
                    }))
                }
                Err(e) => {
                    if let Some(404) = google_cloud_storage::Error::http_status_code(&e) {
                        Ok(None)
                    } else {
                        Err(StorageError::Other(e.to_string()))
                    }
                }
            }
        }

        async fn get_remote_index(&self) -> Result<Option<RDIndex>, StorageError> {
            let path = self.get_index_path();

            let data = self
                .client
                .read_object(&self.config.bucket, &path)
                .send()
                .await;

            match data {
                Ok(response) => {
                    let bytes = response
                        .into_stream()
                        .try_fold(Vec::new(), |mut acc, chunk| async move {
                            acc.extend_from_slice(&chunk);
                            Ok(acc)
                        })
                        .await
                        .map_err(|e| StorageError::Other(e.to_string()))?;

                    let string = String::from_utf8(bytes.to_vec())
                        .map_err(|e| StorageError::Other(e.to_string()))?;

                    let index: RDIndex = serde_json::from_str(&string)
                        .map_err(|e| StorageError::Other(e.to_string()))?;

                    Ok(Some(index))
                }
                Err(e) => {
                    if let Some(404) = google_cloud_storage::Error::http_status_code(&e) {
                        Ok(None)
                    } else {
                        Err(StorageError::Other(e.to_string()))
                    }
                }
            }
        }

        async fn put_remote_index(&self, index: RDIndex) -> Result<(), StorageError> {
            let path = self.get_index_path();

            let contents = serde_json::to_string(&index)?;

            self.client
                .write_object(&self.config.bucket, &path, contents)
                .send_buffered()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;
            Ok(())
        }
    }
}
