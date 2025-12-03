#[cfg(feature = "azure")]
pub mod azure_adapter {
    use async_trait::async_trait;

    use azure_core::credentials::TokenCredential;
    use azure_core::error::ErrorKind;
    use azure_core::http::RequestContent;
    use azure_storage_blob::{BlobContainerClient, BlobContainerClientOptions};

    use std::{any::Any, collections::HashMap, sync::Arc};

    use tokio::io::{AsyncRead, AsyncReadExt};

    use futures::{StreamExt, TryStreamExt};

    use crate::{
        AzureBlobStorageGenericConfig, AzureStorageCredential, BlobInfo, HashStorageAdapter,
        PutChunkOptions, RDIndex, StorageAdapter, StorageError,
    };

    #[derive(Clone, Debug)]
    pub struct AzureCredentialAdapter<T: TokenCredential + Clone>(pub T);

    pub trait IntoTokenCredential {
        fn into_token_credential(self) -> Arc<dyn TokenCredential>;
    }

    impl<T: TokenCredential + Clone + 'static> AzureStorageCredential for AzureCredentialAdapter<T> {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    impl<T: TokenCredential + Clone + 'static> AzureCredentialAdapter<T> {
        pub fn to_dyn_cred(&self) -> Arc<dyn TokenCredential> {
            Arc::new(self.0.clone())
        }
    }

    pub struct AzureBlobStorageAdapter {
        container_client: BlobContainerClient,
        prefix: String,
    }

    impl AzureBlobStorageAdapter {
        pub async fn new(config: AzureBlobStorageGenericConfig) -> Result<Self, StorageError> {
            let prefix = config
                .base
                .path_prefix
                .clone()
                .unwrap_or_default()
                .trim_end_matches('/')
                .to_string();

            let cred = config
                .credential
                .as_any()
                .downcast_ref::<Arc<dyn TokenCredential>>()
                .ok_or_else(|| StorageError::Other("Invalid Azure credential type".into()))?;

            let container_client = BlobContainerClient::new(
                &config.endpoint,
                config.container,
                cred.clone(),
                Some(BlobContainerClientOptions::default()),
            )
            .map_err(|e| StorageError::Other(e.to_string()))?;

            Ok(Self {
                container_client,
                prefix,
            })
        }

        fn get_chunk_path(&self, hash: &str) -> String {
            if self.prefix.is_empty() {
                format!("chunks/{}", hash)
            } else {
                format!("{}/chunks/{}", self.prefix, hash)
            }
        }

        fn get_index_path(&self) -> String {
            if self.prefix.is_empty() {
                "rd-index.json".to_string()
            } else {
                format!("{}/rd-index.json", self.prefix)
            }
        }
    }

    #[async_trait]
    impl StorageAdapter for AzureBlobStorageAdapter {
        async fn dispose(&self) {}
    }

    #[async_trait]
    impl HashStorageAdapter for AzureBlobStorageAdapter {
        async fn get_chunk(
            &self,
            hash: &str,
        ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin + 'static>>, StorageError> {
            let chunk_path = self.get_chunk_path(hash);
            let blob = self.container_client.blob_client(chunk_path);

            match blob.get_properties(None).await {
                Ok(_) => {
                    let download = blob
                        .download(None)
                        .await
                        .map_err(|e| StorageError::Other(e.to_string()))?;

                    let body = download
                        .into_body()
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));

                    let reader: Box<dyn AsyncRead + Send + Unpin + 'static> =
                        Box::new(tokio_util::io::StreamReader::new(body));

                    Ok(Some(reader))
                }
                Err(err) => {
                    match err.kind() {
                        ErrorKind::HttpResponse { status, .. } => {
                            if *status == 404 {
                                return Ok(None);
                            }
                        }
                        _ => {}
                    }
                    Err(StorageError::Other(err.to_string()))
                }
            }
        }

        async fn put_chunk(
            &self,
            hash: &str,
            mut data: Box<dyn AsyncRead + Send + Unpin + 'static>,
            opts: Option<PutChunkOptions>,
        ) -> Result<(), StorageError> {
            if let Some(ref o) = opts {
                if o.overwrite == Some(false) && self.chunk_exists(hash).await? {
                    return Ok(());
                }
            }

            let chunk_path = self.get_chunk_path(hash);

            let chunk_size = match opts.as_ref().and_then(|o| o.size) {
                Some(size) => size,
                None => return Err(StorageError::Other("chunk size not specified".to_string())),
            };

            let mut buffer = Vec::new();
            data.read_to_end(&mut buffer).await?;

            let request_content = RequestContent::from(buffer);

            // Another Rust SDK that lacks of stream support... Or at least idk how to do it, nice
            self.container_client
                .blob_client(chunk_path)
                .upload(request_content, true, chunk_size, None)
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            Ok(())
        }

        async fn chunk_exists(&self, hash: &str) -> Result<bool, StorageError> {
            let chunk_path = self.get_chunk_path(hash);
            let blob = self.container_client.blob_client(chunk_path);

            match blob.get_properties(None).await {
                Ok(_) => Ok(true),
                Err(err) => {
                    match err.kind() {
                        ErrorKind::HttpResponse { status, .. } => {
                            if *status == 404 {
                                return Ok(false);
                            }
                        }
                        _ => {}
                    }
                    Err(StorageError::Other(err.to_string()))
                }
            }
        }

        async fn delete_chunk(&self, hash: &str) -> Result<(), StorageError> {
            let chunk_path = self.get_chunk_path(hash);
            let blob = self.container_client.blob_client(chunk_path);

            match blob.delete(None).into_future().await {
                Ok(_) => Ok(()),
                Err(err) => {
                    match err.kind() {
                        ErrorKind::HttpResponse { status, .. } => {
                            if *status == 404 {
                                return Ok(());
                            }
                        }
                        _ => {}
                    }
                    Err(StorageError::Other(err.to_string()))
                }
            }
        }

        async fn list_chunks(&self) -> Result<Option<Vec<String>>, StorageError> {
            let prefix = if self.prefix.is_empty() {
                "chunks/".to_string()
            } else {
                format!("{}/chunks/", self.prefix)
            };

            let mut names = Vec::new();

            let options =
                azure_storage_blob::models::BlobContainerClientListBlobFlatSegmentOptions {
                    prefix: Some(prefix.clone()),
                    ..Default::default()
                };

            let mut stream = self
                .container_client
                .list_blobs(Some(options))
                .map_err(|e| StorageError::Other(e.to_string()))?
                .into_stream();

            while let Some(next) = stream.next().await {
                let page = next.map_err(|e| StorageError::Other(e.to_string()))?;

                let body = page
                    .into_body()
                    .map_err(|e| StorageError::Other(e.to_string()))?;

                for blob in body.segment.blob_items {
                    if let Some(clean) = blob.name.and_then(|n| {
                        n.content
                            .map(|c| c.strip_prefix(&prefix).unwrap_or(&c).to_string())
                    }) {
                        names.push(clean);
                    }
                }
            }

            Ok(Some(names))
        }

        async fn get_chunk_info(&self, hash: &str) -> Result<Option<BlobInfo>, StorageError> {
            let chunk_path = self.get_chunk_path(hash);
            let blob = self.container_client.blob_client(chunk_path);

            match blob.get_properties(None).await {
                Ok(resp) => {
                    let body = resp.into_raw_body();
                    let json_str = body
                        .into_string()
                        .map_err(|e| StorageError::Other(e.to_string()))?;

                    let v: serde_json::Value = serde_json::from_str(&json_str)?;

                    let size = v["content_length"].as_u64().unwrap_or_default();

                    let metadata = v["metadata"].as_object().map(|map| {
                        map.iter()
                            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                            .collect::<HashMap<_, _>>()
                    });

                    Ok(Some(BlobInfo {
                        hash: hash.to_string(),
                        size,
                        modified: None,
                        metadata,
                    }))
                }
                Err(err) => {
                    match err.kind() {
                        ErrorKind::HttpResponse { status, .. } => {
                            if *status == 404 {
                                return Ok(None);
                            }
                        }
                        _ => {}
                    }
                    Err(StorageError::Other(err.to_string()))
                }
            }
        }

        async fn put_remote_index(&self, index: RDIndex) -> Result<(), StorageError> {
            let key = self.get_index_path();
            let blob = self.container_client.blob_client(key);

            let json_bytes =
                serde_json::to_vec_pretty(&index).map_err(|e| StorageError::SerdeJson(e))?;

            let request_content = RequestContent::from(json_bytes.clone());

            blob.upload(request_content, true, json_bytes.len() as u64, None)
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            Ok(())
        }

        async fn get_remote_index(&self) -> Result<Option<RDIndex>, StorageError> {
            let key = self.get_index_path();
            let blob = self.container_client.blob_client(key);

            match blob.get_properties(None).await {
                Ok(_) => {
                    let download = blob
                        .download(None)
                        .await
                        .map_err(|e| StorageError::Other(e.to_string()))?;

                    let body = download
                        .into_body()
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));

                    let mut reader = tokio_util::io::StreamReader::new(body);

                    let mut collected = Vec::new();
                    reader
                        .read_to_end(&mut collected)
                        .await
                        .map_err(|e| StorageError::Other(e.to_string()))?;

                    let idx: RDIndex = serde_json::from_slice(&collected)
                        .map_err(|e| StorageError::Other(e.to_string()))?;

                    Ok(Some(idx))
                }
                Err(err) => {
                    match err.kind() {
                        ErrorKind::HttpResponse { status, .. } => {
                            if *status == 404 {
                                return Ok(None);
                            }
                        }
                        _ => {}
                    }
                    Err(StorageError::Other(err.to_string()))
                }
            }
        }
    }
}
