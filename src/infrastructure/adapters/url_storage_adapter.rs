#[cfg(feature = "url")]
pub mod url_adapter {
    use async_trait::async_trait;

    use futures::TryStreamExt;

    use reqwest::Client;

    use std::collections::HashMap;
    use std::sync::Arc;

    use tokio::io::{AsyncRead, AsyncReadExt};
    use tokio_util::io::StreamReader;

    use crate::{BlobInfo, RDIndex, StorageAdapter, StorageError, UrlStorageAdapter};

    #[derive(Clone)]
    pub struct DefaultUrlStorageAdapter {
        client: Arc<Client>,
    }

    impl DefaultUrlStorageAdapter {
        pub fn new() -> Self {
            Self {
                client: Arc::new(Client::new()),
            }
        }
    }

    #[async_trait]
    impl StorageAdapter for DefaultUrlStorageAdapter {
        async fn dispose(&self) {}
    }

    #[async_trait]
    impl UrlStorageAdapter for DefaultUrlStorageAdapter {
        async fn get_chunk_by_url(
            &self,
            url: &str,
        ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin + 'static>>, StorageError> {
            let res = self
                .client
                .get(url)
                .send()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            if res.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }

            if !res.status().is_success() {
                return Err(StorageError::Other(format!(
                    "GET {} failed: {}",
                    url,
                    res.status()
                )));
            }

            let stream = res
                .bytes_stream()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

            let reader = StreamReader::new(stream);

            Ok(Some(Box::new(reader)))
        }

        async fn put_chunk_by_url(
            &self,
            url: &str,
            mut data: Box<dyn AsyncRead + Send + Unpin + 'static>,
        ) -> Result<(), StorageError> {
            let mut buf = Vec::new();
            data.read_to_end(&mut buf).await.map_err(StorageError::Io)?;

            let res = self
                .client
                .put(url)
                .header("Content-Type", "application/octet-stream")
                .body(buf)
                .send()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            if !res.status().is_success() {
                return Err(StorageError::Other(format!(
                    "PUT {} failed: {}",
                    url,
                    res.status()
                )));
            }

            Ok(())
        }

        async fn delete_chunk_by_url(&self, url: &str) -> Result<(), StorageError> {
            let res = self
                .client
                .delete(url)
                .send()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            if res.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(());
            }

            if !res.status().is_success() {
                return Err(StorageError::Other(format!(
                    "DELETE {} failed: {}",
                    url,
                    res.status()
                )));
            }

            Ok(())
        }

        async fn chunk_exists_by_url(&self, url: &str) -> Result<bool, StorageError> {
            let res = self
                .client
                .head(url)
                .send()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            match res.status() {
                reqwest::StatusCode::NOT_FOUND => Ok(false),
                status if status.is_success() => Ok(true),
                status => Err(StorageError::Other(format!(
                    "HEAD {} failed: {}",
                    url, status
                ))),
            }
        }

        async fn get_chunk_info_by_url(
            &self,
            hash: &str,
            url: &str,
        ) -> Result<Option<BlobInfo>, StorageError> {
            let res = self
                .client
                .head(url)
                .send()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            if res.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }

            if !res.status().is_success() {
                return Err(StorageError::Other(format!(
                    "HEAD {} failed: {}",
                    url,
                    res.status()
                )));
            }

            let size = res
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            let mut metadata = HashMap::new();
            for (key, value) in res.headers().iter() {
                if let Some(k) = key.as_str().strip_prefix("x-meta-") {
                    if let Ok(v) = value.to_str() {
                        metadata.insert(k.to_string(), v.to_string());
                    }
                }
            }

            Ok(Some(BlobInfo {
                hash: hash.to_string(),
                size,
                modified: None,
                metadata: if metadata.is_empty() {
                    None
                } else {
                    Some(metadata)
                },
            }))
        }

        async fn get_remote_index_by_url(
            &self,
            url: &str,
        ) -> Result<Option<RDIndex>, StorageError> {
            let res = self
                .client
                .get(url)
                .send()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            if res.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }

            if !res.status().is_success() {
                return Err(StorageError::Other(format!(
                    "GET {} failed: {}",
                    url,
                    res.status()
                )));
            }

            let bytes = res
                .bytes()
                .await
                .map_err(|e| StorageError::Other(format!("Failed to read bytes: {}", e)))?;

            let index: RDIndex = serde_json::from_slice(&bytes)?;
            Ok(Some(index))
        }

        async fn put_remote_index_by_url(
            &self,
            url: &str,
            index: RDIndex,
        ) -> Result<(), StorageError> {
            let res = self
                .client
                .put(url)
                .header("Content-Type", "application/json")
                .json(&index)
                .send()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            if !res.status().is_success() {
                return Err(StorageError::Other(format!(
                    "PUT {} failed: {}",
                    url,
                    res.status()
                )));
            }

            Ok(())
        }
    }
}
