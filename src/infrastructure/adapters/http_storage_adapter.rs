#[cfg(feature = "http")]
pub mod http_adapter {
    use async_trait::async_trait;

    use futures::TryStreamExt;

    use reqwest::Client;

    use tokio::io::{AsyncRead, AsyncReadExt};
    use tokio_util::io::StreamReader;

    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::{
        BlobInfo, HTTPStorageConfig, HashStorageAdapter, PutChunkOptions, RDIndex, StorageAdapter,
        StorageError,
    };

    pub struct HTTPStorageAdapter {
        client: Client,
        base_url: String,
        token: Option<String>,
        api_key: Option<String>,
        config: Arc<HTTPStorageConfig>,
    }

    impl HTTPStorageAdapter {
        pub fn new(config: HTTPStorageConfig) -> Self {
            let client = Client::new();

            let base_url = config.endpoint.trim_end_matches('/').to_string();

            let token = config
                .credentials
                .as_ref()
                .and_then(|c| c.bearer_token.clone());

            let api_key = config.credentials.as_ref().and_then(|c| c.api_key.clone());

            Self {
                client,
                base_url,
                token,
                api_key,
                config: Arc::new(config),
            }
        }

        fn build_url(&self, hash: &str) -> String {
            let prefix = self
                .config
                .base
                .path_prefix
                .as_ref()
                .map(|p| format!("/{}", p.trim_matches('/')))
                .unwrap_or_default();

            let base_url = self.base_url.clone();

            format!("{base_url}{prefix}/{}", urlencoding::encode(hash))
        }

        fn build_index_url(&self) -> String {
            let prefix = self
                .config
                .base
                .path_prefix
                .as_ref()
                .map(|p| format!("/{}", p.trim_matches('/')))
                .unwrap_or_default();

            let index_file = self
                .config
                .index_file_path
                .clone()
                .unwrap_or_else(|| "rd-index.json".to_string());

            let base_url = self.base_url.clone();

            format!("{base_url}{prefix}/{}", index_file)
        }

        fn headers(&self, extra: Option<HashMap<&str, &str>>) -> reqwest::header::HeaderMap {
            let mut headers = reqwest::header::HeaderMap::new();

            if let Some(t) = &self.token {
                headers.insert(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {}", t).parse().unwrap(),
                );
            }

            if let Some(k) = &self.api_key {
                headers.insert("x-api-key", k.parse().unwrap());
            }

            if let Some(extra) = extra {
                for (k, v) in extra.iter() {
                    headers.insert(
                        reqwest::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                        reqwest::header::HeaderValue::from_str(v).unwrap(),
                    );
                }
            }

            headers
        }
    }

    #[async_trait]
    impl StorageAdapter for HTTPStorageAdapter {
        async fn dispose(&self) {}
    }

    #[async_trait]
    impl HashStorageAdapter for HTTPStorageAdapter {
        async fn get_chunk(
            &self,
            hash: &str,
        ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin>>, StorageError> {
            let url = self.build_url(hash);

            let resp = self
                .client
                .get(&url)
                .headers(self.headers(None))
                .send()
                .await
                .map_err(|e| StorageError::Other(format!("HTTP GET error: {}", e)))?;

            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }

            if !resp.status().is_success() {
                return Err(StorageError::Other(format!(
                    "GET chunk failed: {} {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                )));
            }

            let stream = resp
                .bytes_stream()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

            let reader = StreamReader::new(stream);

            Ok(Some(Box::new(reader)))
        }

        async fn put_chunk(
            &self,
            hash: &str,
            mut data: Box<dyn AsyncRead + Send + Unpin>,
            _opts: Option<PutChunkOptions>,
        ) -> Result<(), StorageError> {
            let url = self.build_url(hash);

            let mut buf = Vec::new();
            data.read_to_end(&mut buf).await.map_err(StorageError::Io)?;

            let resp = self
                .client
                .put(&url)
                .headers(self.headers(Some(HashMap::from([(
                    "Content-Type",
                    "application/octet-stream",
                )]))))
                .body(buf)
                .send()
                .await
                .map_err(|e| StorageError::Other(format!("HTTP PUT error: {}", e)))?;

            if !resp.status().is_success() {
                return Err(StorageError::Other(format!(
                    "PUT chunk failed: {} {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                )));
            }

            Ok(())
        }

        async fn chunk_exists(&self, hash: &str) -> Result<bool, StorageError> {
            let url = self.build_url(hash);
            let resp = self
                .client
                .head(&url)
                .headers(self.headers(None))
                .send()
                .await
                .map_err(|e| StorageError::Other(format!("HEAD error: {}", e)))?;

            match resp.status() {
                reqwest::StatusCode::NOT_FOUND => Ok(false),
                s if s.is_success() => Ok(true),
                s => Err(StorageError::Other(format!("HEAD failed: {}", s))),
            }
        }

        async fn delete_chunk(&self, hash: &str) -> Result<(), StorageError> {
            let url = self.build_url(hash);
            let resp = self
                .client
                .delete(&url)
                .headers(self.headers(None))
                .send()
                .await
                .map_err(|e| StorageError::Other(format!("DELETE error: {}", e)))?;

            if resp.status().is_success() || resp.status() == reqwest::StatusCode::NOT_FOUND {
                Ok(())
            } else {
                Err(StorageError::Other(format!(
                    "DELETE failed: {}",
                    resp.status()
                )))
            }
        }

        async fn get_chunk_info(&self, hash: &str) -> Result<Option<BlobInfo>, StorageError> {
            let url = self.build_url(hash);
            let resp = self
                .client
                .head(&url)
                .headers(self.headers(None))
                .send()
                .await
                .map_err(|e| StorageError::Other(format!("HEAD error: {}", e)))?;

            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }

            if !resp.status().is_success() {
                return Err(StorageError::Other(format!(
                    "HEAD failed: {}",
                    resp.status()
                )));
            }

            let size = resp
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            let mut metadata = HashMap::new();
            for (k, v) in resp.headers().iter() {
                let k_str = k.as_str();
                if k_str.starts_with("x-meta-") {
                    if let Ok(v_str) = v.to_str() {
                        metadata.insert(k_str[7..].to_string(), v_str.to_string());
                    }
                }
            }

            Ok(Some(BlobInfo {
                hash: hash.to_string(),
                size,
                modified: None, //TODO: Use chrono to parse last-modified header?
                metadata: Some(metadata),
            }))
        }

        async fn get_remote_index(&self) -> Result<Option<RDIndex>, StorageError> {
            let url = self.build_index_url();

            let resp = self
                .client
                .get(&url)
                .headers(self.headers(None))
                .send()
                .await
                .map_err(|e| StorageError::Other(format!("GET index error: {}", e)))?;

            match resp.status() {
                reqwest::StatusCode::NOT_FOUND => Ok(None),
                s if s.is_success() => {
                    let bytes = resp
                        .bytes()
                        .await
                        .map_err(|e| StorageError::Other(format!("Failed to read bytes: {}", e)))?;

                    let index: RDIndex = serde_json::from_slice(&bytes)?;
                    Ok(Some(index))
                }
                s => Err(StorageError::Other(format!("GET index failed: {}", s))),
            }
        }

        async fn put_remote_index(&self, index: RDIndex) -> Result<(), StorageError> {
            let url = self.build_index_url();

            let body = serde_json::to_vec(&index)?;
            let resp = self
                .client
                .put(&url)
                .headers(self.headers(Some(HashMap::from([("Content-Type", "application/json")]))))
                .body(body)
                .send()
                .await
                .map_err(|e| StorageError::Other(format!("PUT index error: {}", e)))?;

            if resp.status().is_success() {
                Ok(())
            } else {
                Err(StorageError::Other(format!(
                    "PUT index failed: {}",
                    resp.status()
                )))
            }
        }
    }
}
