#[cfg(feature = "s3")]
pub mod s3_adapter {
    use async_trait::async_trait;

    use aws_sdk_s3::config::Region;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::{Client as S3Client, Config as AwsConfig};

    use std::collections::HashMap;

    use tokio::io::{AsyncRead, AsyncReadExt};

    use crate::{
        BlobInfo, HashStorageAdapter, PutChunkOptions, RDIndex, S3StorageConfig, StorageAdapter,
        StorageError,
    };

    pub struct S3StorageAdapter {
        client: S3Client,
        config: S3StorageConfig,
    }

    impl S3StorageAdapter {
        pub async fn new(config: S3StorageConfig) -> Result<Self, StorageError> {
            let region = config.region.clone().unwrap_or("us-east-1".into());

            let creds = aws_sdk_s3::config::Credentials::new(
                &config.credentials.access_key_id,
                &config.credentials.secret_access_key,
                config.credentials.session_token.clone(),
                config
                    .credentials
                    .expiration
                    .map(|e| std::time::UNIX_EPOCH + std::time::Duration::from_secs(e)),
                "custom",
            );

            let mut aws_config = AwsConfig::builder()
                .region(Region::new(region))
                .credentials_provider(creds);

            if let Some(endpoint) = &config.endpoint {
                aws_config = aws_config.endpoint_url(endpoint);
            }

            let aws_config = aws_config.build();

            let client = S3Client::from_conf(aws_config);

            Ok(Self { client, config })
        }

        fn resolve_key(&self, hash: &str) -> String {
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

        fn index_key(&self) -> String {
            self.config.base.path_prefix.as_ref().map_or_else(
                || "rd-index.json".to_string(),
                |p| format!("{}/rd-index.json", p),
            )
        }
    }

    #[async_trait]
    impl StorageAdapter for S3StorageAdapter {
        async fn dispose(&self) {}
    }

    #[async_trait]
    impl HashStorageAdapter for S3StorageAdapter {
        async fn get_chunk(
            &self,
            hash: &str,
        ) -> Result<Option<Box<dyn AsyncRead + Send + Unpin + 'static>>, StorageError> {
            let key = self.resolve_key(hash);
            let res = self
                .client
                .get_object()
                .bucket(&self.config.bucket)
                .key(&key)
                .send()
                .await;

            match res {
                Ok(output) => {
                    let body = output.body.into_async_read();
                    Ok(Some(Box::new(body)))
                }

                Err(aws_sdk_s3::error::SdkError::ServiceError(service_err)) => {
                    let err = service_err.err();
                    if matches!(
                        err,
                        aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_)
                    ) {
                        Ok(None)
                    } else {
                        Err(StorageError::Other(err.to_string()))
                    }
                }

                Err(e) => Err(StorageError::Other(e.to_string())),
            }
        }

        // For now, aws-sdk-s3 does not support streaming upload from AsyncRead,
        // so we read the entire content into memory before uploading
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

            let key = self.resolve_key(hash);
            let mut buf = Vec::new();
            data.read_to_end(&mut buf).await?;
            let body = ByteStream::from(buf);

            self.client
                .put_object()
                .bucket(&self.config.bucket)
                .key(&key)
                .body(body) // Why no streaming in Rust, Amazon? :(
                .content_length(opts.and_then(|o| o.size).unwrap_or_default() as i64)
                .send()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            Ok(())
        }

        async fn chunk_exists(&self, hash: &str) -> Result<bool, StorageError> {
            let key = self.resolve_key(hash);
            let res = self
                .client
                .head_object()
                .bucket(&self.config.bucket)
                .key(&key)
                .send()
                .await;

            Ok(res.is_ok())
        }

        async fn delete_chunk(&self, hash: &str) -> Result<(), StorageError> {
            let key = self.resolve_key(hash);
            self.client
                .delete_object()
                .bucket(&self.config.bucket)
                .key(&key)
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
                .map(|p| format!("{}/chunks", p));

            let mut chunks = Vec::new();
            let mut continuation: Option<String> = None;

            loop {
                let mut req = self.client.list_objects_v2().bucket(&self.config.bucket);

                if let Some(p) = &prefix {
                    req = req.prefix(p);
                }

                if let Some(ref t) = continuation {
                    req = req.continuation_token(t);
                }

                let res = req
                    .send()
                    .await
                    .map_err(|e| StorageError::Other(e.to_string()))?;
                if let Some(objects) = res.contents {
                    for obj in objects {
                        if let Some(k) = obj.key {
                            let clean = if let Some(prefix) = &self.config.base.path_prefix {
                                k.strip_prefix(&format!("{}/chunks/", prefix))
                                    .unwrap_or(&k)
                                    .to_string()
                            } else {
                                k
                            };
                            chunks.push(clean);
                        }
                    }
                }

                continuation = res.next_continuation_token;
                if continuation.is_none() {
                    break;
                }
            }

            Ok(Some(chunks))
        }

        async fn get_chunk_info(&self, hash: &str) -> Result<Option<BlobInfo>, StorageError> {
            let key = self.resolve_key(hash);
            let res = self
                .client
                .head_object()
                .bucket(&self.config.bucket)
                .key(&key)
                .send()
                .await;

            match res {
                Ok(obj) => Ok(Some(BlobInfo {
                    hash: hash.to_string(),
                    size: obj.content_length.unwrap_or_default() as u64,
                    modified: obj.last_modified.map(|d| d.secs() as u64),
                    metadata: obj.metadata.map(|m| {
                        m.into_iter()
                            .map(|(k, v)| (k, v))
                            .collect::<HashMap<String, String>>()
                    }),
                })),
                Err(aws_sdk_s3::error::SdkError::ServiceError(service_err)) => {
                    let err = service_err.err();
                    if matches!(
                        err,
                        aws_sdk_s3::operation::head_object::HeadObjectError::NotFound(_)
                    ) {
                        Ok(None)
                    } else {
                        Err(StorageError::Other(err.to_string()))
                    }
                }

                Err(e) => Err(StorageError::Other(e.to_string())),
            }
        }

        async fn put_remote_index(&self, index: RDIndex) -> Result<(), StorageError> {
            let key = self.index_key();
            let json = serde_json::to_vec(&index)?;
            let stream = ByteStream::from(json);

            self.client
                .put_object()
                .bucket(&self.config.bucket)
                .key(&key)
                .body(stream)
                .content_type("application/json")
                .send()
                .await
                .map_err(|e| StorageError::Other(e.to_string()))?;

            Ok(())
        }

        async fn get_remote_index(&self) -> Result<Option<RDIndex>, StorageError> {
            let key = self.index_key();
            let res = self
                .client
                .get_object()
                .bucket(&self.config.bucket)
                .key(&key)
                .send()
                .await;

            match res {
                Ok(obj) => {
                    let data = obj
                        .body
                        .collect()
                        .await
                        .map_err(|e| StorageError::Other(e.to_string()))?;
                    let index: RDIndex = serde_json::from_slice(&data.into_bytes())?;
                    Ok(Some(index))
                }
                Err(aws_sdk_s3::error::SdkError::ServiceError(service_err)) => {
                    let err = service_err.err();
                    if matches!(
                        err,
                        aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_)
                    ) {
                        Ok(None)
                    } else {
                        Err(StorageError::Other(err.to_string()))
                    }
                }

                Err(e) => Err(StorageError::Other(e.to_string())),
            }
        }
    }
}
