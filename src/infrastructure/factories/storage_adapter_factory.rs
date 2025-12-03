use std::sync::Arc;

use crate::{LocalStorageAdapter, StorageAdapterEnum, StorageError, core::config::StorageConfig};

#[cfg(feature = "ssh")]
use crate::SSHStorageAdapter;

#[cfg(feature = "http")]
use crate::HTTPStorageAdapter;

#[cfg(feature = "azure")]
use crate::AzureBlobStorageAdapter;

#[cfg(feature = "s3")]
use crate::S3StorageAdapter;

#[cfg(feature = "gcs")]
use crate::GCSStorageAdapter;

#[cfg(feature = "url")]
use crate::DefaultUrlStorageAdapter;

pub struct StorageAdapterFactory;

impl StorageAdapterFactory {
    pub async fn create(config: &StorageConfig) -> Result<StorageAdapterEnum, StorageError> {
        match config {
            #[cfg(feature = "s3")]
            StorageConfig::S3(cfg) => {
                let adapter = S3StorageAdapter::new(cfg.clone()).await?;
                Ok(StorageAdapterEnum::Hash(Arc::new(adapter)))
            }
            #[cfg(not(feature = "s3"))]
            StorageConfig::S3(_) => {
                return Err(StorageError::Other("s3 feature not enabled".to_string()));
            }
            #[cfg(feature = "azure")]
            StorageConfig::Azure(cfg) => {
                let adapter = AzureBlobStorageAdapter::new(cfg.clone()).await?;
                Ok(StorageAdapterEnum::Hash(Arc::new(adapter)))
            }
            #[cfg(not(feature = "azure"))]
            StorageConfig::Azure(_) => {
                return Err(StorageError::Other("azure feature not enabled".to_string()));
            }
            #[cfg(feature = "gcs")]
            StorageConfig::GCS(cfg) => {
                let adapter = GCSStorageAdapter::new(cfg.clone()).await?;
                Ok(StorageAdapterEnum::Hash(Arc::new(adapter)))
            }
            #[cfg(not(feature = "gcs"))]
            StorageConfig::GCS(_) => {
                return Err(StorageError::Other(
                    "gcs (google cloud storage) feature not enabled".to_string(),
                ));
            }
            #[cfg(feature = "http")]
            StorageConfig::HTTP(cfg) => {
                let adapter = HTTPStorageAdapter::new(cfg.clone());
                Ok(StorageAdapterEnum::Hash(Arc::new(adapter)))
            }
            #[cfg(not(feature = "http"))]
            StorageConfig::HTTP(_) => {
                return Err(StorageError::Other("http feature not enabled".to_string()));
            }
            #[cfg(feature = "ssh")]
            StorageConfig::SSH(cfg) => {
                let adapter = SSHStorageAdapter::new(cfg.clone());
                Ok(StorageAdapterEnum::Hash(Arc::new(adapter)))
            }
            #[cfg(not(feature = "ssh"))]
            StorageConfig::SSH(_) => {
                return Err(StorageError::Other("ssh feature not enabled".to_string()));
            }
            #[cfg(feature = "url")]
            StorageConfig::URL(_) => {
                let adapter = DefaultUrlStorageAdapter::new();
                Ok(StorageAdapterEnum::Url(Arc::new(adapter)))
            }
            #[cfg(not(feature = "url"))]
            StorageConfig::URL(_) => {
                return Err(StorageError::Other("url feature not enabled".to_string()));
            }
            StorageConfig::Local(cfg) => {
                let adapter = LocalStorageAdapter::new(cfg.clone());
                Ok(StorageAdapterEnum::Hash(Arc::new(adapter)))
            }
        }
    }
}
