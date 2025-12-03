pub mod local_storage_adapter;
pub use local_storage_adapter::*;

#[cfg(feature = "url")]
pub mod url_storage_adapter;
#[cfg(feature = "url")]
pub use url_storage_adapter::url_adapter::DefaultUrlStorageAdapter;

#[cfg(feature = "ssh")]
pub mod ssh_storage_adapter;
#[cfg(feature = "ssh")]
pub use ssh_storage_adapter::ssh_adapter::SSHStorageAdapter;

#[cfg(feature = "http")]
pub mod http_storage_adapter;
#[cfg(feature = "http")]
pub use http_storage_adapter::http_adapter::HTTPStorageAdapter;

#[cfg(feature = "s3")]
pub mod s3_storage_adapter;
#[cfg(feature = "s3")]
pub use s3_storage_adapter::s3_adapter::S3StorageAdapter;

#[cfg(feature = "azure")]
pub mod azure_blob_storage_adapter;
#[cfg(feature = "azure")]
pub use azure_blob_storage_adapter::azure_adapter::AzureBlobStorageAdapter;

#[cfg(feature = "gcs")]
pub mod gcs_storage_adapter;
#[cfg(feature = "gcs")]
pub use gcs_storage_adapter::gcs_adapter::GCSStorageAdapter;
