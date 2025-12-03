use std::any::Any;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct BaseStorageConfig {
    /// Example: my-prefix/updates/42
    pub path_prefix: Option<String>,
}

#[derive(Debug, Clone)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub expiration: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct S3StorageConfig {
    pub base: BaseStorageConfig,
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub credentials: S3Credentials,
}

pub trait AzureStorageCredential: Any + std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug, Clone)]
pub struct AzureBlobStorageConfig<T: AzureStorageCredential + Clone> {
    pub base: BaseStorageConfig,
    pub container: String,
    pub endpoint: String,
    pub account_url: String,
    /// Credential used for authenticating with Azure Blob Storage, it must implement TokenCredential trait from Azure SDK.
    /// For now, SAS token and Account Key credentials are not supported in Rust
    pub credential: T,
}

#[derive(Debug, Clone)]
pub struct AzureBlobStorageGenericConfig {
    pub base: BaseStorageConfig,
    pub container: String,
    pub endpoint: String,
    pub account_url: String,
    pub credential: Arc<dyn AzureStorageCredential>,
}

impl<T> From<AzureBlobStorageConfig<T>> for AzureBlobStorageGenericConfig
where
    T: AzureStorageCredential + Clone + 'static,
{
    fn from(cfg: AzureBlobStorageConfig<T>) -> Self {
        Self {
            base: cfg.base,
            container: cfg.container,
            endpoint: cfg.endpoint,
            account_url: cfg.account_url,
            credential: Arc::new(cfg.credential),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GCSCredentials {
    pub project_id: String,
    pub client_email: String,
    pub private_key: String,
}

#[derive(Debug, Clone)]
pub struct GCSStorageConfig {
    pub base: BaseStorageConfig,
    pub bucket: String,
    pub api_endpoint: Option<String>,
    pub credentials: GCSCredentials,
}

#[derive(Debug, Clone)]
pub struct HTTPCredentials {
    pub bearer_token: Option<String>,
    pub api_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HTTPStorageConfig {
    pub base: BaseStorageConfig,

    /// * Base URL used for all chunk operations (GET, PUT, DELETE, HEAD). * * Each chunk will be accessed as: *
    /// {endpoint}/{pathPrefix?}/{chunk-hash}
    ///
    /// * Example: *
    /// endpoint: "https://ducks.com/api"
    /// pathPrefix: "uploads"
    ///
    /// => https://ducks.com/api/uploads/{chunk-hash}
    ///
    /// * This should point to an HTTP server or compatible API that supports * binary upload and download via standard HTTP verbs. */
    pub endpoint: String,

    /// * Optional path (relative to endpoint and pathPrefix) where the remote * index file (rd-index.json) can be found. * * If omitted, the adapter will automatically look for: *
    /// {endpoint}/{pathPrefix?}/rd-index.json
    ///
    /// * Example: *
    /// indexFilePath: "index"
    /// => https://ducks.com/api/index
    ///
    /// indexFilePath: "metadata/rd-index.json"
    /// => https://ducks.com/api/metadata/rd-index.json
    ///
    ///
    pub index_file_path: Option<String>,

    /// * Optional credentials used for authenticated HTTP requests. * * Supports both Bearer tokens and API keys. * * Example: *
    /// credentials: {
    ///   bearerToken: "eyJhbGciOiJIUzI1...",
    ///   apiKey: "my-secret-key"
    /// }
    ///
    /// * The adapter automatically includes the corresponding headers: * - Authorization: Bearer <token> * - x-api-key: <apiKey> */
    pub credentials: Option<HTTPCredentials>,
}

#[derive(Debug, Clone)]
pub struct URLStorageConfig {
    pub base: BaseStorageConfig,
}

#[derive(Debug, Clone)]
pub struct SSHCredentials {
    pub username: String,
    pub password: Option<String>,
    /// Optional path to private key file content
    pub private_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SSHStorageConfig {
    pub base: BaseStorageConfig,
    pub host: String,
    pub port: Option<u16>,
    pub credentials: SSHCredentials,
}

#[derive(Debug, Clone)]
pub struct LocalStorageConfig {
    pub base: BaseStorageConfig,
    pub base_path: PathBuf,
}

#[derive(Debug, Clone)]
pub enum StorageConfig {
    S3(S3StorageConfig),
    Azure(AzureBlobStorageGenericConfig),
    GCS(GCSStorageConfig),
    HTTP(HTTPStorageConfig),
    SSH(SSHStorageConfig),
    Local(LocalStorageConfig),
    URL(URLStorageConfig),
}

#[derive(Debug, Clone)]
pub struct RacDeltaConfig {
    /// Recommended: 1MB
    pub chunk_size: usize,
    /// Max concurrency of workers like upload chunks, delete chunks, etc. Each service has default values
    pub max_concurrency: Option<usize>,
    pub storage: StorageConfig,
}
