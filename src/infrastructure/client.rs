use std::sync::Arc;

use crate::{
    DeltaService, HasherService, PipelineBundle, PipelineFactory, RacDeltaConfig,
    ReconstructionService, ServiceFactory, StorageAdapterEnum, StorageAdapterFactory, StorageError,
    ValidationService,
};

/// Main entry point of the RacDelta SDK.
///
/// `RacDeltaClient` acts as a high-level orchestrator that initializes all
/// core services, adapters, and pipelines used for differential upload and
/// download operations.
///
/// It is designed to provide a single, easy-to-use API for performing
/// file synchronization, hashing, validation, and reconstruction.
///
/// # Example
/// ```ignore
/// use rac_delta::client::RacDeltaClient;
/// use rac_delta::config::{RacDeltaConfig, StorageConfig};
///
/// let config = RacDeltaConfig {
///     chunk_size: 1024 * 1024,
///     max_concurrency: Some(4),
///     storage: StorageConfig::Local(LocalStorageConfig {
///         base: BaseStorageConfig { path_prefix: None },
///         base_path: "./remote".into(),
///     }),
/// };
///
/// let client = RacDeltaClient::new(config).await.unwrap();
/// ```
pub struct RacDeltaClient {
    pub config: RacDeltaConfig,
    pub storage: StorageAdapterEnum,
    pub delta: Arc<dyn DeltaService + Send + Sync>,
    pub hasher: Arc<dyn HasherService + Send + Sync>,
    pub validation: Arc<dyn ValidationService + Send + Sync>,
    pub reconstruction: Arc<dyn ReconstructionService + Send + Sync>,
    pub pipelines: PipelineBundle,
}

impl RacDeltaClient {
    /// Create a new `RacDeltaClient` with the given configuration.
    pub async fn new(config: RacDeltaConfig) -> Result<Self, StorageError> {
        let storage_enum = StorageAdapterFactory::create(&config.storage).await?;

        let services = ServiceFactory::create();

        let pipelines =
            PipelineFactory::create(storage_enum.clone(), &services, Arc::new(config.clone()));

        Ok(Self {
            config,
            storage: storage_enum,
            delta: services.delta,
            hasher: services.hasher,
            validation: services.validation,
            reconstruction: services.reconstruction,
            pipelines,
        })
    }
}
