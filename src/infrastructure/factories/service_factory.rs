use std::sync::Arc;

use crate::{
    Blake3HasherService, DeltaService, HasherService, MemoryDeltaService,
    MemoryReconstructionService, MemoryValidationService, ReconstructionService, ValidationService,
};

pub struct ServiceBundle {
    pub delta: Arc<dyn DeltaService + Send + Sync>,
    pub hasher: Arc<dyn HasherService + Send + Sync>,
    pub validation: Arc<dyn ValidationService + Send + Sync>,
    pub reconstruction: Arc<dyn ReconstructionService + Send + Sync>,
}

pub struct ServiceFactory;

impl ServiceFactory {
    pub fn create() -> ServiceBundle {
        let hasher: Arc<dyn HasherService + Send + Sync> = Arc::new(Blake3HasherService::new());

        ServiceBundle {
            delta: Arc::new(MemoryDeltaService::new(hasher.clone())),
            validation: Arc::new(MemoryValidationService::new(hasher.clone())),
            reconstruction: MemoryReconstructionService::new(hasher.clone()),
            hasher,
        }
    }
}
