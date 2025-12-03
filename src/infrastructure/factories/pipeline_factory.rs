use std::sync::Arc;

use crate::{
    DefaultHashDownloadPipeline, DefaultHashUploadPipeline, DefaultUrlDownloadPipeline,
    DefaultUrlUploadPipeline, HashDownloadPipeline, HashUploadPipeline, RacDeltaConfig,
    ServiceBundle, StorageAdapterEnum, UrlDownloadPipeline, UrlUploadPipeline,
};

pub enum UploadPipelineBundle {
    Hash(Arc<dyn HashUploadPipeline>),
    Url(Arc<dyn UrlUploadPipeline>),
}

pub enum DownloadPipelineBundle {
    Hash(Arc<dyn HashDownloadPipeline>),
    Url(Arc<dyn UrlDownloadPipeline>),
}

pub struct PipelineBundle {
    pub upload: UploadPipelineBundle,
    pub download: DownloadPipelineBundle,
}

pub struct PipelineFactory;

impl PipelineFactory {
    pub fn create(
        storage: StorageAdapterEnum,
        services: &ServiceBundle,
        config: Arc<RacDeltaConfig>,
    ) -> PipelineBundle {
        match storage {
            StorageAdapterEnum::Hash(hash_storage) => PipelineBundle {
                upload: UploadPipelineBundle::Hash(Arc::new(DefaultHashUploadPipeline::new(
                    hash_storage.clone(),
                    services.delta.clone(),
                    config.clone(),
                ))),
                download: DownloadPipelineBundle::Hash(Arc::new(DefaultHashDownloadPipeline::new(
                    services.reconstruction.clone(),
                    services.validation.clone(),
                    hash_storage,
                    config,
                    services.delta.clone(),
                ))),
            },

            StorageAdapterEnum::Url(url_storage) => PipelineBundle {
                upload: UploadPipelineBundle::Url(Arc::new(DefaultUrlUploadPipeline::new(
                    url_storage.clone(),
                    config.clone(),
                ))),
                download: DownloadPipelineBundle::Url(Arc::new(DefaultUrlDownloadPipeline::new(
                    services.reconstruction.clone(),
                    services.validation.clone(),
                    url_storage,
                    config,
                    services.delta.clone(),
                ))),
            },
        }
    }
}
