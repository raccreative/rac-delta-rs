use async_trait::async_trait;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::fs::{self};
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;

use crate::{
    ChunkEntry, ChunkSource, ChunkUrlInfo, DeltaPlan, DeltaService, DiskChunkSource, DownloadError,
    DownloadOptions, DownloadPhase, DownloadResult, DownloadState, DownloadTarget, DownloadUrls,
    FileVerificationResult, MemoryChunkSource, RDIndex, RacDeltaConfig, ReconstructionOptions,
    ReconstructionService, StorageAdapterEnum, StorageChunkSource, UpdateStrategy,
    UrlDownloadPipeline, UrlStorageAdapter, ValidationService,
};

pub struct DefaultUrlDownloadPipeline {
    storage: Arc<dyn UrlStorageAdapter + Send + Sync>,
    reconstruction: Arc<dyn ReconstructionService + Send + Sync>,
    validation: Arc<dyn ValidationService + Send + Sync>,
    delta: Arc<dyn DeltaService + Send + Sync>,
    config: Arc<RacDeltaConfig>,
}

impl DefaultUrlDownloadPipeline {
    pub fn new(
        reconstruction: Arc<dyn ReconstructionService + Send + Sync>,
        validation: Arc<dyn ValidationService + Send + Sync>,
        storage: Arc<dyn UrlStorageAdapter + Send + Sync>,
        config: Arc<RacDeltaConfig>,
        delta: Arc<dyn DeltaService + Send + Sync>,
    ) -> Self {
        Self {
            reconstruction,
            validation,
            storage,
            config,
            delta,
        }
    }

    fn group_by_file(&self, chunks: &[ChunkEntry]) -> HashMap<String, Vec<ChunkEntry>> {
        let mut map: HashMap<String, Vec<ChunkEntry>> = HashMap::new();

        for chunk in chunks {
            map.entry(chunk.file_path.clone())
                .or_default()
                .push(chunk.clone());
        }

        map
    }
}

#[async_trait]
impl UrlDownloadPipeline for DefaultUrlDownloadPipeline {
    fn new(
        storage: Arc<dyn UrlStorageAdapter>,
        reconstruction: Arc<dyn ReconstructionService>,
        validation: Arc<dyn ValidationService>,
        delta: Arc<dyn DeltaService>,
        config: Arc<RacDeltaConfig>,
    ) -> Self
    where
        Self: Sized,
    {
        Self::new(reconstruction, validation, storage, config.clone(), delta)
    }

    async fn execute(
        &self,
        local_dir: &Path,
        urls: DownloadUrls,
        strategy: UpdateStrategy,
        plan: Option<DeltaPlan>,
        options: Option<DownloadOptions>,
    ) -> DownloadResult<()> {
        if let Some(opts) = &options {
            if let Some(cb) = &opts.on_state_change {
                cb(DownloadState::Scanning);
            }
        }

        let remote_index_to_use: Option<RDIndex> = self
            .storage
            .get_remote_index_by_url(&urls.index_url)
            .await
            .map_err(|e| DownloadError::Index(format!("Remote index could not be found: {}", e)))?;

        if remote_index_to_use.is_none() {
            return Err(DownloadError::Index(
                "Remote rd-index not provided and was not found in storage. Check if url is correct".to_string(),
            ));
        }

        let local_index: Option<RDIndex> = if plan.is_some() {
            None
        } else if options
            .as_ref()
            .and_then(|o| o.use_existing_index)
            .unwrap_or(false)
        {
            self.find_local_index(local_dir).await?
        } else {
            Some(self.load_local_index(local_dir).await?)
        };

        let plan_to_use: DeltaPlan = if plan.is_some() {
            plan.unwrap()
        } else {
            self.delta
                .compare_for_download(local_index.as_ref(), remote_index_to_use.as_ref().unwrap())
                .await
                .map_err(|e| DownloadError::Delta(format!("{}", e)))?
        };

        // pick chunk source according to strategy
        let mut chunk_source: Option<Arc<dyn ChunkSource + Send + Sync>> = None;

        if strategy == UpdateStrategy::DownloadAllFirstToMemory {
            if let Some(opts) = &options {
                if let Some(callback) = &opts.on_state_change {
                    callback(DownloadState::Downloading);
                }
            }

            chunk_source = Some(
                self.download_all_missing_chunks(
                    urls.download_urls.clone(),
                    DownloadTarget::Memory,
                    options.clone(),
                )
                .await?,
            );
        }

        if strategy == UpdateStrategy::StreamFromNetwork {
            let storage_chunk_source =
                StorageChunkSource::new(StorageAdapterEnum::Url(Arc::clone(&self.storage)), None);

            chunk_source = Some(Arc::new(storage_chunk_source));
        }

        if strategy == UpdateStrategy::DownloadAllFirstToDisk {
            if let Some(opts) = &options {
                if let Some(callback) = &opts.on_state_change {
                    callback(DownloadState::Downloading);
                }
            }

            chunk_source = Some(
                self.download_all_missing_chunks(
                    urls.download_urls,
                    DownloadTarget::Disk,
                    options.clone(),
                )
                .await?,
            );
        }

        if chunk_source.is_none() {
            return Err(DownloadError::InvalidArgument(
                "No chunkSource found".to_string(),
            ));
        }

        // Reconstruct files if any
        if !plan_to_use.new_and_modified_files.is_empty() {
            if let Some(opts) = &options {
                if let Some(callback) = &opts.on_state_change {
                    callback(DownloadState::Reconstructing);
                }
            }

            let options_for_closure = options.clone();

            let local_options_ref = options.as_ref();

            let on_progress_cb = local_options_ref.and_then(|o| o.on_progress.clone());

            let recon_opts = ReconstructionOptions {
                force_rebuild: local_options_ref.and_then(|o| o.force),
                verify_after_rebuild: Some(true),
                file_concurrency: local_options_ref.and_then(|o| o.file_reconstruction_concurrency),
                in_place_reconstruction_threshold: local_options_ref
                    .and_then(|o| o.in_place_reconstruction_threshold),
                on_progress: on_progress_cb.map(|_cb| {
                    let opts = options_for_closure.clone();

                    Arc::new(
                        move |reconstruct_progress: f64,
                              disk_speed: usize,
                              network_progress: Option<f64>,
                              network_speed: Option<usize>| {
                            DefaultUrlDownloadPipeline::update_progress(
                                reconstruct_progress,
                                DownloadPhase::Reconstructing,
                                Some(disk_speed as f64),
                                network_speed.map(|s| s as f64),
                                opts.as_ref(),
                            );

                            if let Some(np) = network_progress {
                                DefaultUrlDownloadPipeline::update_progress(
                                    np,
                                    DownloadPhase::Download,
                                    None,
                                    network_speed.map(|s| s as f64),
                                    opts.as_ref(),
                                );
                            }
                        },
                    )
                        as Arc<dyn Fn(f64, usize, Option<f64>, Option<usize>) + Send + Sync>
                }),
            };

            let reconstruction = Arc::clone(&self.reconstruction);

            reconstruction
                .reconstruct_all(
                    &plan_to_use,
                    local_dir,
                    chunk_source.clone().unwrap(),
                    Some(&recon_opts),
                )
                .await
                .map_err(|e| DownloadError::Reconstruction(format!("{}", e)))?;
        }

        if !plan_to_use.obsolete_chunks.is_empty() || !plan_to_use.deleted_files.is_empty() {
            if let Some(opts) = &options {
                if let Some(callback) = &opts.on_state_change {
                    callback(DownloadState::Cleaning);
                }
            }

            self.verify_and_delete_obsolete_chunks(
                &plan_to_use,
                local_dir,
                &remote_index_to_use.clone().unwrap(),
                chunk_source.clone().unwrap(),
                options,
            )
            .await?;
        }

        self.save_local_index(local_dir, &remote_index_to_use.unwrap())
            .await?;

        if let Some(disk) = chunk_source
            .unwrap()
            .as_any()
            .downcast_ref::<DiskChunkSource>()
        {
            disk.clear().await?;
        }

        Ok(())
    }

    async fn save_local_index(
        &self,
        local_dir: &Path,
        index: &RDIndex,
    ) -> Result<(), DownloadError> {
        let dir = if Path::new(local_dir).is_absolute() {
            PathBuf::from(local_dir)
        } else {
            std::env::current_dir()
                .map_err(|e| DownloadError::Io(e))?
                .join(local_dir)
        };

        fs::create_dir_all(&dir).await.map_err(DownloadError::Io)?;

        let index_path = dir.join("rd-index.json");
        let json =
            serde_json::to_string_pretty(index).map_err(|e| DownloadError::Index(e.to_string()))?;

        fs::write(index_path, json)
            .await
            .map_err(DownloadError::Io)?;

        Ok(())
    }

    async fn load_local_index(&self, dir: &Path) -> DownloadResult<RDIndex> {
        let index = self
            .delta
            .create_index_from_directory(
                dir,
                self.config.chunk_size as u64,
                self.config.max_concurrency,
                None,
            )
            .await
            .map_err(|e| DownloadError::Delta(format!("{}", e)))?;

        Ok(index)
    }

    async fn find_local_index(&self, local_dir: &Path) -> Result<Option<RDIndex>, DownloadError> {
        let dir = if Path::new(local_dir).is_absolute() {
            PathBuf::from(local_dir)
        } else {
            std::env::current_dir()
                .map_err(|e| DownloadError::Io(e))?
                .join(local_dir)
        };

        let rd_path = dir.join("rd-index.json");
        if !rd_path.exists() {
            return Ok(None);
        }

        let data = fs::read_to_string(&rd_path)
            .await
            .map_err(DownloadError::Io)?;

        let parsed: RDIndex =
            serde_json::from_str(&data).map_err(|e| DownloadError::Index(e.to_string()))?;

        Ok(Some(parsed))
    }

    async fn download_all_missing_chunks(
        &self,
        download_urls: HashMap<String, ChunkUrlInfo>,
        target: DownloadTarget,
        options: Option<DownloadOptions>,
    ) -> DownloadResult<Arc<dyn ChunkSource>> {
        let options = options.map(Arc::new);

        if target == DownloadTarget::Disk {
            if options
                .as_ref()
                .and_then(|o| o.chunks_save_path.clone())
                .is_none()
            {
                return Err(DownloadError::InvalidArgument(
                    "chunks_save_path must be provided".into(),
                ));
            }
        }

        let chunks_save_path: Option<String> = if target == DownloadTarget::Disk {
            if let Some(raw) = options.as_ref().and_then(|o| o.chunks_save_path.clone()) {
                let path_buf = Path::new(&raw);
                let path = if path_buf.is_absolute() {
                    path_buf.to_path_buf()
                } else {
                    std::env::current_dir()
                        .map_err(|e| DownloadError::Io(e))?
                        .join(path_buf)
                };
                Some(path.to_string_lossy().to_string())
            } else {
                None
            }
        } else {
            None
        };

        let chunk_source: Arc<dyn ChunkSource> = if target == DownloadTarget::Memory {
            Arc::new(MemoryChunkSource::new())
        } else {
            Arc::new(DiskChunkSource::new(chunks_save_path.unwrap()))
        };

        let entries: Vec<(String, ChunkUrlInfo)> = download_urls.clone().into_iter().collect();
        let completed = Arc::new(Mutex::new(0usize));
        let total_bytes = Arc::new(Mutex::new(0usize));

        let concurrency = self.config.max_concurrency.unwrap_or(6usize);
        let queue = Arc::new(Mutex::new(entries));

        let last_update_time = Arc::new(Mutex::new(Instant::now()));
        let last_bytes = Arc::new(Mutex::new(0usize));

        let mut handles = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let storage = Arc::clone(&self.storage);
            let queue = Arc::clone(&queue);
            let chunk_source = Arc::clone(&chunk_source);
            let completed = Arc::clone(&completed);
            let total_bytes = Arc::clone(&total_bytes);
            let last_update_time = Arc::clone(&last_update_time);
            let last_bytes = Arc::clone(&last_bytes);
            let options = options.clone();

            let handle = tokio::spawn(async move {
                loop {
                    let next_entry_opt = {
                        let mut q = queue.lock().await;
                        q.pop()
                    };

                    let (hash, chunk_info) = match next_entry_opt {
                        Some(e) => e,
                        None => break,
                    };

                    let mut data = storage
                        .get_chunk_by_url(&chunk_info.url)
                        .await
                        .map_err(|e| DownloadError::Storage(format!("{}", e)))?
                        .ok_or_else(|| {
                            DownloadError::Storage(format!("Missing chunk {}", &hash))
                        })?;

                    let mut buffer = Vec::new();
                    data.read_to_end(&mut buffer)
                        .await
                        .map_err(|e| DownloadError::Storage(format!("{}", e)))?;

                    let data_len = buffer.len();

                    match target {
                        DownloadTarget::Memory => {
                            if let Some(mem) =
                                chunk_source.as_any().downcast_ref::<MemoryChunkSource>()
                            {
                                mem.set_chunk(hash.clone(), buffer).await;
                            }
                        }
                        DownloadTarget::Disk => {
                            if let Some(disk) =
                                chunk_source.as_any().downcast_ref::<DiskChunkSource>()
                            {
                                disk.set_chunk_bytes(&hash, &buffer).await?;
                            }
                        }
                    }

                    {
                        let mut comp = completed.lock().await;
                        *comp += 1;
                    }
                    {
                        let mut tb = total_bytes.lock().await;
                        *tb += data_len;
                    }

                    // progress update every 100ms
                    {
                        let now = Instant::now();
                        let mut lut = last_update_time.lock().await;

                        if now.duration_since(*lut) >= Duration::from_millis(100) {
                            let mut lb = last_bytes.lock().await;
                            let tbv = *total_bytes.lock().await;
                            let bytes_diff = tbv.saturating_sub(*lb) as f64;
                            let elapsed = now.duration_since(*lut).as_secs_f64().max(0.001);
                            let speed = bytes_diff / elapsed;

                            *lut = now;
                            *lb = tbv;

                            DefaultUrlDownloadPipeline::update_progress(
                                (*(completed.lock().await) as f64
                                    / (queue.lock().await.len() as f64 + 1.0))
                                    * 100.0,
                                DownloadPhase::Download,
                                None,
                                Some(speed),
                                options.as_ref().map(|arc| arc.as_ref()),
                            );
                        }
                    }
                }

                Ok::<(), DownloadError>(())
            });

            handles.push(handle);
        }

        for handle in handles {
            let _ = handle
                .await
                .map_err(|e| DownloadError::Other(format!("{}", e)))??;
        }

        DefaultUrlDownloadPipeline::update_progress(
            100.0,
            DownloadPhase::Download,
            None,
            Some(0.0),
            options.as_ref().map(|arc| arc.as_ref()),
        );

        Ok(chunk_source)
    }

    async fn verify_and_delete_obsolete_chunks(
        &self,
        plan: &DeltaPlan,
        local_dir: &Path,
        remote_index: &RDIndex,
        chunk_source: Arc<dyn ChunkSource>,
        options: Option<DownloadOptions>,
    ) -> DownloadResult<FileVerificationResult> {
        let dir = if Path::new(local_dir).is_absolute() {
            PathBuf::from(local_dir)
        } else {
            std::env::current_dir()
                .map_err(|e| DownloadError::Io(e))?
                .join(local_dir)
        };

        let obsolete_by_file = self.group_by_file(&plan.obsolete_chunks);

        let mut deleted_files: Vec<String> = Vec::new();
        let mut verified_files: Vec<String> = Vec::new();
        let mut rebuilt_files: Vec<String> = Vec::new();

        let all_files_set: HashSet<String> = plan
            .deleted_files
            .iter()
            .cloned()
            .chain(obsolete_by_file.keys().cloned())
            .collect();

        let total_files = all_files_set.len();
        let mut completed_files = 0usize;

        for file_path in all_files_set {
            let abs_path = dir.join(&file_path);
            let remote_file_opt = remote_index.files.iter().find(|f| f.path == file_path);

            if remote_file_opt.is_none() || plan.deleted_files.iter().any(|p| p == &file_path) {
                let _ = fs::remove_file(&abs_path).await;
                deleted_files.push(file_path.clone());
            } else {
                let remote_file = remote_file_opt.unwrap();

                let is_valid: bool = self
                    .validation
                    .validate_file(remote_file, abs_path.to_str().unwrap())
                    .await
                    .map_err(|e| DownloadError::Validation(format!("{}", e)))?;

                if !is_valid {
                    let reconstruction = Arc::clone(&self.reconstruction);

                    reconstruction
                        .reconstruct_file(remote_file, &abs_path, chunk_source.as_ref(), None, None)
                        .await
                        .map_err(|e| DownloadError::Reconstruction(format!("{}", e)))?;
                    rebuilt_files.push(file_path.clone());
                } else {
                    verified_files.push(file_path.clone());
                }
            }

            completed_files += 1;

            DefaultUrlDownloadPipeline::update_progress(
                (completed_files as f64 / total_files as f64) * 100.0,
                DownloadPhase::Deleting,
                None,
                None,
                options.as_ref(),
            );
        }

        Ok(FileVerificationResult {
            deleted_files,
            verified_files,
            rebuilt_files,
        })
    }
}
