use async_trait::async_trait;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use futures::io::Cursor;

use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::Mutex;
use tokio_util::compat::FuturesAsyncReadCompatExt;

use crate::{
    ChunkEntry, DeltaPlan, DeltaService, HashStorageAdapter, HashUploadPipeline, PipelineError,
    PutChunkOptions, RDIndex, RacDeltaConfig, UploadOptions, UploadPhase, UploadResult,
    UploadState,
};

fn update_progress(
    value: f64,
    phase: UploadPhase,
    speed: Option<f64>,
    options: Option<&UploadOptions>,
) {
    if let Some(opts) = options {
        if let Some(cb) = &opts.on_progress {
            cb(phase, value, speed);
        }
    }
}

fn change_state(state: UploadState, options: Option<&UploadOptions>) {
    if let Some(opts) = options {
        if let Some(cb) = &opts.on_state_change {
            cb(state);
        }
    }
}

pub struct DefaultHashUploadPipeline {
    storage: Arc<dyn HashStorageAdapter + Send + Sync>,
    delta: Arc<dyn DeltaService + Send + Sync>,
    config: Arc<RacDeltaConfig>,
}

impl DefaultHashUploadPipeline {
    pub fn new(
        storage: Arc<dyn HashStorageAdapter + Send + Sync>,
        delta: Arc<dyn DeltaService + Send + Sync>,
        config: Arc<RacDeltaConfig>,
    ) -> Self {
        Self {
            storage,
            delta,
            config,
        }
    }

    fn group_chunks_by_file(chunks: &[ChunkEntry]) -> Vec<(String, Vec<ChunkEntry>)> {
        let mut map: HashMap<String, Vec<ChunkEntry>> = HashMap::new();
        for c in chunks {
            map.entry(c.file_path.clone()).or_default().push(c.clone());
        }

        map.into_iter().collect()
    }
}

#[async_trait]
impl HashUploadPipeline for DefaultHashUploadPipeline {
    fn new(
        storage: Arc<dyn HashStorageAdapter>,
        delta: Arc<dyn DeltaService>,
        config: Arc<RacDeltaConfig>,
    ) -> Self
    where
        Self: Sized,
    {
        Self::new(storage, delta, config)
    }

    async fn execute(
        &self,
        directory: &Path,
        remote_index: Option<RDIndex>,
        options: Option<UploadOptions>,
    ) -> UploadResult<RDIndex> {
        change_state(UploadState::Scanning, options.as_ref());

        let local_index = self
            .scan_directory(
                directory,
                options.as_ref().and_then(|o| o.ignore_patterns.clone()),
            )
            .await?;

        let mut remote_index_to_use: Option<RDIndex> = remote_index;

        if remote_index_to_use.is_none() && !options.as_ref().and_then(|o| o.force).unwrap_or(false)
        {
            let got: Option<RDIndex> = self
                .storage
                .get_remote_index()
                .await
                .map_err(|e| PipelineError::Storage(format!("{}", e)))?;

            if got.is_none()
                && options
                    .as_ref()
                    .and_then(|o| o.require_remote_index)
                    .unwrap_or(false)
            {
                return Err(PipelineError::InvalidArgument(
                    "Remote rd-index.json could not be found".into(),
                ));
            }

            remote_index_to_use = got;
        }

        if options.as_ref().and_then(|o| o.force).unwrap_or(false) {
            remote_index_to_use = None;
        }

        change_state(UploadState::Comparing, options.as_ref());
        let delta_plan = self
            .delta
            .compare_for_upload(&local_index, remote_index_to_use.clone().as_ref())
            .await
            .map_err(|e| PipelineError::Delta(format!("{}", e)))?;

        if !options.as_ref().and_then(|o| o.force).unwrap_or(false)
            && delta_plan.missing_chunks.is_empty()
            && delta_plan.obsolete_chunks.is_empty()
            && delta_plan.deleted_files.is_empty()
        {
            println!("No changes to upload or delete, remote is up to date.");

            let _ = self.storage.dispose().await;
            return Ok(local_index);
        }

        if !delta_plan.missing_chunks.is_empty() {
            change_state(UploadState::Uploading, options.as_ref());
            self.upload_missing_chunks(
                &delta_plan.clone(),
                directory,
                options.as_ref().and_then(|o| o.force).unwrap_or(false),
                options.clone(),
            )
            .await?;
        }

        if remote_index_to_use.is_some() && !delta_plan.obsolete_chunks.is_empty() {
            change_state(UploadState::Cleaning, options.as_ref());
            self.delete_obsolete_chunks(&delta_plan.clone(), options.clone())
                .await?;
        }

        change_state(UploadState::Finalizing, options.as_ref());
        self.upload_index(&local_index).await?;

        let _ = self.storage.dispose().await;
        Ok(local_index)
    }

    async fn scan_directory(
        &self,
        dir: &Path,
        ignore_patterns: Option<Vec<String>>,
    ) -> UploadResult<RDIndex> {
        let index = self
            .delta
            .create_index_from_directory(
                dir,
                self.config.chunk_size as u64,
                self.config.max_concurrency,
                ignore_patterns,
            )
            .await
            .map_err(|e| PipelineError::Delta(format!("{}", e)))?;

        Ok(index)
    }

    async fn upload_missing_chunks(
        &self,
        plan: &DeltaPlan,
        base_dir: &Path,
        force: bool,
        options: Option<UploadOptions>,
    ) -> UploadResult<()> {
        if plan.missing_chunks.is_empty() {
            return Ok(());
        }

        let dir = if Path::new(base_dir).is_absolute() {
            PathBuf::from(base_dir)
        } else {
            std::env::current_dir()
                .map_err(|e| PipelineError::Io(e))?
                .join(base_dir)
        };

        let concurrency = self.config.max_concurrency.unwrap_or(5usize);
        let files = Self::group_chunks_by_file(&plan.missing_chunks);
        let total_chunks = plan.missing_chunks.len();

        let queue = Arc::new(Mutex::new(files));

        let uploaded_chunks = Arc::new(Mutex::new(0usize));
        let uploaded_bytes = Arc::new(Mutex::new(0usize));
        let start_time = Instant::now();

        let mut handles = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let queue = Arc::clone(&queue);
            let storage = Arc::clone(&self.storage);
            let cfg_chunk_size = self.config.chunk_size;
            let opts = options.clone();
            let dir = dir.clone();
            let uploaded_chunks = Arc::clone(&uploaded_chunks);
            let uploaded_bytes = Arc::clone(&uploaded_bytes);

            let handle = tokio::spawn(async move {
                loop {
                    let next_opt = {
                        let mut q = queue.lock().await;
                        q.pop()
                    };

                    let (relative_path, mut chunks) = match next_opt {
                        Some(pair) => pair,
                        None => break,
                    };

                    chunks.sort_by_key(|c| c.chunk.offset);

                    let file_path = dir.join(&relative_path);

                    let mut fh = OpenOptions::new()
                        .read(true)
                        .open(&file_path)
                        .await
                        .map_err(|e| PipelineError::Io(e))?;

                    let mut buffer = vec![0u8; cfg_chunk_size];

                    for c in chunks {
                        fh.seek(SeekFrom::Start(c.chunk.offset as u64))
                            .await
                            .map_err(|e| PipelineError::Io(e))?;

                        let to_read = c.chunk.size as usize;

                        buffer.resize(to_read, 0);
                        fh.read_exact(&mut buffer[..to_read])
                            .await
                            .map_err(|e| PipelineError::Io(e))?;

                        let put_chunk_options = PutChunkOptions {
                            overwrite: Some(force),
                            size: Some(c.chunk.size),
                        };

                        let chunk_owned = buffer[..to_read].to_vec();
                        let reader = Cursor::new(chunk_owned).compat();

                        storage
                            .put_chunk(&c.chunk.hash, Box::new(reader), Some(put_chunk_options))
                            .await
                            .map_err(|e| PipelineError::Storage(format!("{}", e)))?;

                        {
                            let mut uc = uploaded_chunks.lock().await;
                            *uc += 1;
                        }
                        {
                            let mut ub = uploaded_bytes.lock().await;
                            *ub += to_read;
                        }

                        let percent = {
                            let uc = *uploaded_chunks.lock().await as f64;
                            (uc / (total_chunks as f64)) * 100.0
                        };

                        let elapsed = start_time.elapsed().as_secs_f64().max(0.0001);
                        let speed = (*uploaded_bytes.lock().await as f64) / elapsed;

                        update_progress(percent, UploadPhase::Upload, Some(speed), opts.as_ref());
                    }
                }

                Ok::<(), PipelineError>(())
            });

            handles.push(handle);
        }

        for h in handles {
            let _ = h
                .await
                .map_err(|e| PipelineError::Other(format!("{}", e)))??;
        }

        update_progress(100.0, UploadPhase::Upload, Some(0.0), options.as_ref());

        Ok(())
    }

    async fn upload_index(&self, index: &RDIndex) -> UploadResult<()> {
        self.storage
            .put_remote_index(index.clone())
            .await
            .map_err(|e| PipelineError::Storage(format!("{}", e)))?;
        Ok(())
    }

    async fn delete_obsolete_chunks(
        &self,
        plan: &DeltaPlan,
        options: Option<UploadOptions>,
    ) -> UploadResult<()> {
        if plan.obsolete_chunks.is_empty() {
            return Ok(());
        }

        let concurrency = self.config.max_concurrency.unwrap_or(5usize);
        let queue = plan.obsolete_chunks.clone();
        let total = queue.len();

        if total == 0 {
            return Ok(());
        }

        let failed = Arc::new(Mutex::new(Vec::<String>::new()));
        let queue = Arc::new(Mutex::new(queue));
        let max_retries = 3usize;

        let mut handles = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let q = Arc::clone(&queue);
            let storage = Arc::clone(&self.storage);
            let failed = Arc::clone(&failed);
            let opts = options.clone();

            let handle = tokio::spawn(async move {
                loop {
                    let chunk_opt = {
                        let mut qlock = q.lock().await;
                        qlock.pop()
                    };

                    let chunk = match chunk_opt {
                        Some(c) => c,
                        None => break,
                    };

                    for attempt in 1..=max_retries {
                        let res = storage.delete_chunk(&chunk.chunk.hash).await;
                        match res {
                            Ok(_) => {
                                break;
                            }
                            Err(_e) => {
                                if attempt == max_retries {
                                    let mut f = failed.lock().await;
                                    f.push(chunk.chunk.hash.clone());
                                } else {
                                    tokio::time::sleep(std::time::Duration::from_millis(
                                        100 * attempt as u64,
                                    ))
                                    .await;
                                }
                            }
                        }
                    }

                    let done = {
                        let qlen = q.lock().await.len();
                        (total - qlen) as f64
                    };
                    let percent = (done / (total as f64)) * 100.0;
                    update_progress(percent, UploadPhase::Deleting, None, opts.as_ref());
                }

                Ok::<(), PipelineError>(())
            });

            handles.push(handle);
        }

        for h in handles {
            let _ = h
                .await
                .map_err(|e| PipelineError::Other(format!("{}", e)))??;
        }

        let failed = failed.lock().await;
        if !failed.is_empty() {
            return Err(PipelineError::Other(format!(
                "Failed to delete {} chunks: {:?}",
                failed.len(),
                failed
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod default_hash_upload_pipeline_tests {
    use std::{path::PathBuf, sync::Arc};

    use tokio::io::AsyncReadExt;

    use crate::{
        BaseStorageConfig, Blake3HasherService, DefaultHashUploadPipeline, DeltaService,
        HashStorageAdapter, HashUploadPipeline, HasherService, LocalStorageAdapter,
        LocalStorageConfig, MemoryDeltaService, RacDeltaConfig, UploadOptions,
    };

    fn tmp_dir(test_name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("default_hash_download_pipeline_tests_{test_name}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    async fn create_file(
        path_base: &PathBuf,
        name: &str,
        content: &str,
    ) -> (PathBuf, String, String) {
        let path = path_base.join(name);
        if let Some(parent) = path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        tokio::fs::write(&path, content).await.unwrap();
        (path, content.to_string(), name.to_string())
    }

    #[tokio::test]
    async fn uploads_all_chunks_and_remote_index() {
        let tmp = tmp_dir("case1");
        let local_storage_config = LocalStorageConfig {
            base_path: tmp.clone(),
            base: BaseStorageConfig { path_prefix: None },
        };

        let storage: Arc<dyn HashStorageAdapter + Send + Sync> =
            Arc::new(LocalStorageAdapter::new(local_storage_config.clone()));
        let hasher: Arc<dyn HasherService + Send + Sync> = Arc::new(Blake3HasherService::new());

        let delta: Arc<dyn DeltaService + Send + Sync> =
            Arc::new(MemoryDeltaService::new(hasher.clone()));

        let pipeline = DefaultHashUploadPipeline::new(
            Arc::clone(&storage),
            Arc::clone(&delta),
            Arc::new(RacDeltaConfig {
                chunk_size: 5,
                max_concurrency: Some(2),
                storage: crate::StorageConfig::Local(local_storage_config.clone()),
            }),
        );

        let (_path, content, name) = create_file(&tmp, "file.txt", "hello world").await;

        let result = pipeline
            .execute(&tmp, None, None)
            .await
            .expect("pipeline execute failed");

        let file_index_opt = result.files.iter().find(|f| f.path == name);
        assert!(file_index_opt.is_some());
        let file_index = file_index_opt.unwrap();

        for chunk in &file_index.chunks {
            let opt_stream = storage
                .get_chunk(&chunk.hash)
                .await
                .expect("get_chunk failed");

            let mut reader = opt_stream.expect("chunk not found in storage");
            let mut buf = Vec::new();

            reader.read_to_end(&mut buf).await.unwrap();

            let expected_slice = &content.as_bytes()
                [chunk.offset as usize..(chunk.offset as usize + chunk.size as usize)];
            assert_eq!(buf.as_slice(), expected_slice);
        }

        let remote = storage
            .get_remote_index()
            .await
            .expect("get_remote_index failed");
        assert!(remote.is_some());
        let remote_index = remote.unwrap();
        assert!(remote_index.files.iter().any(|f| f.path == "file.txt"));
    }

    #[tokio::test]
    async fn uploads_multiple_files_with_multiple_chunks() {
        let tmp = tmp_dir("case_multi");
        let local_storage_config = LocalStorageConfig {
            base_path: tmp.clone(),
            base: BaseStorageConfig { path_prefix: None },
        };

        let storage: std::sync::Arc<dyn HashStorageAdapter + Send + Sync> =
            std::sync::Arc::new(LocalStorageAdapter::new(local_storage_config.clone()));

        let hasher: Arc<dyn HasherService + Send + Sync> = Arc::new(Blake3HasherService::new());

        let delta: Arc<dyn DeltaService + Send + Sync> =
            Arc::new(MemoryDeltaService::new(hasher.clone()));

        let pipeline = DefaultHashUploadPipeline::new(
            std::sync::Arc::clone(&storage),
            std::sync::Arc::clone(&delta),
            std::sync::Arc::new(RacDeltaConfig {
                chunk_size: 5,
                max_concurrency: Some(2),
                storage: crate::StorageConfig::Local(local_storage_config.clone()),
            }),
        );

        let (_p1, content1, name1) = create_file(&tmp, "file1.txt", "abcdefghij12345").await;
        let (_p2, content2, name2) = create_file(&tmp, "file2.txt", "9876543210xyz").await;

        let result = pipeline
            .execute(&tmp, None, None)
            .await
            .expect("pipeline execute failed");

        for (name, content) in vec![(name1, content1), (name2, content2)].into_iter() {
            let file_index = result
                .files
                .iter()
                .find(|f| f.path == name)
                .expect("file index missing");

            for chunk in &file_index.chunks {
                let opt_stream = storage
                    .get_chunk(&chunk.hash)
                    .await
                    .expect("get_chunk failed");

                let mut reader = opt_stream.expect("chunk not found in storage");
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.unwrap();

                let expected_slice = &content.as_bytes()
                    [chunk.offset as usize..(chunk.offset as usize + chunk.size as usize)];
                assert_eq!(buf.as_slice(), expected_slice);
            }
        }
    }

    #[tokio::test]
    async fn uploads_everything_if_force_option_is_set() {
        let tmp = tmp_dir("case_force");
        let local_storage_config = LocalStorageConfig {
            base_path: tmp.clone(),
            base: BaseStorageConfig { path_prefix: None },
        };

        let storage: std::sync::Arc<dyn HashStorageAdapter + Send + Sync> =
            std::sync::Arc::new(LocalStorageAdapter::new(local_storage_config.clone()));

        let hasher: Arc<dyn HasherService + Send + Sync> = Arc::new(Blake3HasherService::new());

        let delta: Arc<dyn DeltaService + Send + Sync> =
            Arc::new(MemoryDeltaService::new(hasher.clone()));

        let pipeline = DefaultHashUploadPipeline::new(
            std::sync::Arc::clone(&storage),
            std::sync::Arc::clone(&delta),
            std::sync::Arc::new(RacDeltaConfig {
                chunk_size: 5,
                max_concurrency: Some(2),
                storage: crate::StorageConfig::Local(local_storage_config.clone()),
            }),
        );

        let (_p, content, name) = create_file(&tmp, "force.txt", "force content").await;

        let opts = UploadOptions {
            force: Some(true),
            ignore_patterns: None,
            require_remote_index: None,
            on_progress: None,
            on_state_change: None,
        };

        let result = pipeline
            .execute(&tmp, None, Some(opts))
            .await
            .expect("pipeline execute failed");

        let file_index = result
            .files
            .iter()
            .find(|f| f.path == name)
            .expect("file missing");

        for chunk in &file_index.chunks {
            let opt_stream = storage
                .get_chunk(&chunk.hash)
                .await
                .expect("get_chunk failed");
            let mut reader = opt_stream.expect("chunk not found in storage");
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).await.unwrap();

            let expected_slice = &content.as_bytes()
                [chunk.offset as usize..(chunk.offset as usize + chunk.size as usize)];
            assert_eq!(buf.as_slice(), expected_slice);
        }
    }

    #[tokio::test]
    async fn deletes_obsolete_chunks_when_remote_index_exists() {
        let tmp = tmp_dir("case_delete1");
        let local_storage_config = LocalStorageConfig {
            base_path: tmp.clone(),
            base: BaseStorageConfig { path_prefix: None },
        };

        let storage: std::sync::Arc<dyn HashStorageAdapter + Send + Sync> =
            std::sync::Arc::new(LocalStorageAdapter::new(local_storage_config.clone()));

        let hasher: Arc<dyn HasherService + Send + Sync> = Arc::new(Blake3HasherService::new());

        let delta: Arc<dyn DeltaService + Send + Sync> =
            Arc::new(MemoryDeltaService::new(hasher.clone()));

        let pipeline = DefaultHashUploadPipeline::new(
            std::sync::Arc::clone(&storage),
            std::sync::Arc::clone(&delta),
            std::sync::Arc::new(RacDeltaConfig {
                chunk_size: 5,
                max_concurrency: Some(2),
                storage: crate::StorageConfig::Local(local_storage_config.clone()),
            }),
        );

        let (_p, _content, _name) = create_file(&tmp, "file.txt", "1234567890").await;
        let first_index = pipeline
            .execute(&tmp, None, None)
            .await
            .expect("first execute failed");

        let tmp2 = tmp_dir("case_delete2");
        let local_storage_config2 = LocalStorageConfig {
            base_path: tmp.clone(),
            base: BaseStorageConfig { path_prefix: None },
        };

        let storage2: std::sync::Arc<dyn HashStorageAdapter + Send + Sync> =
            std::sync::Arc::new(LocalStorageAdapter::new(local_storage_config2.clone()));

        let hasher2: Arc<dyn HasherService + Send + Sync> = Arc::new(Blake3HasherService::new());

        let delta2: Arc<dyn DeltaService + Send + Sync> =
            Arc::new(MemoryDeltaService::new(hasher2.clone()));

        let pipeline2 = DefaultHashUploadPipeline::new(
            std::sync::Arc::clone(&storage2),
            std::sync::Arc::clone(&delta2),
            std::sync::Arc::new(RacDeltaConfig {
                chunk_size: 5,
                max_concurrency: Some(2),
                storage: crate::StorageConfig::Local(local_storage_config2.clone()),
            }),
        );

        let new_index = pipeline2
            .execute(&tmp2, Some(first_index.clone()), None)
            .await
            .expect("second execute failed");

        for old_file in first_index.files {
            for chunk in old_file.chunks {
                let res = storage.get_chunk(&chunk.hash).await;
                match res {
                    Ok(opt) => assert!(opt.is_none(), "chunk {} still exists", chunk.hash),
                    Err(_) => { /* treat error as missing */ }
                }
            }
        }

        assert_eq!(new_index.files.len(), 0);
    }
}
