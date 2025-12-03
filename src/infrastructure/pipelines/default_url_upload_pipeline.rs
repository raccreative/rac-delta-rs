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
    ChunkUrlInfo, PipelineError, RDIndex, RacDeltaConfig, UploadOptions, UploadPhase, UploadResult,
    UploadState, UploadUrls, UrlStorageAdapter, UrlUploadPipeline,
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

pub struct DefaultUrlUploadPipeline {
    storage: Arc<dyn UrlStorageAdapter + Send + Sync>,
    config: Arc<RacDeltaConfig>,
}

impl DefaultUrlUploadPipeline {
    pub fn new(
        storage: Arc<dyn UrlStorageAdapter + Send + Sync>,
        config: Arc<RacDeltaConfig>,
    ) -> Self {
        Self { storage, config }
    }

    fn group_chunks_by_file(chunks: &[ChunkUrlInfo]) -> Vec<(String, Vec<ChunkUrlInfo>)> {
        let mut map: HashMap<String, Vec<ChunkUrlInfo>> = HashMap::new();
        for c in chunks {
            map.entry(c.file_path.clone()).or_default().push(c.clone());
        }

        map.into_iter().collect()
    }
}

#[async_trait]
impl UrlUploadPipeline for DefaultUrlUploadPipeline {
    fn new(storage: Arc<dyn UrlStorageAdapter>, config: Arc<RacDeltaConfig>) -> Self
    where
        Self: Sized,
    {
        Self::new(storage, config)
    }

    async fn execute(
        &self,
        local_index: RDIndex,
        urls: UploadUrls,
        options: Option<UploadOptions>,
    ) -> UploadResult<RDIndex> {
        if !urls.upload_urls.is_empty() {
            change_state(UploadState::Uploading, options.as_ref());
            let _ = self.upload_missing_chunks(urls.upload_urls, options.clone());
        }

        if urls
            .delete_urls
            .clone()
            .is_some_and(|delete_urls| !delete_urls.is_empty())
        {
            change_state(UploadState::Cleaning, options.as_ref());
            let _ = self.delete_obsolete_chunks(urls.delete_urls.unwrap(), options.clone());
        }

        change_state(UploadState::Finalizing, options.as_ref());
        self.upload_index(&local_index, &urls.index_url).await?;

        Ok(local_index)
    }

    async fn upload_missing_chunks(
        &self,
        upload_urls: HashMap<String, ChunkUrlInfo>,
        options: Option<UploadOptions>,
    ) -> UploadResult<()> {
        let chunks: Vec<ChunkUrlInfo> = upload_urls.values().cloned().collect();
        if chunks.is_empty() {
            return Ok(());
        }

        let concurrency = self.config.max_concurrency.unwrap_or(5usize);
        let grouped = Self::group_chunks_by_file(&chunks);
        let total_chunks = chunks.len();

        let uploaded_chunks = Arc::new(Mutex::new(0usize));
        let uploaded_bytes = Arc::new(Mutex::new(0usize));
        let start_time = Instant::now();

        let queue = Arc::new(Mutex::new(grouped));

        let mut handles = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let queue = Arc::clone(&queue);
            let storage = Arc::clone(&self.storage);
            let cfg_chunk_size = self.config.chunk_size;
            let opts = options.clone();
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

                    let final_file_path = if Path::new(&relative_path).is_absolute() {
                        PathBuf::from(relative_path)
                    } else {
                        std::env::current_dir()
                            .map_err(|e| PipelineError::Io(e))?
                            .join(relative_path)
                    };

                    chunks.sort_by_key(|c| c.offset);

                    let mut fh = OpenOptions::new()
                        .read(true)
                        .open(&final_file_path)
                        .await
                        .map_err(|e| PipelineError::Io(e))?;

                    let mut buffer = vec![0u8; cfg_chunk_size];

                    for chunk in chunks {
                        fh.seek(SeekFrom::Start(chunk.offset as u64))
                            .await
                            .map_err(|e| PipelineError::Io(e))?;

                        let to_read = chunk.size as usize;

                        buffer.resize(to_read, 0);
                        fh.read_exact(&mut buffer[..to_read])
                            .await
                            .map_err(|e| PipelineError::Io(e))?;

                        let chunk_owned = buffer[..to_read].to_vec();
                        let reader = Cursor::new(chunk_owned).compat();

                        storage
                            .put_chunk_by_url(&chunk.url, Box::new(reader))
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

        for handle in handles {
            let _ = handle
                .await
                .map_err(|e| PipelineError::Other(format!("{}", e)))??;
        }

        update_progress(100.0, UploadPhase::Upload, Some(0.0), options.as_ref());

        Ok(())
    }

    async fn upload_index(&self, index: &RDIndex, upload_url: &str) -> UploadResult<()> {
        self.storage
            .put_remote_index_by_url(upload_url, index.clone())
            .await
            .map_err(|e| PipelineError::Storage(format!("{}", e)))?;
        Ok(())
    }

    async fn delete_obsolete_chunks(
        &self,
        delete_urls: Vec<String>,
        options: Option<UploadOptions>,
    ) -> UploadResult<()> {
        if delete_urls.is_empty() {
            return Ok(());
        }

        let concurrency = self.config.max_concurrency.unwrap_or(5usize);
        let queue = delete_urls.clone();
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
                    let url_opt = {
                        let mut qlock = q.lock().await;
                        qlock.pop()
                    };

                    let url = match url_opt {
                        Some(c) => c,
                        None => break,
                    };

                    for attempt in 1..=max_retries {
                        let res = storage.delete_chunk_by_url(&url).await;
                        match res {
                            Ok(_) => {
                                break;
                            }
                            Err(_e) => {
                                if attempt == max_retries {
                                    let mut f = failed.lock().await;
                                    f.push(url.clone());
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

        for handle in handles {
            let _ = handle
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
