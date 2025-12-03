use async_trait::async_trait;

use futures::future::join_all;
use futures::{StreamExt, stream};

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use tokio::fs::{self, File, OpenOptions};
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, DuplexStream, SeekFrom,
    duplex,
};
use tokio::sync::Mutex;

use crate::{
    Chunk, ChunkSource, DEFAULT_IN_PLACE_RECONSTRUCTION_THRESHOLD, DeltaPlan, FileEntry,
    FileProgressCallback, HasherService, ReconstructionError, ReconstructionOptions,
    ReconstructionService, StreamChunksOptions,
};

pub enum WriteData {
    Buffer(Vec<u8>),
    Stream(Pin<Box<dyn AsyncRead + Send + Unpin + 'static>>),
}

pub struct MemoryReconstructionService {
    hasher: Arc<dyn HasherService + Send + Sync>,
}

impl MemoryReconstructionService {
    pub fn new(hasher: Arc<dyn HasherService + Send + Sync>) -> Arc<Self> {
        Arc::new(Self { hasher })
    }

    async fn file_exists(&self, path: &str) -> bool {
        fs::metadata(path).await.is_ok()
    }

    async fn reconstruct_in_place(
        &self,
        entry: &FileEntry,
        output_path: &str,
        chunk_source: &dyn ChunkSource,
        progress_cb: Option<Arc<dyn Fn(usize, usize, usize) + Send + Sync + 'static>>,
    ) -> Result<(), ReconstructionError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(output_path)
            .await?;

        let is_network_source = chunk_source.stream_chunks(&[], None).await.is_some();

        let chunk_map: HashMap<String, Chunk> = entry
            .chunks
            .iter()
            .map(|c| (c.hash.clone(), c.clone()))
            .collect();

        let mut processed = 0usize;

        let mut stream = self
            .fetch_chunks_smart(&entry.chunks, chunk_source, false)
            .await;

        while let Some((hash, write_data)) = stream.next().await {
            let chunk = match chunk_map.get(&hash) {
                Some(v) => v,
                None => continue,
            };

            let buffer: Vec<u8> = match write_data {
                WriteData::Buffer(b) => b.to_vec(),
                WriteData::Stream(mut s) => {
                    let mut buf = Vec::new();
                    s.read_to_end(&mut buf).await?;

                    buf
                }
            };

            file.seek(SeekFrom::Start(chunk.offset as u64)).await?;
            file.write_all(&buffer).await?;

            processed += 1;

            if let Some(cb) = &progress_cb {
                let net_bytes = if is_network_source { buffer.len() } else { 0 };
                cb(buffer.len(), net_bytes, processed);
            }
        }

        file.flush().await?;
        Ok(())
    }

    async fn reconstruct_to_temp(
        &self,
        entry: &FileEntry,
        output_path: &str,
        temp_path: &str,
        chunk_source: &dyn ChunkSource,
        verify_after_rebuild: bool,
        file_exists: bool,
        force: bool,
        progress_cb: Option<Arc<dyn Fn(usize, usize, usize) + Send + Sync + 'static>>,
    ) -> Result<(), ReconstructionError> {
        let mut write_file = File::create(temp_path).await?;

        let mut processed: usize = 0;
        let is_network_source = chunk_source.stream_chunks(&[], None).await.is_some();

        // Partial reconstruction
        if file_exists && !force {
            let mut read_file = File::open(output_path).await?;

            for chunk in &entry.chunks {
                self.process_chunk_data_smart(
                    &chunk,
                    &mut read_file,
                    Some(chunk_source),
                    Some(&mut write_file),
                )
                .await
                .map_err(|e| {
                    ReconstructionError::Other(format!("Failed to process chunk data smart: {e}"))
                })?;

                processed += 1;

                if let Some(cb) = &progress_cb {
                    cb(chunk.size as usize, 0usize, processed);
                }
            }
        }

        // Full reconstruction
        if !file_exists || force {
            let mut stream = self
                .fetch_chunks_smart(&entry.chunks, chunk_source, true)
                .await;

            while let Some((_, data)) = stream.next().await {
                let cb = progress_cb.clone();
                processed += 1;

                self.write_to_stream(
                    data,
                    Some(&mut write_file),
                    cb.map(|cb| {
                        Arc::new(move |total_written: usize| {
                            cb(
                                total_written,
                                if is_network_source { total_written } else { 0 },
                                processed,
                            )
                        }) as Arc<dyn Fn(usize) + Send + Sync>
                    }),
                )
                .await?;
            }

            write_file.flush().await?;
        }

        write_file.flush().await?;
        drop(write_file);

        if verify_after_rebuild {
            let ok = self
                .hasher
                .verify_file(temp_path, &entry.hash)
                .await
                .map_err(|e| ReconstructionError::Other(format!("Failed to verify file: {e}")))?;
            if !ok {
                return Err(ReconstructionError::Other(format!(
                    "Hash mismatch after reconstructing {}",
                    entry.path
                )));
            }
        }

        fs::rename(temp_path, output_path).await?;

        Ok(())
    }

    async fn process_chunk_data_smart(
        &self,
        chunk: &Chunk,
        fd: &mut File,
        chunk_source: Option<&dyn ChunkSource>,
        write_stream: Option<&mut (dyn tokio::io::AsyncWrite + Send + Unpin)>,
    ) -> Result<(), ReconstructionError> {
        let mut buffer = vec![0u8; chunk.size as usize];

        fd.seek(SeekFrom::Start(chunk.offset as u64))
            .await
            .map_err(|e| ReconstructionError::Other(format!("Failed to seek: {e}")))?;
        fd.read_exact(&mut buffer)
            .await
            .map_err(|e| ReconstructionError::Other(format!("Failed to read chunk: {e}")))?;

        if self
            .hasher
            .verify_chunk(&buffer, &chunk.hash)
            .await
            .map_err(|e| ReconstructionError::Other(format!("Failed to verify chunk: {e}")))?
        {
            self.write_to_stream(WriteData::Buffer(buffer), write_stream, None)
                .await?;
            return Ok(());
        }

        let data = self
            .get_chunk_data(chunk, chunk_source)
            .await
            .map_err(|e| ReconstructionError::Other(format!("Failed to get chunk data: {e}")))?;

        self.write_to_stream(WriteData::Buffer(data), write_stream, None)
            .await?;

        Ok(())
    }

    async fn get_chunk_data(
        &self,
        chunk: &Chunk,
        chunk_source: Option<&dyn ChunkSource>,
    ) -> Result<Vec<u8>, ReconstructionError> {
        let source = chunk_source.ok_or_else(|| {
            ReconstructionError::Other(format!("ChunkSource not provided for chunk {}", chunk.hash))
        })?;

        let data = source.get_chunk(&chunk.hash).await.map_err(|e| {
            ReconstructionError::Other(format!("Failed to get chunk {}: {e}", chunk.hash))
        })?;

        Ok(data)
    }

    async fn write_to_stream(
        &self,
        data: WriteData,
        stream: Option<&mut (dyn AsyncWrite + Send + Unpin)>,
        on_finish: Option<Arc<dyn Fn(usize) + Send + Sync>>,
    ) -> Result<(), ReconstructionError> {
        let stream = match stream {
            Some(s) => s,
            None => return Ok(()),
        };

        match data {
            WriteData::Buffer(buf) => {
                stream.write_all(&buf).await.map_err(|e| {
                    ReconstructionError::Other(format!("Failed to write buffer: {e}"))
                })?;

                if let Some(cb) = on_finish {
                    cb(buf.len());
                }
            }
            WriteData::Stream(mut reader) => {
                let mut total_written = 0usize;
                let mut buf = [0u8; 8192];

                loop {
                    let n = reader.as_mut().read(&mut buf).await.map_err(|e| {
                        ReconstructionError::Other(format!("Failed to read from stream: {e}"))
                    })?;

                    if n == 0 {
                        break;
                    }

                    let mut offset = 0;
                    while offset < n {
                        let written = stream.write(&buf[offset..n]).await.map_err(|e| {
                            ReconstructionError::Other(format!("Failed to write to stream: {e}"))
                        })?;
                        offset += written;
                        total_written += written;
                    }
                }

                if let Some(cb) = on_finish {
                    cb(total_written);
                }
            }
        }

        Ok(())
    }

    async fn fetch_chunks_smart<'a>(
        &self,
        chunks: &'a [Chunk],
        chunk_source: &'a dyn ChunkSource,
        preserve_order: bool,
    ) -> Pin<Box<dyn futures::Stream<Item = (String, WriteData)> + Send + 'a>> {
        let hashes: Vec<String> = chunks.iter().map(|c| c.hash.clone()).collect();

        if let Some(stream) = chunk_source
            .stream_chunks(
                &hashes,
                Some(StreamChunksOptions {
                    preserve_order: Some(preserve_order),
                    concurrency: None,
                }),
            )
            .await
        {
            let mapped = stream.filter_map(|res| async move {
                match res {
                    Ok(chunk_data) => Some((
                        chunk_data.hash.clone(),
                        WriteData::Stream(Box::pin(chunk_data.data)),
                    )),
                    Err(_) => None,
                }
            });

            return Box::pin(mapped);
        }

        if let Ok(map) = chunk_source.get_chunks(&hashes, None).await {
            let ordered_hashes = hashes.clone();
            let stream = stream::iter(ordered_hashes.into_iter().filter_map(move |hash| {
                map.get(&hash)
                    .cloned()
                    .map(|bytes| (hash.clone(), WriteData::Buffer(bytes)))
            }));

            return Box::pin(stream);
        }

        let stream = stream::iter(hashes.into_iter())
            .then(move |hash| async move {
                match chunk_source.get_chunk(&hash).await {
                    Ok(bytes) => Some((hash.clone(), WriteData::Buffer(bytes))),
                    Err(_) => None,
                }
            })
            .filter_map(async move |o| o);

        Box::pin(stream)
    }
}

#[async_trait]
impl ReconstructionService for MemoryReconstructionService {
    async fn reconstruct_file(
        self: Arc<Self>,
        entry: &FileEntry,
        output_path: &Path,
        chunk_source: &dyn ChunkSource,
        options: Option<&ReconstructionOptions>,
        cb: Option<FileProgressCallback>,
    ) -> Result<(), ReconstructionError> {
        let def_output_path = Path::new(output_path).to_path_buf();

        let parent = def_output_path.parent().unwrap_or(Path::new(""));
        fs::create_dir_all(parent).await?;

        let opts = options.cloned().unwrap_or_default();
        let force_rebuild = opts.force_rebuild.unwrap_or(false);
        let verify_after_rebuild = opts.verify_after_rebuild.unwrap_or(true);
        let threshold = opts
            .in_place_reconstruction_threshold
            .unwrap_or(DEFAULT_IN_PLACE_RECONSTRUCTION_THRESHOLD);

        let temp_path = PathBuf::from(format!("{}.tmp", def_output_path.to_string_lossy()));

        let exists = self.file_exists(def_output_path.to_str().unwrap()).await;
        if exists && !force_rebuild {
            match self
                .hasher
                .verify_file(def_output_path.to_str().unwrap(), &entry.hash)
                .await
            {
                Ok(matches) => {
                    if matches {
                        return Ok(());
                    }
                }
                Err(e) => {
                    return Err(ReconstructionError::Other(format!(
                        "Hasher verification failed: {}",
                        e
                    )));
                }
            }
        }

        let is_large_file = if exists {
            let metadata = fs::metadata(&def_output_path).await?;
            metadata.len() > threshold.try_into().unwrap()
        } else {
            false
        };

        let existing_large_no_rebuild = exists && !force_rebuild && is_large_file;

        let entry_chunks_len = entry.chunks.len() as f64;

        let progress_for_internal: Option<
            Arc<dyn Fn(usize, usize, usize) + Send + Sync + 'static>,
        > = cb.map(|outer_cb| {
            let outer_cb = outer_cb.clone();

            Arc::new(
                move |chunk_bytes: usize, net_bytes: usize, processed_chunks: usize| {
                    let file_progress = (processed_chunks as f64 / entry_chunks_len) * 100.0;
                    outer_cb(file_progress, chunk_bytes, Some(net_bytes));
                },
            ) as Arc<dyn Fn(usize, usize, usize) + Send + Sync + 'static>
        });

        let result = if existing_large_no_rebuild && threshold != 0 {
            self.reconstruct_in_place(
                entry,
                def_output_path.to_str().unwrap(),
                chunk_source,
                progress_for_internal.clone(),
            )
            .await
            .map_err(|e| ReconstructionError::Other(format!("{}", e)))
        } else {
            self.reconstruct_to_temp(
                entry,
                def_output_path.to_str().unwrap(),
                temp_path.to_str().unwrap(),
                chunk_source,
                verify_after_rebuild,
                exists,
                force_rebuild,
                progress_for_internal.clone(),
            )
            .await
            .map_err(|e| ReconstructionError::Other(format!("{}", e)))
        };

        if let Err(e) = result {
            let _ = fs::remove_file(&temp_path).await;
            return Err(e);
        }

        Ok(())
    }

    async fn reconstruct_all(
        self: Arc<Self>,
        plan: &DeltaPlan,
        output_dir: &Path,
        chunk_source: Arc<dyn ChunkSource>,
        options: Option<&ReconstructionOptions>,
    ) -> Result<(), ReconstructionError> {
        let dir = output_dir.to_path_buf();

        fs::create_dir_all(&dir).await?;

        let queue = Arc::new(Mutex::new(VecDeque::from(
            plan.new_and_modified_files.clone(),
        )));

        let owned_options: Option<ReconstructionOptions> = options.cloned();

        let global_on_progress: Option<
            Arc<dyn Fn(f64, usize, Option<f64>, Option<usize>) + Send + Sync>,
        > = owned_options.as_ref().and_then(|o| o.on_progress.clone());

        let is_network_source = chunk_source.stream_chunks(&[], None).await.is_some();

        let total_bytes_to_write = {
            let q = queue.lock().await;
            q.iter().map(|f| f.size).sum::<u64>()
        };

        let total_network_bytes: u64 = is_network_source
            .then(|| {
                plan.missing_chunks
                    .iter()
                    .map(|c| c.chunk.size)
                    .sum::<u64>()
            })
            .unwrap_or(0);

        let global_bytes_written = Arc::new(Mutex::new(0usize));
        let global_bytes_received = Arc::new(Mutex::new(0usize));
        let file_progress_map = Arc::new(Mutex::new(HashMap::<String, f64>::new()));
        let error = Arc::new(Mutex::new(None::<ReconstructionError>));
        let start_time = std::time::Instant::now();
        let desired_concurrency = owned_options
            .as_ref()
            .and_then(|o| o.file_concurrency)
            .unwrap_or(5);

        let concurrency = {
            let q = queue.lock().await;
            desired_concurrency.min(q.len()).max(1)
        };

        let dir_for_workers = dir.clone();
        let global_on_progress = global_on_progress;
        let owned_options = owned_options;

        let worker = || {
            let chunk_source = chunk_source.clone();
            let queue = queue.clone();
            let global_bytes_written = global_bytes_written.clone();
            let global_bytes_received = global_bytes_received.clone();
            let file_progress_map = file_progress_map.clone();
            let error = error.clone();
            let dir = dir_for_workers.clone();
            let start_time = start_time.clone();
            let global_on_progress = global_on_progress.clone();
            let owned_options = owned_options.clone();
            let service = self.clone();

            async move {
                loop {
                    if error.lock().await.is_some() {
                        break;
                    }

                    let maybe_entry = {
                        let mut q = queue.lock().await;
                        q.pop_front()
                    };

                    let entry = match maybe_entry {
                        Some(e) => e,
                        None => break,
                    };

                    let entry_for_reconstruct = entry.clone();
                    let entry_path = Arc::new(entry_for_reconstruct.path.clone());
                    let output_path = dir.join((*entry_path).clone());

                    let internal_callback: FileProgressCallback = Arc::new({
                        let gbw = global_bytes_written.clone();
                        let gbr = global_bytes_received.clone();
                        let fpm = file_progress_map.clone();
                        let start_time = start_time.clone();
                        let global_on_progress = global_on_progress.clone();

                        move |file_progress: f64,
                              file_bytes_written: usize,
                              file_bytes_received: Option<usize>| {
                            let gbw = gbw.clone();
                            let gbr = gbr.clone();
                            let fpm = fpm.clone();
                            let start_time = start_time.clone();
                            let gcb = global_on_progress.clone();
                            let entry_path_cb = entry_path.clone();

                            tokio::spawn(async move {
                                {
                                    let mut map = fpm.lock().await;
                                    map.insert((*entry_path_cb).clone(), file_progress);
                                }
                                {
                                    let mut gw = gbw.lock().await;
                                    *gw += file_bytes_written;
                                }

                                if let Some(bytes) = file_bytes_received {
                                    let mut gr = gbr.lock().await;
                                    *gr += bytes;
                                }

                                if let Some(cb) = &gcb {
                                    let written = *gbw.lock().await;
                                    let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
                                    let reconstruct_progress =
                                        (written as f64 / total_bytes_to_write as f64 * 100.0)
                                            .min(100.0);
                                    let disk_speed = written as f64 / elapsed;

                                    if is_network_source {
                                        let received = *gbr.lock().await;
                                        let network_progress =
                                            (received as f64 / total_network_bytes as f64 * 100.0)
                                                .min(100.0);
                                        let net_speed = received as f64 / elapsed;
                                        cb(
                                            reconstruct_progress,
                                            disk_speed as usize,
                                            Some(network_progress),
                                            Some(net_speed as usize),
                                        );
                                    } else {
                                        cb(reconstruct_progress, disk_speed as usize, None, None);
                                    }
                                }
                            });
                        }
                    });

                    let call_opts = owned_options.as_ref();
                    let service_local = service.clone();

                    match service_local
                        .reconstruct_file(
                            &entry_for_reconstruct,
                            &output_path,
                            chunk_source.as_ref(),
                            call_opts,
                            Some(internal_callback),
                        )
                        .await
                    {
                        Ok(_) => {
                            file_progress_map
                                .lock()
                                .await
                                .insert(entry_for_reconstruct.path.clone(), 100.0);
                        }
                        Err(e) => {
                            *error.lock().await = Some(e);
                            break;
                        }
                    }
                }
            }
        };

        let tasks: Vec<_> = (0..concurrency).map(|_| tokio::spawn(worker())).collect();

        join_all(tasks).await;

        if let Some(err) = error.lock().await.take() {
            return Err(err);
        }

        Ok(())
    }

    async fn reconstruct_to_stream(
        self: Arc<Self>,
        entry: FileEntry,
        chunk_source: Arc<dyn ChunkSource + Send + Sync>,
    ) -> Result<Pin<Box<dyn AsyncRead + Send + Sync>>, ReconstructionError> {
        let chunks = entry.chunks.clone();
        let (mut writer, reader): (DuplexStream, DuplexStream) = duplex(64 * 1024);

        let chunk_source_clone = Arc::clone(&chunk_source);

        tokio::spawn(async move {
            let mut stream = self
                .fetch_chunks_smart(&chunks, chunk_source_clone.as_ref(), true)
                .await;

            while let Some((_hash, data)) = stream.next().await {
                let result = self.write_to_stream(data, Some(&mut writer), None).await;

                if let Err(_) = result {
                    let _ = writer.shutdown().await;
                    return;
                }
            }

            let _ = writer.shutdown().await;
        });

        Ok(Box::pin(reader))
    }
}

#[cfg(test)]
mod memory_reconstruction_service_tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::io::{AsyncRead, AsyncReadExt};

    use crate::{
        BaseStorageConfig, Blake3HasherService, Chunk, ChunkSource, DeltaPlan, FileEntry,
        HashStorageAdapter, HasherService, LocalStorageAdapter, LocalStorageConfig,
        MemoryReconstructionService, ReconstructionService, StorageAdapterEnum, StorageChunkSource,
    };

    fn buffer_to_box_read(buf: Vec<u8>) -> Box<dyn tokio::io::AsyncRead + Send + Unpin> {
        Box::new(std::io::Cursor::new(buf))
    }

    #[tokio::test]
    async fn reconstructs_single_file_from_chunks() {
        let base_dir = PathBuf::from("tmp-test-rust");
        fs::create_dir_all(&base_dir).unwrap();

        let adapter = LocalStorageAdapter::new(LocalStorageConfig {
            base_path: base_dir.clone(),
            base: BaseStorageConfig { path_prefix: None },
        });
        let arc_adapter: Arc<dyn HashStorageAdapter + Send + Sync> = Arc::new(adapter);
        let chunk_source =
            StorageChunkSource::new(StorageAdapterEnum::Hash(arc_adapter.clone()), None);

        let hasher = Arc::new(Blake3HasherService::new());
        let service = MemoryReconstructionService::new(hasher.clone());

        let data1 = b"Hello ".to_vec();
        let data2 = b"World!".to_vec();
        let hash1 = hasher.hash_buffer(&data1).await.unwrap();
        let hash2 = hasher.hash_buffer(&data2).await.unwrap();

        arc_adapter
            .put_chunk(&hash1, buffer_to_box_read(data1.clone()), None)
            .await
            .unwrap();
        arc_adapter
            .put_chunk(&hash2, buffer_to_box_read(data2.clone()), None)
            .await
            .unwrap();

        let entry = FileEntry {
            path: "file.txt".to_string(),
            hash: hasher
                .hash_buffer(&[data1.clone(), data2.clone()].concat())
                .await
                .unwrap(),
            modified_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            size: (data1.len() + data2.len()) as u64,
            chunks: vec![
                Chunk {
                    hash: hash1.clone(),
                    offset: 0,
                    size: data1.len() as u64,
                },
                Chunk {
                    hash: hash2.clone(),
                    offset: data1.len() as u64,
                    size: data2.len() as u64,
                },
            ],
        };

        let plan = DeltaPlan {
            new_and_modified_files: vec![entry.clone()],
            obsolete_chunks: vec![],
            missing_chunks: vec![],
            deleted_files: vec![],
        };

        let output_dir = base_dir.join("output");
        fs::create_dir_all(&output_dir).unwrap();

        service
            .reconstruct_all(&plan, &output_dir, Arc::new(chunk_source), None)
            .await
            .unwrap();

        let chunk_opt: Option<Box<dyn AsyncRead + Send + Unpin + 'static>> =
            arc_adapter.get_chunk(&hash1).await.unwrap();
        assert!(chunk_opt.is_some());

        let mut reader = chunk_opt.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();

        assert!(!buf.is_empty());

        let info = arc_adapter.get_chunk_info(&hash1).await.unwrap();
        assert!(info.is_some());

        fs::remove_dir_all(&base_dir).unwrap();
    }

    #[tokio::test]
    async fn reconstruct_to_stream_returns_correct_buffer() {
        let base_dir = PathBuf::from("tmp-test-stream");
        fs::create_dir_all(&base_dir).unwrap();

        let adapter = LocalStorageAdapter::new(LocalStorageConfig {
            base_path: base_dir.clone(),
            base: BaseStorageConfig { path_prefix: None },
        });
        let arc_adapter: Arc<dyn HashStorageAdapter + Send + Sync> = Arc::new(adapter);
        let chunk_source: Arc<dyn ChunkSource + Send + Sync> = Arc::new(StorageChunkSource::new(
            StorageAdapterEnum::Hash(arc_adapter.clone()),
            None,
        ));
        let hasher = Arc::new(Blake3HasherService::new());
        let service = MemoryReconstructionService::new(hasher.clone());

        let data = b"Stream me".to_vec();
        let hash = hasher.hash_buffer(&data).await.unwrap();

        arc_adapter
            .put_chunk(&hash, buffer_to_box_read(data.clone()), None)
            .await
            .unwrap();

        let entry = FileEntry {
            path: "stream.txt".to_string(),
            hash: hash.clone(),
            modified_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            size: data.len() as u64,
            chunks: vec![Chunk {
                hash: hash.clone(),
                offset: 0,
                size: data.len() as u64,
            }],
        };

        let stream = service
            .clone()
            .reconstruct_to_stream(entry, chunk_source.clone())
            .await
            .unwrap();

        let mut reader = stream;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(buf, data);

        fs::remove_dir_all(&base_dir).unwrap();
    }

    #[tokio::test]
    async fn throws_if_chunk_is_missing() {
        let base_dir = PathBuf::from("tmp-test-missing");
        fs::create_dir_all(&base_dir).unwrap();

        let adapter = LocalStorageAdapter::new(LocalStorageConfig {
            base_path: base_dir.clone(),
            base: BaseStorageConfig { path_prefix: None },
        });
        let arc_adapter: Arc<dyn HashStorageAdapter + Send + Sync> = Arc::new(adapter);
        let chunk_source: Arc<dyn ChunkSource + Send + Sync> = Arc::new(StorageChunkSource::new(
            StorageAdapterEnum::Hash(arc_adapter.clone()),
            None,
        ));
        let hasher = Arc::new(Blake3HasherService::new());
        let service = MemoryReconstructionService::new(hasher.clone());

        let missing_hash = "not-exist".to_string();
        let entry = FileEntry {
            path: "fail.txt".to_string(),
            hash: missing_hash.clone(),
            modified_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            size: 5,
            chunks: vec![Chunk {
                hash: missing_hash.clone(),
                offset: 0,
                size: 5,
            }],
        };

        let result = service
            .reconstruct_file(
                &entry,
                &base_dir.join("fail.txt"),
                chunk_source.as_ref(),
                None,
                None,
            )
            .await;

        assert!(result.is_err());

        fs::remove_dir_all(&base_dir).unwrap();
    }

    #[tokio::test]
    async fn performs_in_place_reconstruction_for_large_file() {
        let base_dir = PathBuf::from("tmp-test-large");
        fs::create_dir_all(&base_dir).unwrap();

        let adapter = LocalStorageAdapter::new(LocalStorageConfig {
            base_path: base_dir.clone(),
            base: BaseStorageConfig { path_prefix: None },
        });
        let arc_adapter: Arc<dyn HashStorageAdapter + Send + Sync> = Arc::new(adapter);
        let chunk_source: Arc<dyn ChunkSource + Send + Sync> = Arc::new(StorageChunkSource::new(
            StorageAdapterEnum::Hash(arc_adapter.clone()),
            None,
        ));
        let hasher = Arc::new(Blake3HasherService::new());
        let service = MemoryReconstructionService::new(hasher.clone());

        let data1 = b"AAAA".to_vec();
        let data2 = b"BBBB".to_vec();
        let full_data = [data1.clone(), data2.clone()].concat();

        let hash1 = hasher.hash_buffer(&data1).await.unwrap();
        let hash2 = hasher.hash_buffer(&data2).await.unwrap();
        let full_hash = hasher.hash_buffer(&full_data).await.unwrap();

        arc_adapter
            .put_chunk(&hash1, buffer_to_box_read(data1.clone()), None)
            .await
            .unwrap();
        arc_adapter
            .put_chunk(&hash2, buffer_to_box_read(data2.clone()), None)
            .await
            .unwrap();

        let output_path = base_dir.join("bigfile.txt");
        let large_buffer = vec![b'Z'; 1024 * 1024 * 2]; // 2MB
        fs::write(&output_path, &large_buffer).unwrap();

        let entry = FileEntry {
            path: "bigfile.txt".to_string(),
            hash: full_hash,
            modified_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            size: full_data.len() as u64,
            chunks: vec![
                Chunk {
                    hash: hash1.clone(),
                    offset: 0,
                    size: data1.len() as u64,
                },
                Chunk {
                    hash: hash2.clone(),
                    offset: data1.len() as u64,
                    size: data2.len() as u64,
                },
            ],
        };

        let plan = DeltaPlan {
            new_and_modified_files: vec![entry.clone()],
            obsolete_chunks: vec![],
            missing_chunks: vec![],
            deleted_files: vec![],
        };

        service
            .reconstruct_all(
                &plan,
                &base_dir,
                chunk_source,
                Some(&crate::ReconstructionOptions {
                    in_place_reconstruction_threshold: Some(1024),
                    verify_after_rebuild: Some(true),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        let rebuilt = fs::read(&output_path).unwrap();
        assert_eq!(&rebuilt[..full_data.len()], &full_data[..]);

        fs::remove_dir_all(&base_dir).unwrap();
    }

    #[tokio::test]
    async fn reconstructs_via_temp_file_for_small_file() {
        let base_dir = PathBuf::from("tmp-test-tempfile");
        fs::create_dir_all(&base_dir).unwrap();

        let adapter = LocalStorageAdapter::new(LocalStorageConfig {
            base_path: base_dir.clone(),
            base: BaseStorageConfig { path_prefix: None },
        });
        let arc_adapter: Arc<dyn HashStorageAdapter + Send + Sync> = Arc::new(adapter);
        let chunk_source: Arc<dyn ChunkSource + Send + Sync> = Arc::new(StorageChunkSource::new(
            StorageAdapterEnum::Hash(arc_adapter.clone()),
            None,
        ));
        let hasher = Arc::new(Blake3HasherService::new());
        let service = MemoryReconstructionService::new(hasher.clone());

        let original = b"OLD".to_vec();
        let chunk = b"NEW".to_vec();
        let hash = hasher.hash_buffer(&chunk).await.unwrap();

        arc_adapter
            .put_chunk(&hash, buffer_to_box_read(chunk.clone()), None)
            .await
            .unwrap();

        let entry = FileEntry {
            path: "small.txt".to_string(),
            hash: hash.clone(),
            modified_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            size: chunk.len() as u64,
            chunks: vec![Chunk {
                hash: hash.clone(),
                offset: 0,
                size: chunk.len() as u64,
            }],
        };

        let output_path = base_dir.join("small.txt");
        fs::write(&output_path, &original).unwrap();

        let plan = DeltaPlan {
            new_and_modified_files: vec![entry.clone()],
            obsolete_chunks: vec![],
            missing_chunks: vec![],
            deleted_files: vec![],
        };

        service
            .reconstruct_all(
                &plan,
                &base_dir,
                chunk_source,
                Some(&crate::ReconstructionOptions {
                    verify_after_rebuild: Some(true),
                    in_place_reconstruction_threshold: Some(1024 * 1024),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        let rebuilt = fs::read(&output_path).unwrap();
        assert_eq!(rebuilt, b"NEW");

        fs::remove_dir_all(&base_dir).unwrap();
    }
}
