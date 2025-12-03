use futures::pin_mut;
use futures::stream;
use futures::stream::{Stream, StreamExt, TryStreamExt};

use async_stream::try_stream;

use async_trait::async_trait;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::fs::{metadata, read_dir};
use tokio::sync::Mutex;

use crate::Chunk;
use crate::{
    AsyncChunkStream, ChunkEntry, DeltaError, DeltaPlan, DeltaService, FileEntry, HasherService,
    RDIndex,
};

pub struct MemoryDeltaService {
    pub hasher: Arc<dyn HasherService + Send + Sync>,
}

impl MemoryDeltaService {
    pub fn new(hasher: Arc<dyn HasherService + Send + Sync>) -> Self {
        Self { hasher }
    }

    /// Walks recursively a directory returning relative paths, optionally ignoring patterns
    fn walk_files<'a>(
        &'a self,
        dir: &'a Path,
        prefix: Option<&'a Path>,
        ignore_patterns: Option<&'a [String]>,
    ) -> impl Stream<Item = Result<PathBuf, DeltaError>> + 'a {
        try_stream! {
            let mut entries = read_dir(dir).await?;

            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let rel_path = if let Some(pfx) = prefix {
                    pfx.join(entry.file_name())
                } else {
                    PathBuf::from(entry.file_name())
                };
                let rel_path_str = rel_path.to_string_lossy().replace("\\", "/");

                if let Some(patterns) = ignore_patterns {
                    if patterns.iter().any(|pat| self.matches_pattern(&rel_path_str, pat)) {
                        continue;
                    }
                }

                let file_type = entry.file_type().await?;
                if file_type.is_file() {
                    yield rel_path;
                } else if file_type.is_dir() {
                    let nested_stream = Box::pin(self.walk_files(&path, Some(&rel_path), ignore_patterns));
                    pin_mut!(nested_stream);

                    while let Some(nested) = nested_stream.next().await {
                        yield nested?;
                    }
                }
            }
        }
    }

    fn matches_pattern(&self, path: &str, pattern: &str) -> bool {
        let regex = self.glob_to_regex(pattern);
        regex.is_match(path)
    }

    fn glob_to_regex(&self, glob: &str) -> regex::Regex {
        let glob = glob.replace("\\", "/");

        let mut pattern = regex::escape(&glob);
        pattern = pattern.replace(r"\*\*/?", "(?:.*/)?");
        pattern = pattern.replace(r"\*", "[^/]*");

        let final_pattern = format!("^{}", pattern);

        regex::Regex::new(&final_pattern).unwrap()
    }
}

#[async_trait]
impl DeltaService for MemoryDeltaService {
    async fn create_index_from_directory(
        &self,
        root_path: &Path,
        chunk_size: u64,
        concurrency: Option<usize>,
        ignore_patterns: Option<Vec<String>>,
    ) -> Result<RDIndex, DeltaError> {
        let root_dir = {
            let path = Path::new(root_path);
            if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::env::current_dir()?.join(path)
            }
        };

        let mut file_paths: Vec<PathBuf> = self
            .walk_files(&root_dir, None, ignore_patterns.as_deref())
            .try_collect()
            .await?;

        file_paths.retain(|p| p.to_string_lossy() != "rd-index.json");

        let results: Vec<FileEntry> = stream::iter(file_paths)
            .map(|rel_path| {
                let root_dir = root_dir.clone();
                let hasher = &self.hasher;
                async move {
                    let full_path = root_dir.join(&rel_path);
                    let stats = metadata(&full_path).await?;
                    let file_entry: FileEntry = hasher
                        .hash_file(
                            &rel_path.to_string_lossy(),
                            &root_dir.to_string_lossy(),
                            chunk_size,
                        )
                        .await?;

                    Ok::<FileEntry, DeltaError>(FileEntry {
                        path: rel_path.to_string_lossy().replace("\\", "/"),
                        size: file_entry.size,
                        hash: file_entry.hash,
                        modified_at: stats.modified()?.duration_since(UNIX_EPOCH)?.as_millis()
                            as u64,
                        chunks: file_entry.chunks,
                    })
                }
            })
            .buffered(concurrency.unwrap_or(6))
            .try_collect::<Vec<FileEntry>>()
            .await?;

        Ok(RDIndex {
            version: 1,
            created_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            chunk_size,
            files: results,
        })
    }

    async fn create_file_entry_from_stream(
        &self,
        stream: &mut (dyn AsyncChunkStream + Send),
        path: &str,
    ) -> Result<FileEntry, DeltaError> {
        let file_hasher = Arc::new(Mutex::new(self.hasher.create_streaming_hasher().await));

        let chunks = self
            .hasher
            .hash_stream(
                stream,
                Some(Box::new({
                    let file_hasher = Arc::clone(&file_hasher);
                    move |chunk: Vec<u8>| {
                        let file_hasher = Arc::clone(&file_hasher);
                        tokio::task::block_in_place(move || {
                            futures::executor::block_on(async {
                                let mut hasher = file_hasher.lock().await;
                                hasher.update(&chunk);
                            });
                        });
                    }
                })),
            )
            .await?;

        let total_size: u64 = chunks.iter().map(|c| c.size).sum();
        let file_entry = FileEntry {
            path: path.replace("\\", "/"),
            size: total_size,
            hash: file_hasher.lock().await.digest(),
            modified_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            chunks,
        };

        Ok(file_entry)
    }

    fn compare(&self, source: &RDIndex, target: Option<&RDIndex>) -> Result<DeltaPlan, DeltaError> {
        let mut delta = DeltaPlan::default();

        let mut target_map = HashMap::new();
        let source_paths: HashSet<_> = source.files.iter().map(|f| &f.path).collect();

        if let Some(target) = target {
            for file in &target.files {
                target_map.insert(&file.path, file);

                if !source_paths.contains(&file.path) {
                    delta.deleted_files.push(file.path.clone());

                    for chunk in &file.chunks {
                        delta.obsolete_chunks.push(ChunkEntry {
                            file_path: file.path.clone(),
                            chunk: chunk.clone(),
                        });
                    }
                }
            }
        }

        for src_file in &source.files {
            let target_file = target_map.get(&src_file.path);

            if target_file.is_none() {
                delta
                    .missing_chunks
                    .extend(src_file.chunks.iter().map(|c| ChunkEntry {
                        file_path: src_file.path.clone(),
                        chunk: c.clone(),
                    }));
                delta.new_and_modified_files.push(src_file.clone());
                continue;
            }

            let target_file = target_file.unwrap();
            let target_chunks: HashMap<String, &Chunk> = target_file
                .chunks
                .iter()
                .map(|c| (format!("{}@{}", c.hash, c.offset), c))
                .collect();
            let source_keys: HashSet<String> = src_file
                .chunks
                .iter()
                .map(|c| format!("{}@{}", c.hash, c.offset))
                .collect();

            if src_file.hash == target_file.hash {
                continue;
            }

            let mut modified = false;

            for chunk in &src_file.chunks {
                let key = format!("{}@{}", chunk.hash, chunk.offset);
                if !target_chunks.contains_key(&key) {
                    delta.missing_chunks.push(ChunkEntry {
                        file_path: src_file.path.clone(),
                        chunk: chunk.clone(),
                    });
                    modified = true;
                }
            }

            if modified {
                delta.new_and_modified_files.push(src_file.clone());
            }

            for chunk in &target_file.chunks {
                let key = format!("{}@{}", chunk.hash, chunk.offset);
                if !source_keys.contains(&key) {
                    delta.obsolete_chunks.push(ChunkEntry {
                        file_path: src_file.path.clone(),
                        chunk: chunk.clone(),
                    });
                }
            }
        }

        Ok(delta)
    }

    fn merge_plans(&self, base: &DeltaPlan, updates: &DeltaPlan) -> Result<DeltaPlan, DeltaError> {
        fn merge_files(a: &[FileEntry], b: &[FileEntry]) -> Vec<FileEntry> {
            let mut seen = HashSet::new();
            let mut result = Vec::new();
            for f in a.iter().chain(b.iter()) {
                if seen.insert(&f.path) {
                    result.push(f.clone());
                }
            }
            result
        }

        fn merge_chunks(a: &[ChunkEntry], b: &[ChunkEntry]) -> Vec<ChunkEntry> {
            use std::collections::BTreeSet;
            let mut seen = BTreeSet::new();
            let mut result = Vec::new();
            for c in a.iter().chain(b.iter()) {
                let key = (c.file_path.clone(), c.chunk.hash.clone(), c.chunk.offset);
                if seen.insert(key) {
                    result.push(c.clone());
                }
            }
            result
        }

        fn merge_strings(a: &[String], b: &[String]) -> Vec<String> {
            a.iter()
                .chain(b.iter())
                .cloned()
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()
        }

        Ok(DeltaPlan {
            deleted_files: merge_strings(&base.deleted_files, &updates.deleted_files),
            missing_chunks: merge_chunks(&base.missing_chunks, &updates.missing_chunks),
            obsolete_chunks: merge_chunks(&base.obsolete_chunks, &updates.obsolete_chunks),
            new_and_modified_files: merge_files(
                &base.new_and_modified_files,
                &updates.new_and_modified_files,
            ),
        })
    }

    async fn compare_for_upload(
        &self,
        local_index: &RDIndex,
        remote_index: Option<&RDIndex>,
    ) -> Result<DeltaPlan, DeltaError> {
        self.compare(local_index, remote_index)
    }

    async fn compare_for_download(
        &self,
        local_index: Option<&RDIndex>,
        remote_index: &RDIndex,
    ) -> Result<DeltaPlan, DeltaError> {
        self.compare(remote_index, local_index)
    }
}

#[cfg(test)]
mod memory_delta_service_tests {
    use super::*;
    use crate::{AsyncChunkStream, Blake3HasherService};

    struct MockChunkStream {
        data: Vec<Vec<u8>>,
        index: usize,
    }

    #[async_trait]
    impl AsyncChunkStream for MockChunkStream {
        async fn next_chunk(&mut self) -> Option<Vec<u8>> {
            if self.index >= self.data.len() {
                None
            } else {
                let chunk = self.data[self.index].clone();
                self.index += 1;
                Some(chunk)
            }
        }
    }

    fn make_sample_index() -> RDIndex {
        RDIndex {
            version: 1,
            created_at: 123456,
            chunk_size: 5,
            files: vec![FileEntry {
                path: "file1.txt".to_string(),
                size: 10,
                hash: "filehash".to_string(),
                modified_at: 123456,
                chunks: vec![
                    Chunk {
                        hash: "c1".to_string(),
                        offset: 0,
                        size: 5,
                    },
                    Chunk {
                        hash: "c2".to_string(),
                        offset: 5,
                        size: 5,
                    },
                ],
            }],
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_file_entry_from_stream_works() {
        let hasher = Arc::new(Blake3HasherService::new());
        let service = MemoryDeltaService::new(hasher.clone());

        let data = b"HelloWorld".to_vec();
        let mut stream = MockChunkStream {
            data: vec![data.clone()],
            index: 0,
        };

        let file_entry: FileEntry = service
            .create_file_entry_from_stream(&mut stream, "test.txt")
            .await
            .unwrap();

        assert_eq!(file_entry.path, "test.txt");
        assert_eq!(file_entry.size, data.len() as u64);
        assert_eq!(file_entry.chunks.len(), 1);
        assert_eq!(file_entry.chunks[0].size, data.len() as u64);

        let expected_hash = hasher.hash_buffer(&data).await.unwrap();
        assert_eq!(file_entry.hash, expected_hash);
    }

    #[tokio::test]
    async fn merge_delta_plans_without_duplicates() {
        let hasher = Arc::new(Blake3HasherService::new());
        let service = MemoryDeltaService::new(hasher);

        let plan_a = DeltaPlan {
            deleted_files: vec!["a.txt".into()],
            missing_chunks: vec![ChunkEntry {
                file_path: "a.txt".into(),
                chunk: Chunk {
                    hash: "c1".into(),
                    offset: 0,
                    size: 10,
                },
            }],
            ..Default::default()
        };

        let plan_b = DeltaPlan {
            deleted_files: vec!["a.txt".into(), "b.txt".into()],
            missing_chunks: vec![
                ChunkEntry {
                    file_path: "a.txt".into(),
                    chunk: Chunk {
                        hash: "c1".into(),
                        offset: 0,
                        size: 10,
                    },
                },
                ChunkEntry {
                    file_path: "b.txt".into(),
                    chunk: Chunk {
                        hash: "c2".into(),
                        offset: 0,
                        size: 10,
                    },
                },
            ],
            ..Default::default()
        };

        let merged = service.merge_plans(&plan_a, &plan_b).unwrap();
        assert_eq!(merged.deleted_files.len(), 2);
        assert!(merged.deleted_files.contains(&"a.txt".to_string()));
        assert!(merged.deleted_files.contains(&"b.txt".to_string()));
        assert_eq!(merged.missing_chunks.len(), 2);
    }

    #[tokio::test]
    async fn compare_indices_and_produce_missing_chunks() {
        let hasher = Arc::new(Blake3HasherService::new());
        let service = MemoryDeltaService::new(hasher);

        let source_index = make_sample_index();
        let target_index = RDIndex {
            files: vec![],
            ..source_index.clone()
        };

        let delta_plan = service.compare(&source_index, Some(&target_index)).unwrap();

        assert_eq!(delta_plan.missing_chunks.len(), 2);
        assert_eq!(delta_plan.new_and_modified_files.len(), 1);
    }
}
