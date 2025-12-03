use async_trait::async_trait;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::fs;

use crate::{FileEntry, HasherService, RDIndex, ValidationError, ValidationService};

pub struct MemoryValidationService {
    hasher: Arc<dyn HasherService + Send + Sync>,
}

impl MemoryValidationService {
    pub fn new(hasher: Arc<dyn HasherService + Send + Sync>) -> Self {
        Self { hasher }
    }

    fn resolve_path(path: &str) -> PathBuf {
        let path_buf = Path::new(path);
        if path_buf.is_absolute() {
            path_buf.to_path_buf()
        } else {
            std::env::current_dir().unwrap().join(path_buf)
        }
    }
}

#[async_trait]
impl ValidationService for MemoryValidationService {
    async fn validate_file(&self, entry: &FileEntry, path: &str) -> Result<bool, ValidationError> {
        let final_path = Self::resolve_path(path);

        let metadata = match fs::metadata(&final_path).await {
            Ok(meta) => meta,
            Err(_) => return Ok(false),
        };

        if metadata.len() != entry.size {
            return Ok(false);
        }

        match self
            .hasher
            .verify_file(final_path.to_str().unwrap(), &entry.hash)
            .await
        {
            Ok(valid) => Ok(valid),
            Err(_) => Ok(false),
        }
    }

    async fn validate_index(
        &self,
        index: &RDIndex,
        base_path: &str,
    ) -> Result<bool, ValidationError> {
        let directory = Self::resolve_path(base_path);

        for file in &index.files {
            let file_path = directory.join(&file.path);

            let valid = self
                .validate_file(file, file_path.to_str().unwrap())
                .await?;
            if !valid {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[cfg(test)]
mod memory_validation_service_tests {
    use crate::Blake3HasherService;
    use std::io::Write;
    use std::{fs, time::SystemTime};

    use super::*;

    fn tmp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("validation_service_test_{name}"))
    }

    #[tokio::test]
    async fn validate_file_correct_size_and_hash() {
        let tmp = tmp_dir("validate_file_correct_size_and_hash");
        let base_dir = tmp.as_path();

        let hasher = Arc::new(Blake3HasherService::new());
        let service = MemoryValidationService::new(hasher.clone());

        let file_path = base_dir.join("file.txt");
        let content = b"hello".to_vec();

        let _ = fs::create_dir_all(base_dir);
        let _ = fs::write(&file_path, &content);

        let hash = hasher.hash_buffer(&content).await.unwrap();
        let entry = FileEntry {
            path: "file.txt".to_string(),
            hash,
            modified_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            size: content.len() as u64,
            chunks: vec![],
        };

        let valid = service
            .validate_file(&entry, file_path.to_str().unwrap())
            .await
            .unwrap();
        assert!(valid);
    }

    #[tokio::test]
    async fn validate_file_nonexistent_returns_false() {
        let tmp = tmp_dir("validate_file_nonexistent_returns_false");
        let base_dir = tmp.as_path();

        let hasher = Arc::new(Blake3HasherService::new());
        let service = MemoryValidationService::new(hasher);

        let file_path = base_dir.join("nonexistent.txt");

        let entry = FileEntry {
            path: "nonexistent.txt".to_string(),
            hash: "fakehash".to_string(),
            modified_at: 0,
            size: 10,
            chunks: vec![],
        };

        let valid = service
            .validate_file(&entry, file_path.to_str().unwrap())
            .await
            .unwrap();
        assert!(!valid);
    }

    #[tokio::test]
    async fn validate_file_wrong_size_returns_false() {
        let tmp = tmp_dir("validate_file_wrong_size_returns_false");
        let base_dir = tmp.as_path();

        let hasher = Arc::new(Blake3HasherService::new());
        let service = MemoryValidationService::new(hasher.clone());

        let file_path = base_dir.join("wrong-size.txt");
        let content = b"data".to_vec();

        let _ = fs::create_dir_all(base_dir);
        let _ = fs::write(&file_path, &content);

        let hash = hasher.hash_buffer(&content).await.unwrap();
        let entry = FileEntry {
            path: "wrong-size.txt".to_string(),
            hash,
            modified_at: 0,
            size: 999,
            chunks: vec![],
        };

        let valid = service
            .validate_file(&entry, file_path.to_str().unwrap())
            .await
            .unwrap();
        assert!(!valid);
    }

    #[tokio::test]
    async fn validate_file_wrong_hash_returns_false() {
        let tmp = tmp_dir("validate_file_wrong_hash_returns_false");
        let base_dir = tmp.as_path();

        let hasher = Arc::new(Blake3HasherService::new());
        let service = MemoryValidationService::new(hasher);

        let file_path = base_dir.join("wrong-hash.txt");
        let content = b"abc".to_vec();

        let _ = fs::create_dir_all(base_dir);
        let _ = fs::write(&file_path, &content);

        let entry = FileEntry {
            path: "wrong-hash.txt".to_string(),
            hash: "invalidhash".to_string(),
            modified_at: 0,
            size: content.len() as u64,
            chunks: vec![],
        };

        let valid = service
            .validate_file(&entry, file_path.to_str().unwrap())
            .await
            .unwrap();
        assert!(!valid);
    }

    #[tokio::test]
    async fn validate_index_all_valid() {
        let tmp = tmp_dir("validate_index_all_valid");
        let base_dir = tmp.as_path();

        std::fs::create_dir_all(base_dir).unwrap();

        let hasher = Arc::new(Blake3HasherService::new());
        let service = MemoryValidationService::new(hasher.clone());

        let file1 = base_dir.join("a.txt");
        let file2 = base_dir.join("b.txt");
        let c1 = b"x".to_vec();
        let c2 = b"y".to_vec();

        let mut f1 = std::fs::File::create(&file1).unwrap();
        f1.write_all(&c1).unwrap();
        f1.sync_all().unwrap();

        let mut f2 = std::fs::File::create(&file2).unwrap();
        f2.write_all(&c2).unwrap();
        f2.sync_all().unwrap();

        let h1 = hasher.hash_buffer(&c1).await.unwrap();
        let h2 = hasher.hash_buffer(&c2).await.unwrap();

        let index = RDIndex {
            version: 1,
            chunk_size: 10,
            created_at: 0,
            files: vec![
                FileEntry {
                    path: "a.txt".to_string(),
                    hash: h1,
                    modified_at: 0,
                    size: c1.len() as u64,
                    chunks: vec![],
                },
                FileEntry {
                    path: "b.txt".to_string(),
                    hash: h2,
                    modified_at: 0,
                    size: c2.len() as u64,
                    chunks: vec![],
                },
            ],
        };

        let valid = service
            .validate_index(&index, base_dir.to_str().unwrap())
            .await
            .unwrap();
        assert!(valid);
    }

    #[tokio::test]
    async fn validate_index_any_invalid() {
        let tmp = tmp_dir("validate_index_any_invalid");
        let base_dir = tmp.as_path();

        let hasher = Arc::new(Blake3HasherService::new());
        let service = MemoryValidationService::new(hasher.clone());

        let file1 = base_dir.join("valid.txt");
        let c1 = b"hello".to_vec();

        let _ = fs::write(&file1, &c1);
        let h1 = hasher.hash_buffer(&c1).await.unwrap();

        let index = RDIndex {
            version: 1,
            chunk_size: 10,
            created_at: 0,
            files: vec![
                FileEntry {
                    path: "valid.txt".to_string(),
                    hash: h1,
                    modified_at: 0,
                    size: c1.len() as u64,
                    chunks: vec![],
                },
                FileEntry {
                    path: "missing.txt".to_string(),
                    hash: "fake".to_string(),
                    modified_at: 0,
                    size: 5,
                    chunks: vec![],
                },
            ],
        };

        let valid = service
            .validate_index(&index, base_dir.to_str().unwrap())
            .await
            .unwrap();
        assert!(!valid);
    }
}
