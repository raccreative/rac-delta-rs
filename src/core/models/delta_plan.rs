use crate::{chunk::Chunk, file_entry::FileEntry};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChunkEntry {
    #[serde(flatten)]
    pub chunk: Chunk,
    pub file_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaPlan {
    pub new_and_modified_files: Vec<FileEntry>,
    pub deleted_files: Vec<String>,
    pub missing_chunks: Vec<ChunkEntry>,
    pub obsolete_chunks: Vec<ChunkEntry>,
}

impl Default for DeltaPlan {
    fn default() -> Self {
        Self {
            new_and_modified_files: [].to_vec(),
            deleted_files: [].to_vec(),
            missing_chunks: [].to_vec(),
            obsolete_chunks: [].to_vec(),
        }
    }
}
