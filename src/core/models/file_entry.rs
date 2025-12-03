use serde::{Deserialize, Serialize};

use crate::chunk::Chunk;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FileEntry {
    pub path: String,
    pub size: u64,
    pub hash: String,
    pub modified_at: u64,
    pub chunks: Vec<Chunk>,
}
