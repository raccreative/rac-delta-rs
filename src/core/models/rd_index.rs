use crate::file_entry::FileEntry;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RDIndex {
    pub version: u32,
    pub created_at: u64,
    pub chunk_size: u64,
    pub files: Vec<FileEntry>,
}
