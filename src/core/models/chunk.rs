use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Chunk {
    pub hash: String,
    pub offset: u64,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkUrlInfo {
    pub url: String,
    pub offset: u64,
    pub size: u64,
    pub file_path: String,
}
