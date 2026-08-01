//! JSON contracts shared between head and worker.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Special metadata: ab-av1 indicated no suitable CRF exists.
/// The head must copy the original file to the output.
pub const NO_CRF_METADATA: &str = "no-suitable-crf";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkType {
    CrfSearch,
    Encode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkStatus {
    Succeeded,
    Failed,
}

/// Work request sent by the head to the worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkRequest {
    pub task_id: Uuid,
    pub filename: String,
    pub arguments: Vec<String>,
    pub container: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningWork {
    pub id: Uuid,
    pub work_type: WorkType,
    pub filename: String,
    pub start_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningResponse {
    pub works: Vec<RunningWork>,
    pub max_works: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinishedWork {
    pub task_id: Uuid,
    pub work_type: WorkType,
    /// Final name the result must have on the head (e.g. "pelicula.mp4").
    pub filename: String,
    /// For CrfSearch: the found CRF (or NO_CRF_METADATA). For Encode: empty.
    pub metadata: String,
    pub status: WorkStatus,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinishedResponse {
    pub finished: Vec<FinishedWork>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadedResponse {
    pub files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClearRequest {
    pub task_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
