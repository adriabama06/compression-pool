//! Contratos JSON compartidos entre head y worker.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Metadato especial: ab-av1 indicó que no existe un CRF adecuado.
/// El head debe copiar el archivo original a la salida.
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

/// Solicitud de trabajo enviada por el head al worker.
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
    /// Nombre final que debe tener el resultado en el head (p. ej. "pelicula.mp4").
    pub filename: String,
    /// Para CrfSearch: el CRF encontrado (o NO_CRF_METADATA). Para Encode: vacío.
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
