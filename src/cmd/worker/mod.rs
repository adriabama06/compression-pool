//! Worker: servidor HTTP que recibe vídeos y tareas, ejecuta ab-av1/ffmpeg y
//! publica resultados.
//!
//! Los archivos se guardan internamente con el ID de la tarea (no con el
//! nombre original) para evitar conflictos entre archivos con el mismo nombre.
//! El nombre final solo se expone al head (metadatos y Content-Disposition).

pub mod files;
pub mod jobs;
pub mod status;

use crate::types::{FinishedWork, RunningWork};
use anyhow::Result;
use axum::{routing::{delete, get, post}, Router};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub const LOADED_DIR: &str = "./loaded";
pub const FINISHED_DIR: &str = "./finished";

pub struct RunningEntry {
    pub work: RunningWork,
}

#[derive(Default)]
pub struct Inner {
    pub running: HashMap<Uuid, RunningEntry>,
    pub finished: HashMap<Uuid, FinishedWork>,
}

pub struct WorkerState {
    pub inner: Mutex<Inner>,
    pub max_works: usize,
}

pub type Shared = Arc<WorkerState>;

pub fn loaded_path(task_id: &Uuid) -> PathBuf {
    Path::new(LOADED_DIR).join(task_id.to_string())
}

pub fn finished_path(task_id: &Uuid) -> PathBuf {
    Path::new(FINISHED_DIR).join(task_id.to_string())
}

pub fn encode_tmp_path(task_id: &Uuid, container: &str) -> PathBuf {
    Path::new(FINISHED_DIR).join(format!(".encode-{task_id}.tmp.{container}"))
}

pub async fn run(port: u16, max_works: usize) -> Result<()> {
    tokio::fs::create_dir_all(LOADED_DIR).await?;
    tokio::fs::create_dir_all(FINISHED_DIR).await?;

    let state: Shared = Arc::new(WorkerState {
        inner: Mutex::new(Inner::default()),
        max_works,
    });

    let app = Router::new()
        .route("/health", get(status::health))
        .route("/running", get(status::running))
        .route("/finished", get(status::finished))
        .route("/load", post(files::load))
        .route("/loaded", get(files::loaded))
        .route("/crf-search", post(jobs::crf_search))
        .route("/encode", post(jobs::encode))
        .route("/finished/download/{task_id}", get(files::download))
        .route("/finished/clear", delete(files::clear))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, max_works, "worker escuchando");
    axum::serve(listener, app).await?;
    Ok(())
}
