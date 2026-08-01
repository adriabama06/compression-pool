//! Status endpoints: health, running and finished.

use super::{Shared};
use crate::types::{FinishedResponse, RunningResponse};
use axum::extract::State;
use axum::Json;

pub async fn health() -> &'static str {
    "ok"
}

pub async fn running(State(state): State<Shared>) -> Json<RunningResponse> {
    let works = state.works.lock().await;
    Json(RunningResponse {
        works: works.running.values().map(|e| e.work.clone()).collect(),
        max_works: state.max_works,
    })
}

pub async fn finished(State(state): State<Shared>) -> Json<FinishedResponse> {
    let works = state.works.lock().await;
    Json(FinishedResponse {
        finished: works.finished.values().cloned().collect(),
    })
}
