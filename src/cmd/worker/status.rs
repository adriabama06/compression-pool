//! Endpoints de estado: health, running y finished.

use super::{Shared, WorkerState};
use crate::types::{FinishedResponse, RunningResponse};
use axum::extract::State;
use axum::Json;

pub async fn health() -> &'static str {
    "ok"
}

pub async fn running(State(state): State<Shared>) -> Json<RunningResponse> {
    let inner = state.inner.lock().await;
    Json(RunningResponse {
        works: inner.running.values().map(|e| e.work.clone()).collect(),
        max_works: state.max_works,
    })
}

pub async fn finished(State(state): State<Shared>) -> Json<FinishedResponse> {
    let inner = state.inner.lock().await;
    Json(FinishedResponse {
        finished: inner.finished.values().cloned().collect(),
    })
}

#[allow(dead_code)]
fn _assert_send(_: &WorkerState) {}
