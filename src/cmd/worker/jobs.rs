//! Reserva atómica de capacidad y ejecución de ab-av1 / ffmpeg.

use super::{encode_tmp_path, finished_path, loaded_path, RunningEntry, Shared};
use crate::crf;
use crate::types::{
    now_unix, FinishedWork, RunningWork, WorkRequest, WorkStatus, WorkType, NO_CRF_METADATA,
};
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use tokio::process::Command;

/// Reserva capacidad para una tarea de forma atómica (bajo el mutex) y lanza
/// el proceso en segundo plano. Idempotente: mismo task_id + mismo tipo y
/// archivo => éxito sin relanzar.
async fn reserve(
    state: &Shared,
    work_type: WorkType,
    req: &WorkRequest,
) -> Result<(), (StatusCode, String)> {
    // Validación de los datos recibidos por red (no confiables).
    crate::paths::validate_filename(&req.filename)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    crate::paths::normalize_container(&req.container)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let mut works = state.works.lock().await;

    // ¿Ya existe este ID? Comprobar running y finished.
    let existing = works
        .running
        .get(&req.task_id)
        .map(|e| (e.work.work_type, e.work.filename.clone()))
        .or_else(|| {
            works
                .finished
                .get(&req.task_id)
                .map(|f| (f.work_type, f.filename.clone()))
        });
    if let Some((ty, fname)) = existing {
        let same_file = match ty {
            // En finished de Encode el nombre es el de salida; comparar por stem.
            WorkType::Encode => {
                crate::paths::output_name(&req.filename, &req.container) == fname
                    || req.filename == fname
            }
            WorkType::CrfSearch => req.filename == fname,
        };
        if ty == work_type && same_file {
            return Ok(()); // duplicado idempotente
        }
        return Err((
            StatusCode::CONFLICT,
            format!("task_id {} ya existe como otra tarea", req.task_id),
        ));
    }

    // Capacidad.
    if works.running.len() >= state.max_works {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "worker sin capacidad disponible".to_string(),
        ));
    }

    // Reserva atómica: registrar como activo antes de lanzar el proceso.
    works.running.insert(
        req.task_id,
        RunningEntry {
            work: RunningWork {
                id: req.task_id,
                work_type,
                filename: req.filename.clone(),
                start_time: now_unix(),
            },
        },
    );
    Ok(())
}

/// Mueve una tarea de running a finished (una única entrada de resultado).
async fn publish(state: &Shared, result: FinishedWork) {
    let mut works = state.works.lock().await;
    works.running.remove(&result.task_id);
    works.finished.insert(result.task_id, result);
}

pub async fn crf_search(
    State(state): State<Shared>,
    Json(req): Json<WorkRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    reserve(&state, WorkType::CrfSearch, &req).await?;
    let st = state.clone();
    tokio::spawn(async move {
        let result = run_crf_search(&req).await;
        publish(&st, result).await;
    });
    Ok(StatusCode::ACCEPTED)
}

pub async fn encode(
    State(state): State<Shared>,
    Json(req): Json<WorkRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    reserve(&state, WorkType::Encode, &req).await?;
    let st = state.clone();
    tokio::spawn(async move {
        let result = run_encode(&req).await;
        publish(&st, result).await;
    });
    Ok(StatusCode::ACCEPTED)
}

fn failed(req: &WorkRequest, work_type: WorkType, filename: String, error: String) -> FinishedWork {
    FinishedWork {
        task_id: req.task_id,
        work_type,
        filename,
        metadata: String::new(),
        status: WorkStatus::Failed,
        error,
    }
}

async fn run_crf_search(req: &WorkRequest) -> FinishedWork {
    let input = loaded_path(&req.task_id);
    let output = Command::new("ab-av1")
        .arg("crf-search")
        .arg("--input")
        .arg(&input)
        .args(&req.arguments)
        .output()
        .await;

    match output {
        Err(e) => failed(
            req,
            WorkType::CrfSearch,
            req.filename.clone(),
            format!("no se pudo ejecutar ab-av1: {e}"),
        ),
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            tracing::info!(task = %req.task_id, %stdout, %stderr, "ab-av1 finalizado");
            let combined = format!("{stdout}\n{stderr}");
            if !out.status.success() {
                if crf::is_no_suitable_crf(&combined) {
                    // Caso especial: no fallar el vídeo; el head copiará el original.
                    return FinishedWork {
                        task_id: req.task_id,
                        work_type: WorkType::CrfSearch,
                        filename: req.filename.clone(),
                        metadata: NO_CRF_METADATA.to_string(),
                        status: WorkStatus::Succeeded,
                        error: String::new(),
                    };
                }
                return failed(
                    req,
                    WorkType::CrfSearch,
                    req.filename.clone(),
                    format!("ab-av1 exited unsuccessfully: {}", tail(&stderr)),
                );
            }
            match crf::parse_crf(&combined) {
                Some(v) => FinishedWork {
                    task_id: req.task_id,
                    work_type: WorkType::CrfSearch,
                    filename: req.filename.clone(),
                    metadata: v.to_string(),
                    status: WorkStatus::Succeeded,
                    error: String::new(),
                },
                None if crf::is_no_suitable_crf(&combined) => FinishedWork {
                    task_id: req.task_id,
                    work_type: WorkType::CrfSearch,
                    filename: req.filename.clone(),
                    metadata: NO_CRF_METADATA.to_string(),
                    status: WorkStatus::Succeeded,
                    error: String::new(),
                },
                None => failed(
                    req,
                    WorkType::CrfSearch,
                    req.filename.clone(),
                    "no se pudo extraer un CRF válido de la salida de ab-av1".to_string(),
                ),
            }
        }
    }
}

async fn run_encode(req: &WorkRequest) -> FinishedWork {
    let input = loaded_path(&req.task_id);
    let tmp = encode_tmp_path(&req.task_id, &req.container);
    let final_path = finished_path(&req.task_id);
    // Nombre final que tendrá el archivo en el head.
    let final_name = crate::paths::output_name(&req.filename, &req.container);

    let output = Command::new("ffmpeg")
        .arg("-i")
        .arg(&input)
        .args(&req.arguments)
        .arg("-y")
        .arg(&tmp)
        .output()
        .await;

    match output {
        Err(e) => failed(
            req,
            WorkType::Encode,
            req.filename.clone(),
            format!("no se pudo ejecutar ffmpeg: {e}"),
        ),
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            tracing::info!(task = %req.task_id, %stderr, "ffmpeg finalizado");
            if !out.status.success() {
                let _ = tokio::fs::remove_file(&tmp).await;
                return failed(
                    req,
                    WorkType::Encode,
                    req.filename.clone(),
                    format!("ffmpeg exited unsuccessfully: {}", tail(&stderr)),
                );
            }
            // Debe producir un archivo no vacío.
            match tokio::fs::metadata(&tmp).await {
                Ok(m) if m.len() > 0 => {}
                _ => {
                    let _ = tokio::fs::remove_file(&tmp).await;
                    return failed(
                        req,
                        WorkType::Encode,
                        req.filename.clone(),
                        "ffmpeg produjo un archivo vacío".to_string(),
                    );
                }
            }
            // Publicación atómica del resultado.
            if let Err(e) = tokio::fs::rename(&tmp, &final_path).await {
                return failed(
                    req,
                    WorkType::Encode,
                    req.filename.clone(),
                    format!("no se pudo publicar el resultado: {e}"),
                );
            }
            FinishedWork {
                task_id: req.task_id,
                work_type: WorkType::Encode,
                filename: final_name,
                metadata: String::new(),
                status: WorkStatus::Succeeded,
                error: String::new(),
            }
        }
    }
}

fn tail(s: &str) -> String {
    s.lines().rev().take(10).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::worker::{Works, WorkerState};
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    fn state(max_works: usize) -> Shared {
        Arc::new(WorkerState {
            works: Mutex::new(Works::default()),
            max_works,
        })
    }

    fn req(id: Uuid, filename: &str) -> WorkRequest {
        WorkRequest {
            task_id: id,
            filename: filename.to_string(),
            arguments: vec![],
            container: "mp4".to_string(),
        }
    }

    #[tokio::test]
    async fn concurrent_reservation_never_exceeds_max_works() {
        let st = state(1);
        let a = req(Uuid::new_v4(), "a.mkv");
        let b = req(Uuid::new_v4(), "b.mkv");
        let (r1, r2) = tokio::join!(
            reserve(&st, WorkType::Encode, &a),
            reserve(&st, WorkType::Encode, &b),
        );
        let ok = [r1.is_ok(), r2.is_ok()].iter().filter(|b| **b).count();
        let busy = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Err((StatusCode::TOO_MANY_REQUESTS, _))))
            .count();
        assert_eq!(ok, 1, "solo una reserva puede tener éxito");
        assert_eq!(busy, 1, "la otra debe recibir 429");
    }

    #[tokio::test]
    async fn idempotent_same_task_id() {
        let st = state(1);
        let id = Uuid::new_v4();
        reserve(&st, WorkType::Encode, &req(id, "a.mkv")).await.unwrap();
        // Mismo ID, misma tarea: éxito sin lanzar otro proceso.
        reserve(&st, WorkType::Encode, &req(id, "a.mkv")).await.unwrap();
        assert_eq!(st.works.lock().await.running.len(), 1);
        // Mismo ID, otra tarea: conflicto 409.
        let other = reserve(&st, WorkType::CrfSearch, &req(id, "a.mkv")).await;
        assert!(matches!(other, Err((StatusCode::CONFLICT, _))));
        let other = reserve(&st, WorkType::Encode, &req(id, "b.mkv")).await;
        assert!(matches!(other, Err((StatusCode::CONFLICT, _))));
    }

    #[tokio::test]
    async fn rejects_untrusted_filenames() {
        let st = state(1);
        for bad in ["../x.mkv", "carpeta/x.mkv", "carpeta\\x.mkv", ".", "..", "a\0b.mkv"] {
            let r = reserve(&st, WorkType::Encode, &req(Uuid::new_v4(), bad)).await;
            assert!(matches!(r, Err((StatusCode::BAD_REQUEST, _))), "{bad:?}");
        }
        assert!(st.works.lock().await.running.is_empty());
    }
}
