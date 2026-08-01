//! Carga, descarga y limpieza de archivos del worker.
//!
//! Los archivos se almacenan por task_id, no por nombre original, evitando
//! conflictos entre archivos con el mismo nombre. El nombre final viaja en los
//! metadatos y en la cabecera Content-Disposition de la descarga.

use super::{finished_path, loaded_path, Shared, LOADED_DIR};
use crate::types::{ClearRequest, ErrorResponse, LoadedResponse, WorkStatus, WorkType};
use axum::body::Body;
use axum::extract::{Multipart, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::Json;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

fn err(status: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (
        status,
        Json(ErrorResponse { error: msg.into() }),
    )
}

/// Parámetros de query de POST /load: task_id va en la URL, no en el multipart.
#[derive(serde::Deserialize)]
pub struct LoadParams {
    pub task_id: Uuid,
}

/// POST /load?task_id={uuid}: recibe multipart con el campo "file".
/// Escritura en streaming a un temporal y renombrado atómico.
pub async fn load(
    State(_state): State<Shared>,
    Query(params): Query<LoadParams>,
    mut multipart: Multipart,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?
    {
        match field.name() {
            Some("file") => {
                if let Some(fname) = field.file_name() {
                    crate::paths::validate_filename(fname)
                        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
                }

                let tmp = std::path::Path::new(LOADED_DIR)
                    .join(format!(".upload-{}.part", params.task_id));

                let mut file = tokio::fs::File::create(&tmp)
                    .await
                    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                let write_result: Result<(), String> = async {
                    while let Some(chunk) = field.chunk().await.map_err(|e| e.to_string())? {
                        file.write_all(&chunk).await.map_err(|e| e.to_string())?;
                    }

                    file.flush().await.map_err(|e| e.to_string())
                }
                .await;

                if let Err(e) = write_result {
                    let _ = tokio::fs::remove_file(&tmp).await;
                    return Err(err(StatusCode::INTERNAL_SERVER_ERROR, e));
                }

                tokio::fs::rename(&tmp, loaded_path(&params.task_id))
                    .await
                    .map_err(|e| err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                return Ok(StatusCode::OK);
            }
            _ => {}
        }
    }

    Err(err(StatusCode::BAD_REQUEST, "falta el campo file"))
}

/// GET /loaded: lista los archivos cargados (IDs de tarea). Informativo.
pub async fn loaded(State(_state): State<Shared>) -> Json<LoadedResponse> {
    let mut files = Vec::new();
    if let Ok(mut rd) = tokio::fs::read_dir(LOADED_DIR).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.starts_with('.') {
                files.push(name);
            }
        }
    }
    files.sort();
    Json(LoadedResponse { files })
}

/// GET /finished/download/{task_id}: descarga en streaming un resultado.
/// Solo se permite si existe una tarea Encode terminada con éxito que declara
/// ese resultado; no se exponen rutas arbitrarias de finished/.
pub async fn download(
    State(state): State<Shared>,
    Path(task_id): Path<String>,
) -> Result<(HeaderMap, Body), (StatusCode, Json<ErrorResponse>)> {
    let id = Uuid::parse_str(&task_id)
        .map_err(|_| err(StatusCode::BAD_REQUEST, "task_id no es un UUID"))?;

    let final_name = {
        let works = state.works.lock().await;
        match works.finished.get(&id) {
            Some(f)
                if f.work_type == WorkType::Encode && f.status == WorkStatus::Succeeded =>
            {
                f.filename.clone()
            }
            _ => return Err(err(StatusCode::NOT_FOUND, "resultado no disponible")),
        }
    };

    let file = tokio::fs::File::open(finished_path(&id))
        .await
        .map_err(|_| err(StatusCode::NOT_FOUND, "archivo de resultado no encontrado"))?;

    let mut headers = HeaderMap::new();
    // El head debe recibirlo con el nombre final que tendrá el archivo.
    let disposition = format!("attachment; filename=\"{}\"", final_name.replace('"', "_"));
    headers.insert(header::CONTENT_DISPOSITION, disposition.parse().unwrap());
    headers.insert(
        header::CONTENT_TYPE,
        "application/octet-stream".parse().unwrap(),
    );

    Ok((headers, Body::from_stream(ReaderStream::new(file))))
}

/// DELETE /finished/clear: el head confirma que procesó el resultado.
/// Idempotente: si ya no existe, responde éxito igualmente.
pub async fn clear(
    State(state): State<Shared>,
    Json(req): Json<ClearRequest>,
) -> StatusCode {
    let exists = {
        let mut works = state.works.lock().await;
        works.finished.remove(&req.task_id).is_some()
    };
    if exists {
        let _ = tokio::fs::remove_file(finished_path(&req.task_id)).await;
        let _ = tokio::fs::remove_file(loaded_path(&req.task_id)).await;
    }
    StatusCode::OK
}
