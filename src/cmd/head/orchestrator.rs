//! Orquestador del head: escaneo, planificación, sondeo, reintentos, descarga
//! y publicación.

use super::client::{SendOutcome, WorkerClient};
use super::queue::{Queues, Task, MAX_ATTEMPTS};
use crate::config::Config;
use crate::types::{FinishedWork, WorkRequest, WorkStatus, WorkType, NO_CRF_METADATA};
use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use uuid::Uuid;

const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Tiempo que una tarea aceptada puede desaparecer de /running y /finished
/// antes de reenviarla al mismo worker con el mismo task_id.
const MISSING_TIMEOUT: Duration = Duration::from_secs(5);
const DOWNLOAD_ATTEMPTS: u32 = 3;

struct ActiveTask {
    task: Task,
    worker: usize,
    missing_since: Option<Instant>,
}

pub struct Orchestrator {
    config: Config,
    clients: Vec<WorkerClient>,
    queues: Queues,
    active: HashMap<Uuid, ActiveTask>,
    failures: Vec<(String, String)>,
}

impl Orchestrator {
    /// Construye el orquestador: escanea la entrada y crea las tareas iniciales.
    pub fn new(config: Config) -> Result<Self> {
        let videos = crate::paths::scan_videos(&config.input_folder)?;
        crate::paths::check_output_collisions(&videos, &config.container)?;

        let skip_crf = crate::config::args_fix_quality(&config.ffmpeg_args);
        let mut queues = Queues::default();
        for v in videos {
            if skip_crf {
                queues
                    .encode
                    .push_back(Task::new(v, WorkType::Encode, config.ffmpeg_args.clone()));
            } else {
                queues
                    .crf_search
                    .push_back(Task::new(v, WorkType::CrfSearch, config.ab_av1_args.clone()));
            }
        }

        let clients = config
            .workers
            .iter()
            .map(|w| WorkerClient::new(w))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            config,
            clients,
            queues,
            active: HashMap::new(),
            failures: Vec::new(),
        })
    }

    /// Espera a que todos los workers configurados respondan a /health.
    async fn wait_all_healthy(&self) {
        loop {
            let mut pending = Vec::new();
            for (i, c) in self.clients.iter().enumerate() {
                if c.health().await.is_err() {
                    pending.push(self.config.workers[i].clone());
                }
            }
            if pending.is_empty() {
                return;
            }
            tracing::info!(?pending, "esperando a que todos los workers respondan");
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    pub async fn run(mut self) -> Result<()> {
        self.wait_all_healthy().await;
        tracing::info!("todos los workers disponibles; comienza la planificación");

        while !self.queues.is_empty() || !self.active.is_empty() {
            let snapshots = self.poll_workers().await;
            self.adopt_and_track(&snapshots);
            self.handle_finished(&snapshots).await;
            self.resend_missing(&snapshots).await;
            self.schedule(&snapshots).await;
            tokio::time::sleep(POLL_INTERVAL).await;
        }

        if !self.failures.is_empty() {
            for (f, why) in &self.failures {
                tracing::error!("fallo en {f}: {why}");
            }
            bail!("{} vídeo(s) fallaron", self.failures.len());
        }
        tracing::info!("todos los vídeos procesados correctamente");
        Ok(())
    }

    /// Sondea /running y /finished de cada worker sin que un fallo bloquee a los demás.
    async fn poll_workers(&self) -> Vec<Option<(crate::types::RunningResponse, crate::types::FinishedResponse)>> {
        let futures = self.clients.iter().map(|c| async {
            let running = c.running().await.ok()?;
            let finished = c.finished().await.ok()?;
            Some((running, finished))
        });
        futures_util::future::join_all(futures).await
    }

    /// Adopta trabajos que un worker ejecuta pero el head aún tiene en cola, y
    /// marca como presentes los activos observados.
    fn adopt_and_track(
        &mut self,
        snapshots: &[Option<(crate::types::RunningResponse, crate::types::FinishedResponse)>],
    ) {
        for (i, snap) in snapshots.iter().enumerate() {
            let Some((running, finished)) = snap else { continue };
            for w in &running.works {
                if let Some(task) = self.queues.remove(&w.id) {
                    tracing::info!(task = %w.id, worker = i, "adoptando tarea ya en ejecución");
                    self.active.insert(
                        w.id,
                        ActiveTask { task, worker: i, missing_since: None },
                    );
                }
            }
            let observed = |id: &Uuid| {
                running.works.iter().any(|w| &w.id == id)
                    || finished.finished.iter().any(|f| &f.task_id == id)
            };
            for a in self.active.values_mut() {
                if a.worker == i && observed(&a.task.id) {
                    a.missing_since = None;
                }
            }
        }
    }

    /// Procesa los resultados terminados que correspondan a tareas activas.
    async fn handle_finished(
        &mut self,
        snapshots: &[Option<(crate::types::RunningResponse, crate::types::FinishedResponse)>],
    ) {
        for (i, snap) in snapshots.iter().enumerate() {
            let Some((_, finished)) = snap else { continue };
            for f in &finished.finished {
                let Some(active) = self.active.get(&f.task_id) else { continue };
                if active.worker != i {
                    continue;
                }
                // Validar que el resultado corresponde al tipo de tarea y archivo esperados.
                if f.work_type != active.task.work_type {
                    tracing::warn!(task = %f.task_id, "resultado con tipo inesperado; ignorando");
                    continue;
                }
                let task = active.task.clone();
                match f.status {
                    WorkStatus::Succeeded => {
                        if let Err(e) = self.handle_success(i, &task, f).await {
                            tracing::error!("error procesando resultado: {e:#}");
                        }
                        self.active.remove(&f.task_id);
                    }
                    WorkStatus::Failed => {
                        self.active.remove(&f.task_id);
                        let _ = self.clients[i].clear(f.task_id).await;
                        self.retry_or_fail(task, &f.error);
                    }
                }
            }
        }
    }

    async fn handle_success(&mut self, worker: usize, task: &Task, f: &FinishedWork) -> Result<()> {
        match task.work_type {
            WorkType::CrfSearch => {
                if f.metadata == NO_CRF_METADATA {
                    // No hay CRF adecuado: copiar el original preservándolo.
                    let src = self.config.input_folder.join(&task.filename);
                    let dst = self.config.output_folder.join(&task.filename);
                    tokio::fs::copy(&src, &dst).await?;
                    tracing::info!("{}: sin CRF adecuado; original copiado a salida", task.filename);
                } else {
                    let crf: u32 = f
                        .metadata
                        .parse()
                        .context("CRF devuelto no es un entero")?;
                    if crf > 63 {
                        bail!("CRF fuera de rango: {crf}");
                    }
                    let mut args = self.config.ffmpeg_args.clone();
                    args.push("-crf".into());
                    args.push(crf.to_string());
                    let mut encode = Task::new(task.filename.clone(), WorkType::Encode, args);
                    encode.affinity = Some(worker); // el archivo ya está en ese worker
                    tracing::info!("{}: CRF {crf} encontrado; encolando codificación", task.filename);
                    self.queues.encode.push_back(encode);
                }
                // El resultado de crf-search ya se procesó: limpiar.
                self.clients[worker].clear(task.id).await?;
            }
            WorkType::Encode => {
                self.download_and_publish(worker, task, f).await?;
                // Solo después de publicar correctamente se limpia el worker.
                self.clients[worker].clear(task.id).await?;
            }
        }
        Ok(())
    }

    /// Descarga el resultado (con reintentos) a un temporal en outputs/, copia
    /// las fechas del original y renombra atómicamente al destino final.
    async fn download_and_publish(
        &self,
        worker: usize,
        task: &Task,
        f: &FinishedWork,
    ) -> Result<()> {
        crate::paths::validate_filename(&f.filename)?;
        let tmp = self
            .config
            .output_folder
            .join(format!(".download-{}.part", Uuid::new_v4()));

        let mut last_err = None;
        for attempt in 1..=DOWNLOAD_ATTEMPTS {
            match self.clients[worker].download(task.id, &tmp).await {
                Ok(()) => {
                    last_err = None;
                    break;
                }
                Err(e) => {
                    tracing::warn!(attempt, "descarga fallida: {e:#}");
                    last_err = Some(e);
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        }
        if let Some(e) = last_err {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e).context("descarga agotó los reintentos");
        }

        let meta = tokio::fs::metadata(&tmp).await?;
        if meta.len() == 0 {
            let _ = tokio::fs::remove_file(&tmp).await;
            bail!("resultado descargado vacío");
        }

        // Copiar fechas de acceso y modificación del archivo original.
        let original = self.config.input_folder.join(&task.filename);
        let orig_meta = std::fs::metadata(&original)?;
        filetime::set_file_times(
            &tmp,
            filetime::FileTime::from_last_access_time(&orig_meta),
            filetime::FileTime::from_last_modification_time(&orig_meta),
        )?;

        let dest: PathBuf = self.config.output_folder.join(&f.filename);
        tokio::fs::rename(&tmp, &dest).await?;
        tracing::info!("{} publicado en {}", task.filename, dest.display());
        Ok(())
    }

    /// Reintenta una tarea fallida (hasta MAX_ATTEMPTS) o registra el fallo.
    fn retry_or_fail(&mut self, mut task: Task, error: &str) {
        task.attempts += 1;
        if task.attempts >= MAX_ATTEMPTS {
            self.failures
                .push((task.filename.clone(), error.to_string()));
        } else {
            tracing::warn!(
                "{}: intento {} fallido ({error}); reintentando",
                task.filename,
                task.attempts
            );
            self.queues.requeue_front(task);
        }
    }

    /// Reenvía tareas aceptadas que desaparecieron de running y finished.
    async fn resend_missing(
        &mut self,
        snapshots: &[Option<(crate::types::RunningResponse, crate::types::FinishedResponse)>],
    ) {
        let ids: Vec<Uuid> = self.active.keys().cloned().collect();
        for id in ids {
            let Some(a) = self.active.get_mut(&id) else { continue };
            if snapshots.get(a.worker).and_then(|s| s.as_ref()).is_none() {
                continue; // worker caído: esperar al sondeo en el que vuelva
            }
            let missing_since = a.missing_since.get_or_insert_with(Instant::now);
            if missing_since.elapsed() < MISSING_TIMEOUT {
                continue;
            }
            let task = a.task.clone();
            let worker = a.worker;
            tracing::warn!(task = %id, worker, "tarea desaparecida; reenviando con el mismo task_id");
            match self.dispatch(worker, &task).await {
                Ok(SendOutcome::Accepted) => {
                    self.active.get_mut(&id).unwrap().missing_since = None;
                }
                Ok(_) | Err(_) => {} // se volverá a intentar en el próximo ciclo
            }
        }
    }

    /// Llena las plazas disponibles de cada worker que responde.
    async fn schedule(
        &mut self,
        snapshots: &[Option<(crate::types::RunningResponse, crate::types::FinishedResponse)>],
    ) {
        for (i, snap) in snapshots.iter().enumerate() {
            let Some((running, _)) = snap else { continue };
            let free = running.max_works.saturating_sub(running.works.len());
            for _ in 0..free {
                let Some(task) = self.queues.pop_for(i) else { break };
                match self.dispatch(i, &task).await {
                    Ok(SendOutcome::Accepted) => {
                        self.active.insert(
                            task.id,
                            ActiveTask { task, worker: i, missing_since: None },
                        );
                    }
                    Ok(SendOutcome::Busy) => {
                        // No la aceptó: liberar afinidad y volver a cola.
                        let mut task = task;
                        task.affinity = None;
                        self.queues.requeue_front(task);
                        break;
                    }
                    Ok(SendOutcome::Conflict(e)) => {
                        self.retry_or_fail(task, &format!("conflicto en worker: {e}"));
                    }
                    Err(e) => {
                        // Respuesta ambigua: mantener afinidad con este worker.
                        tracing::warn!("envío ambiguo a worker {i}: {e:#}");
                        let mut task = task;
                        task.affinity = Some(i);
                        self.queues.requeue_front(task);
                    }
                }
            }
        }
    }

    /// Sube el archivo (si procede) y envía la solicitud de trabajo.
    async fn dispatch(&self, worker: usize, task: &Task) -> Result<SendOutcome> {
        let client = &self.clients[worker];
        let input = self.config.input_folder.join(&task.filename);
        client.upload(task.id, &task.filename, &input).await?;
        let req = WorkRequest {
            task_id: task.id,
            filename: task.filename.clone(),
            arguments: task.arguments.clone(),
            container: self.config.container.clone(),
        };
        match task.work_type {
            WorkType::CrfSearch => client.send_crf_search(&req).await,
            WorkType::Encode => client.send_encode(&req).await,
        }
    }
}
