//! Head orchestrator: scanning, scheduling, polling, retries, download
//! and publishing.

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
/// How long an accepted task may disappear from /running and /finished
/// before it is resent to the same worker with the same task_id.
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
    /// Builds the orchestrator: scans the input and creates the initial tasks.
    pub fn new(config: Config) -> Result<Self> {
        let videos = crate::paths::scan_videos(&config.input_folder)?;
        crate::paths::check_output_collisions(&videos, &config.container)?;

        let has_fixed_quality = crate::config::args_fixed_quality(&config.ffmpeg_args);
        let mut queues = Queues::default();
        for v in videos {
            if has_fixed_quality {
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
            .collect::<Result<Vec<WorkerClient>>>()?;

        Ok(Self {
            config,
            clients,
            queues,
            active: HashMap::new(),
            failures: Vec::new(),
        })
    }

    /// Waits until all configured workers respond to /health.
    async fn wait_all_healthy(&self) {
        loop {
            // Add to pending only the clients that give error on health check, so if all clients are ok pending will be empty and the pending.is_empty() will let the code exit
            let mut pending = Vec::new();
            for (i, c) in self.clients.iter().enumerate() {
                if c.health().await.is_err() {
                    pending.push(self.config.workers[i].clone());
                }
            }

            if pending.is_empty() {
                return;
            }

            tracing::info!(?pending, "waiting for all workers to respond");
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    pub async fn run(mut self) -> Result<()> {
        self.wait_all_healthy().await;
        tracing::info!("all workers available; scheduling begins");

        while !self.queues.is_empty() || !self.active.is_empty() {
            // snapshots equivalent to the status of the workers, snapshots is a vector of the status of the worker (the running works and the finished works).
            let snapshots: Vec<Option<(crate::types::RunningResponse, crate::types::FinishedResponse)>> = self.poll_workers().await;
            self.adopt_and_track(&snapshots);
            self.handle_finished(&snapshots).await;
            self.resend_missing(&snapshots).await;
            self.schedule(&snapshots).await;
            tokio::time::sleep(POLL_INTERVAL).await;
        }

        if !self.failures.is_empty() {
            for (f, why) in &self.failures {
                tracing::error!("failure on {f}: {why}");
            }
            bail!("{} video(s) failed", self.failures.len());
        }
        tracing::info!("all videos processed successfully");
        Ok(())
    }

    /// Polls /running and /finished of each worker without a failure blocking the others.
    async fn poll_workers(&self) -> Vec<Option<(crate::types::RunningResponse, crate::types::FinishedResponse)>> {
        let futures = self.clients.iter().map(|c| async {
            let running = c.running().await.ok()?;
            let finished = c.finished().await.ok()?;
            Some((running, finished))
        });
        futures_util::future::join_all(futures).await
    }

    /// Adopts jobs a worker is running but the head still has queued, and
    /// marks observed active tasks as present.
    fn adopt_and_track(
        &mut self,
        snapshots: &[Option<(crate::types::RunningResponse, crate::types::FinishedResponse)>],
    ) {
        for (i, snap) in snapshots.iter().enumerate() {
            let Some((running, finished)) = snap else { continue };
            for w in &running.works {
                if let Some(task) = self.queues.remove(&w.id) {
                    tracing::info!(task = %w.id, worker = i, "adopting already-running task");
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
            for active in self.active.values_mut() {
                if active.worker == i && observed(&active.task.id) {
                    active.missing_since = None;
                }
            }
        }
    }

    /// Processes finished results that match active tasks.
    async fn handle_finished(
        &mut self,
        snapshots: &[Option<(crate::types::RunningResponse, crate::types::FinishedResponse)>],
    ) {
        for (i, snap) in snapshots.iter().enumerate() {
            let Some((_, finished)) = snap else { continue };
            for finished in &finished.finished {
                let Some(active) = self.active.get(&finished.task_id) else { continue };
                if active.worker != i {
                    continue;
                }
                // Verify the result matches the expected task type and file.
                if finished.work_type != active.task.work_type {
                    tracing::warn!(task = %finished.task_id, "result with unexpected type; ignoring");
                    continue;
                }
                let task = active.task.clone();
                match finished.status {
                    WorkStatus::Succeeded => {
                        if let Err(e) = self.handle_success(i, &task, finished).await {
                            tracing::error!("error processing result: {e:#}");
                        }
                        self.active.remove(&finished.task_id);
                    }
                    WorkStatus::Failed => {
                        self.active.remove(&finished.task_id);
                        let _ = self.clients[i].clear(finished.task_id).await;
                        self.retry_or_fail(task, &finished.error);
                    }
                }
            }
        }
    }

    async fn handle_success(&mut self, worker: usize, task: &Task, f: &FinishedWork) -> Result<()> {
        match task.work_type {
            WorkType::CrfSearch => {
                if f.metadata == NO_CRF_METADATA {
                    // No suitable CRF: copy the original preserving it.
                    let src = self.config.input_folder.join(&task.filename);
                    let dst = self.config.output_folder.join(&task.filename);
                    tokio::fs::copy(&src, &dst).await?;
                    tracing::info!("{}: no suitable CRF; original copied to output", task.filename);
                } else {
                    let crf: u32 = f
                        .metadata
                        .parse()
                        .context("returned CRF is not an integer")?;
                    if crf > 63 {
                        bail!("CRF out of range: {crf}");
                    }
                    let mut args = self.config.ffmpeg_args.clone();
                    args.push("-crf".into());
                    args.push(crf.to_string());
                    let mut encode = Task::new(task.filename.clone(), WorkType::Encode, args);
                    encode.preferred_worker = Some(worker); // the file is already on that worker
                    tracing::info!("{}: CRF {crf} found; queuing encode", task.filename);
                    self.queues.encode.push_back(encode);
                }
                // The crf-search result has already been processed: clean up.
                self.clients[worker].clear(task.id).await?;
            }
            WorkType::Encode => {
                self.download_and_publish(worker, task, f).await?;
                // Only after publishing successfully is the worker cleaned up.
                self.clients[worker].clear(task.id).await?;
            }
        }
        Ok(())
    }

    /// Downloads the result (with retries) to a temporary file in outputs/, copies
    /// the original timestamps and atomically renames to the final destination.
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
                    tracing::warn!(attempt, "download failed: {e:#}");
                    last_err = Some(e);
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        }
        if let Some(e) = last_err {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e).context("download exhausted its retries");
        }

        let meta = tokio::fs::metadata(&tmp).await?;
        if meta.len() == 0 {
            let _ = tokio::fs::remove_file(&tmp).await;
            bail!("downloaded result is empty");
        }

        // Copy access and modification timestamps of the original file.
        let original = self.config.input_folder.join(&task.filename);
        let orig_meta = std::fs::metadata(&original)?;
        filetime::set_file_times(
            &tmp,
            filetime::FileTime::from_last_access_time(&orig_meta),
            filetime::FileTime::from_last_modification_time(&orig_meta),
        )?;

        let dest: PathBuf = self.config.output_folder.join(&f.filename);
        tokio::fs::rename(&tmp, &dest).await?;
        tracing::info!("{} published to {}", task.filename, dest.display());
        Ok(())
    }

    /// Retries a failed task (up to MAX_ATTEMPTS) or records the failure.
    fn retry_or_fail(&mut self, mut task: Task, error: &str) {
        task.attempts += 1;
        if task.attempts >= MAX_ATTEMPTS {
            self.failures
                .push((task.filename.clone(), error.to_string()));
        } else {
            tracing::warn!(
                "{}: attempt {} failed ({error}); retrying",
                task.filename,
                task.attempts
            );
            self.queues.requeue_front(task);
        }
    }

    /// Resends accepted tasks that disappeared from running and finished.
    async fn resend_missing(
        &mut self,
        snapshots: &[Option<(crate::types::RunningResponse, crate::types::FinishedResponse)>],
    ) {
        let ids: Vec<Uuid> = self.active.keys().cloned().collect();
        for id in ids {
            let Some(active) = self.active.get_mut(&id) else { continue };
            if snapshots.get(active.worker).and_then(|s| s.as_ref()).is_none() {
                continue; // worker down: wait until a poll where it returns
            }
            let missing_since = active.missing_since.get_or_insert_with(Instant::now);
            if missing_since.elapsed() < MISSING_TIMEOUT {
                continue;
            }
            let task = active.task.clone();
            let worker = active.worker;
            tracing::warn!(task = %id, worker, "task disappeared; resending with the same task_id");
            match self.dispatch(worker, &task).await {
                Ok(SendOutcome::Accepted) => {
                    self.active.get_mut(&id).unwrap().missing_since = None;
                }
                Ok(_) | Err(_) => {} // will be retried on the next cycle
            }
        }
    }

    /// Fills the available slots of each responding worker.
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
                        // It did not accept: release affinity and requeue.
                        let mut task = task;
                        task.preferred_worker = None;
                        self.queues.requeue_front(task);
                        break;
                    }
                    Ok(SendOutcome::Conflict(e)) => {
                        self.retry_or_fail(task, &format!("worker conflict: {e}"));
                    }
                    Err(e) => {
                        // Ambiguous response: keep affinity with this worker.
                        tracing::warn!("ambiguous send to worker {i}: {e:#}");
                        let mut task = task;
                        task.preferred_worker = Some(i);
                        self.queues.requeue_front(task);
                    }
                }
            }
        }
    }

    /// Uploads the file (if applicable) and sends the work request.
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
        
        return match task.work_type {
            WorkType::CrfSearch => client.send_crf_search(&req).await,
            WorkType::Encode => client.send_encode(&req).await,
        };
    }
}
