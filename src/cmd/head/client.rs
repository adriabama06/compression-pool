//! Cliente HTTP del head: timeouts, upload y descarga en streaming.

use crate::types::{ClearRequest, FinishedResponse, RunningResponse, WorkRequest};
use anyhow::{bail, Context, Result};
use futures_util::StreamExt;
use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use url::Url;
use uuid::Uuid;

pub struct WorkerClient {
    pub base: Url,
    http: reqwest::Client,
}

/// Resultado de enviar un trabajo a un worker.
pub enum SendOutcome {
    /// El worker aceptó (o ya tenía la tarea: idempotente).
    Accepted,
    /// 429: no la aceptó; se puede liberar la afinidad.
    Busy,
    /// 409: el task_id ya existe como otra tarea.
    Conflict(String),
}

impl WorkerClient {
    pub fn new(base: &str) -> Result<Self> {
        let mut base = Url::parse(base).context("URL de worker no válida")?;
        if !base.path().ends_with('/') {
            base.set_path(&(base.path().to_string() + "/"));
        }
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(300))
            .build()?;
        Ok(Self { base, http })
    }

    /// Construye una URL uniendo segmentos codificados correctamente
    /// (espacios, '+', '#', etc. no alteran la ruta).
    pub fn url_with_segments(&self, segments: &[&str]) -> Result<Url> {
        let mut url = self.base.clone();
        {
            let mut segs = url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("URL base no admite segmentos"))?;
            segs.pop_if_empty();
            for s in segments {
                segs.push(s);
            }
        }
        Ok(url)
    }

    pub async fn health(&self) -> Result<()> {
        let url = self.base.join("health")?;
        let resp = self.http.get(url).send().await?;
        if !resp.status().is_success() {
            bail!("health respondió {}", resp.status());
        }
        Ok(())
    }

    pub async fn running(&self) -> Result<RunningResponse> {
        let url = self.base.join("running")?;
        Ok(self.http.get(url).send().await?.error_for_status()?.json().await?)
    }

    pub async fn finished(&self) -> Result<FinishedResponse> {
        let url = self.base.join("finished")?;
        Ok(self.http.get(url).send().await?.error_for_status()?.json().await?)
    }

    /// Sube un archivo en streaming como multipart (campo file; task_id en query).
    pub async fn upload(&self, task_id: Uuid, filename: &str, path: &Path) -> Result<()> {
        let mut url = self.base.join("load")?;
        url.set_query(Some(&format!("task_id={task_id}")));
        let file = tokio::fs::File::open(path).await?;
        let part = reqwest::multipart::Part::stream(reqwest::Body::wrap_stream(
            ReaderStream::new(file),
        ))
        .file_name(filename.to_string());
        let form = reqwest::multipart::Form::new().part("file", part);
        let resp = self.http.post(url).multipart(form).send().await?;
        if !resp.status().is_success() {
            bail!("upload respondió {}", resp.status());
        }
        Ok(())
    }

    async fn send_work(&self, route: &str, req: &WorkRequest) -> Result<SendOutcome> {
        let url = self.base.join(route)?;
        let resp = self.http.post(url).json(req).send().await?;
        Ok(match resp.status() {
            s if s.is_success() => SendOutcome::Accepted,
            reqwest::StatusCode::TOO_MANY_REQUESTS => SendOutcome::Busy,
            reqwest::StatusCode::CONFLICT => {
                SendOutcome::Conflict(resp.text().await.unwrap_or_default())
            }
            s => bail!("{route} respondió {s}"),
        })
    }

    pub async fn send_crf_search(&self, req: &WorkRequest) -> Result<SendOutcome> {
        self.send_work("crf-search", req).await
    }

    pub async fn send_encode(&self, req: &WorkRequest) -> Result<SendOutcome> {
        self.send_work("encode", req).await
    }

    /// Descarga en streaming un resultado a un archivo temporal local.
    pub async fn download(&self, task_id: Uuid, dest: &Path) -> Result<()> {
        let url = self.url_with_segments(&["finished", "download", &task_id.to_string()])?;
        let resp = self.http.get(url).send().await?.error_for_status()?;
        let mut file = tokio::fs::File::create(dest).await?;
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            file.write_all(&chunk?).await?;
        }
        file.flush().await?;
        Ok(())
    }

    /// Confirma que el head ya procesó el resultado. Idempotente.
    pub async fn clear(&self, task_id: Uuid) -> Result<()> {
        let url = self.base.join("finished/clear")?;
        let resp = self
            .http
            .delete(url)
            .json(&ClearRequest { task_id })
            .send()
            .await?;
        if !resp.status().is_success() {
            bail!("clear respondió {}", resp.status());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_segments_are_encoded() {
        let c = WorkerClient::new("http://localhost:9111").unwrap();
        let url = c
            .url_with_segments(&["finished", "download", "video clip+#.webm"])
            .unwrap();
        assert_eq!(
            url.as_str(),
            "http://localhost:9111/finished/download/video%20clip+%23.webm"
        );
        // Debe ser un solo segmento: no altera la ruta.
        assert_eq!(url.path_segments().unwrap().count(), 3);
    }
}
