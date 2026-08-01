# AGENTS.md

`compression-pool`: compresión de vídeo distribuida en Rust (binario único con subcomandos `head` y `worker`).

## Comandos

- `cargo build` / `cargo test` — compila y corre los 17 tests unitarios (todos en `#[cfg(test)]` dentro de cada módulo; no hay `tests/`).
- Un test concreto: `cargo test paths::tests::collisions`.
- Ejecutar: `cargo run -- worker --port 9111 --max-works 1` y `cargo run -- head --settings settings.toml` (plantilla: `settings.toml.example`).
- El worker necesita `ffmpeg` y `ab-av1` en `PATH` y usa rutas relativas `./loaded/` y `./finished/`: lánzalo siempre desde su propio directorio de trabajo, nunca desde la raíz del repo.

## Convenciones que no son obvias

- **El worker guarda archivos por `task_id` (UUID), no por nombre original** (decisión deliberada del usuario para evitar colisiones de nombres). El nombre final solo viaja en `FinishedWork.filename` y en `Content-Disposition`. No "simplifiques" esto volviendo a nombres de archivo.
- Los argumentos de `ab-av1`/`ffmpeg` los define el head y viajan en cada `WorkRequest`; el worker NO tiene CLI para ellos. Se separan con `shell-words`; nunca invocar un shell.
- El temporal de ffmpeg debe terminar en `.{container}` (`.encode-{id}.tmp.{container}`): sin extensión ffmpeg no deduce el muxer y falla con "Invalid argument".
- Enums del API en PascalCase (`CrfSearch`, `Encode`, `Succeeded`, `Failed`); el metadato `no-suitable-crf` (`NO_CRF_METADATA` en `types.rs`) es el convenio head↔worker para "copiar el original sin codificar".
- Todo nombre recibido por HTTP es no confiable: validar con `paths::validate_filename` antes de tocar el disco; construir URLs con `client.url_with_segments` (nunca interpolando nombres).
- Reserva de capacidad e idempotencia por `task_id` viven en `worker/jobs.rs::reserve` bajo un único mutex; cualquier cambio debe mantener los tests `concurrent_reservation_never_exceeds_max_works` e `idempotent_same_task_id`.

## Estructura

- `src/types.rs` — contratos JSON compartidos head↔worker (cambiar aquí rompe el protocolo; tocar ambos lados).
- `src/cmd/head/` — `client.rs` (HTTP, streaming), `queue.rs` (FIFO con prioridad de `Encode`), `orchestrator.rs` (sondeo ~1 s, reintentos máx. 3, descarga y publicación atómica).
- `src/cmd/worker/` — `jobs.rs` (reserva + procesos), `files.rs` (upload/download/clear), `status.rs` (endpoints de estado).

## Verificación E2E

No hay suite de integración automatizada; la verificación real es manual: worker en un dir temporal + head con un vídeo de prueba (`ffmpeg -f lavfi -i testsrc=duration=2 ...`), comprobando que `outputs/` recibe el resultado, que conserva atime/mtime del original y que `loaded/` y `finished/` del worker quedan vacíos. Probar ambos caminos: con `-crf`/`-b:v` en `ffmpeg-arguments` (salta búsqueda) y sin ellos (búsqueda con ab-av1 → encode).
