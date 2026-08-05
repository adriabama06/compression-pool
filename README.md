# compression-pool

Distributed video compression written in Rust. A `head` coordinator scans a folder of videos and distributes compression jobs to one or more `worker` servers, which run [`ab-av1`](https://github.com/alexheretic/ab-av1) CRF search and [`ffmpeg`](https://ffmpeg.org) encoding. The compressed results are downloaded back and published to an output folder, preserving the original files' timestamps.

Everything ships as a single binary exposing two subcommands: `head` (the coordinator) and `worker` (the encoder).

## How it works

1. **Head scans** the input folder for videos (`mp4`, `mkv`, `mov`, `avi`, `webm`).
2. It **schedules** tasks to workers based on their reported free capacity, and polls each worker ~1 s.
3. For every video:
   - If the configured `ffmpeg-arguments` already fix quality (`-crf`/`-b:v`), the worker goes straight to `ffmpeg` encode.
   - Otherwise the worker runs `ab-av1 crf-search` first, then a second stage re-encodes with the found CRF.
   - If `ab-av1` finds no suitable CRF, the original file is copied to the output unchanged.
4. The **head downloads** each result (with retries), restores the original's atime/mtime, and atomically publishes it to the `output-folder`.

Workers are resilient: capacity is reserved atomically and tasks are idempotent by `task_id` (a UUID), so a task can be resent after failures (up to 3 attempts) without double-encoding. Files are stored per-`task_id` on the worker to avoid name collisions; the final filename travels only inside the transfer metadata.

## Requirements

- [Rust](https://rustup.rs/) (stable) to build.
- `ffmpeg` and `ab-av1` installed and on `PATH` on every machine running a worker.
- The worker uses the relative directories `./loaded/` and `./finished/` — always launch it from its own working directory.

## Getting the binary

The binary is built for Linux (x64/arm64) and Windows (x64/arm64) on every release. Download the matching archive from the **[Releases](https://github.com/adriabama06/compression-pool/releases)** page of this repository:

- `compression-pool-linux-x64` / `compression-pool-linux-arm64`
- `compression-pool-windows-x64.exe` / `compression-pool-windows-arm64.exe`

Make it executable and rename it to `compression-pool` (on Linux):

```sh
chmod +x compression-pool-linux-x64
mv compression-pool-linux-x64 compression-pool
```

Alternatively build from source:

```sh
cargo build --release
# binary at target/release/compression-pool
```

## Usage

### 1. Run the workers (one per machine)

Put the binary on each machine. For example, you have three computers: **PC1**, **PC2** and **PC3**. Each one runs a worker (each in its own working directory, since the worker needs the relative `./loaded/` and `./finished/` folders):

On **PC1**:

```sh
./compression-pool worker --port 9111 --max-works 2
```

On **PC2**:

```sh
./compression-pool worker --port 9111 --max-works 2
```

On **PC3**:

```sh
./compression-pool worker --port 9111 --max-works 1
```

- `--port` — the HTTP port the worker listens on (default `9111`).
- `--max-works` — how many encodes this worker can run at the same time (default `1`).

Always start each worker from its own working directory.

### 2. Configure the head

The head tells the workers what to compress and where to find them. If **PC1** and **PC2** are on your local network (`192.168.1.10` and `192.168.1.11`), put their addresses in the `workers` list. The head can run on the same machine as one of the workers — for example on PC1, using `http://127.0.0.1:9111` for itself plus PC2's address:

```toml
#         PC1,                      PC2,                        PC3
workers = ["http://127.0.0.1:9111", "http://192.168.1.11:9111", "http://192.168.1.12:9111"]

[folders]
input-folder = "./inputs"
output-folder = "./outputs"

[crf-search]
ab-av1-arguments = "--preset 4 --scd false --pix-format yuv420p --min-vmaf 95"

[encoder]
ffmpeg-arguments = "-c:v libsvtav1 -preset 4 -pix_fmt yuv420p -svtav1-params scd=0 -c:a libopus -b:a 96k"
ffmpeg-container = "mp4"
```

- `workers` — a list of `http://host:port` worker addresses. To use PC3 too, just add it, e.g. `"http://192.168.1.12:9111"`. The head reaches out to every worker in this list and waits until all of them respond.
- `input-folder` / `output-folder` — where to read videos and write results.
- `ab-av1-arguments` and `ffmpeg-arguments` — split with shell syntax (never a real shell). The head sends these verbatim to workers in every work request; the worker has no CLI for them. To force a straight encode without a CRF search, include either `-crf` or `-b:v` in `ffmpeg-arguments`. If `ffmpeg-container` is empty it defaults to `mp4`.

### 3. Run the head

Run the head on whichever machine holds the inputs and outputs (it can be the same machine as a worker, e.g. PC1). It must be launched from the directory containing the input/output folders:

```sh
./compression-pool head --settings settings.toml
```

The head waits until every configured worker answers `/health`, then processes the queue and stops when everything has been encoded and published.

## Worker HTTP API

Exposed by each worker for the head:

| Method | Path | Purpose |
| ------ | ---- | ------- |
| GET | `/health` | liveness probe |
| GET | `/running` | currently running jobs + `max_works` |
| GET | `/finished` | finished results |
| POST | `/load` | upload a video (multipart) |
| POST | `/crf-search` | start an `ab-av1` CRF search |
| POST | `/encode` | start an `ffmpeg` encode |
| GET | `/finished/download/{task_id}` | download a result |
| DELETE | `/finished/clear` | clear a finished task |

## Testing

There are no `tests/` files; all tests are unit tests inside each module.

```sh
cargo test
cargo test paths::tests::collisions   # run a single test
```

End-to-end verification is manual: run a worker in a temp directory, place a test video in the head's `inputs/` (e.g. `ffmpeg -f lavfi -i testsrc=duration=2 ...`), run the head, and confirm `outputs/` receives the result with the original timestamps while the worker's `loaded/` and `finished/` end up empty. Test both paths: with `-crf`/`-b:v` present (skips CRF search) and without (search + encode).

## License

[MIT](./LICENSE)