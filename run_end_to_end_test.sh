#!/usr/bin/env bash
# End-to-end test: builds the binary, launches worker + head over sample.mp4 and
# verifies the result reaches outputs/. On finish it does NOT delete tmp/ so you
# can manually inspect what was generated.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$REPO_DIR/tmp"
WORKER_DIR="$TMP/worker"
PORT="${E2E_PORT:-9111}"
OUTPUT="$TMP/outputs/sample.mp4"

for tool in ffmpeg ab-av1; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "error: '$tool' missing from PATH" >&2
        exit 1
    fi
done

echo "==> building binary"
cargo build --manifest-path "$REPO_DIR/Cargo.toml"

echo "==> preparing $TMP"
rm -rf "$TMP"
mkdir -p "$TMP/inputs" "$TMP/outputs" "$WORKER_DIR"
cp "$REPO_DIR/target/debug/compression-pool" "$TMP/"
cp "$REPO_DIR/sample.mp4" "$TMP/inputs/sample.mp4"

cat > "$TMP/settings.toml" <<EOF
workers = ["http://127.0.0.1:$PORT"]

[folders]
input-folder = "./inputs"
output-folder = "./outputs"

[crf-search]
ab-av1-arguments = "--preset 8 --scd false --pix-format yuv420p --min-vmaf 95"

[encoder]
ffmpeg-arguments = "-c:v libsvtav1 -preset 8 -pix_fmt yuv420p -svtav1-params scd=0 -c:a libopus -b:a 96k"
ffmpeg-container = "mp4"
EOF

echo "==> launching worker (port $PORT)"
(
    cd "$WORKER_DIR"
    exec "$TMP/compression-pool" worker --port "$PORT" --max-works 1 > worker.log 2>&1
) &
WORKER_PID=$!
cleanup() {
    kill "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> launching head"
(
    cd "$TMP"
    exec ./compression-pool head --settings settings.toml > head.log 2>&1
)
HEAD_STATUS=$?

cleanup
trap - EXIT

echo
echo "==> summary"
if [ "$HEAD_STATUS" -ne 0 ]; then
    echo "ERROR: head exited with code $HEAD_STATUS" >&2
    tail -n 30 "$TMP/head.log" >&2
    exit 1
fi

if [ -s "$OUTPUT" ]; then
    echo "OK: final result at $OUTPUT"
    ls -l "$OUTPUT"
else
    echo "ERROR: final result does not exist at $OUTPUT" >&2
    exit 1
fi

if [ -n "$(ls -A "$WORKER_DIR/loaded" 2>/dev/null)" ] \
    || [ -n "$(ls -A "$WORKER_DIR/finished" 2>/dev/null)" ]; then
    echo "WARNING: files remain in worker loaded/ or finished/"
else
    echo "OK: worker loaded/ and finished/ are empty"
fi

echo "==> everything was left in $TMP for manual inspection"
