#!/usr/bin/env bash
# Prueba end-to-end: compila el binario, lanza worker + head sobre sample.mp4 y
# verifica que el resultado llega a outputs/. Al terminar NO borra tmp/ para
# poder inspeccionar manualmente lo que se generó.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$REPO_DIR/tmp"
WORKER_DIR="$TMP/worker"
PORT="${E2E_PORT:-9111}"
OUTPUT="$TMP/outputs/sample.mp4"

for tool in ffmpeg ab-av1; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "error: falta '$tool' en PATH" >&2
        exit 1
    fi
done

echo "==> compilando binario"
cargo build --manifest-path "$REPO_DIR/Cargo.toml"

echo "==> preparando $TMP"
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

echo "==> lanzando worker (puerto $PORT)"
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

echo "==> lanzando head"
(
    cd "$TMP"
    exec ./compression-pool head --settings settings.toml > head.log 2>&1
)
HEAD_STATUS=$?

cleanup
trap - EXIT

echo
echo "==> resumen"
if [ "$HEAD_STATUS" -ne 0 ]; then
    echo "ERROR: el head terminó con código $HEAD_STATUS" >&2
    tail -n 30 "$TMP/head.log" >&2
    exit 1
fi

if [ -s "$OUTPUT" ]; then
    echo "OK: resultado final en $OUTPUT"
    ls -l "$OUTPUT"
else
    echo "ERROR: no existe el resultado final en $OUTPUT" >&2
    exit 1
fi

if [ -n "$(ls -A "$WORKER_DIR/loaded" 2>/dev/null)" ] \
    || [ -n "$(ls -A "$WORKER_DIR/finished" 2>/dev/null)" ]; then
    echo "AVISO: quedan archivos en loaded/ o finished/ del worker"
else
    echo "OK: loaded/ y finished/ del worker quedaron vacíos"
fi

echo "==> todo quedó en $TMP para inspección manual"
