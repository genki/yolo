#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="${YOLO_PHOENIX_TEST_IMAGE:-rust:1.95-bookworm}"
container_name="yolo-phoenix-test-$$"
docker_cmd="${YOLO_DOCKER:-docker}"
if ! $docker_cmd info >/dev/null 2>&1 && sudo -n docker info >/dev/null 2>&1; then
  docker_cmd="sudo -n docker"
fi

$docker_cmd run --rm --name "$container_name" \
  -v "$repo_root:/work:rw" \
  -w /work \
  "$image" \
  bash -lc '
set -euo pipefail
export PATH="/usr/local/cargo/bin:$PATH"
apt-get update >/dev/null
apt-get install -y --no-install-recommends python3 jq >/dev/null
cargo build --release
tmp="$(mktemp -d)"
export YOLO_RUNTIME_DIR="$tmp/runtime"
export YOLO_CODEX="/work/tests/fake_codex.py"
export FAKE_CODEX_RUN_LOG="$tmp/runs.jsonl"
export FAKE_CODEX_VERSION_FILE="$tmp/version"
export FAKE_CODEX_THREAD_ID="019e0000-0000-7000-8000-000000000137"
export FAKE_CODEX_CWD="/work"
export YOLO_CODEX_UPGRADE_COMMAND="printf 0.137.0 > \"$FAKE_CODEX_VERSION_FILE\""
export YOLO_UPGRADE_IDLE_WAIT_TIMEOUT_SECS=10
printf 0.135.0 > "$FAKE_CODEX_VERSION_FILE"

./target/release/yolo server --daemon
./target/release/yolo resume "$FAKE_CODEX_THREAD_ID" >"$tmp/client.out" 2>"$tmp/client.err" &
client_pid=$!

for _ in $(seq 1 50); do
  if ./target/release/yolo status --json | jq -e ".clients | length > 0" >/dev/null; then
    break
  fi
  sleep 0.2
done

before_count="$(grep -c "\"kind\": \"client\"" "$FAKE_CODEX_RUN_LOG" || true)"
if [ "$before_count" -ne 1 ]; then
  echo "expected one client launch before upgrade, got $before_count" >&2
  cat "$FAKE_CODEX_RUN_LOG" >&2 || true
  exit 1
fi

./target/release/yolo upgrade-resume-all >"$tmp/upgrade.out"

for _ in $(seq 1 100); do
  count="$(grep -c "\"kind\": \"client\"" "$FAKE_CODEX_RUN_LOG" || true)"
  if [ "$count" -ge 2 ] && grep -q "\"version\": \"0.137.0\"" "$FAKE_CODEX_RUN_LOG"; then
    break
  fi
  sleep 0.2
done

if ! grep -q "\"kind\": \"client\".*\"version\": \"0.137.0\"" "$FAKE_CODEX_RUN_LOG"; then
  echo "phoenix did not relaunch client with upgraded fake codex" >&2
  echo "--- runs ---" >&2
  cat "$FAKE_CODEX_RUN_LOG" >&2 || true
  echo "--- upgrade ---" >&2
  cat "$tmp/upgrade.out" >&2 || true
  echo "--- client stderr ---" >&2
  cat "$tmp/client.err" >&2 || true
  exit 1
fi

if ! kill -0 "$client_pid" 2>/dev/null; then
  echo "yolo client wrapper exited instead of surviving phoenix relaunch" >&2
  cat "$tmp/client.err" >&2 || true
  exit 1
fi

./target/release/yolo status --json | jq -e ".resume_generation == 1" >/dev/null
./target/release/yolo status --json | jq -e ".clients[] | select(.status == \"running\")" >/dev/null
kill "$client_pid" 2>/dev/null || true
./target/release/yolo stop >/dev/null 2>&1 || true
echo "phoenix docker test passed"
'
