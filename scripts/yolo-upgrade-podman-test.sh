#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="${YOLO_PODMAN_TEST_IMAGE:-docker.io/library/rust:1.95-bookworm}"
container_name="yolo-upgrade-podman-test-$$"
podman_cmd="${YOLO_PODMAN:-podman}"

test_script="$(mktemp)"
trap 'rm -f "$test_script"' EXIT

cat >"$test_script" <<'SCRIPT'
set -euo pipefail
apt-get update >/dev/null
apt-get install -y --no-install-recommends python3 jq curl >/dev/null
cargo build --release
tmp="$(mktemp -d)"
export YOLO_RUNTIME_DIR="$tmp/runtime"
export YOLO_CODEX="/work/tests/fake_codex.py"
export FAKE_CODEX_RUN_LOG="$tmp/runs.jsonl"
export FAKE_CODEX_VERSION_FILE="$tmp/version"
export FAKE_CODEX_THREAD_ID="019e0000-0000-7000-8000-000000000247"
export FAKE_CODEX_CWD="/work"
export YOLO_UPGRADE_IDLE_WAIT_TIMEOUT_SECS=10
export YOLO_SELF_UPGRADE_COMMAND="printf upgraded > '$tmp/yolo-upgraded'"
export YOLO_MASTER_URL="http://127.0.0.1:47040"
export YOLO_SLAVE_ID="self"
printf 0.140.0 > "$FAKE_CODEX_VERSION_FILE"

./target/release/yolo server --daemon --federation-listen 127.0.0.1:47040
./target/release/yolo resume "$FAKE_CODEX_THREAD_ID" >"$tmp/client.out" 2>"$tmp/client.err" &
client_pid=$!

for _ in $(seq 1 80); do
  if curl -fsS http://127.0.0.1:47040/federation/slaves 2>/dev/null |
    jq -e '.slaves[] | select(.id == "self")' >/dev/null; then
    break
  fi
  sleep 0.2
done
if ! curl -fsS http://127.0.0.1:47040/federation/slaves |
  jq -e '.slaves[] | select(.id == "self")' >/dev/null; then
  echo "self slave did not register" >&2
  ./target/release/yolo status --json >&2 || true
  exit 1
fi

for _ in $(seq 1 80); do
  if ./target/release/yolo status --json | jq -e '.clients | length > 0' >/dev/null; then
    break
  fi
  sleep 0.2
done
for _ in $(seq 1 80); do
  count="$(grep -c '"kind": "client"' "$FAKE_CODEX_RUN_LOG" 2>/dev/null || true)"
  if [ "$count" -ge 1 ]; then
    break
  fi
  sleep 0.2
done
before="$(grep -c '"kind": "client"' "$FAKE_CODEX_RUN_LOG" 2>/dev/null || true)"
if [ "$before" -ne 1 ]; then
  echo "expected one client before yolo-upgrade, got $before" >&2
  cat "$FAKE_CODEX_RUN_LOG" >&2 || true
  cat "$tmp/client.err" >&2 || true
  exit 1
fi

curl -fsS -X POST -H 'Content-Type: application/json' \
  --data '{"id":"cmd-yolo-upgrade-test","action":"yolo-upgrade"}' \
  http://127.0.0.1:47040/federation/slaves/self/commands >"$tmp/command.out"

for _ in $(seq 1 120); do
  count="$(grep -c '"kind": "client"' "$FAKE_CODEX_RUN_LOG" 2>/dev/null || true)"
  generation="$(./target/release/yolo status --json | jq -r '.resume_generation')"
  if [ "$count" -ge 2 ] && [ "$generation" = "1" ] && [ -f "$tmp/yolo-upgraded" ]; then
    break
  fi
  sleep 0.2
done

after="$(grep -c '"kind": "client"' "$FAKE_CODEX_RUN_LOG" 2>/dev/null || true)"
generation="$(./target/release/yolo status --json | jq -r '.resume_generation')"
if [ "$after" -lt 2 ] || [ "$generation" != "1" ] || [ ! -f "$tmp/yolo-upgraded" ]; then
  echo "yolo-upgrade did not trigger safe client reexec" >&2
  echo "after=$after generation=$generation marker=$(test -f "$tmp/yolo-upgraded" && echo yes || echo no)" >&2
  echo "--- command ---" >&2
  cat "$tmp/command.out" >&2 || true
  echo "--- slaves ---" >&2
  curl -fsS http://127.0.0.1:47040/federation/slaves >&2 || true
  echo "--- status ---" >&2
  ./target/release/yolo status --json >&2 || true
  echo "--- runs ---" >&2
  cat "$FAKE_CODEX_RUN_LOG" >&2 || true
  echo "--- client err ---" >&2
  cat "$tmp/client.err" >&2 || true
  exit 1
fi
if ! kill -0 "$client_pid" 2>/dev/null; then
  echo "client wrapper exited after yolo-upgrade reexec" >&2
  cat "$tmp/client.err" >&2 || true
  exit 1
fi

kill "$client_pid" 2>/dev/null || true
./target/release/yolo stop >/dev/null 2>&1 || true
echo "yolo-upgrade podman test passed"
SCRIPT

"$podman_cmd" run --rm --name "$container_name" \
  -v "$repo_root:/work:rw" \
  -v "$test_script:/tmp/yolo-upgrade-test.sh:ro" \
  -w /work \
  "$image" \
  bash /tmp/yolo-upgrade-test.sh
