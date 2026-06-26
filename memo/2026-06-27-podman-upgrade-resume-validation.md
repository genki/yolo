# Podman upgrade/resume validation

Date: 2026-06-27

## Scope

Validated yolo upgrade/resume behavior in Podman containers using
`tests/fake_codex.py`, isolated `YOLO_RUNTIME_DIR`, and no host tmux sessions.

## Results

Passed:

- `YOLO_DOCKER=podman YOLO_PHOENIX_TEST_IMAGE=docker.io/library/rust:1.95-bookworm scripts/phoenix-docker-test.sh`
- `scripts/yolo-upgrade-podman-test.sh`

The first test covers Codex CLI `upgrade-resume-all`: app-server restart,
resume generation advance, and live client in-place relaunch.

The second test covers yolo binary `yolo-upgrade`: federation command delivery,
idle wait, self-upgrade command execution, resume generation advance, and live
client in-place relaunch.

## Bug Found

Podman validation exposed that `yolo server --daemon --federation-listen ...`
discarded server arguments while daemonizing. The daemon child was always
started as `yolo server --foreground`, so the federation listener was not
enabled.

## Fix

Daemon startup now preserves server arguments, replacing only `--daemon` with
`--foreground`. This allows daemonized federation listeners and slave connector
configuration to survive the daemon transition.
