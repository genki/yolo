# yolo daemon startup review

Date: 2026-06-27

## Review Findings

The previous recovery hardening fixed the main deleted-binary and app-server
churn issues, but a few startup edge cases remained:

- `ensure_server()` treated any existing `/proc/<pid>` from `server.pid` as an
  active yolo server. A zombie process or PID reuse by an unrelated process
  could make clients refuse to start.
- Direct `yolo server --foreground` startup could still remove a stale-looking
  API socket without first checking whether `server.pid` pointed to a live yolo
  server.
- Direct `yolo server --daemon` could spawn a short-lived child even when a
  yolo server was already running. It was harmless after the socket protection
  change, but the `started yolo server pid ...` message was misleading.
- Daemon startup still preferred `current_exe()` over the `yolo` on `PATH`.
  That is less ideal for upgrade/recovery paths where the installed binary is
  the intended source of truth.

## Changes

- Added `running_yolo_server_pid()` to validate both liveness and cmdline:
  the PID must be alive, non-zombie, named `yolo`, and have a `server` argument.
- `run_server()` refuses to replace `api.sock` when `server.pid` identifies a
  running yolo server.
- `spawn_server_daemon()` now checks for an already reachable server, or a live
  yolo server with unreachable API, before spawning a child.
- Daemon executable selection now prefers:
  1. `YOLO_REEXEC_BIN`
  2. `yolo` found on `PATH`
  3. non-deleted `current_exe()`

## Validation

- `cargo fmt --check`
- `cargo test`
- `git diff --check`
- `cargo install --path . --force`
- Restarted only the yolo API server; existing Codex app-server PID was
  preserved.
- `yolo refresh-permissions --all`
- Verified duplicate `yolo server --daemon` exits with code 1 and does not
  change the running server/app-server PID.
