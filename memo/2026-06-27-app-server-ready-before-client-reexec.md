# app-server readiness before client reexec

Date: 2026-06-27

## Finding

The previous upgrade/resume fixes did not fully cover a frequent client startup
failure mode after app-server restart.

`restart_tracked_app_server` spawned the new Codex app-server and returned as
soon as the child process existed. `run_codex_upgrade_resume_all_local` then
advanced `resume_generation`, allowing live yolo clients to re-exec and launch
native Codex immediately. If the app-server socket existed late or accepted the
WebSocket handshake late, the resumed client could start Codex against a remote
socket that was not ready yet.

`ensure_server` had the same weakness for already-running yolo servers: it
accepted `/status` as sufficient even when the app-server socket was still
coming up.

## Change

Readiness now requires a successful WebSocket connection to the app-server:

- app-server spawn and restart paths wait for `AppServerRpcClient::connect`;
- `ensure_server` waits for app-server readiness even when the yolo API server
  is already running;
- `wait_for_server_ready` also checks app-server RPC readiness, not just the API
  socket and `/status`.

This makes `resume_generation` advancement happen only after the replacement
app-server is actually reachable by clients.
