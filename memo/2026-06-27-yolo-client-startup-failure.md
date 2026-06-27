# yolo client startup failure follow-up

Date: 2026-06-27

## Incident

All yolo clients on the vagrant host dropped and new clients could not start
until yolo was reinstalled with `cargo install --path . --force`.

During manual resume, yolo printed:

```text
yolo: failed to update loaded Codex thread settings for ...:
app-server request 2 failed: {"code":-32600,"message":"thread not found: ..."}
```

The Codex resume itself succeeded, so this message was a false failure signal
for the overall resume path.

## Findings

1. `spawn_server_daemon()` used `env::current_exe()` unconditionally.

   After `cargo install --force`, already-running yolo clients and servers can
   keep executing the old inode, visible as `(deleted)`. If such a process
   needed to start the yolo daemon, it attempted to spawn that deleted path
   instead of the fresh `yolo` on `PATH`.

2. The status listener self-heal path was too aggressive.

   A WebSocket listener disconnect, such as `failed to fill whole buffer`, does
   not necessarily mean the app-server has disappeared. Treating it as a reason
   to run the full tracked app-server ensure path can cause unnecessary
   app-server churn and disconnect all clients.

3. `thread not found` during the initial permissions update is expected.

   On resume, yolo starts Codex and also updates app-server live thread
   settings. The early settings update can race before the resumed thread is
   loaded. The post-launch reinforcement retry handles that case, so the early
   `thread not found` should not be printed as a resume failure.

## Fix

- Daemon startup now skips a `(deleted)` current executable and falls back to
  the `yolo` found on `PATH`.
- Status listener self-heal is non-destructive:
  - reconnectable socket: do nothing;
  - existing app-server PID: adopt it;
  - no socket and no app-server PID: spawn a replacement.
- Initial permissions reinforcement suppresses `thread not found` because the
  post-launch retry is responsible for waiting until the thread is loaded.

## Recovery Applied

Reinstalled yolo from this tree and restarted only the yolo API server. The
existing Codex app-server PID was preserved. Then:

```sh
yolo refresh-permissions --all
```

updated the current resumed websh/head/jotter/moon clients.
