# Resume permissions reinforcement

Date: 2026-06-27

## Problem

Some clients resumed through `yolo resume` showed non-YOLO permissions even
though yolo always launches Codex with
`--dangerously-bypass-approvals-and-sandbox`.

The launch flags were correct. The weak point was the app-server live thread
settings update. `yolo` attempted `thread/settings/update` before the resumed
Codex thread was necessarily loaded in the app-server. If that early call did
not take effect, the client process stayed alive but the UI-visible thread
settings could remain non-YOLO.

During recovery, another safety issue was observed: a duplicate yolo server
startup could unlink `/run/user/1000/yolo/api.sock` before failing on the
federation port. That left the running server alive but unreachable through the
API socket.

## Fix

- Resume clients now start a bounded post-launch reinforcer that retries the
  app-server live settings update for up to 120 seconds.
- Added `yolo refresh-permissions` to reapply YOLO live settings to already
  loaded resume threads without restarting yolo clients or Codex children.
- Server startup no longer unlinks an active API socket owned by a reachable
  yolo server.
- `ensure_server()` now refuses to start a second server when a server PID is
  alive but the API socket is unreachable.
- `spawn_server_daemon()` passes `--` to `setsid` so yolo's `--foreground`
  argument is not consumed by `setsid`.

## Recovery Applied

The local yolo binary was reinstalled from this tree. The yolo API server was
restarted while preserving the existing Codex app-server PID, then:

```sh
yolo refresh-permissions --all
```

updated the four running resumed clients:

- `/home/vagrant/websh`
- `/home/vagrant/head`
- `/home/vagrant/jotter`
- `/home/vagrant/moon`

The non-resume head client without a `thread_id` was skipped.
