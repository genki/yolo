# yolo

`yolo` launches Codex through a yolo-managed Codex app-server with YOLO
permissions and web search enabled.

The default client runs:

```sh
codex --remote unix://$XDG_RUNTIME_DIR/yolo/app-server/codex-app-server.sock \
  --search \
  --dangerously-bypass-approvals-and-sandbox \
  "$@"
```

`yolo server` starts `codex app-server` as a child process and exposes a small
local HTTP-over-UNIX-socket API. `yolo` / `yolo client` starts Codex as a child
process with stdio passed through to the terminal, then reports client process
state, model, service tier, and fast-mode state to the server while it runs.

The server owns app-server lifecycle, client/session registry, persistent
settings, resume candidate selection, telemetry, and upgrade coordination. The
client owns the terminal-bound Codex child, its process tree, heartbeat, and the
minimal transport adapter required to connect that child to the server-managed
app-server. The yolo client wrapper is intentionally persistent: server/API
failure, app-server failure, proxy disconnect, child exit, and wait errors only
restart or reconnect the Codex child. The wrapper itself may terminate only on
the user's Ctrl+C or an authorized upgrade-resume after the client is idle.
Normal client launch does not rewrite Codex rollout or state files.

Every managed client launch, including `yolo resume`, enforces YOLO
permissions (`approval_policy=never`, `sandbox_mode=danger-full-access`, and
`--dangerously-bypass-approvals-and-sandbox`) and web search. Conflicting
approval/sandbox arguments are removed before launch. When model settings are
omitted, the wrapper uses the defaults saved by the websh yolo widget (including
its model, reasoning effort, and fast setting). The values are persisted by the
yolo server so new launches and resumes use the same widget configuration after
a restart.

YOLO clients also disable Codex's startup update prompt. Codex CLI updates are
managed explicitly by `yolo upgrade-resume` or `yolo upgrade-resume-all`, so a
new release cannot switch a managed client into an interactive prompt and
break a resume.

The server persists active client resume metadata in
`$XDG_STATE_HOME/yolo/active-sessions.json` (or `~/.local/state/yolo` when
`XDG_STATE_HOME` is unset). The file is written atomically with mode `0600` and
contains the thread ID, working directory, resume arguments, and effective
model, service-tier, reasoning-effort, and fast-mode settings needed to
recreate a client after a reboot. Schema v2 also records whether the settings
are complete, where they were observed (`app_server`, `configure`, or a
fallback source), and when they were observed. Startup reconciliation merges
records by client/thread identity and does not replace complete settings with
an incomplete process scan; the initial app-server snapshot is persisted after
reconciliation. Saved records are
available through `GET /saved-sessions`, the `saved_sessions` field in
`/status`, or `yolo saved-sessions`. A record is removed when its client exits;
the server does not start a headless Codex process automatically, so a saved
record can be reviewed before running `yolo resume THREAD_ID` in the intended
tmux pane.

## Install

```sh
cargo install --path .
```

## Usage

```sh
yolo --cd /home/vagrant/websh
yolo resume --last
yolo codex resume 019e9c04-eaa2-7c20-bd0e-13c297d4dc45
yolo upgrade-resume --last
yolo upgrade-resume-all
yolo external-codex-upgrade-resume --codex-version 0.137.0 --system
yolo server --daemon
yolo server --daemon --federation-listen 127.0.0.1:47040
yolo status
yolo stop
```

`yolo upgrade-resume [RESUME_ARGS...]` waits until all managed clients have an
explicit `idle`/`waiting` app-server status, installs the latest Codex CLI into
a yolo-managed user-writable npm prefix, asks the live yolo wrappers to
re-exec through the authorized idle gate, and only then restarts the
yolo-managed app-server so it uses the upgraded Codex binary. Missing,
unknown, active, and `notLoaded` statuses remain blocking. With no arguments it
resumes `--last`.

`yolo codex [CODEX_ARGS...]` is an emergency escape hatch for yolo
server/app-server trouble. It does not contact the yolo API and does not use
the yolo-managed app-server. Instead it execs the native `codex` CLI directly,
passes through all following arguments, and only adds YOLO mode flags plus the
launch cwd. It does not rewrite rollout/state files. It uses
`YOLO_NATIVE_CODEX` when set, then `codex` on `PATH`, and finally the yolo-managed
Codex binary. Set
`YOLO_NATIVE_CODEX` when a different native binary is required.

`yolo upgrade-resume-all` queues a job on the local yolo server. The server
waits for every target client to report an explicit `idle`/`waiting`
app-server status, updates the yolo-managed Codex CLI, re-execs target yolo
wrappers one at a time while the current app-server is still reachable, and
only then restarts the app-server. Missing, unknown, active, and `notLoaded`
statuses remain blocking. Live clients do not manipulate tmux panes during
this flow; the client also rechecks its own thread status and does not
terminate while a turn is working. Only after both idle checks and the
serialized permit succeed is the yolo wrapper re-execed for resume. Federation
callers receive queue acceptance immediately; the destination yolo server owns
the install, restart, and resume lifecycle. When run from inside Codex it uses
Phoenix mode: the caller thread is excluded from the idle wait and remains
under the persistent wrapper while the other targets migrate.

After installing a new yolo binary manually, use
`POST /upgrade-resume-reexec` before restarting `yolo.service`. This endpoint
waits for explicit idle/waiting status and migrates live wrappers without
restarting the app-server. It is the safe bridge for legacy wrappers whose
`/proc/<pid>/exe` still shows `(deleted)`.

Phoenix mode only applies to Codex processes launched by the yolo wrapper. A
legacy pane launched with `codex` directly does not heartbeat to the yolo server,
so it cannot receive the resume generation signal. Use
`external-codex-upgrade-resume` to migrate such panes without replacing the
existing pane.

Managed client proxies report an unexpected app-server WebSocket close,
including `turn/steer transport error` and `Connection reset without closing
handshake`, then close only that child connection. The listener remains bound;
after the API and app-server recover, the wrapper starts a new Codex child on
the same proxy and session metadata. Transport loss, ordinary app-server
restarts, child exits, and unknown client state are never treated as yolo
wrapper termination or upgrade authorization.

Robot/widget settings are first applied to a loaded thread through the
app-server `thread/settings/update` request. After that RPC succeeds, the
terminal-bound Codex child is restarted in place with the new model, service
tier, and reasoning effort. This is also allowed while the client is working so
the CLI mode changes immediately; the yolo wrapper remains alive and settings
are never applied before the server has acknowledged the update.

Managed `resume --last` candidate selection is performed by the server and
returns only the selected thread ID to the client. Resume policy (cwd,
workspace roots, approval, sandbox, and widget settings) is applied through the
app-server. Rollout/state repair is retained only for explicit
`refresh-resume`/`refresh-permissions` operations; it is not part of normal
client launch. Those explicit operations process rollout JSONL incrementally.

`yolo external-codex-upgrade-resume` is for legacy tmux panes that were launched
with `codex` directly instead of through yolo. It updates the user npm prefix
used by `~/.npm-global/bin/codex`, optionally updates `/usr/local/bin/codex`
with `--system`, detects non-yolo Codex panes, skips panes that appear busy, and
opens a new tmux window in the same session with `yolo resume <thread-id>`.
Use `--include-busy` only when duplicating active panes is intentional.

## API

```sh
curl --unix-socket "$XDG_RUNTIME_DIR/yolo/api.sock" http://yolo/clients
```

The `/clients` response includes:

- yolo client PID and Codex child PID
- cwd and Codex arguments
- model
- service tier
- fast flag
- lifecycle status and timestamps
- `telemetry_summary` with the known thread, subagent, active tool, running hook,
  captured turn counts, and captured commentary/reasoning counts

The app-server event aggregator is also available through:

```sh
curl --unix-socket "$XDG_RUNTIME_DIR/yolo/api.sock" http://yolo/telemetry
curl --unix-socket "$XDG_RUNTIME_DIR/yolo/api.sock" http://yolo/agents
curl --unix-socket "$XDG_RUNTIME_DIR/yolo/api.sock" http://yolo/subagents
curl --unix-socket "$XDG_RUNTIME_DIR/yolo/api.sock" 'http://yolo/turns?limit=20'
curl --unix-socket "$XDG_RUNTIME_DIR/yolo/api.sock" \
  'http://yolo/turns/history?thread_id=THREAD_ID&limit=20'
curl -X POST --unix-socket "$XDG_RUNTIME_DIR/yolo/api.sock" \
  http://yolo/upgrade-resume-reexec
```

`/agents` returns known threads with `parent_thread_id`, direct
`subagent_count`, `active_subagent_count`, and recursive descendant counts.
`/subagents` filters that list to child agents. `/telemetry` additionally
returns bounded recent `tool_calls` and `hook_runs`. Tool calls expose their
`pre`/`post` lifecycle phase, type, status, timing, success, and spawned child
thread IDs; command arguments and tool output are intentionally omitted.
Hook runs expose lifecycle events such as `preToolUse`, `postToolUse`, and
`subagentStart`/`subagentStop` when those hooks are configured and trusted in
Codex.

Turn capture records the prompt submitted at turn start, user-visible assistant
commentary, Codex reasoning summaries/raw reasoning deltas when emitted, and
the latest completed assistant message as `result` (legacy `report` is still
accepted when reading old archives). The live collector observes both the
app-server item lifecycle and the yolo client proxy's `turn/start` request.
The fields are exposed by `/turns` and `/turns/history` as `commentary`,
`reasoning_summary`, and `reasoning_raw`. Text is bounded to 16 KiB per field.
Codex does not expose a guaranteed complete internal chain-of-thought, so
there is no full chain-of-thought field.
`/turns` returns the bounded local archive; `/turns/history` reads a requested
thread from app-server and imports the selected recent turns. The archive is
stored as mode-0600 `turns.jsonl` in the yolo runtime directory and is capped
at 512 turns with 16 KiB per captured text field. Set `YOLO_TURN_CAPTURE=off` to stop
new capture while retaining the existing local archive for reference.

## Master/slave federation

`yolo server` can expose a master API for other yolo servers. Bind the API to a
local TCP port and publish that port through agent-gate with a fine grained
token. yolo does not implement its own federation authentication; HTTPS and
authorization are the responsibility of agent-gate.

Master:

```sh
yolo server --daemon --federation-listen 127.0.0.1:47040
```

Slave:

```sh
YOLO_MASTER_URL=https://agent-gate.example/<token>/@localhost:47040 \
YOLO_SLAVE_ID=mars \
YOLO_MASTER_BEARER_TOKEN=<agent-gate-fine-grained-token> \
yolo server --daemon
```

Slave servers first establish a WebSocket push connection. For an HTTPS
agent-gate URL, the TLS handshake and the `/token/@host:port` URL prefix are
preserved so commands arrive without a polling interval. If push is
unavailable, a bounded 100 ms HTTP polling fallback keeps older/proxied
deployments usable. The connector reconnects automatically after host or yolo
server restarts as long as the same environment is provided when the server is
started.

Master API:

```sh
curl -H "Authorization: Bearer $AGENT_GATE_YOLO_TOKEN" \
  http://127.0.0.1:47040/federation/slaves

curl -X POST -H "Authorization: Bearer $AGENT_GATE_YOLO_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"action":"codex-upgrade-resume","codex_version":"0.136.0"}' \
  http://127.0.0.1:47040/federation/slaves/mars/commands

curl -X POST -H "Authorization: Bearer $AGENT_GATE_YOLO_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"action":"yolo-upgrade","yolo_version":"0.5.0"}' \
  http://127.0.0.1:47040/federation/slaves/mars/commands
```

The local master also exposes a WebSocket status stream for trusted local
consumers such as websh:

```text
ws://127.0.0.1:47040/federation/events
```

The stream emits a `status` event after local client settings or permissions
change and whenever a slave pushes a fresh status. It sends an initial event
when connected, so consumers can refresh once per event instead of polling the
federation API.

`codex-upgrade-resume` queues the requested `@openai/codex` version on the
destination yolo server. That server performs the idle wait, install, app-server
restart, and one-at-a-time client resume. The federation caller does not wait
for completion. This updates the Codex CLI used inside yolo; it is distinct
from upgrading the yolo binary itself.

`yolo-upgrade` waits until Codex clients become idle, runs
`cargo install --git https://github.com/genki/yolo` by default, then advances the
resume generation so live yolo clients re-exec in place through the newly
installed binary. It upgrades the yolo binary and does not imply a Codex CLI
package update. The yolo server process itself is left running and reports that
a server restart is still deferred; this avoids resetting active Codex clients.
Override it with `YOLO_SELF_UPGRADE_COMMAND` when the slave needs a local
package or different installer.

## Tests

```sh
./scripts/phoenix-docker-test.sh
```

The Phoenix test runs inside Docker with `tests/fake_codex.py`, a fake Codex
app-server/client pair. It does not touch host tmux sessions or real Codex
processes.

## Environment

- `YOLO_CODEX`: Codex executable to run. Defaults to yolo's managed Codex when
  present, otherwise `codex` on `PATH`.
- `YOLO_NATIVE_CODEX`: native Codex executable used by `yolo codex`.
- `YOLO_CODEX_UPGRADE_COMMAND`: override command used by `upgrade-resume`.
- `YOLO_CODEX_PREFIX`: managed Codex npm prefix. Defaults to
  `$XDG_DATA_HOME/yolo/codex-npm` or `~/.local/share/yolo/codex-npm`.
- `YOLO_REMOTE`: override app-server endpoint for the client.
- `YOLO_RUNTIME_DIR`: runtime directory for sockets. Defaults to
  `$XDG_RUNTIME_DIR/yolo` or `/tmp/yolo`.
- `YOLO_STATE_DIR`: persistent state directory for active session metadata.
  Defaults to `$XDG_STATE_HOME/yolo` or `~/.local/state/yolo`.
- `YOLO_FEDERATION_LISTEN`: default master listen address.
- `YOLO_MASTER_URL`, `YOLO_SLAVE_ID`: slave connector settings.
- `YOLO_MASTER_BEARER_TOKEN`: optional Bearer token sent to the master URL.
  Use the agent-gate fine grained token when the master is exposed through
  agent-gate.
- `YOLO_SELF_UPGRADE_COMMAND`: override command used by remote `yolo-upgrade`.
