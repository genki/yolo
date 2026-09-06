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
the user's Ctrl+C (including Codex TUI's raw-mode exit) or an authorized
upgrade-resume after the client is idle. A child exit code of 1 is treated as a
user exit only when the proxy did not observe an upstream app-server failure;
transport failures remain recoverable and restart only the child.
Normal client launch does not rewrite Codex rollout or state files.

The server also guards background terminals against a specific shell-construction
deadlock: `while pgrep -f ...; do sleep ...; done` can match the waiting shell's
own command line forever. The guard terminates only that self-matching shell
subtree after confirming that no independent matching process exists. It is not
a generic runtime or CPU timeout, so legitimate long-running computation is
left alone. Set `YOLO_BACKGROUND_TERMINAL_GUARD_INTERVAL_MS` to tune the scan
interval; values below one second are clamped.

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
recreate a client after a reboot. Schema v3 keeps the wrapper-owned `yolo_id`
separate from the process ID and Codex thread ID, and records the thread
binding state (`pending`, `bound`, `loaded`, or `unloaded`). During an upgrade
from a legacy server, a valid `yolo_id` received in a wrapper heartbeat replaces
the scanner fallback and is persisted before the next restart. The schema also
records whether the settings
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

Linux user-unit deployments can isolate the yolo control plane from the
Codex worker and its tools. The websh repository provides a staging installer
and an explicit activation command:

```sh
./scripts/install-yolo-user-systemd.sh
./scripts/activate-yolo-user-isolation.sh
```

The installer does not restart the live service. After the idle gate is
confirmed, activation moves the app-server to the stable
`yolo-app-server.service` worker, which has its own cgroup and restart policy.
Thread-tagged tools are then moved into per-session cgroups with an 8 GiB hard
limit and 1 GiB swap limit. An OOM in a tool or worker can no longer stop the
separate yolo control-plane unit.

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
The preflight snapshot used by this gate is also published as a fresh,
upgrade-verified per-thread status. A current wrapper may use that narrowly
scoped proof to initialize a missing local TUI status, or repair a missed final
idle notification, before claiming the permit. The proof must have been
observed after the wrapper's current proxy connection began, so a restarted
client cannot consume a pre-restart idle result. Generic inventory, stale idle,
an active status, or active flags still cannot authorize a re-exec.

Codex 0.149.0 and later can store long legacy JSONL rollouts in paginated
thread history. If a saved session has a very large rollout and `thread/read`
times out during resume, stop only the confirmed failing client, preserve the
original JSONL, and migrate that thread with:

```sh
codex migrate-rollouts --thread THREAD_ID --apply --json --verbose
```

The migration keeps the thread ID and the original rollout as a source record;
the app-server then reads the bounded paginated projection instead of loading
the entire rollout for every TUI lookup. Verify `history_mode=paginated` in
the Codex state DB and resume the same thread in the original tmux window.
Do not use a broad process kill for a merely alive client: the migration is
authorized only after the client is explicitly waiting or is confirmed to be
in a resume crash loop.

Each blue/green generation owns a distinct app-server socket and `CODEX_HOME`.
This keeps the Codex model cache and state DB version-aligned with that
generation's CLI, and prevents an old app-server from overwriting a new model
catalog. The destination is seeded from consistent SQLite backups rather than
sharing or byte-copying a live WAL. Before an idle wrapper leaves the source,
it copies only its exact canonical rollout into the destination home, verifies
that the source did not change during the copy, and runs the destination
Codex CLI's bounded `migrate-rollouts` command for that exact thread. The
source server grants only one copy/projection/resume lease at a time. The
wrapper returns that lease on failure, rechecks the live idle status, and only
then resumes the same thread ID against the destination app-server.

A legacy wrapper cannot claim this handoff until it has re-execed into a
version that implements the current state-copy protocol. On that re-exec, a
new wrapper checks for its pending handoff before starting another source
Codex child, so a multi-gigabyte source session is not cold-loaded solely to
move generations. The source withholds the handoff event from an incompatible
wrapper until its bootstrap heartbeat advertises the new protocol; otherwise
the old wrapper would prioritize an unclaimable handoff over its own upgrade.
Compatible wrappers are not restarted for the cutover.

As of 0.5.43, pending handoffs are atomically persisted in
`blue-green-handoffs.json` in the source state directory. A source restart
restores pending requests, while completed requests stay completed. Repeating
the same request is safe; changing an outstanding target or expected thread is
rejected. The rollout script schedules all peers before waiting for legacy
wrapper upgrades, and retains a checkpoint for retrying interrupted cutovers.

Generation homes retain an ordered `yolo-rollout-sources.json` lineage. An
exact resume, including a selection made in Codex's picker, materializes a
missing dormant rollout from the nearest retained generation. Recovery is
locked per thread, checks every JSONL record, and retries an interrupted
projection before launching Codex. Keep these source homes until their
historical sessions are no longer needed. Migration stdout/stderr are drained
concurrently with bounded capture so progress output cannot deadlock a copy.

For the isolated restart/claim/completion integration test (no OpenAI calls):

```sh
python3 tests/bg_restart.py target/x86_64-unknown-linux-musl/release/yolo
```

The destination runtime contains a `codex-executable` file whose single line
is an absolute executable path. A newly re-execed wrapper selects this
destination-generation pin before an inherited `YOLO_CODEX` value. An atomic
`~/.local/state/yolo/active-generation.json` pointer directs new clients to
the promoted generation; already-running wrappers remain on their source
generation until their own thread is idle. Invalid or unhealthy pointers are
ignored rather than partially applying a generation tuple.

Each server process reconciles wrappers only when the Codex child uses a
managed proxy socket directly below that server's runtime directory. This
runtime-scoped scan also runs for standby generations, so a server-only
restart reconstructs its live clients from their unchanged wrappers without
importing clients attached to blue or another green slot.

Phoenix mode only applies to Codex processes launched by the yolo wrapper. A
legacy pane launched with `codex` directly does not heartbeat to the yolo server,
so it cannot receive the resume generation signal. Use
`external-codex-upgrade-resume` to migrate such panes. Once the pane is explicitly
waiting, yolo sends EOF to the direct Codex process, confirms that its process
tree has exited, and starts `yolo resume <thread-id>` in the same tmux pane. A
busy pane is never terminated; `--defer-busy` waits until its next waiting state.

Managed client proxies report an unexpected app-server WebSocket close,
including `turn/steer transport error` and `Connection reset without closing
handshake`, then close only that child connection. The listener remains bound;
after the API and app-server recover, the wrapper starts a new Codex child on
the same proxy and session metadata. The proxy records whether the upstream or
the terminal-bound child closed first, so an upstream transport failure cannot
be mistaken for a raw-mode user exit. Transport loss, ordinary app-server
restarts, and unknown client state remain recovery paths rather than yolo
wrapper termination or upgrade authorization.

Robot/widget settings are first applied to a loaded thread through the
app-server `thread/settings/update` request. After that RPC succeeds, the
terminal-bound Codex child remains attached and is not restarted: the loaded
thread already owns the new model, service tier, and reasoning effort. The
server keeps those acknowledged settings authoritative over stale launch
metadata. If an unrelated transport recovery or an explicitly authorized
upgrade later launches a child, that launch reads the latest durable settings.

Managed `resume --last` candidate selection is performed by the server and
returns only the selected thread ID to the client. Resume policy (cwd,
workspace roots, approval, sandbox, and widget settings) is applied through the
app-server. Rollout/state repair is retained only for explicit
`refresh-resume`/`refresh-permissions` operations; it is not part of normal
client launch. Those explicit operations process rollout JSONL incrementally.

`yolo external-codex-upgrade-resume` is for legacy tmux panes that were launched
with `codex` directly instead of through yolo. It updates the user npm prefix
used by `~/.npm-global/bin/codex`, optionally updates `/usr/local/bin/codex`
with `--system`, detects non-yolo Codex panes, and hands explicitly waiting
panes over in place to `yolo resume <thread-id>`. `--defer-busy` starts a
background watcher that performs this handoff at the first waiting state.
`--include-busy` remains a compatibility escape hatch for intentionally
duplicating active panes and does not authorize in-place termination.

The verified operating procedure, success criteria, and abort conditions are
recorded in
[`memo/2026-08-23-external-phoenix-in-place-runbook.md`](memo/2026-08-23-external-phoenix-in-place-runbook.md).

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

# Read one command without transferring the full federation snapshot.
curl -H "Authorization: Bearer $AGENT_GATE_YOLO_TOKEN" \
  http://127.0.0.1:47040/federation/slaves/mars/commands/<command-id>
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
- `YOLO_CLIENT_ID`: internal re-exec handoff for a managed client; normally
  unset. The API exposes this logical identity as `yolo_id`, separately from
  the process `id` and Codex `thread_id`.
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
