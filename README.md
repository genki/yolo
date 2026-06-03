# yolo

`yolo` launches Codex through a yolo-managed Codex app-server with YOLO
permissions and web search enabled.

The default client runs:

```sh
codex --remote unix://$XDG_RUNTIME_DIR/yolo/codex-app-server.sock \
  --search \
  --dangerously-bypass-approvals-and-sandbox \
  "$@"
```

`yolo server` starts `codex app-server` as a child process and exposes a small
local HTTP-over-UNIX-socket API. `yolo` / `yolo client` starts Codex as a child
process with stdio passed through to the terminal, then reports client process
state, model, service tier, and fast-mode state to the server while it runs.

## Install

```sh
cargo install --path .
```

## Usage

```sh
yolo --cd /home/vagrant/websh
yolo resume --last
yolo upgrade-resume --last
yolo server --daemon
yolo server --daemon --federation-listen 127.0.0.1:47040
yolo status
yolo stop
```

`yolo upgrade-resume [RESUME_ARGS...]` installs the latest Codex CLI into a
yolo-managed user-writable npm prefix, restarts the yolo-managed app-server so
it uses the upgraded Codex binary, then launches `codex resume` through yolo.
With no arguments it resumes `--last`.

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
YOLO_SLAVE_ID=kagura \
YOLO_MASTER_BEARER_TOKEN=<agent-gate-fine-grained-token> \
yolo server --daemon
```

Slave servers poll the master and reconnect automatically after host or yolo
server restarts as long as the same environment is provided when the server is
started.

Master API:

```sh
curl -H "Authorization: Bearer $AGENT_GATE_YOLO_TOKEN" \
  http://127.0.0.1:47040/federation/slaves

curl -X POST -H "Authorization: Bearer $AGENT_GATE_YOLO_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"action":"codex-upgrade-resume","codex_version":"0.136.0"}' \
  http://127.0.0.1:47040/federation/slaves/kagura/commands

curl -X POST -H "Authorization: Bearer $AGENT_GATE_YOLO_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"action":"yolo-upgrade","yolo_version":"0.5.0"}' \
  http://127.0.0.1:47040/federation/slaves/kagura/commands
```

`codex-upgrade-resume` waits until slave Codex clients become idle, installs the
requested `@openai/codex` version into yolo's managed user-writable npm prefix,
restarts the slave app-server, and asks running yolo clients to resume.

`yolo-upgrade` runs `cargo install --git https://github.com/genki/yolo` by
default, then restarts the yolo server. Override it with
`YOLO_SELF_UPGRADE_COMMAND` when the slave needs a local package or different
installer.

## Environment

- `YOLO_CODEX`: Codex executable to run. Defaults to yolo's managed Codex when
  present, otherwise `codex` on `PATH`.
- `YOLO_CODEX_UPGRADE_COMMAND`: override command used by `upgrade-resume`.
- `YOLO_CODEX_PREFIX`: managed Codex npm prefix. Defaults to
  `$XDG_DATA_HOME/yolo/codex-npm` or `~/.local/share/yolo/codex-npm`.
- `YOLO_REMOTE`: override app-server endpoint for the client.
- `YOLO_RUNTIME_DIR`: runtime directory for sockets. Defaults to
  `$XDG_RUNTIME_DIR/yolo` or `/tmp/yolo`.
- `YOLO_FEDERATION_LISTEN`: default master listen address.
- `YOLO_MASTER_URL`, `YOLO_SLAVE_ID`: slave connector settings.
- `YOLO_MASTER_BEARER_TOKEN`: optional Bearer token sent to the master URL.
  Use the agent-gate fine grained token when the master is exposed through
  agent-gate.
- `YOLO_SELF_UPGRADE_COMMAND`: override command used by remote `yolo-upgrade`.
