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

## Environment

- `YOLO_CODEX`: Codex executable to run. Defaults to yolo's managed Codex when
  present, otherwise `codex` on `PATH`.
- `YOLO_CODEX_UPGRADE_COMMAND`: override command used by `upgrade-resume`.
- `YOLO_CODEX_PREFIX`: managed Codex npm prefix. Defaults to
  `$XDG_DATA_HOME/yolo/codex-npm` or `~/.local/share/yolo/codex-npm`.
- `YOLO_REMOTE`: override app-server endpoint for the client.
- `YOLO_RUNTIME_DIR`: runtime directory for sockets. Defaults to
  `$XDG_RUNTIME_DIR/yolo` or `/tmp/yolo`.
