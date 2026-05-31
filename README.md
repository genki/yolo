# yolo

`yolo` launches Codex through the local app-server with YOLO permissions and
web search enabled.

It runs:

```sh
codex --remote unix:// --search --dangerously-bypass-approvals-and-sandbox "$@"
```

Before launching Codex, it best-effort starts the user systemd service
`codex-app-server.service`.

## Install

```sh
cargo install --path .
```

## Usage

```sh
yolo --cd /home/vagrant/websh
yolo resume --last
```

## Environment

- `YOLO_CODEX`: Codex executable to run. Defaults to `codex`.
- `YOLO_REMOTE`: app-server endpoint. Defaults to `unix://`.
- `YOLO_NO_SERVICE_START`: if set, skips starting the systemd user service.
