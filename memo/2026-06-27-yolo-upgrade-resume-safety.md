# yolo-upgrade resume safety

Date: 2026-06-27

## Finding

`upgrade-resume-all` was already safe for Codex CLI package updates: it waits
for running Codex threads to become idle, upgrades the managed Codex CLI,
restarts the app-server, advances the resume generation, and live yolo clients
re-exec in place.

The separate `yolo-upgrade` slave command was incomplete for yolo binary
updates. It installed the new yolo binary and returned `restart_required`, but
it did not advance `resume_generation`. Existing yolo clients therefore kept
running the old executable image, often visible as `(deleted)` after
`cargo install --force`.

## Change

`yolo-upgrade` now:

1. waits for Codex clients to become idle using the same idle check as
   `upgrade-resume-all`;
2. runs the yolo self-upgrade command;
3. advances `resume_generation` so live yolo clients re-exec in place through
   the newly installed yolo binary.

The yolo server process itself is not force-restarted in this flow. The result
keeps `server_restart_required=true` and reports the restart policy as
`clients_reexec_in_place_after_idle_server_restart_deferred`.

This avoids resetting active Codex clients while still replacing old client
wrappers safely.
