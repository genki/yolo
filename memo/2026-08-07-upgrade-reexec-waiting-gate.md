# Upgrade-resume client termination gate

Date: 2026-08-07

## Policy

The yolo client process tree must not be terminated or re-execed for ordinary
transport loss, app-server restart, settings changes, child errors, or an
unknown status. The only permitted client termination/re-exec flow is an
explicit `upgrade-resume` operation after the target client reports an
explicit `idle` or `waiting` state with no active flags.

`working`, `active`, `notLoaded`, missing, ambiguous, and RPC-timeout states
are blocking. An RPC timeout is retried until the configured upgrade idle
deadline; it is never converted into waiting.

## Implementation

- Removed transport-loss and app-server-generation recovery re-exec paths.
- Kept `terminate_pid_tree(child_pid)` behind the serialized upgrade gate and
  a final server-side waiting-state claim.
- Changed upgrade snapshots to require explicit idle/waiting status and to
  treat missing or ambiguous thread ownership as working.
- Added `/upgrade-resume-preflight` for direct upgrade-resume polling.
- Moved Codex package installation in the all-client worker after the idle
  condition is satisfied, with status retries on transient app-server timeouts.
- Added regression tests for gate absence, active/unknown status, missing or
  ambiguous ownership, and explicit waiting snapshots.

## Validation and deployment

- `cargo fmt -- --check`
- `cargo test`: 90 passed
- `cargo build --release`
- Release binary SHA-256: `06355c83448b9a989d5fa97585c02e1733df5aea88171c4604376b3553b960ca`
- yolo server is running from that binary at PID `392257`.
- The shared app-server was preserved during server replacement; its PID was
  `394583` after the explicit upgrade-resume operation.
- Seven existing clients were migrated through `upgrade-resume-all` only after
  preflight reported all targets idle. No client was killed while working.
- cgroup `memory.events`: `oom=0`, `oom_kill=0`, `oom_group_kill=0`.
