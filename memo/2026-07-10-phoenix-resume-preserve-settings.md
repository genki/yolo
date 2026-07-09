# Phoenix resume settings preservation

Date: 2026-07-10

## Problem

After `upgrade-resume-all`, some yolo clients did not resume against their
own loaded Codex thread. Plain `yolo` clients could be re-executed as
`resume --last`, which is ambiguous when multiple threads share a cwd. In
addition, a resumed client could inherit the current Codex defaults instead
of the client/thread model, service tier, and reasoning effort that were in
use before the restart.

This made non-fast sessions vulnerable to becoming fast, or to other model
selection drift, during Phoenix recovery.

## Fix

- Phoenix re-exec now looks up the server-known client entry and resumes the
  exact `thread_id` for that client.
- Existing explicit resume thread arguments are kept.
- `resume --last` is replaced with the known thread id when available.
- The previous client/thread launch settings are injected with Codex `-c`
  arguments for:
  - `model`
  - `service_tier`
  - `model_reasoning_effort`
- Explicit launch settings already present in the command line are not
  overwritten.
- yolo server startup now performs an initial app-server thread snapshot
  immediately after process scanning, so scanned clients regain thread-owned
  model/service tier/effort before restart instructions can rely on stale
  defaults.

## Verification

- `cargo fmt --check`
- `cargo test`
- `git diff --check`
- `cargo install --path . --force`
- yolo server wrapper restarted while preserving the existing app-server PID.
- `yolo refresh-permissions --all` updated all 6 live clients.

Observed live clients after repair:

- `jotter`: `019e9c3c-0913-71b2-86de-d0825dbeec49`
- `head#0`: `019f062c-f46b-7431-9ac9-e7b3fd8f7ab4`
- `head#1`: `019f40b9-b257-77d1-8ad5-62f7321bde95`
- `head#2`: `019e9c3a-1cf1-7d71-8cb2-fe7552a087a8`
- `moon`: `019e9c3b-00c1-7c71-9d99-ef26e7051369`
- `websh`: `019e9c04-eaa2-7c20-bd0e-13c297d4dc45`

