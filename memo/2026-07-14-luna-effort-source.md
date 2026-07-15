# luna reasoning effort source correction

Date: 2026-07-14

## Finding

The local yolo client process arguments contained
`model_reasoning_effort=xhigh`, while the persisted app-server thread settings
returned `reasoningEffort=max`. The yolo server used the launch argument first
when applying a thread snapshot, so `/clients` and the websh widget reported
`xhigh` even though the resumed thread was `max`.

## Fix

When a client is bound to a resumed thread, app-server thread settings now take
precedence for model, service tier, and reasoning effort. The launch command is
only a fallback when the thread response does not contain that setting. This
logic is applied both to the initial snapshot and to incremental thread
updates.

## Validation

- Added a regression test for stale `xhigh` launch settings versus thread
  setting `max`.
- `cargo test --locked`: 39 passed.
- Installed yolo `0.5.18` and restarted the system service without killing the
  existing app-server or clients.
- After the asynchronous initial snapshot completed, all 7 local clients
  reported `reasoning_effort=max`.
- The websh widget snapshot displayed `max` for all 7 local rows.

The initial snapshot is asynchronous to keep the API available during restart;
the values may be temporarily stale until the thread responses arrive.
