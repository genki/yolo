# app-server subagent and lifecycle telemetry

Date: 2026-07-14

## Goal

Expose a bounded, server-side view of the app-server thread tree and lifecycle
events so a parent agent can be correlated with its direct and nested
subagents, tool calls, and configured hook runs.

## Implementation

- `thread/list` is sampled every five seconds over a private app-server RPC
  connection. The inventory includes `source.subAgent.thread_spawn.parent_thread_id`,
  which is required for older records whose top-level `parentThreadId` is null.
- App-server notifications are aggregated for `thread/*`, `turn/*`,
  `item/started`, `item/completed`, `hook/started`, and `hook/completed`.
- `subAgentActivity` and `collabAgentToolCall` update parent-child edges and
  child status immediately; inventory refresh repairs or enriches the state.
- `/agents` returns direct and recursive child counts, including active counts.
  `/subagents` filters the same view to child agents.
- `/telemetry` returns the agent snapshot plus bounded recent tool and hook
  records. Tool records expose `pre`/`post`, status, timing, success, and child
  thread IDs. Raw commands, arguments, and output are intentionally excluded.
- Hook records expose event name, lifecycle phase, handler type, scope, status,
  and timing. Hook data is present when hooks are configured and trusted; the
  app-server item lifecycle remains available without adding a hook script.

## Validation

- `cargo test --locked`: 38 passed.
- `cargo build --release --locked`: passed.
- Installed and restarted the system service with yolo `0.5.17`.
- Live `/status`: 22 threads, 6 subagents, 1 active agent.
- Live `/agents` correctly reports the root thread's 6 direct and recursive
  descendants; `/telemetry` contains completed command tool records.
- The five-second inventory monitor remained healthy with no telemetry errors
  in the service journal after deployment.
- The initial loaded-thread settings snapshot runs in the background. This
  keeps the API socket available while a busy app-server is being inspected.
  After this change, a live service restart exposed the API in about 1.4
  seconds instead of waiting for every loaded thread to be resumed serially.

## Operational limits

Telemetry is intentionally bounded to 2048 threads, 512 tool calls, and 512
hook runs. This prevents long-lived app-server state from causing unbounded
memory growth while preserving recent lifecycle evidence.
