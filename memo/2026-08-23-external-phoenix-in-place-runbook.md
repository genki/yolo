# External Codex in-place Phoenix runbook

Date: 2026-08-23 (JST)

## Purpose

Migrate a Codex session launched directly in a tmux pane to the persistent yolo
wrapper without changing its thread ID or creating a duplicate live session.

## Command

```sh
yolo external-codex-upgrade-resume \
  --codex-version <target-version> \
  --defer-busy
```

`--defer-busy` does not terminate a working turn. It observes the pane and
performs the handoff as soon as the session reaches waiting state; there is no
additional 30-second stable-wait requirement.

## Handoff sequence

1. Detect direct Codex panes by process identity, not by a `/yolo/` substring
   in an executable path.
2. Resolve and retain the exact thread ID.
3. If the pane is working, leave it untouched and continue observing it.
4. At waiting state, send EOF (`C-d`) to request normal Codex termination.
5. Confirm that the exact old Codex process tree has exited. Use `C-c` only as
   a late fallback after the normal-exit timeout.
6. Execute `yolo resume <thread-id>` in the same pane.
7. Verify the new yolo wrapper, Codex child, proxy socket, and server client
   record all refer to the retained thread ID.

## Abort and safety conditions

- Never start the replacement while an old process for the same thread is
  still alive.
- If the old process tree does not exit, abort with `codex_exit_timeout`; do not
  create a duplicate session.
- A working pane is not upgrade authorization and must not be interrupted.
- `--include-busy` is an explicit compatibility escape hatch that may create a
  new-window duplicate. It is not the in-place Phoenix procedure.

## Verified production result

The user confirmed the Phoenix transition succeeded. Host-side verification
also established all of the following:

- Thread ID remained `019ff70c-640d-7b60-a057-2fa51bae02b8`.
- The original pane process became yolo wrapper PID `1941611` running
  `yolo resume` for that same thread.
- Codex child PID `4093515` was attached through client
  `1941611-1787413642088`.
- The child and server record used proxy socket
  `/run/user/1000/yolo/client-proxies/1941611-1787413642088.sock`.
- The yolo API reported the client as `running`, with no end time or exit code.
- App-server health reported progress ready, zero consecutive failures, and no
  current error.

This verifies an in-place handoff rather than a second tmux window or a new
thread.

## Incident note

The earlier compatibility attempt started a replacement before closing the
direct client. Client `4048271-1787412832351` consequently entered
`crash-loop` with exit code 1. The ordered EOF, exact-PID exit check, and
same-pane execution above remove that race.

The patched binary was validated with 136 tests and a release build. The
MachinaAI aarch64 job `7666da4efc05402a8f70c47702d52b6c` and x86_64 job
`efde1f1528a94b0198c486a1d09fb0f7` were attempted first but failed because of
a containerd blob I/O error, so the host build was used as the documented
fallback.
