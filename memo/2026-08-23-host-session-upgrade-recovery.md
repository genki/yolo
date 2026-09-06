# Host yolo/Codex session upgrade and recovery

Date: 2026-08-23 (JST)

## Scope

The host had several sessions that had not completed the yolo/Codex 0.149.0
upgrade. Federation targets had already completed successfully. The local
target was to migrate only waiting sessions, preserve working sessions, and
restore the failed head0 session in its original tmux window.

## Root cause

Head0 thread `019f4803-9827-7391-bc3c-e707ecb08caa` used the legacy JSONL
rollout
`~/.codex/sessions/2026/07/10/rollout-2026-07-10T02-53-42-019f4803-9827-7391-bc3c-e707ecb08caa.jsonl`.
The file was about 4.3 GiB and contained 696 turns. Codex 0.149.0's resume
path attempted a `thread/read` against the legacy record, which exceeded the
app-server request window. The old head0 client then repeatedly restarted
Codex after `thread/read failed during TUI session lookup`; this was a genuine
resume crash loop, not an alive-but-idle session.

The host also had old yolo wrapper/server inodes still resident after the
binary replacement. `/proc/<pid>/exe` showed the previous yolo hash
`9c150af0...` with `(deleted)`, while the installed release was
`aa7a7ee44ada775285eed836ac786099d21921169099beb84442a01013134ff8`.

Head1 exposed a second part of the same problem after its server restart:
thread `019f40b9-b257-77d1-8ad5-62f7321bde95` was another legacy rollout
(about 1.9 GiB). The host runs multiple yolo runtimes against the shared
`CODEX_HOME`; the main Codex app-server retained the thread's writer lock, so
the newly restarted head1 app-server returned `thread ... already has an
active writer`. The two app-servers must not concurrently own that thread.

## Remediation

1. Confirmed the head0 loop from tmux output and stopped only its exact
   wrapper/child (`528827` and its failed child). The original rollout was
   not deleted or overwritten.
2. Ran the Codex-supported migration:

   ```sh
   codex migrate-rollouts \
     --thread 019f4803-9827-7391-bc3c-e707ecb08caa \
     --apply --json --verbose --max-mib-per-second 100
   ```

   Result: `migrated`, `bytes_processed=18512210279`; the paginated projection
   contains 696 turns and 12,521 items, and the state DB reports
   `history_mode=paginated`. The original 4.3 GiB JSONL remains in place.
3. Recreated `head:0` and resumed the exact same thread as yolo PID `4192878`.
   The pane displayed Codex 0.149.0 and the previous session tail without the
   `thread/read` failure or restart loop.
4. Used the explicit idle gate for the other local clients. Seven main-side
   waiting clients, head0 client `2149143`, and head1 client `548168` were
   reexeced to the release hash above. The head4 client `2168499` was then
   rechecked after its turn became explicitly idle and reexeced through the
   same gate; its replacement client ID is `2168499-1787416565151`. The
   current client `1941611` remained attached to the active turn.
5. Restarted head0 and head1 yolo servers on 0.5.30. Both report
   `progress_ready=true` and zero consecutive app-server failures. Head1's
   app-server is also from the managed Codex 0.149.0 package.
6. Migrated the head1 legacy rollout with the same official command. It
   produced a paginated projection with 727 turns and 8,782 items. After
   detecting the shared writer lock, the head1 pane was restored through the
   existing main app-server (the unique owner of that thread) as yolo PID
   `55274`; its thread is the original ID and reports `idle`. The failed
   duplicate records were marked exited without removing the active session.

An active-history audit found three further, non-failing legacy threads:
`019fd3f8...` (598 MiB), `019e9c3b...` (480 MiB), and `019e9c3c...`
(178 MiB). Their clients are on the new yolo/Codex binaries and passed the
idle gate, but the official migration correctly returned `skipped_busy` because
the main app-server still holds their writer locks. The head4 thread
`019fd39d...` (386 MiB) is now running under the new yolo wrapper, but its
legacy projection was not touched. These projections require the main
app-server rolling handoff after the current active turn; forcing it now would
break the current session.

The MachinaAI build server was attempted for both check and release jobs, but
the remote containerd store returned an I/O error while opening a required
blob. Local `cargo fmt --check`, `cargo test --locked` (137 passed), and the
release build succeeded and produced the installed hash above.

## Verification and remaining safe handoff

- Restored tmux windows are alive; head windows are again 0 through 4.
- The repaired thread ID is unchanged and has one live restored wrapper.
- Head1 thread `019f40b9...` is also restored with its original ID and no
  duplicate live owner; the pane uses the main runtime deliberately because
  the main app-server owns the shared writer lock.
- No session was killed merely because it was alive.
- The main yolo server remains the old resident inode while the current active
  session is attached; restarting that shared server is a separate rolling
  handoff and must not be performed during the current turn.
