# Live upgrade/resume failure follow-up

Date: 2026-06-27

## Incident

A live `upgrade-resume-all` on the vagrant host failed Phoenix recovery for
several clients:

- clients re-execed while the Codex app-server was still starting;
- the 10 second app-server readiness wait was too short for the real upgraded
  Codex app-server startup time;
- the yolo server itself was still an old `(deleted)` executable, so server-side
  readiness fixes installed on disk were not active yet.

Affected panes were manually or programmatically resumed afterward.

## Fixes

1. Increase app-server readiness timeout to 180 seconds for:
   - daemon startup readiness;
   - app-server spawn/restart readiness;
   - client `ensure_server` readiness.

2. Bound the resume context repair watcher:
   - it now exits after two stable no-change checks;
   - it has a hard 60 second limit;
   - it checks every 10 seconds instead of every 2 seconds.

The second fix addresses a separate CPU regression exposed during recovery:
the watcher was rereading and potentially rewriting active session JSONL files
whenever Codex appended to them, causing yolo clients to burn CPU even after the
event-driven child wait fix.

## Validation

After reinstalling yolo, the failed head/jotter/moon panes were resumed again.
The replacement clients reached running state. A 10 second CPU tick delta on
the replacement yolo clients was 0 for all checked clients after the bounded
watcher exited.

The current websh session was manually resumed by the user and was left
untouched to avoid disrupting the active conversation.
