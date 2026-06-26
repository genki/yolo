# yolo client wait loop CPU issue

Date: 2026-06-27

## Background

Several long-running `yolo resume` wrapper processes were observed consuming
roughly 26-28% CPU each while their child native `codex` processes were mostly
idle. `/proc/<pid>/wchan` showed `hrtimer_nanosleep`, and CPU accounting was
concentrated in one yolo thread, suggesting a client-side wait/reconnect loop
rather than intentional Codex work.

## Change

The client process monitor no longer polls the child with
`Child::try_wait()` plus a fixed sleep. Instead:

- a dedicated waiter thread blocks in `Child::wait()`;
- the heartbeat/restart path sends `ClientEvent::RestartRequested` through an
  `mpsc` channel;
- the main client thread blocks on `recv()` and reacts only to child-exit or
  restart events.

This removes the regular child-process polling from live yolo clients. The
heartbeat still exists because it is also the current server liveness and
generation-notification mechanism.

## Operational note

Installing the new binary does not change already-running yolo client
processes. Old clients that show `/home/vagrant/.cargo/bin/yolo (deleted)` or
continue burning CPU need to be replaced by a safe session resume/reexec flow,
not by killing panes blindly.
