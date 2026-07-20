use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_CODEX: &str = "codex";
const CODEX_PACKAGE: &str = "@openai/codex@latest";
const RUNTIME_DIR_NAME: &str = "yolo";
const API_SOCKET_NAME: &str = "api.sock";
const APP_SERVER_SOCKET_NAME: &str = "codex-app-server.sock";
const PID_FILE_NAME: &str = "server.pid";
const MANAGED_CODEX_DIR_NAME: &str = "codex-npm";
const THREAD_MONITOR_INTERVAL: Duration = Duration::from_secs(2);
const UPGRADE_IDLE_POLL_INTERVAL: Duration = Duration::from_secs(2);
const RESUME_GENERATION_GRACE: Duration = Duration::from_secs(20);
const DEFAULT_UPGRADE_IDLE_WAIT_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const FEDERATION_POLL_INTERVAL: Duration = Duration::from_secs(5);
const APP_SERVER_RPC_READ_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const APP_SERVER_RPC_READ_RETRY_INTERVAL: Duration = Duration::from_millis(25);
const APP_SERVER_CONFIGURE_MAX_ATTEMPTS: usize = 3;
const APP_SERVER_CONFIGURE_RETRY_DELAY: Duration = Duration::from_secs(1);
const APP_SERVER_READY_TIMEOUT: Duration = Duration::from_secs(180);
const RESUME_CONTEXT_REPAIR_WATCH_TIMEOUT: Duration = Duration::from_secs(60);
const RESUME_CONTEXT_REPAIR_WATCH_INTERVAL: Duration = Duration::from_secs(10);
const RESUME_PERMISSIONS_REINFORCE_TIMEOUT: Duration = Duration::from_secs(120);
const RESUME_PERMISSIONS_REINFORCE_INTERVAL: Duration = Duration::from_secs(2);
const APP_SERVER_SELF_HEAL_STABLE_AFTER: Duration = Duration::from_secs(60);
const APP_SERVER_SELF_HEAL_MAX_BACKOFF: Duration = Duration::from_secs(60);
const CLIENT_PROXY_DIR_NAME: &str = "client-proxies";
const CLIENT_PENDING_SETTINGS_DIR_NAME: &str = "client-pending-settings";
const TURN_ARCHIVE_FILE_NAME: &str = "turns.jsonl";
const APP_SERVER_TELEMETRY_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const MAX_TELEMETRY_THREADS: usize = 2048;
const MAX_TELEMETRY_TOOL_CALLS: usize = 512;
const MAX_TELEMETRY_HOOK_RUNS: usize = 512;
const MAX_TELEMETRY_TURNS: usize = 512;
const MAX_TURN_TEXT_BYTES: usize = 16 * 1024;
const MAX_PENDING_TURN_INPUTS: usize = 128;
const MAX_PENDING_TURN_INPUT_AGE_SECS: u64 = 600;

#[derive(Clone, Debug)]
struct RuntimePaths {
    dir: PathBuf,
    api_socket: PathBuf,
    app_server_socket: PathBuf,
    pid_file: PathBuf,
    log_file: PathBuf,
    turn_archive: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ClientInfo {
    id: String,
    yolo_pid: u32,
    codex_pid: Option<u32>,
    cwd: String,
    args: Vec<String>,
    remote: String,
    model: Option<String>,
    service_tier: Option<String>,
    #[serde(default)]
    reasoning_effort: Option<String>,
    fast: bool,
    thread_id: Option<String>,
    #[serde(default)]
    thread_id_source: String,
    started_at: u64,
    updated_at: u64,
    ended_at: Option<u64>,
    exit_code: Option<i32>,
    status: String,
    #[serde(default)]
    codex_status: Option<String>,
    #[serde(default)]
    codex_active_flags: Vec<String>,
    #[serde(default)]
    codex_status_updated_at: Option<u64>,
    #[serde(default)]
    settings_updated_at: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ClientResumeSettings {
    thread_id: Option<String>,
    model: Option<String>,
    service_tier: Option<String>,
    reasoning_effort: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ServerInfo {
    version: String,
    pid: u32,
    app_server_pid: Option<u32>,
    #[serde(default)]
    app_server_generation: u64,
    resume_generation: u64,
    api_socket: String,
    app_server_socket: String,
    clients: Vec<ClientInfo>,
    #[serde(default)]
    slaves: Vec<SlaveInfo>,
    #[serde(default)]
    tmux_panes: Vec<TmuxPaneInfo>,
    #[serde(default)]
    telemetry_summary: TelemetrySummary,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct CodexUiStatus {
    model: Option<String>,
    effort: Option<String>,
    fast: Option<bool>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct TmuxPaneInfo {
    session_name: Option<String>,
    window_index: Option<u32>,
    pane_index: Option<u32>,
    pane_pid: Option<u32>,
    #[serde(default)]
    yolo_pid: Option<u32>,
    cwd: Option<String>,
    command: Option<String>,
    #[serde(default)]
    codex_ui_status: Option<CodexUiStatus>,
}

#[derive(Debug)]
struct ServerState {
    started_at: u64,
    app_server_pid: Option<u32>,
    app_server_generation: u64,
    resume_generation: u64,
    clients: BTreeMap<String, ClientInfo>,
    slaves: BTreeMap<String, SlaveInfo>,
    telemetry: AgentTelemetry,
    #[allow(dead_code)]
    federation_push_senders: BTreeMap<String, mpsc::Sender<Value>>,
    status_event_senders: BTreeMap<u64, mpsc::Sender<Value>>,
    next_status_event_id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AppServerRpcPriority {
    Control,
    Background,
}

#[derive(Debug, Default)]
struct AppServerRpcGateState {
    active: bool,
    control_waiters: usize,
}

#[derive(Debug)]
struct AppServerRpcGate {
    state: Mutex<AppServerRpcGateState>,
    changed: Condvar,
}

#[derive(Debug)]
struct AppServerRpcLease {
    gate: &'static AppServerRpcGate,
}

static APP_SERVER_RPC_GATE: OnceLock<AppServerRpcGate> = OnceLock::new();

fn app_server_rpc_gate() -> &'static AppServerRpcGate {
    APP_SERVER_RPC_GATE.get_or_init(|| AppServerRpcGate {
        state: Mutex::new(AppServerRpcGateState::default()),
        changed: Condvar::new(),
    })
}

fn acquire_app_server_rpc(priority: AppServerRpcPriority) -> AppServerRpcLease {
    let gate = app_server_rpc_gate();
    let mut state = gate.state.lock().expect("app-server RPC gate poisoned");
    if priority == AppServerRpcPriority::Control {
        state.control_waiters = state.control_waiters.saturating_add(1);
        while state.active {
            state = gate
                .changed
                .wait(state)
                .expect("app-server RPC gate poisoned");
        }
        state.control_waiters = state.control_waiters.saturating_sub(1);
    } else {
        while state.active || state.control_waiters > 0 {
            state = gate
                .changed
                .wait(state)
                .expect("app-server RPC gate poisoned");
        }
    }
    state.active = true;
    drop(state);
    AppServerRpcLease { gate }
}

impl Drop for AppServerRpcLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock() {
            state.active = false;
            self.gate.changed.notify_all();
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SlaveInfo {
    id: String,
    #[serde(default)]
    host: Option<String>,
    version: String,
    pid: u32,
    last_seen_at: u64,
    status: String,
    #[serde(default)]
    commands: Vec<SlaveCommandRecord>,
    #[serde(default)]
    latest_status: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SlavePollRequest {
    slave_id: String,
    version: String,
    pid: u32,
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    status: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SlaveResultRequest {
    slave_id: String,
    command_id: String,
    ok: bool,
    result: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SlaveCommand {
    #[serde(default)]
    id: String,
    action: String,
    #[serde(default)]
    codex_version: Option<String>,
    #[serde(default)]
    yolo_version: Option<String>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    configure: Option<ConfigureClientsRequest>,
    #[serde(default)]
    thread_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SlaveCommandRecord {
    command: SlaveCommand,
    status: String,
    created_at: u64,
    #[serde(default)]
    started_at: Option<u64>,
    #[serde(default)]
    finished_at: Option<u64>,
    #[serde(default)]
    result: Option<Value>,
}

#[derive(Clone, Debug)]
struct AppThreadSnapshot {
    id: String,
    cwd: String,
    status: String,
    active_flags: Vec<String>,
    model: Option<String>,
    service_tier: Option<String>,
    reasoning_effort: Option<String>,
}

#[derive(Clone, Debug)]
struct AppThreadStatusUpdate {
    thread_id: String,
    status: String,
    active_flags: Vec<String>,
}

#[derive(Clone, Debug, Default)]
struct AgentTelemetry {
    threads: BTreeMap<String, AgentThreadRecord>,
    tool_calls: BTreeMap<String, ToolCallRecord>,
    hook_runs: BTreeMap<String, HookRunRecord>,
    turns: BTreeMap<String, TurnRecord>,
    pending_turn_inputs: BTreeMap<String, VecDeque<PendingTurnInput>>,
    last_event_at: Option<u64>,
}

#[derive(Clone, Debug, Default)]
struct AgentThreadRecord {
    thread_id: String,
    parent_thread_id: Option<String>,
    session_id: Option<String>,
    cwd: Option<String>,
    name: Option<String>,
    agent_role: Option<String>,
    agent_nickname: Option<String>,
    source: Option<String>,
    status: String,
    active_flags: Vec<String>,
    created_at: Option<u64>,
    updated_at: u64,
    last_activity: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct ToolCallRecord {
    key: String,
    item_id: String,
    thread_id: String,
    turn_id: String,
    item_type: String,
    tool_name: Option<String>,
    phase: String,
    status: String,
    started_at_ms: Option<u64>,
    completed_at_ms: Option<u64>,
    duration_ms: Option<u64>,
    success: Option<bool>,
    receiver_thread_ids: Vec<String>,
    updated_at: u64,
}

#[derive(Clone, Debug, Default)]
struct HookRunRecord {
    key: String,
    run_id: String,
    thread_id: String,
    turn_id: Option<String>,
    event_name: String,
    phase: String,
    status: String,
    handler_type: Option<String>,
    scope: Option<String>,
    started_at: Option<u64>,
    completed_at: Option<u64>,
    duration_ms: Option<u64>,
    updated_at: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct TurnRecord {
    key: String,
    thread_id: String,
    turn_id: String,
    status: String,
    #[serde(default)]
    started_at_ms: Option<u64>,
    #[serde(default)]
    completed_at_ms: Option<u64>,
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default)]
    report: Option<String>,
    updated_at: u64,
}

#[derive(Clone, Debug, Default)]
struct PendingTurnInput {
    prompt: String,
    captured_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TurnInfo {
    thread_id: String,
    turn_id: String,
    status: String,
    started_at_ms: Option<u64>,
    completed_at_ms: Option<u64>,
    prompt: Option<String>,
    report: Option<String>,
    updated_at: u64,
}

#[derive(Clone, Debug, Serialize)]
struct TurnArchiveSnapshot {
    generated_at: u64,
    turns: Vec<TurnInfo>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct TelemetrySummary {
    thread_count: usize,
    subagent_count: usize,
    active_agent_count: usize,
    active_tool_call_count: usize,
    running_hook_count: usize,
    turn_count: usize,
    captured_prompt_count: usize,
    captured_report_count: usize,
    last_event_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct AgentInfo {
    thread_id: String,
    parent_thread_id: Option<String>,
    session_id: Option<String>,
    cwd: Option<String>,
    name: Option<String>,
    agent_role: Option<String>,
    agent_nickname: Option<String>,
    source: Option<String>,
    status: String,
    active_flags: Vec<String>,
    is_subagent: bool,
    subagent_count: usize,
    active_subagent_count: usize,
    descendant_count: usize,
    active_descendant_count: usize,
    created_at: Option<u64>,
    updated_at: u64,
    last_activity: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ToolCallInfo {
    item_id: String,
    thread_id: String,
    turn_id: String,
    item_type: String,
    tool_name: Option<String>,
    phase: String,
    status: String,
    started_at_ms: Option<u64>,
    completed_at_ms: Option<u64>,
    duration_ms: Option<u64>,
    success: Option<bool>,
    receiver_thread_ids: Vec<String>,
    updated_at: u64,
}

#[derive(Clone, Debug, Serialize)]
struct HookRunInfo {
    run_id: String,
    thread_id: String,
    turn_id: Option<String>,
    event_name: String,
    phase: String,
    status: String,
    handler_type: Option<String>,
    scope: Option<String>,
    started_at: Option<u64>,
    completed_at: Option<u64>,
    duration_ms: Option<u64>,
    updated_at: u64,
}

#[derive(Clone, Debug, Serialize)]
struct TelemetrySnapshot {
    generated_at: u64,
    summary: TelemetrySummary,
    agents: Vec<AgentInfo>,
    tool_calls: Vec<ToolCallInfo>,
    hook_runs: Vec<HookRunInfo>,
}

impl AgentTelemetry {
    fn summary(&self) -> TelemetrySummary {
        TelemetrySummary {
            thread_count: self.threads.len(),
            subagent_count: self
                .threads
                .values()
                .filter(|thread| thread.parent_thread_id.is_some())
                .count(),
            active_agent_count: self
                .threads
                .values()
                .filter(|thread| is_active_agent_status(&thread.status))
                .count(),
            active_tool_call_count: self
                .tool_calls
                .values()
                .filter(|call| is_running_tool_status(&call.status))
                .count(),
            running_hook_count: self
                .hook_runs
                .values()
                .filter(|run| is_running_hook_status(&run.status))
                .count(),
            turn_count: self.turns.len(),
            captured_prompt_count: self
                .turns
                .values()
                .filter(|turn| turn.prompt.is_some())
                .count(),
            captured_report_count: self
                .turns
                .values()
                .filter(|turn| turn.report.is_some())
                .count(),
            last_event_at: self.last_event_at,
        }
    }

    fn snapshot(&self) -> TelemetrySnapshot {
        let mut agents = self
            .threads
            .values()
            .map(|thread| {
                let direct_children = self
                    .threads
                    .values()
                    .filter(|candidate| {
                        candidate.parent_thread_id.as_deref() == Some(thread.thread_id.as_str())
                    })
                    .collect::<Vec<_>>();
                let descendants = self
                    .threads
                    .values()
                    .filter(|candidate| {
                        candidate.thread_id != thread.thread_id
                            && self.is_descendant_of(&candidate.thread_id, &thread.thread_id)
                    })
                    .collect::<Vec<_>>();
                AgentInfo {
                    thread_id: thread.thread_id.clone(),
                    parent_thread_id: thread.parent_thread_id.clone(),
                    session_id: thread.session_id.clone(),
                    cwd: thread.cwd.clone(),
                    name: thread.name.clone(),
                    agent_role: thread.agent_role.clone(),
                    agent_nickname: thread.agent_nickname.clone(),
                    source: thread.source.clone(),
                    status: thread.status.clone(),
                    active_flags: thread.active_flags.clone(),
                    is_subagent: thread.parent_thread_id.is_some(),
                    subagent_count: direct_children.len(),
                    active_subagent_count: direct_children
                        .iter()
                        .filter(|child| is_active_agent_status(&child.status))
                        .count(),
                    descendant_count: descendants.len(),
                    active_descendant_count: descendants
                        .iter()
                        .filter(|child| is_active_agent_status(&child.status))
                        .count(),
                    created_at: thread.created_at,
                    updated_at: thread.updated_at,
                    last_activity: thread.last_activity.clone(),
                }
            })
            .collect::<Vec<_>>();
        agents.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.thread_id.cmp(&right.thread_id))
        });

        let mut tool_calls = self
            .tool_calls
            .values()
            .map(|call| ToolCallInfo {
                item_id: call.item_id.clone(),
                thread_id: call.thread_id.clone(),
                turn_id: call.turn_id.clone(),
                item_type: call.item_type.clone(),
                tool_name: call.tool_name.clone(),
                phase: call.phase.clone(),
                status: call.status.clone(),
                started_at_ms: call.started_at_ms,
                completed_at_ms: call.completed_at_ms,
                duration_ms: call.duration_ms,
                success: call.success,
                receiver_thread_ids: call.receiver_thread_ids.clone(),
                updated_at: call.updated_at,
            })
            .collect::<Vec<_>>();
        tool_calls.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.item_id.cmp(&right.item_id))
        });

        let mut hook_runs = self
            .hook_runs
            .values()
            .map(|run| HookRunInfo {
                run_id: run.run_id.clone(),
                thread_id: run.thread_id.clone(),
                turn_id: run.turn_id.clone(),
                event_name: run.event_name.clone(),
                phase: run.phase.clone(),
                status: run.status.clone(),
                handler_type: run.handler_type.clone(),
                scope: run.scope.clone(),
                started_at: run.started_at,
                completed_at: run.completed_at,
                duration_ms: run.duration_ms,
                updated_at: run.updated_at,
            })
            .collect::<Vec<_>>();
        hook_runs.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.run_id.cmp(&right.run_id))
        });

        TelemetrySnapshot {
            generated_at: now_secs(),
            summary: self.summary(),
            agents,
            tool_calls,
            hook_runs,
        }
    }

    fn turns_snapshot(&self, thread_id: Option<&str>, limit: usize) -> TurnArchiveSnapshot {
        let mut turns = self
            .turns
            .values()
            .filter(|turn| {
                thread_id
                    .map(|thread_id| turn.thread_id == thread_id)
                    .unwrap_or(true)
            })
            .map(turn_info)
            .collect::<Vec<_>>();
        turns.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.turn_id.cmp(&right.turn_id))
        });
        turns.truncate(limit.clamp(1, MAX_TELEMETRY_TURNS));
        TurnArchiveSnapshot {
            generated_at: now_secs(),
            turns,
        }
    }

    fn merge_turn_infos(&mut self, infos: Vec<TurnInfo>) {
        for info in infos {
            let record = turn_record_from_info(info);
            self.turns.insert(record.key.clone(), record);
        }
        self.trim_turns();
    }

    fn record_turn_input(&mut self, thread_id: &str, turn_id: Option<&str>, prompt: &str) -> bool {
        if !turn_capture_enabled() {
            return false;
        }
        let thread_id = thread_id.trim();
        let prompt = bounded_turn_text(prompt);
        if thread_id.is_empty() || prompt.is_empty() {
            return false;
        }
        if let Some(turn_id) = turn_id.map(str::trim).filter(|turn_id| !turn_id.is_empty()) {
            let record = self.ensure_turn(thread_id, turn_id);
            record.prompt = Some(prompt);
            record.updated_at = now_secs();
            self.last_event_at = Some(now_secs());
            self.trim_turns();
            return true;
        }

        if let Some(record) = self
            .turns
            .values_mut()
            .filter(|turn| {
                turn.thread_id == thread_id
                    && is_active_turn_status(&turn.status)
                    && turn.prompt.is_none()
            })
            .max_by_key(|turn| turn.updated_at)
        {
            record.prompt = Some(prompt);
            record.updated_at = now_secs();
            self.last_event_at = Some(now_secs());
            self.trim_turns();
            return true;
        }

        let pending = self
            .pending_turn_inputs
            .entry(thread_id.to_string())
            .or_default();
        pending.push_back(PendingTurnInput {
            prompt,
            captured_at: now_secs(),
        });
        while pending.len() > MAX_PENDING_TURN_INPUTS {
            pending.pop_front();
        }
        self.last_event_at = Some(now_secs());
        false
    }

    fn record_turn_started(&mut self, thread_id: &str, turn_id: &str, started_at_ms: Option<u64>) {
        if !turn_capture_enabled() {
            return;
        }
        let thread_id = thread_id.trim();
        let turn_id = turn_id.trim();
        if thread_id.is_empty() || turn_id.is_empty() {
            return;
        }
        let now = now_secs();
        let pending_prompt = self
            .pending_turn_inputs
            .get_mut(thread_id)
            .and_then(|pending| {
                while pending.front().is_some_and(|input| {
                    now.saturating_sub(input.captured_at) > MAX_PENDING_TURN_INPUT_AGE_SECS
                }) {
                    pending.pop_front();
                }
                pending.pop_front()
            })
            .map(|pending| pending.prompt);
        let record = self.ensure_turn(thread_id, turn_id);
        record.status = "active".to_string();
        record.started_at_ms = started_at_ms.or(record.started_at_ms);
        if record.prompt.is_none() {
            record.prompt = pending_prompt;
        }
        record.updated_at = now_secs();
        self.last_event_at = Some(now_secs());
        self.trim_turns();
    }

    fn record_turn_completed(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        status: &str,
        completed_at_ms: Option<u64>,
    ) {
        if !turn_capture_enabled() {
            return;
        }
        let thread_id = thread_id.trim();
        let turn_id = turn_id.trim();
        if thread_id.is_empty() || turn_id.is_empty() {
            return;
        }
        let record = self.ensure_turn(thread_id, turn_id);
        record.status = if status.trim().is_empty() {
            "completed".to_string()
        } else {
            status.to_string()
        };
        record.completed_at_ms = completed_at_ms.or(record.completed_at_ms);
        record.updated_at = now_secs();
        self.last_event_at = Some(now_secs());
        self.trim_turns();
    }

    fn record_turn_message(&mut self, thread_id: &str, turn_id: Option<&str>, item: &Value) {
        if !turn_capture_enabled() {
            return;
        }
        let Some(text) = extract_message_text(item) else {
            return;
        };
        let Some(turn_id) = turn_id
            .map(str::trim)
            .filter(|turn_id| !turn_id.is_empty())
            .map(ToString::to_string)
            .or_else(|| {
                self.turns
                    .values()
                    .filter(|turn| {
                        turn.thread_id == thread_id && is_active_turn_status(&turn.status)
                    })
                    .max_by_key(|turn| turn.updated_at)
                    .map(|turn| turn.turn_id.clone())
            })
        else {
            return;
        };
        let record = self.ensure_turn(thread_id, &turn_id);
        if is_user_message_item(item) {
            record.prompt = Some(text.clone());
        }
        if is_assistant_message_item(item) {
            record.report = Some(text);
        }
        record.updated_at = now_secs();
        self.last_event_at = Some(now_secs());
        self.trim_turns();
    }

    fn ensure_turn(&mut self, thread_id: &str, turn_id: &str) -> &mut TurnRecord {
        let key = turn_key(thread_id, turn_id);
        let thread_id = thread_id.to_string();
        let turn_id = turn_id.to_string();
        self.turns.entry(key.clone()).or_insert_with(|| TurnRecord {
            key,
            thread_id,
            turn_id,
            status: "unknown".to_string(),
            updated_at: now_secs(),
            ..TurnRecord::default()
        })
    }

    fn trim_turns(&mut self) {
        while self.turns.len() > MAX_TELEMETRY_TURNS {
            let Some(candidate) = self
                .turns
                .values()
                .min_by_key(|turn| turn.updated_at)
                .map(|turn| turn.key.clone())
            else {
                break;
            };
            self.turns.remove(&candidate);
        }
    }

    fn is_descendant_of(&self, candidate_id: &str, ancestor_id: &str) -> bool {
        let mut current = self
            .threads
            .get(candidate_id)
            .and_then(|thread| thread.parent_thread_id.clone());
        let mut visited = BTreeSet::new();
        while let Some(parent_id) = current {
            if parent_id == ancestor_id {
                return true;
            }
            if !visited.insert(parent_id.clone()) {
                return false;
            }
            current = self
                .threads
                .get(&parent_id)
                .and_then(|thread| thread.parent_thread_id.clone());
        }
        false
    }

    fn record_thread_value(&mut self, value: &Value) {
        let Some(incoming) = parse_agent_thread_record(value) else {
            return;
        };
        self.upsert_thread(incoming);
    }

    fn upsert_thread(&mut self, incoming: AgentThreadRecord) {
        let now = now_secs();
        let thread_id = incoming.thread_id.clone();
        let record = self
            .threads
            .entry(thread_id)
            .or_insert_with(|| AgentThreadRecord {
                thread_id: incoming.thread_id.clone(),
                status: "unknown".to_string(),
                updated_at: now,
                ..AgentThreadRecord::default()
            });
        if incoming.parent_thread_id.is_some() {
            record.parent_thread_id = incoming.parent_thread_id;
        }
        if incoming.session_id.is_some() {
            record.session_id = incoming.session_id;
        }
        if incoming.cwd.is_some() {
            record.cwd = incoming.cwd;
        }
        if incoming.name.is_some() {
            record.name = incoming.name;
        }
        if incoming.agent_role.is_some() {
            record.agent_role = incoming.agent_role;
        }
        if incoming.agent_nickname.is_some() {
            record.agent_nickname = incoming.agent_nickname;
        }
        if incoming.source.is_some() {
            record.source = incoming.source;
        }
        let has_status = !incoming.status.is_empty() && incoming.status != "unknown";
        if has_status {
            record.status = incoming.status.clone();
        }
        if has_status || !incoming.active_flags.is_empty() {
            record.active_flags = incoming.active_flags;
        }
        if incoming.created_at.is_some() {
            record.created_at = incoming.created_at;
        }
        record.updated_at = incoming.updated_at.max(now);
        self.last_event_at = Some(now);
        self.trim_threads();
    }

    fn record_minimal_thread(
        &mut self,
        thread_id: &str,
        parent_thread_id: Option<&str>,
        status: Option<&str>,
    ) {
        let thread_id = thread_id.trim();
        if thread_id.is_empty() {
            return;
        }
        self.upsert_thread(AgentThreadRecord {
            thread_id: thread_id.to_string(),
            parent_thread_id: parent_thread_id
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string),
            status: status.unwrap_or("unknown").to_string(),
            updated_at: now_secs(),
            ..AgentThreadRecord::default()
        });
    }

    fn record_thread_status(&mut self, thread_id: &str, status: &str, active_flags: Vec<String>) {
        self.record_minimal_thread(thread_id, None, Some(status));
        if let Some(thread) = self.threads.get_mut(thread_id) {
            thread.status = status.to_string();
            thread.active_flags = active_flags;
            thread.updated_at = now_secs();
        }
        self.last_event_at = Some(now_secs());
    }

    fn record_item_event(&mut self, params: &Value, completed: bool) -> bool {
        let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
            return false;
        };
        let turn_id = params
            .get("turnId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(item) = params.get("item") else {
            return false;
        };
        let Some(item_type) = item.get("type").and_then(Value::as_str) else {
            return false;
        };

        self.record_minimal_thread(thread_id, None, Some("active"));
        self.record_subagent_item(thread_id, item);
        if item_type == "collabAgentToolCall" {
            self.record_collab_agent_states(thread_id, item);
        }
        let captures_turn_text =
            is_user_message_item(item) || (completed && is_assistant_message_item(item));
        if captures_turn_text {
            self.record_turn_message(thread_id, Some(turn_id), item);
        }
        if !is_tracked_tool_call_type(item_type) {
            self.last_event_at = Some(now_secs());
            self.trim_turns();
            return captures_turn_text;
        }

        let Some(item_id) = item.get("id").and_then(Value::as_str) else {
            return captures_turn_text;
        };
        let key = format!("{thread_id}:{turn_id}:{item_id}");
        let item_updated_at = if completed {
            params
                .get("completedAtMs")
                .and_then(Value::as_u64)
                .map(|value| value / 1000)
                .unwrap_or_else(now_secs)
        } else {
            params
                .get("startedAtMs")
                .and_then(Value::as_u64)
                .map(|value| value / 1000)
                .unwrap_or_else(now_secs)
        };
        let status = item
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or(if completed { "completed" } else { "running" })
            .to_string();
        let tool_name = item
            .get("tool")
            .and_then(Value::as_str)
            .or_else(|| {
                if item_type == "commandExecution" {
                    Some("commandExecution")
                } else if item_type == "fileChange" {
                    Some("fileChange")
                } else {
                    None
                }
            })
            .map(ToString::to_string);
        let receiver_thread_ids = item
            .get("receiverThreadIds")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let success = item
            .get("success")
            .and_then(Value::as_bool)
            .or_else(|| {
                item.get("exitCode")
                    .and_then(Value::as_i64)
                    .map(|code| code == 0)
            })
            .or_else(|| match status.as_str() {
                "completed" | "succeeded" => Some(true),
                "failed" | "error" | "interrupted" => Some(false),
                _ => None,
            });

        let record = self
            .tool_calls
            .entry(key.clone())
            .or_insert_with(|| ToolCallRecord {
                key: key.clone(),
                item_id: item_id.to_string(),
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                item_type: item_type.to_string(),
                phase: "pre".to_string(),
                status: "running".to_string(),
                updated_at: item_updated_at,
                ..ToolCallRecord::default()
            });
        record.item_type = item_type.to_string();
        record.tool_name = tool_name;
        record.receiver_thread_ids = receiver_thread_ids.clone();
        record.phase = if completed { "post" } else { "pre" }.to_string();
        record.status = status;
        record.success = success;
        if completed {
            record.completed_at_ms = params.get("completedAtMs").and_then(Value::as_u64);
        } else {
            record.started_at_ms = params.get("startedAtMs").and_then(Value::as_u64);
        }
        record.duration_ms = item.get("durationMs").and_then(Value::as_u64);
        record.updated_at = item_updated_at;
        for child_thread_id in receiver_thread_ids {
            self.record_minimal_thread(child_thread_id.as_str(), Some(thread_id), Some("active"));
        }
        self.last_event_at = Some(now_secs());
        self.trim_tool_calls();
        captures_turn_text
    }

    fn record_subagent_item(&mut self, parent_thread_id: &str, item: &Value) {
        if item.get("type").and_then(Value::as_str) != Some("subAgentActivity") {
            return;
        }
        let Some(child_thread_id) = item.get("agentThreadId").and_then(Value::as_str) else {
            return;
        };
        let kind = item
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let status = match kind {
            "started" => "active",
            "interrupted" => "interrupted",
            _ => "unknown",
        };
        self.record_minimal_thread(child_thread_id, Some(parent_thread_id), Some(status));
        if let Some(thread) = self.threads.get_mut(child_thread_id) {
            thread.parent_thread_id = Some(parent_thread_id.to_string());
            thread.last_activity = Some(kind.to_string());
            thread.updated_at = now_secs();
        }
    }

    fn record_collab_agent_states(&mut self, parent_thread_id: &str, item: &Value) {
        let Some(states) = item.get("agentsStates").and_then(Value::as_object) else {
            return;
        };
        for (child_thread_id, state) in states {
            let status = state
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            self.record_minimal_thread(child_thread_id, Some(parent_thread_id), Some(status));
            if let Some(thread) = self.threads.get_mut(child_thread_id) {
                thread.parent_thread_id = Some(parent_thread_id.to_string());
                thread.last_activity = state
                    .get("message")
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
                thread.updated_at = now_secs();
            }
        }
    }

    fn record_hook_event(&mut self, params: &Value, completed: bool) {
        let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
            return;
        };
        let Some(run) = params.get("run") else {
            return;
        };
        let Some(run_id) = run.get("id").and_then(Value::as_str) else {
            return;
        };
        let key = format!("{thread_id}:{run_id}");
        let event_name = run
            .get("eventName")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let status = run
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or(if completed { "completed" } else { "running" });
        let record = self
            .hook_runs
            .entry(key.clone())
            .or_insert_with(|| HookRunRecord {
                key,
                run_id: run_id.to_string(),
                thread_id: thread_id.to_string(),
                turn_id: params
                    .get("turnId")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                event_name: event_name.to_string(),
                phase: hook_phase(event_name).to_string(),
                status: status.to_string(),
                updated_at: now_secs(),
                ..HookRunRecord::default()
            });
        record.event_name = event_name.to_string();
        record.phase = hook_phase(event_name).to_string();
        record.status = status.to_string();
        record.handler_type = run
            .get("handlerType")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        record.scope = run
            .get("scope")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        record.started_at = run.get("startedAt").and_then(Value::as_u64);
        record.completed_at = run.get("completedAt").and_then(Value::as_u64);
        record.duration_ms = run.get("durationMs").and_then(Value::as_u64);
        record.updated_at = now_secs();
        self.last_event_at = Some(now_secs());
        self.trim_hook_runs();
    }

    fn record_app_server_event(&mut self, value: &Value) -> bool {
        let Some(method) = value.get("method").and_then(Value::as_str) else {
            if let Some(thread) = value.get("result").and_then(|result| result.get("thread")) {
                self.record_thread_value(thread);
            }
            return false;
        };
        let params = value.get("params").unwrap_or(&Value::Null);
        match method {
            "thread/started" => {
                if let Some(thread) = params.get("thread") {
                    self.record_thread_value(thread);
                }
                false
            }
            "thread/status/changed" => {
                if let (Some(thread_id), Some(status)) = (
                    params.get("threadId").and_then(Value::as_str),
                    params.get("status"),
                ) {
                    let (status, active_flags) = parse_thread_status_value(status)
                        .unwrap_or_else(|| ("unknown".to_string(), Vec::new()));
                    self.record_thread_status(thread_id, &status, active_flags);
                }
                false
            }
            "turn/started" => {
                if let Some(thread_id) = params.get("threadId").and_then(Value::as_str) {
                    if let Some(turn_id) = params.get("turnId").and_then(Value::as_str) {
                        self.record_turn_started(
                            thread_id,
                            turn_id,
                            params.get("startedAtMs").and_then(Value::as_u64),
                        );
                    }
                    self.record_thread_status(thread_id, "active", Vec::new());
                }
                true
            }
            "turn/completed" => {
                if let Some(thread_id) = params.get("threadId").and_then(Value::as_str) {
                    if let Some(turn_id) = params.get("turnId").and_then(Value::as_str) {
                        let status = params
                            .get("status")
                            .and_then(Value::as_str)
                            .unwrap_or("completed");
                        self.record_turn_completed(
                            thread_id,
                            turn_id,
                            status,
                            params.get("completedAtMs").and_then(Value::as_u64),
                        );
                    }
                    self.record_thread_status(thread_id, "idle", Vec::new());
                }
                true
            }
            "thread/closed" => {
                if let Some(thread_id) = params.get("threadId").and_then(Value::as_str) {
                    self.record_thread_status(thread_id, "notLoaded", Vec::new());
                }
                false
            }
            "item/started" => self.record_item_event(params, false),
            "item/completed" => self.record_item_event(params, true),
            "hook/started" => {
                self.record_hook_event(params, false);
                false
            }
            "hook/completed" => {
                self.record_hook_event(params, true);
                false
            }
            _ => false,
        }
    }

    fn trim_threads(&mut self) {
        while self.threads.len() > MAX_TELEMETRY_THREADS {
            let candidate = self
                .threads
                .values()
                .filter(|thread| !is_active_agent_status(&thread.status))
                .min_by_key(|thread| thread.updated_at)
                .map(|thread| thread.thread_id.clone());
            let Some(candidate) = candidate else {
                break;
            };
            self.threads.remove(&candidate);
        }
    }

    fn trim_tool_calls(&mut self) {
        while self.tool_calls.len() > MAX_TELEMETRY_TOOL_CALLS {
            let Some(candidate) = self
                .tool_calls
                .values()
                .min_by_key(|call| call.updated_at)
                .map(|call| call.key.clone())
            else {
                break;
            };
            self.tool_calls.remove(&candidate);
        }
    }

    fn trim_hook_runs(&mut self) {
        while self.hook_runs.len() > MAX_TELEMETRY_HOOK_RUNS {
            let Some(candidate) = self
                .hook_runs
                .values()
                .min_by_key(|run| run.updated_at)
                .map(|run| run.key.clone())
            else {
                break;
            };
            self.hook_runs.remove(&candidate);
        }
    }
}

fn is_active_agent_status(status: &str) -> bool {
    matches!(status, "active" | "inProgress" | "running" | "pendingInit")
}

fn is_running_tool_status(status: &str) -> bool {
    !matches!(
        status,
        "completed" | "succeeded" | "failed" | "error" | "interrupted"
    )
}

fn is_running_hook_status(status: &str) -> bool {
    !matches!(
        status,
        "completed"
            | "succeeded"
            | "failed"
            | "error"
            | "timedOut"
            | "cancelled"
            | "blocked"
            | "stopped"
    )
}

fn is_active_turn_status(status: &str) -> bool {
    matches!(status, "active" | "inProgress" | "running" | "pendingInit")
}

fn turn_capture_enabled() -> bool {
    env::var("YOLO_TURN_CAPTURE")
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off"
            )
        })
        .unwrap_or(true)
}

fn turn_key(thread_id: &str, turn_id: &str) -> String {
    format!("{thread_id}:{turn_id}")
}

fn turn_info(turn: &TurnRecord) -> TurnInfo {
    TurnInfo {
        thread_id: turn.thread_id.clone(),
        turn_id: turn.turn_id.clone(),
        status: turn.status.clone(),
        started_at_ms: turn.started_at_ms,
        completed_at_ms: turn.completed_at_ms,
        prompt: turn.prompt.clone(),
        report: turn.report.clone(),
        updated_at: turn.updated_at,
    }
}

fn turn_record_from_info(info: TurnInfo) -> TurnRecord {
    let key = turn_key(&info.thread_id, &info.turn_id);
    TurnRecord {
        key,
        thread_id: info.thread_id,
        turn_id: info.turn_id,
        status: info.status,
        started_at_ms: info.started_at_ms,
        completed_at_ms: info.completed_at_ms,
        prompt: info.prompt,
        report: info.report,
        updated_at: info.updated_at,
    }
}

fn bounded_turn_text(value: &str) -> String {
    let value = value.trim();
    if value.len() <= MAX_TURN_TEXT_BYTES {
        return value.to_string();
    }
    let suffix = "\n[truncated]";
    let max_body = MAX_TURN_TEXT_BYTES.saturating_sub(suffix.len());
    let mut end = max_body.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &value[..end], suffix)
}

fn collect_message_text(value: &Value, output: &mut Vec<String>, depth: usize) {
    if depth > 8 {
        return;
    }
    match value {
        Value::String(text) => {
            if !text.trim().is_empty() {
                output.push(text.trim().to_string());
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_message_text(value, output, depth + 1);
            }
        }
        Value::Object(object) => {
            if let Some(text) = object.get("text").and_then(Value::as_str) {
                if !text.trim().is_empty() {
                    output.push(text.trim().to_string());
                }
                return;
            }
            for key in ["content", "input", "message", "prompt", "items"] {
                if let Some(value) = object.get(key) {
                    collect_message_text(value, output, depth + 1);
                }
            }
        }
        _ => {}
    }
}

fn extract_message_text(value: &Value) -> Option<String> {
    let mut parts = Vec::new();
    collect_message_text(value, &mut parts, 0);
    if parts.is_empty() {
        return None;
    }
    Some(bounded_turn_text(&parts.join("\n")))
}

fn extract_turn_prompt(params: &Value) -> Option<String> {
    for key in ["input", "prompt", "message", "items"] {
        if let Some(value) = params.get(key)
            && let Some(text) = extract_message_text(value)
            && !text.is_empty()
        {
            return Some(text);
        }
    }
    None
}

fn is_user_message_item(item: &Value) -> bool {
    item.get("type").and_then(Value::as_str) == Some("userMessage")
        || item.get("role").and_then(Value::as_str) == Some("user")
}

fn is_assistant_message_item(item: &Value) -> bool {
    matches!(
        item.get("type").and_then(Value::as_str),
        Some("agentMessage" | "assistantMessage")
    ) || item.get("role").and_then(Value::as_str) == Some("assistant")
}

fn is_tracked_tool_call_type(item_type: &str) -> bool {
    matches!(
        item_type,
        "commandExecution"
            | "fileChange"
            | "mcpToolCall"
            | "dynamicToolCall"
            | "collabAgentToolCall"
            | "webSearch"
            | "imageGeneration"
            | "imageView"
    )
}

fn hook_phase(event_name: &str) -> &'static str {
    match event_name {
        "preToolUse" | "permissionRequest" => "pre",
        "postToolUse" => "post",
        _ => "lifecycle",
    }
}

fn parse_agent_thread_record(value: &Value) -> Option<AgentThreadRecord> {
    let thread_id = value.get("id").and_then(Value::as_str)?.trim();
    if thread_id.is_empty() {
        return None;
    }
    let status_value = value.get("status");
    let status = status_value
        .and_then(|status| status.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let active_flags = status_value
        .and_then(|status| status.get("activeFlags"))
        .and_then(Value::as_array)
        .map(|flags| {
            flags
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let source_value = value.get("source");
    let parent_thread_id = value
        .get("parentThreadId")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            source_value?
                .get("subAgent")
                .and_then(|source| source.get("thread_spawn"))
                .and_then(|spawn| spawn.get("parent_thread_id"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        });
    Some(AgentThreadRecord {
        thread_id: thread_id.to_string(),
        parent_thread_id,
        session_id: value
            .get("sessionId")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        cwd: value
            .get("cwd")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        name: value
            .get("name")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        agent_role: value
            .get("agentRole")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        agent_nickname: value
            .get("agentNickname")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        source: source_value.and_then(json_enum_string),
        status,
        active_flags,
        created_at: value.get("createdAt").and_then(Value::as_u64),
        updated_at: value
            .get("updatedAt")
            .and_then(Value::as_u64)
            .unwrap_or_else(now_secs),
        ..AgentThreadRecord::default()
    })
}

fn json_enum_string(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(ToString::to_string)
        .or_else(|| {
            value
                .get("type")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .or_else(|| {
            value
                .get("kind")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .or_else(|| {
            value
                .as_object()
                .and_then(|object| object.keys().next())
                .map(ToString::to_string)
        })
}

enum ClientEvent {
    RestartRequested,
    ThreadBound(String),
    PendingSettingsApplied(PendingClientSettings),
    TurnInput {
        thread_id: String,
        turn_id: Option<String>,
        prompt: String,
    },
    CodexExited(Result<ExitStatus, String>),
}

struct ClientThreadProxy {
    socket_path: PathBuf,
    pending_settings_path: PathBuf,
    remote: String,
}

struct WebsocketFrame {
    raw: Vec<u8>,
    opcode: u8,
    payload: Vec<u8>,
}

struct ThreadBindingTracker {
    pending_create_request_ids: BTreeSet<String>,
    current_thread_id: Option<String>,
    event_tx: mpsc::Sender<ClientEvent>,
}

#[derive(Debug, Default)]
struct CodexLaunchConfig {
    model: Option<String>,
    service_tier: Option<String>,
    reasoning_effort: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ConfigureClientsRequest {
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    thread_id: Option<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    all: bool,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    fast: Option<bool>,
    #[serde(default)]
    reasoning_effort: Option<String>,
    #[serde(default)]
    timeout_secs: Option<u64>,
    #[serde(default)]
    queue: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct PendingClientSettings {
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    fast: Option<bool>,
    #[serde(default)]
    reasoning_effort: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct UpgradeResumeAllRequest {
    #[serde(default)]
    codex_version: Option<String>,
    #[serde(default)]
    ignore_client_id: Option<String>,
    #[serde(default)]
    ignore_thread_id: Option<String>,
    #[serde(default)]
    ignore_cwd: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RefreshResumeRequest {
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    thread_id: Option<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    all: bool,
}

fn main() {
    let mut args = env::args_os().skip(1).collect::<Vec<_>>();

    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_help();
        return;
    }
    if args.iter().any(|arg| arg == "--version" || arg == "-V") {
        println!("yolo {VERSION}");
        return;
    }

    match args.first().and_then(|arg| arg.to_str()) {
        Some("server") => {
            args.remove(0);
            if let Err(err) = run_server(args) {
                eprintln!("yolo server: {err}");
                std::process::exit(1);
            }
        }
        Some("status") | Some("clients") => {
            if let Err(err) = print_status() {
                eprintln!("yolo status: {err}");
                std::process::exit(1);
            }
        }
        Some("turns") | Some("transcript") => {
            args.remove(0);
            if let Err(err) = print_turns(args) {
                eprintln!("yolo turns: {err}");
                std::process::exit(1);
            }
        }
        Some("stop") => {
            if let Err(err) = stop_server() {
                eprintln!("yolo stop: {err}");
                std::process::exit(1);
            }
        }
        Some("upgrade-resume") | Some("resume-upgrade") | Some("upgrade-and-resume") => {
            args.remove(0);
            run_upgrade_resume(args);
        }
        Some("upgrade-resume-all") | Some("resume-all-upgrade") => {
            args.remove(0);
            if let Err(err) = run_upgrade_resume_all() {
                eprintln!("yolo upgrade-resume-all: {err}");
                std::process::exit(1);
            }
        }
        Some("external-codex-upgrade-resume") | Some("upgrade-external-codex") => {
            args.remove(0);
            if let Err(err) = run_external_codex_upgrade_resume(args) {
                eprintln!("yolo external-codex-upgrade-resume: {err}");
                std::process::exit(1);
            }
        }
        Some("set") | Some("configure") => {
            args.remove(0);
            if let Err(err) = run_configure(args) {
                eprintln!("yolo set: {err}");
                std::process::exit(1);
            }
        }
        Some("refresh-resume") | Some("resume-refresh") => {
            args.remove(0);
            if let Err(err) = run_refresh_resume(args) {
                eprintln!("yolo refresh-resume: {err}");
                std::process::exit(1);
            }
        }
        Some("refresh-permissions") | Some("permissions-refresh") => {
            args.remove(0);
            if let Err(err) = run_refresh_permissions(args) {
                eprintln!("yolo refresh-permissions: {err}");
                std::process::exit(1);
            }
        }
        Some("client") => {
            args.remove(0);
            run_client(args);
        }
        Some("codex") => {
            args.remove(0);
            run_native_codex_passthrough(args);
        }
        _ => run_client(args),
    }
}

fn run_server(args: Vec<OsString>) -> Result<(), String> {
    let daemon = args.iter().any(|arg| arg == "--daemon");
    let foreground = args.iter().any(|arg| arg == "--foreground");
    if daemon && !foreground {
        return spawn_server_daemon(&args);
    }

    let paths = runtime_paths()?;
    fs::create_dir_all(&paths.dir).map_err(|err| format!("create runtime dir: {err}"))?;
    if let Some(pid) = running_yolo_server_pid(&paths) {
        return Err(format!(
            "yolo server pid {pid} is already running; refusing to replace {}",
            paths.api_socket.display()
        ));
    }
    if paths.api_socket.exists() {
        if api_get_json("/status").is_ok() {
            return Err(format!(
                "yolo server is already running at {}",
                paths.api_socket.display()
            ));
        }
        remove_socket_if_present(&paths.api_socket)?;
    }
    fs::write(&paths.pid_file, std::process::id().to_string())
        .map_err(|err| format!("write pid file: {err}"))?;

    let mut telemetry = AgentTelemetry::default();
    load_turn_archive(&paths.turn_archive, &mut telemetry);
    let state = Arc::new(Mutex::new(ServerState {
        started_at: now_secs(),
        app_server_pid: None,
        app_server_generation: 0,
        resume_generation: 0,
        clients: BTreeMap::new(),
        slaves: BTreeMap::new(),
        telemetry,
        federation_push_senders: BTreeMap::new(),
        status_event_senders: BTreeMap::new(),
        next_status_event_id: 0,
    }));
    let app_server_pid = ensure_tracked_app_server(Arc::clone(&state), paths.clone())?;
    scan_existing_yolo_clients(&state);
    spawn_initial_app_server_thread_snapshot(Arc::clone(&state), paths.clone());
    spawn_thread_status_monitor(Arc::clone(&state), paths.clone());
    spawn_agent_telemetry_snapshot_monitor(Arc::clone(&state), paths.clone());
    if let Some(addr) = federation_listen_addr(&args) {
        spawn_federation_listener(Arc::clone(&state), paths.clone(), addr)?;
    }
    spawn_slave_connector_if_configured(Arc::clone(&state), paths.clone());

    let listener = UnixListener::bind(&paths.api_socket)
        .map_err(|err| format!("bind {}: {err}", paths.api_socket.display()))?;
    eprintln!(
        "yolo server {} listening on {}",
        VERSION,
        paths.api_socket.display()
    );
    eprintln!(
        "codex app-server child pid {:?} listening on unix://{}",
        app_server_pid,
        paths.app_server_socket.display()
    );

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let state = Arc::clone(&state);
                let paths = paths.clone();
                thread::spawn(move || handle_api_connection(stream, state, paths));
            }
            Err(err) => eprintln!("yolo server: api accept failed: {err}"),
        }
    }

    Ok(())
}

fn spawn_server_daemon(args: &[OsString]) -> Result<(), String> {
    let paths = runtime_paths()?;
    fs::create_dir_all(&paths.dir).map_err(|err| format!("create runtime dir: {err}"))?;
    if api_get_json("/status").is_ok() {
        return Err(format!(
            "yolo server is already running at {}",
            paths.api_socket.display()
        ));
    }
    if let Some(pid) = running_yolo_server_pid(&paths) {
        return Err(format!(
            "yolo server pid {pid} is already running but {} is not reachable",
            paths.api_socket.display()
        ));
    }
    let exe = yolo_daemon_executable()?;
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.log_file)
        .map_err(|err| format!("open {}: {err}", paths.log_file.display()))?;
    let log2 = log
        .try_clone()
        .map_err(|err| format!("clone daemon log: {err}"))?;
    let foreground_args = server_foreground_args(args);
    let child = Command::new("setsid")
        .arg("--")
        .arg(exe)
        .args(foreground_args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log2))
        .spawn()
        .map_err(|err| format!("spawn yolo server: {err}"))?;
    println!("started yolo server pid {}", child.id());
    wait_for_server_ready(&paths, APP_SERVER_READY_TIMEOUT)
}

fn yolo_daemon_executable() -> Result<PathBuf, String> {
    if let Ok(value) = env::var("YOLO_REEXEC_BIN")
        && !value.trim().is_empty()
    {
        return Ok(PathBuf::from(value));
    }
    if let Some(path_exe) = find_executable_in_path("yolo") {
        return Ok(path_exe);
    }
    if let Ok(exe) = env::current_exe()
        && !exe.to_string_lossy().contains("(deleted)")
    {
        return Ok(exe);
    }
    Err("could not find a non-deleted yolo executable for daemon startup".to_string())
}

fn server_foreground_args(args: &[OsString]) -> Vec<OsString> {
    let mut out = vec![OsString::from("server"), OsString::from("--foreground")];
    out.extend(
        args.iter()
            .filter(|arg| arg.to_str() != Some("--daemon") && arg.to_str() != Some("--foreground"))
            .cloned(),
    );
    out
}

fn spawn_app_server(paths: &RuntimePaths, cwd: Option<&Path>) -> Result<Child, String> {
    let codex = codex_executable();
    if let Some(parent) = paths.app_server_socket.parent() {
        fs::create_dir_all(parent).map_err(|err| format!("create app-server socket dir: {err}"))?;
    }
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.log_file)
        .map_err(|err| format!("open {}: {err}", paths.log_file.display()))?;
    let log2 = log
        .try_clone()
        .map_err(|err| format!("clone app-server log: {err}"))?;
    let mut command = Command::new(codex);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    command
        .arg("app-server")
        .arg("--listen")
        .arg(format!("unix://{}", paths.app_server_socket.display()))
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log2))
        .spawn()
        .map_err(|err| format!("spawn codex app-server: {err}"))
}

fn spawn_tracked_app_server(
    state: Arc<Mutex<ServerState>>,
    paths: RuntimePaths,
) -> Result<u32, String> {
    remove_socket_if_present(&paths.app_server_socket)?;
    let mut app_server = spawn_app_server(&paths, None)?;
    let pid = app_server.id();
    if let Ok(mut state) = state.lock() {
        state.app_server_pid = Some(pid);
    }

    wait_for_app_server_ready(&paths, APP_SERVER_READY_TIMEOUT)?;

    let monitor_state = Arc::clone(&state);
    thread::spawn(move || {
        let status = app_server.wait().ok();
        if let Ok(mut state) = monitor_state.lock()
            && state.app_server_pid == Some(pid)
        {
            state.app_server_pid = None;
            for client in state.clients.values_mut() {
                if client.status == "running" {
                    client.status = "app-server-exited".to_string();
                    client.updated_at = now_secs();
                }
            }
        }
        eprintln!("yolo server: codex app-server pid {pid} exited: {status:?}");
    });

    Ok(pid)
}

fn ensure_tracked_app_server(
    state: Arc<Mutex<ServerState>>,
    paths: RuntimePaths,
) -> Result<Option<u32>, String> {
    let existing_pids = find_app_server_pids(&paths);
    if existing_pids.len() > 1 {
        eprintln!(
            "yolo server: found duplicate codex app-servers for unix://{}: {:?}; restarting app-server",
            paths.app_server_socket.display(),
            existing_pids
        );
        terminate_app_servers_for_socket(&paths, Duration::from_secs(2));
        remove_socket_if_present(&paths.app_server_socket)?;
        return spawn_tracked_app_server(state, paths).map(Some);
    }
    if paths.app_server_socket.exists()
        && AppServerRpcClient::connect(&paths.app_server_socket).is_ok()
    {
        let pid = existing_pids
            .first()
            .copied()
            .or_else(|| find_app_server_pid(&paths));
        if let Ok(mut state) = state.lock() {
            state.app_server_pid = pid;
        }
        eprintln!(
            "yolo server: adopted existing codex app-server at unix://{} pid {:?}",
            paths.app_server_socket.display(),
            pid
        );
        return Ok(pid);
    }
    spawn_tracked_app_server(state, paths).map(Some)
}

fn restart_tracked_app_server(
    state: Arc<Mutex<ServerState>>,
    paths: RuntimePaths,
) -> Result<u32, String> {
    restart_tracked_app_server_with_cwd(state, paths, None)
}

fn restart_tracked_app_server_with_cwd(
    state: Arc<Mutex<ServerState>>,
    paths: RuntimePaths,
    cwd: Option<PathBuf>,
) -> Result<u32, String> {
    let old_pid = state.lock().ok().and_then(|state| state.app_server_pid);
    if let Ok(mut state) = state.lock() {
        state.app_server_generation = state.app_server_generation.saturating_add(1);
        for client in state.clients.values_mut() {
            if client.status == "running" {
                client.status = "restarting".to_string();
                client.updated_at = now_secs();
            }
        }
    }
    if let Some(pid) = old_pid {
        terminate_pid_tree(pid, Duration::from_secs(2));
    }
    terminate_app_servers_for_socket(&paths, Duration::from_secs(2));
    remove_socket_if_present(&paths.app_server_socket)?;
    let mut app_server = spawn_app_server(&paths, cwd.as_deref())?;
    let pid = app_server.id();
    if let Ok(mut state) = state.lock() {
        state.app_server_pid = Some(pid);
    }

    wait_for_app_server_ready(&paths, APP_SERVER_READY_TIMEOUT)?;

    let monitor_state = Arc::clone(&state);
    thread::spawn(move || {
        let status = app_server.wait().ok();
        if let Ok(mut state) = monitor_state.lock()
            && state.app_server_pid == Some(pid)
        {
            state.app_server_pid = None;
            for client in state.clients.values_mut() {
                if matches!(client.status.as_str(), "running" | "restarting") {
                    client.status = "app-server-exited".to_string();
                    client.updated_at = now_secs();
                }
            }
        }
        eprintln!("yolo server: codex app-server pid {pid} exited: {status:?}");
    });

    Ok(pid)
}

fn run_client(args: Vec<OsString>) {
    if let Err(err) = ensure_server() {
        eprintln!("yolo: failed to start server: {err}");
        std::process::exit(1);
    }

    let paths = match runtime_paths() {
        Ok(paths) => paths,
        Err(err) => {
            eprintln!("yolo: {err}");
            std::process::exit(1);
        }
    };
    let upstream_remote = env::var("YOLO_REMOTE")
        .unwrap_or_else(|_| format!("unix://{}", paths.app_server_socket.display()));

    let client_id = format!("{}-{}", std::process::id(), now_millis());
    let cwd = env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .display()
        .to_string();
    let codex_cwd = effective_codex_cwd(&args, &cwd);
    let resolved_args = match resolve_resume_last_args(&args, &codex_cwd) {
        Ok(args) => args,
        Err(err) => {
            eprintln!("yolo: {err}");
            std::process::exit(2);
        }
    };
    let original_args = resolved_args.clone();
    ensure_codex_project_trusted(&codex_cwd);
    let launch_args = codex_args_with_cwd(resolved_args.clone(), &cwd);
    let string_args = resolved_args
        .iter()
        .map(|arg| arg.to_string_lossy().to_string())
        .collect::<Vec<_>>();

    let initial_config = read_codex_config();
    let initial_service_tier = initial_config.service_tier.clone();
    let initial_fast = is_fast_tier(initial_service_tier.as_deref());
    let thread_id = thread_id_from_args(&resolved_args);
    let resume_thread_id = thread_id.clone();
    repair_resume_session_cwd(&resolved_args, &codex_cwd);
    reinforce_loaded_resume_permissions(&paths.app_server_socket, &resolved_args, &codex_cwd);
    if let Some(thread_id) = thread_id.as_deref() {
        spawn_resume_context_repair_watcher(thread_id, &codex_cwd);
    }
    if let Some(thread_id) = thread_id.as_deref()
        && let Some(existing) = running_duplicate_thread_client(thread_id, std::process::id())
    {
        eprintln!(
            "yolo: refusing duplicate resume for thread {thread_id}; already running in {existing}"
        );
        std::process::exit(2);
    }
    let mut info = ClientInfo {
        id: client_id.clone(),
        yolo_pid: std::process::id(),
        codex_pid: None,
        cwd: cwd.clone(),
        args: string_args,
        remote: upstream_remote.clone(),
        model: initial_config.model,
        service_tier: initial_service_tier,
        reasoning_effort: None,
        fast: initial_fast,
        thread_id,
        thread_id_source: if resume_thread_id.is_some() {
            "resume_arg".to_string()
        } else {
            "unresolved".to_string()
        },
        started_at: now_secs(),
        updated_at: now_secs(),
        ended_at: None,
        exit_code: None,
        status: "running".to_string(),
        codex_status: None,
        codex_active_flags: Vec::new(),
        codex_status_updated_at: None,
        settings_updated_at: None,
    };

    let heartbeat_id = client_id.clone();
    let (event_tx, event_rx) = mpsc::channel::<ClientEvent>();
    let client_proxy =
        match spawn_client_thread_proxy(&paths, &client_id, &upstream_remote, event_tx.clone()) {
            Ok(proxy) => Some(proxy),
            Err(err) => {
                eprintln!("yolo: thread binding proxy unavailable: {err}");
                None
            }
        };
    let remote = client_proxy
        .as_ref()
        .map(|proxy| proxy.remote.clone())
        .unwrap_or_else(|| upstream_remote.clone());
    // Register the endpoint Codex actually uses. Keeping the upstream URL
    // here made a newly launched, pre-thread client look like a legacy direct
    // connection until a later process scan reconstructed its command line.
    info.remote = remote.clone();
    let heartbeat_event_tx = event_tx.clone();
    let seen_resume_generation = Arc::new(AtomicU64::new(current_restart_generation()));
    let heartbeat_seen_generation = Arc::clone(&seen_resume_generation);
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(2));
            let body = json!({
                "id": heartbeat_id,
                "status": "running",
                "updated_at": now_secs(),
            });
            match api_post_json("/clients/heartbeat", &body) {
                Ok(value) => {
                    if let Some(generation) = restart_generation_from_status(&value) {
                        let seen = heartbeat_seen_generation.load(Ordering::SeqCst);
                        if generation > seen {
                            heartbeat_seen_generation.store(generation, Ordering::SeqCst);
                            if heartbeat_event_tx
                                .send(ClientEvent::RestartRequested)
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
                Err(_) => continue,
            }
        }
    });

    let codex = codex_executable();
    let mut command = Command::new(codex);
    command
        .current_dir(&cwd)
        .arg("--remote")
        .arg(&remote)
        .arg("--search")
        .arg("--dangerously-bypass-approvals-and-sandbox");
    if resume_target_from_args(&resolved_args).is_some() {
        command.arg("-c").arg("include_environment_context=false");
    }
    command
        .args(&launch_args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            eprintln!("yolo: failed to spawn codex: {err}");
            std::process::exit(127);
        }
    };
    let child_pid = child.id();

    info.codex_pid = Some(child_pid);
    info.updated_at = now_secs();
    info.ended_at = None;
    info.exit_code = None;
    info.status = "running".to_string();
    let _ = api_post_json(
        "/clients/register",
        &serde_json::to_value(&info).unwrap_or_else(|_| json!({})),
    );
    if let Some(thread_id) = resume_thread_id {
        spawn_loaded_resume_permissions_reinforcer(
            paths.app_server_socket.clone(),
            thread_id,
            codex_cwd.clone(),
        );
    }

    let child_event_tx = event_tx.clone();
    thread::spawn(move || {
        let result = child.wait().map_err(|err| err.to_string());
        let _ = child_event_tx.send(ClientEvent::CodexExited(result));
    });
    drop(event_tx);

    let mut pending_settings_applied = None;
    loop {
        match event_rx.recv() {
            Ok(ClientEvent::RestartRequested) => {
                if let Some(proxy) = client_proxy.as_ref() {
                    let _ = remove_socket_if_present(&proxy.socket_path);
                    let _ = fs::remove_file(&proxy.pending_settings_path);
                }
                terminate_pid_tree(child_pid, Duration::from_secs(5));
                reexec_client_for_resume(&original_args, &client_id);
            }
            Ok(ClientEvent::ThreadBound(thread_id)) => {
                if info.thread_id.as_deref() != Some(thread_id.as_str())
                    || info.thread_id_source != "proxy"
                {
                    info.thread_id = Some(thread_id);
                    info.thread_id_source = "proxy".to_string();
                    info.codex_status = None;
                    info.codex_active_flags.clear();
                    info.codex_status_updated_at = Some(now_secs());
                    info.updated_at = now_secs();
                    let _ = api_post_json(
                        "/clients/register",
                        &serde_json::to_value(&info).unwrap_or_else(|_| json!({})),
                    );
                }
                if let Some(settings) = pending_settings_applied.take() {
                    sync_applied_pending_settings(&client_id, &settings);
                }
            }
            Ok(ClientEvent::PendingSettingsApplied(settings)) => {
                apply_pending_settings_to_client_info(&mut info, &settings);
                pending_settings_applied = Some(settings);
                let _ = api_post_json(
                    "/clients/register",
                    &serde_json::to_value(&info).unwrap_or_else(|_| json!({})),
                );
                if info.thread_id.is_some()
                    && let Some(settings) = pending_settings_applied.take()
                {
                    sync_applied_pending_settings(&client_id, &settings);
                }
            }
            Ok(ClientEvent::TurnInput {
                thread_id,
                turn_id,
                prompt,
            }) => {
                let _ = api_post_json(
                    "/turns/input",
                    &json!({
                        "thread_id": thread_id,
                        "turn_id": turn_id,
                        "prompt": prompt,
                    }),
                );
            }
            Ok(ClientEvent::CodexExited(Ok(status))) => {
                if let Some(proxy) = client_proxy.as_ref() {
                    let _ = remove_socket_if_present(&proxy.socket_path);
                    let _ = fs::remove_file(&proxy.pending_settings_path);
                }
                if should_reexec_after_codex_exit(
                    status.success(),
                    &original_args,
                    &seen_resume_generation,
                ) {
                    reexec_client_for_resume(&original_args, &client_id);
                }
                info.updated_at = now_secs();
                info.ended_at = Some(now_secs());
                info.status = "exited".to_string();
                info.exit_code = status.code();
                let _ = api_post_json(
                    "/clients/finish",
                    &serde_json::to_value(&info).unwrap_or_else(|_| json!({})),
                );
                std::process::exit(info.exit_code.unwrap_or(1));
            }
            Ok(ClientEvent::CodexExited(Err(err))) => {
                if let Some(proxy) = client_proxy.as_ref() {
                    let _ = remove_socket_if_present(&proxy.socket_path);
                    let _ = fs::remove_file(&proxy.pending_settings_path);
                }
                eprintln!("yolo: failed to wait for codex: {err}");
                std::process::exit(1);
            }
            Err(_) => std::process::exit(1),
        }
    }
}

fn run_native_codex_passthrough(args: Vec<OsString>) -> ! {
    let cwd = env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .display()
        .to_string();
    let codex_cwd = effective_codex_cwd(&args, &cwd);
    let resolved_args = match resolve_resume_last_args_without_api(&args, &codex_cwd) {
        Ok(args) => args,
        Err(err) => {
            eprintln!("yolo codex: {err}");
            std::process::exit(2);
        }
    };
    ensure_codex_project_trusted(&codex_cwd);
    repair_resume_session_cwd(&resolved_args, &codex_cwd);
    let launch_args = codex_args_with_cwd(resolved_args.clone(), &cwd);
    let mut command = Command::new(native_codex_executable());
    command
        .current_dir(&cwd)
        .arg("--search")
        .arg("--dangerously-bypass-approvals-and-sandbox");
    if resume_target_from_args(&resolved_args).is_some() {
        command.arg("-c").arg("include_environment_context=false");
    }
    let err = command.args(&launch_args).exec();
    eprintln!("yolo codex: failed to exec native codex: {err}");
    std::process::exit(127);
}

fn codex_args_with_cwd(args: Vec<OsString>, cwd: &str) -> Vec<OsString> {
    if has_explicit_codex_cwd_arg(&args) {
        return args;
    }

    let mut out = Vec::with_capacity(args.len() + 2);
    out.push(OsString::from("--cd"));
    out.push(OsString::from(cwd));
    out.extend(args);
    out
}

fn has_explicit_codex_cwd_arg(args: &[OsString]) -> bool {
    args.iter().any(|arg| {
        let Some(arg) = arg.to_str() else {
            return false;
        };
        arg == "--cd" || arg == "-C" || arg.starts_with("--cd=")
    })
}

fn effective_codex_cwd(args: &[OsString], launch_cwd: &str) -> String {
    let Some(raw) = explicit_codex_cwd_arg(args) else {
        return launch_cwd.to_string();
    };
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        path.display().to_string()
    } else {
        PathBuf::from(launch_cwd).join(path).display().to_string()
    }
}

fn explicit_codex_cwd_arg(args: &[OsString]) -> Option<String> {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        let Some(arg) = arg.to_str() else {
            continue;
        };
        if arg == "--cd" || arg == "-C" {
            return iter
                .next()
                .and_then(|value| value.to_str())
                .map(ToString::to_string);
        }
        if let Some(value) = arg.strip_prefix("--cd=") {
            return Some(value.to_string());
        }
    }
    None
}

fn ensure_codex_project_trusted(cwd: &str) {
    if let Err(err) = ensure_codex_project_trusted_inner(cwd) {
        eprintln!("yolo: failed to persist Codex trusted project for {cwd}: {err}");
    }
}

fn ensure_codex_project_trusted_inner(cwd: &str) -> Result<(), String> {
    let path = codex_config_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let input = fs::read_to_string(&path).unwrap_or_default();
    let output = trusted_project_config(&input, cwd);
    if output != input {
        fs::write(&path, output).map_err(|err| err.to_string())?;
    }
    Ok(())
}

fn trusted_project_config(input: &str, cwd: &str) -> String {
    let header = format!("[projects.\"{}\"]", toml_basic_string_escape(cwd));
    let mut lines = input
        .split_inclusive('\n')
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let mut section_start = None;
    for (idx, line) in lines.iter().enumerate() {
        if line.trim() == header {
            section_start = Some(idx);
            break;
        }
    }

    let Some(start) = section_start else {
        let mut output = input.to_string();
        if !output.is_empty() && !output.ends_with('\n') {
            output.push('\n');
        }
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(&header);
        output.push('\n');
        output.push_str("trust_level = \"trusted\"\n");
        return output;
    };

    let section_end = lines
        .iter()
        .enumerate()
        .skip(start + 1)
        .find_map(|(idx, line)| {
            let trimmed = line.trim_start();
            (trimmed.starts_with('[') && !trimmed.starts_with("[[")).then_some(idx)
        })
        .unwrap_or(lines.len());

    for idx in start + 1..section_end {
        let trimmed = lines[idx].trim_start();
        if trimmed.starts_with("trust_level") {
            let newline = if lines[idx].ends_with('\n') { "\n" } else { "" };
            lines[idx] = format!("trust_level = \"trusted\"{newline}");
            return lines.concat();
        }
    }

    lines.insert(start + 1, "trust_level = \"trusted\"\n".to_string());
    lines.concat()
}

fn toml_basic_string_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[derive(Debug, PartialEq, Eq)]
enum ResumeTarget {
    Last,
    Thread(String),
}

#[derive(Debug)]
struct SessionCandidate {
    path: PathBuf,
    modified: SystemTime,
    id: String,
    cwd: Option<String>,
}

fn resolve_resume_last_args(args: &[OsString], cwd: &str) -> Result<Vec<OsString>, String> {
    if resume_target_from_args(args) != Some(ResumeTarget::Last) {
        return Ok(args.to_vec());
    }
    let Some(candidate) = latest_resume_candidate_for_cwd(cwd) else {
        return Err(format!(
            "refusing resume --last for {cwd}: no non-running Codex session with matching cwd"
        ));
    };
    replace_resume_last_with_thread(args, &candidate.id)
}

fn resolve_resume_last_args_without_api(
    args: &[OsString],
    cwd: &str,
) -> Result<Vec<OsString>, String> {
    if resume_target_from_args(args) != Some(ResumeTarget::Last) {
        return Ok(args.to_vec());
    }
    let current_pid = std::process::id();
    let Some(candidate) =
        latest_resume_candidate_for_cwd_from(session_candidates(), cwd, |candidate| {
            running_duplicate_thread_process(&candidate.id, current_pid).is_some()
        })
    else {
        return Err(format!(
            "refusing resume --last for {cwd}: no non-running Codex session with matching cwd"
        ));
    };
    replace_resume_last_with_thread(args, &candidate.id)
}

fn replace_resume_last_with_thread(
    args: &[OsString],
    thread_id: &str,
) -> Result<Vec<OsString>, String> {
    let mut out = Vec::with_capacity(args.len());
    let mut replaced = false;
    for (idx, arg) in args.iter().enumerate() {
        if !replaced && arg.to_str() == Some("--last") {
            out.push(OsString::from(thread_id));
            replaced = true;
        } else if !replaced && arg.to_str() == Some("resume") && idx + 1 == args.len() {
            out.push(arg.clone());
            out.push(OsString::from(thread_id));
            replaced = true;
        } else {
            out.push(arg.clone());
        }
    }
    if replaced {
        Ok(out)
    } else {
        Err("resume --last marker was not found".to_string())
    }
}

fn latest_resume_candidate_for_cwd(cwd: &str) -> Option<SessionCandidate> {
    latest_resume_candidate_for_cwd_from(session_candidates(), cwd, |candidate| {
        resume_candidate_unavailable(candidate, std::process::id())
    })
}

fn latest_resume_candidate_for_cwd_from<F>(
    candidates: Vec<SessionCandidate>,
    cwd: &str,
    mut is_unavailable: F,
) -> Option<SessionCandidate>
where
    F: FnMut(&SessionCandidate) -> bool,
{
    candidates
        .into_iter()
        .filter(|candidate| candidate.cwd.as_deref() == Some(cwd))
        .max_by_key(|candidate| candidate.modified)
        .filter(|candidate| !is_unavailable(candidate))
}

fn resume_candidate_unavailable(candidate: &SessionCandidate, current_pid: u32) -> bool {
    if running_duplicate_thread_client(&candidate.id, current_pid).is_some() {
        return true;
    }
    thread_was_updated_after_yolo_exit(&candidate.id, candidate.modified)
}

fn thread_was_updated_after_yolo_exit(thread_id: &str, modified: SystemTime) -> bool {
    let Some(modified_secs) = modified
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
    else {
        return false;
    };
    let Ok(value) = api_get_json("/clients") else {
        return false;
    };
    let Some(clients) = value.get("clients").and_then(Value::as_array) else {
        return false;
    };
    clients.iter().any(|client| {
        client.get("thread_id").and_then(Value::as_str) == Some(thread_id)
            && client.get("status").and_then(Value::as_str) == Some("exited")
            && client
                .get("ended_at")
                .and_then(Value::as_u64)
                .is_some_and(|ended_at| modified_secs > ended_at.saturating_add(5))
    })
}

fn session_candidates() -> Vec<SessionCandidate> {
    let mut paths = Vec::new();
    let Some(dir) = codex_sessions_dir() else {
        return Vec::new();
    };
    collect_session_paths(&dir, &mut paths);
    paths
        .into_iter()
        .filter_map(|path| {
            let modified = fs::metadata(&path).ok()?.modified().ok()?;
            let (id, cwd) = session_meta_from_path(&path);
            let id = id.or_else(|| session_id_from_filename(&path))?;
            Some(SessionCandidate {
                path,
                modified,
                id,
                cwd,
            })
        })
        .collect()
}

fn repair_resume_session_cwd(args: &[OsString], cwd: &str) {
    let Some(target) = resume_target_from_args(args) else {
        return;
    };
    if let Err(err) = repair_resume_target(&target, cwd) {
        eprintln!(
            "yolo: failed to repair Codex resume state for {:?}: {err}",
            target
        );
    }
}

fn reinforce_loaded_resume_permissions(socket: &Path, args: &[OsString], cwd: &str) {
    let Some(thread_id) = resume_thread_id(args) else {
        return;
    };
    if let Err(err) = update_app_server_resume_thread_settings(socket, &thread_id, cwd) {
        if !is_app_server_thread_not_found_error(&err, &thread_id) {
            eprintln!("yolo: failed to update loaded Codex thread settings for {thread_id}: {err}");
        }
    }
}

fn spawn_loaded_resume_permissions_reinforcer(socket: PathBuf, thread_id: String, cwd: String) {
    thread::spawn(move || {
        let start = Instant::now();
        loop {
            let err = match update_app_server_resume_thread_settings(&socket, &thread_id, &cwd) {
                Ok(()) => return,
                Err(err) => err,
            };
            if start.elapsed() >= RESUME_PERMISSIONS_REINFORCE_TIMEOUT {
                if is_app_server_thread_not_found_error(&err, &thread_id) {
                    eprintln!(
                        "yolo: Codex thread {thread_id} was not loaded before permissions reinforcement timed out"
                    );
                } else {
                    eprintln!(
                        "yolo: failed to reinforce loaded Codex thread settings for {thread_id}: {err}"
                    );
                }
                return;
            }
            thread::sleep(RESUME_PERMISSIONS_REINFORCE_INTERVAL);
        }
    });
}

fn is_app_server_thread_not_found_error(err: &str, thread_id: &str) -> bool {
    err.contains(&format!("thread not found: {thread_id}"))
}

fn resume_thread_id(args: &[OsString]) -> Option<String> {
    let target = resume_target_from_args(args)?;
    match target {
        ResumeTarget::Thread(thread_id) => Some(thread_id),
        ResumeTarget::Last => session_path_for_resume_target(&ResumeTarget::Last)
            .as_deref()
            .and_then(session_id_from_path),
    }
}

fn resume_target_from_args(args: &[OsString]) -> Option<ResumeTarget> {
    let resume_idx = args
        .iter()
        .position(|arg| matches!(arg.to_str(), Some("resume")))?;
    for arg in args
        .iter()
        .skip(resume_idx + 1)
        .filter_map(|arg| arg.to_str())
    {
        if arg == "--last" {
            return Some(ResumeTarget::Last);
        }
        if !arg.starts_with('-') {
            return Some(ResumeTarget::Thread(arg.to_string()));
        }
    }
    Some(ResumeTarget::Last)
}

fn session_path_for_resume_target(target: &ResumeTarget) -> Option<PathBuf> {
    let paths = session_candidates();
    match target {
        ResumeTarget::Thread(thread_id) => paths
            .into_iter()
            .find(|candidate| {
                candidate.id == *thread_id
                    || candidate
                        .path
                        .file_name()
                        .is_some_and(|name| name.to_string_lossy().contains(thread_id))
            })
            .map(|candidate| candidate.path),
        ResumeTarget::Last => paths
            .into_iter()
            .max_by_key(|candidate| candidate.modified)
            .map(|candidate| candidate.path),
    }
}

fn repair_resume_thread_id(thread_id: &str, cwd: &str) -> Result<(), String> {
    repair_resume_target(&ResumeTarget::Thread(thread_id.to_string()), cwd)
}

fn spawn_resume_context_repair_watcher(thread_id: &str, cwd: &str) {
    let thread_id = thread_id.to_string();
    let cwd = cwd.to_string();
    thread::spawn(move || {
        let mut last_modified = None;
        let mut stable_checks = 0_u8;
        let started = Instant::now();
        while started.elapsed() < RESUME_CONTEXT_REPAIR_WATCH_TIMEOUT {
            let target = ResumeTarget::Thread(thread_id.clone());
            if let Some(path) = session_path_for_resume_target(&target) {
                let modified = fs::metadata(&path).and_then(|meta| meta.modified()).ok();
                if modified.is_some() && modified != last_modified {
                    match rewrite_session_meta_cwd(&path, &cwd) {
                        Ok(true) => stable_checks = 0,
                        Ok(false) => {
                            stable_checks = stable_checks.saturating_add(1);
                            if stable_checks >= 2 {
                                return;
                            }
                        }
                        Err(err) => {
                            eprintln!(
                                "yolo: failed to repair Codex rollout context for {}: {err}",
                                path.display()
                            );
                        }
                    }
                    last_modified = fs::metadata(&path)
                        .and_then(|meta| meta.modified())
                        .ok()
                        .or(modified);
                }
            }
            thread::sleep(RESUME_CONTEXT_REPAIR_WATCH_INTERVAL);
        }
    });
}

fn repair_resume_target(target: &ResumeTarget, cwd: &str) -> Result<(), String> {
    let Some(path) = session_path_for_resume_target(target) else {
        return Ok(());
    };
    let thread_id = match target {
        ResumeTarget::Thread(thread_id) => Some(thread_id.clone()),
        ResumeTarget::Last => session_id_from_path(&path),
    };
    rewrite_session_meta_cwd(&path, cwd).map_err(|err| format!("{}: {err}", path.display()))?;
    if let Some(thread_id) = thread_id {
        rewrite_state_thread_cwd(&thread_id, cwd)?;
    }
    Ok(())
}

fn codex_sessions_dir() -> Option<PathBuf> {
    if let Some(codex_home) = env::var_os("CODEX_HOME") {
        return Some(PathBuf::from(codex_home).join("sessions"));
    }
    env::var_os("HOME").map(|home| PathBuf::from(home).join(".codex").join("sessions"))
}

fn collect_session_paths(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_session_paths(&path, out);
        } else if path.extension().and_then(|value| value.to_str()) == Some("jsonl") {
            out.push(path);
        }
    }
}

fn session_id_from_path(path: &Path) -> Option<String> {
    session_meta_from_path(path)
        .0
        .or_else(|| session_id_from_filename(path))
}

fn session_meta_from_path(path: &Path) -> (Option<String>, Option<String>) {
    let mut id = None;
    let mut cwd = None;
    if let Ok(input) = fs::read_to_string(path) {
        for line in input.lines().take(20) {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if value.get("type").and_then(Value::as_str) != Some("session_meta") {
                continue;
            }
            if id.is_none() {
                id = value
                    .get("payload")
                    .and_then(|payload| payload.get("id"))
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
            }
            if cwd.is_none() {
                cwd = value
                    .get("payload")
                    .and_then(|payload| payload.get("cwd"))
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
            }
            if id.is_some() && cwd.is_some() {
                break;
            }
        }
    }
    (id, cwd)
}

fn session_id_from_filename(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_string_lossy();
    let marker = "rollout-";
    let start = name.find(marker)?;
    let suffix = &name[start + marker.len()..];
    let id_start = suffix.find("019e")?;
    let candidate = suffix[id_start..].trim_end_matches(".jsonl");
    (!candidate.is_empty()).then(|| candidate.to_string())
}

fn rewrite_state_thread_cwd(thread_id: &str, cwd: &str) -> Result<(), String> {
    let Some(dir) = codex_home_dir() else {
        return Ok(());
    };
    let mut updated = false;
    for db in codex_state_db_paths(&dir) {
        let sql = format!(
            "UPDATE threads SET cwd = {cwd}, sandbox_policy = {sandbox}, approval_mode = 'never' WHERE id = {thread_id};",
            cwd = sqlite_quote(cwd),
            sandbox = sqlite_quote(YOLO_SANDBOX_POLICY_JSON),
            thread_id = sqlite_quote(thread_id)
        );
        let status = Command::new("sqlite3")
            .arg(&db)
            .arg(sql)
            .status()
            .map_err(|err| err.to_string())?;
        if !status.success() {
            return Err(format_exit_status(
                &format!("sqlite3 {}", db.display()),
                status,
            ));
        }
        updated = true;
    }
    let _ = updated;
    Ok(())
}

fn codex_home_dir() -> Option<PathBuf> {
    if let Some(codex_home) = env::var_os("CODEX_HOME") {
        return Some(PathBuf::from(codex_home));
    }
    env::var_os("HOME").map(|home| PathBuf::from(home).join(".codex"))
}

fn codex_state_db_paths(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                return false;
            };
            name.starts_with("state_") && name.ends_with(".sqlite")
        })
        .collect()
}

const YOLO_SANDBOX_POLICY_JSON: &str = r#"{"type":"disabled"}"#;
const YOLO_APP_SERVER_SANDBOX_POLICY: &str = "dangerFullAccess";
const YOLO_PERMISSIONS_INSTRUCTIONS: &str = r#"<permissions instructions>
Filesystem sandboxing defines which files can be read or written. `sandbox_mode` is `danger-full-access`: No filesystem sandboxing - all commands are permitted. Network access is enabled.
Approval policy is currently never. Do not provide the `sandbox_permissions` for any reason, commands will be rejected.
</permissions instructions>"#;

fn sqlite_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn rewrite_session_meta_cwd(path: &Path, cwd: &str) -> Result<bool, String> {
    let input = fs::read_to_string(path).map_err(|err| err.to_string())?;
    let mut changed = false;
    let mut output = String::with_capacity(input.len());
    for line in input.split_inclusive('\n') {
        let has_newline = line.ends_with('\n');
        let raw = line.trim_end_matches('\n');
        if raw.contains("\"session_meta\"") || raw.contains("\"turn_context\"") {
            if let Ok(mut value) = serde_json::from_str::<Value>(raw)
                && matches!(
                    value.get("type").and_then(Value::as_str),
                    Some("session_meta" | "turn_context")
                )
            {
                let is_turn_context =
                    value.get("type").and_then(Value::as_str) == Some("turn_context");
                if let Some(payload) = value.get_mut("payload").and_then(Value::as_object_mut) {
                    if payload.get("cwd").and_then(Value::as_str) != Some(cwd) {
                        payload.insert("cwd".to_string(), Value::String(cwd.to_string()));
                        changed = true;
                    }
                    if is_turn_context {
                        let workspace_roots = json!([cwd]);
                        if payload.get("workspace_roots") != Some(&workspace_roots) {
                            payload.insert("workspace_roots".to_string(), workspace_roots);
                            changed = true;
                        }
                        let sandbox = json!({"type": "danger-full-access"});
                        if payload.get("sandbox_policy") != Some(&sandbox) {
                            payload.insert("sandbox_policy".to_string(), sandbox);
                            changed = true;
                        }
                        if payload.get("approval_policy").and_then(Value::as_str) != Some("never") {
                            payload.insert(
                                "approval_policy".to_string(),
                                Value::String("never".to_string()),
                            );
                            changed = true;
                        }
                        let permission_profile = json!({"type": "disabled"});
                        if payload.get("permission_profile") != Some(&permission_profile) {
                            payload.insert("permission_profile".to_string(), permission_profile);
                            changed = true;
                        }
                    }
                    output.push_str(&serde_json::to_string(&value).map_err(|err| err.to_string())?);
                    if has_newline {
                        output.push('\n');
                    }
                    continue;
                }
            }
        }
        if raw.contains("<permissions instructions>")
            || raw.contains("<environment_context>")
            || raw.contains("sandbox_mode")
        {
            if let Ok(mut value) = serde_json::from_str::<Value>(raw)
                && value.get("type").and_then(Value::as_str) == Some("response_item")
                && repair_resume_context_message(&mut value, cwd)
            {
                changed = true;
                output.push_str(&serde_json::to_string(&value).map_err(|err| err.to_string())?);
                if has_newline {
                    output.push('\n');
                }
                continue;
            }
        }
        output.push_str(raw);
        if has_newline {
            output.push('\n');
        }
    }
    if changed {
        fs::write(path, output).map_err(|err| err.to_string())?;
    }
    Ok(changed)
}

fn repair_resume_context_message(value: &mut Value, cwd: &str) -> bool {
    let Some(payload) = value.get_mut("payload").and_then(Value::as_object_mut) else {
        return false;
    };
    if payload.get("type").and_then(Value::as_str) != Some("message") {
        return false;
    }
    let role = payload
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let Some(content) = payload.get_mut("content").and_then(Value::as_array_mut) else {
        return false;
    };
    let mut changed = false;
    for item in content {
        let Some(item) = item.as_object_mut() else {
            continue;
        };
        if item.get("type").and_then(Value::as_str) != Some("input_text") {
            continue;
        }
        let Some(text) = item.get("text").and_then(Value::as_str) else {
            continue;
        };
        if role == "developer"
            && text.contains("<permissions instructions>")
            && (text.contains("sandbox_mode") || text.contains("Approval policy"))
            && text != YOLO_PERMISSIONS_INSTRUCTIONS
        {
            item.insert(
                "text".to_string(),
                Value::String(YOLO_PERMISSIONS_INSTRUCTIONS.to_string()),
            );
            changed = true;
            continue;
        }
        if role == "user" && text.contains("<environment_context>") && text.contains("<filesystem>")
        {
            let replacement = yolo_environment_context(cwd);
            if text != replacement {
                item.insert("text".to_string(), Value::String(replacement));
                changed = true;
            }
        }
    }
    changed
}

fn yolo_environment_context(cwd: &str) -> String {
    format!(
        r#"<environment_context>
  <cwd>{cwd}</cwd>
  <filesystem><workspace_roots><root>{cwd}</root></workspace_roots><permission_profile type="disabled"><file_system type="unrestricted" /></permission_profile></filesystem>
</environment_context>"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn os_args(args: &[&str]) -> Vec<OsString> {
        args.iter().map(OsString::from).collect()
    }

    fn string_args(args: Vec<OsString>) -> Vec<String> {
        args.into_iter()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect()
    }

    fn test_client(id: &str, args: &[&str], cwd: &str, thread_id: Option<&str>) -> ClientInfo {
        ClientInfo {
            id: id.to_string(),
            yolo_pid: 1,
            codex_pid: None,
            cwd: cwd.to_string(),
            args: args.iter().map(|arg| arg.to_string()).collect(),
            remote: String::new(),
            model: None,
            service_tier: None,
            reasoning_effort: None,
            fast: false,
            thread_id: thread_id.map(ToString::to_string),
            thread_id_source: if thread_id.is_some() && args.iter().any(|arg| *arg == "resume") {
                "resume_arg".to_string()
            } else {
                "unresolved".to_string()
            },
            started_at: 1,
            updated_at: 1,
            ended_at: None,
            exit_code: None,
            status: "running".to_string(),
            codex_status: thread_id.map(|_| "active".to_string()),
            codex_active_flags: Vec::new(),
            codex_status_updated_at: thread_id.map(|_| 1),
            settings_updated_at: None,
        }
    }

    fn test_state(clients: Vec<ClientInfo>) -> ServerState {
        ServerState {
            started_at: 1,
            app_server_pid: None,
            app_server_generation: 0,
            resume_generation: 0,
            clients: clients
                .into_iter()
                .map(|client| (client.id.clone(), client))
                .collect(),
            slaves: BTreeMap::new(),
            telemetry: AgentTelemetry::default(),
            federation_push_senders: BTreeMap::new(),
            status_event_senders: BTreeMap::new(),
            next_status_event_id: 0,
        }
    }

    #[test]
    fn telemetry_counts_direct_and_nested_subagents() {
        let mut telemetry = AgentTelemetry::default();
        telemetry.record_thread_value(&json!({
            "id": "root",
            "sessionId": "session",
            "cwd": "/tmp/project",
            "status": {"type": "active"},
            "updatedAt": 100
        }));
        telemetry.record_thread_value(&json!({
            "id": "child",
            "source": {
                "subAgent": {
                    "thread_spawn": {
                        "parent_thread_id": "root",
                        "depth": 1
                    }
                }
            },
            "status": {"type": "active"},
            "updatedAt": 101
        }));
        telemetry.record_thread_value(&json!({
            "id": "grandchild",
            "parentThreadId": "child",
            "status": {"type": "idle"},
            "updatedAt": 102
        }));

        let snapshot = telemetry.snapshot();
        let root = snapshot
            .agents
            .iter()
            .find(|agent| agent.thread_id == "root")
            .unwrap();
        assert_eq!(root.subagent_count, 1);
        assert_eq!(root.active_subagent_count, 1);
        assert_eq!(root.descendant_count, 2);
        assert_eq!(root.active_descendant_count, 1);
        assert_eq!(snapshot.summary.subagent_count, 2);
    }

    #[test]
    fn telemetry_tracks_tool_pre_post_and_hook_lifecycle() {
        let mut telemetry = AgentTelemetry::default();
        let tool_item = json!({
            "type": "commandExecution",
            "id": "item-1",
            "status": "inProgress"
        });
        telemetry.record_app_server_event(&json!({
            "method": "item/started",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "startedAtMs": 1000,
                "item": tool_item
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/completed",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "completedAtMs": 2500,
                "item": {
                    "type": "commandExecution",
                    "id": "item-1",
                    "status": "completed",
                    "exitCode": 0,
                    "durationMs": 1500
                }
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "hook/started",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "run": {
                    "id": "hook-1",
                    "eventName": "preToolUse",
                    "handlerType": "command",
                    "scope": "turn",
                    "status": "running",
                    "startedAt": 1000
                }
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "hook/completed",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "run": {
                    "id": "hook-1",
                    "eventName": "preToolUse",
                    "handlerType": "command",
                    "scope": "turn",
                    "status": "completed",
                    "startedAt": 1000,
                    "completedAt": 1001,
                    "durationMs": 1
                }
            }
        }));

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.tool_calls.len(), 1);
        assert_eq!(snapshot.tool_calls[0].phase, "post");
        assert_eq!(snapshot.tool_calls[0].success, Some(true));
        assert_eq!(snapshot.tool_calls[0].duration_ms, Some(1500));
        assert_eq!(snapshot.hook_runs.len(), 1);
        assert_eq!(snapshot.hook_runs[0].phase, "pre");
        assert_eq!(snapshot.hook_runs[0].status, "completed");
        assert_eq!(snapshot.summary.active_tool_call_count, 0);
        assert_eq!(snapshot.summary.running_hook_count, 0);
    }

    #[test]
    fn telemetry_captures_turn_prompt_and_final_report() {
        let mut telemetry = AgentTelemetry::default();
        telemetry.record_turn_input("root", None, "Investigate the failing service");
        telemetry.record_app_server_event(&json!({
            "method": "turn/started",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "startedAtMs": 1000
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/completed",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "item": {
                    "type": "userMessage",
                    "id": "user-1",
                    "content": [{"type": "text", "text": "Investigate the failing service"}]
                }
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/completed",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "item": {
                    "type": "agentMessage",
                    "id": "assistant-1",
                    "phase": "final_answer",
                    "text": "The service was restored and verified."
                }
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "turn/completed",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "completedAtMs": 2500
            }
        }));

        let snapshot = telemetry.turns_snapshot(Some("root"), 10);
        assert_eq!(snapshot.turns.len(), 1);
        let turn = &snapshot.turns[0];
        assert_eq!(turn.turn_id, "turn-1");
        assert_eq!(turn.status, "completed");
        assert_eq!(
            turn.prompt.as_deref(),
            Some("Investigate the failing service")
        );
        assert_eq!(
            turn.report.as_deref(),
            Some("The service was restored and verified.")
        );
        assert_eq!(telemetry.summary().captured_prompt_count, 1);
        assert_eq!(telemetry.summary().captured_report_count, 1);
    }

    #[test]
    fn thread_history_extracts_user_prompt_and_final_answer() {
        let history = json!({
            "id": "root",
            "turns": [{
                "id": "turn-1",
                "startedAt": 100,
                "completedAt": 110,
                "status": "completed",
                "items": [
                    {
                        "type": "userMessage",
                        "content": [{"type": "text", "text": "What changed?"}]
                    },
                    {"type": "agentMessage", "phase": "commentary", "text": "I will inspect it."},
                    {"type": "agentMessage", "phase": "final_answer", "text": "The change is complete."}
                ]
            }]
        });

        let turns = parse_thread_history(&history, 10);
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].prompt.as_deref(), Some("What changed?"));
        assert_eq!(turns[0].report.as_deref(), Some("The change is complete."));
        assert_eq!(turns[0].started_at_ms, Some(100_000));
        assert_eq!(turns[0].completed_at_ms, Some(110_000));
    }

    #[test]
    fn proxy_turn_start_request_captures_input_for_server() {
        let (event_tx, event_rx) = mpsc::channel();
        let tracker = Arc::new(Mutex::new(ThreadBindingTracker {
            pending_create_request_ids: BTreeSet::new(),
            current_thread_id: None,
            event_tx,
        }));

        observe_client_app_server_request(
            &tracker,
            &json!({
                "id": 9,
                "method": "turn/start",
                "params": {
                    "threadId": "root",
                    "input": [{"type": "text", "text": "Run the requested check"}]
                }
            }),
        );

        let first = event_rx.recv().unwrap();
        let second = event_rx.recv().unwrap();
        assert!(matches!(first, ClientEvent::ThreadBound(thread_id) if thread_id == "root"));
        assert!(matches!(
            second,
            ClientEvent::TurnInput { thread_id, prompt, .. }
                if thread_id == "root" && prompt == "Run the requested check"
        ));
    }

    #[test]
    fn pending_client_settings_override_the_first_turn_start() {
        let path = env::temp_dir().join(format!(
            "yolo-pending-settings-test-{}-{}.json",
            std::process::id(),
            now_millis()
        ));
        fs::write(
            &path,
            serde_json::to_vec(&PendingClientSettings {
                model: Some("gpt-5.6-luna".to_string()),
                fast: Some(false),
                reasoning_effort: Some("xhigh".to_string()),
            })
            .unwrap(),
        )
        .unwrap();
        let mut request = json!({
            "id": "first-turn",
            "method": "turn/start",
            "params": {
                "threadId": "new-thread",
                "model": "gpt-old",
                "effort": "high",
                "serviceTier": "priority"
            }
        });

        let applied = apply_pending_settings_to_turn_start(&mut request, &path).unwrap();
        assert_eq!(applied.model.as_deref(), Some("gpt-5.6-luna"));
        assert_eq!(request["params"]["model"], "gpt-5.6-luna");
        assert_eq!(request["params"]["effort"], "xhigh");
        assert_eq!(request["params"]["serviceTier"], "default");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn client_id_configuration_does_not_select_other_unresolved_clients() {
        let state = Arc::new(Mutex::new(test_state(vec![
            test_client("selected", &[], "/home/vagrant/head", None),
            test_client("other", &[], "/home/vagrant/moon", None),
        ])));
        let request = ConfigureClientsRequest {
            client_id: Some("selected".to_string()),
            model: Some("gpt-5.6-luna".to_string()),
            ..ConfigureClientsRequest::default()
        };

        let selected = select_configure_clients(&state, &request).unwrap();
        assert_eq!(selected, BTreeSet::from(["selected".to_string()]));
    }

    #[test]
    fn federation_websocket_handshake_uses_rfc_accept_key() {
        let mut headers = BTreeMap::new();
        headers.insert(
            "sec-websocket-key".to_string(),
            "dGhlIHNhbXBsZSBub25jZQ==".to_string(),
        );
        let response = websocket_upgrade_response(&headers).unwrap();
        assert!(response.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
        assert!(response.starts_with("HTTP/1.1 101 Switching Protocols"));
    }

    #[test]
    fn federation_master_socket_parses_http_authority() {
        assert_eq!(
            federation_master_socket("http://kagura-sandbox:47040/api").unwrap(),
            ("kagura-sandbox".to_string(), 47040)
        );
        assert_eq!(
            federation_master_socket("http://127.0.0.1").unwrap(),
            ("127.0.0.1".to_string(), 80)
        );
    }

    #[test]
    fn codex_args_with_cwd_injects_launch_cwd() {
        let args = codex_args_with_cwd(os_args(&["resume", "--last"]), "/home/vagrant/head");
        assert_eq!(
            string_args(args),
            vec!["--cd", "/home/vagrant/head", "resume", "--last"]
        );
    }

    #[test]
    fn codex_args_with_cwd_keeps_explicit_cd() {
        let args = codex_args_with_cwd(os_args(&["--cd", "/tmp", "resume", "--last"]), "/home");
        assert_eq!(string_args(args), vec!["--cd", "/tmp", "resume", "--last"]);

        let args = codex_args_with_cwd(os_args(&["--cd=/tmp", "resume", "--last"]), "/home");
        assert_eq!(string_args(args), vec!["--cd=/tmp", "resume", "--last"]);

        let args = codex_args_with_cwd(os_args(&["-C", "/tmp", "resume", "--last"]), "/home");
        assert_eq!(string_args(args), vec!["-C", "/tmp", "resume", "--last"]);
    }

    #[test]
    fn effective_codex_cwd_uses_explicit_cd() {
        assert_eq!(
            effective_codex_cwd(&os_args(&["resume", "--last"]), "/home/vagrant/head"),
            "/home/vagrant/head"
        );
        assert_eq!(
            effective_codex_cwd(&os_args(&["--cd", "/tmp", "resume"]), "/home/vagrant/head"),
            "/tmp"
        );
        assert_eq!(
            effective_codex_cwd(&os_args(&["--cd=child", "resume"]), "/home/vagrant/head"),
            "/home/vagrant/head/child"
        );
    }

    #[test]
    fn trusted_project_config_adds_or_updates_project() {
        let input = "[projects.\"/home/vagrant/websh\"]\ntrust_level = \"trusted\"\n";
        let output = trusted_project_config(input, "/home/vagrant/head");
        assert!(output.contains("[projects.\"/home/vagrant/websh\"]"));
        assert!(output.contains("[projects.\"/home/vagrant/head\"]\ntrust_level = \"trusted\""));

        let input = "[projects.\"/home/vagrant/head\"]\ntrust_level = \"untrusted\"\n";
        let output = trusted_project_config(input, "/home/vagrant/head");
        assert_eq!(
            output,
            "[projects.\"/home/vagrant/head\"]\ntrust_level = \"trusted\"\n"
        );

        let input = "[projects.\"/home/vagrant/head\"]\nfoo = \"bar\"\n";
        let output = trusted_project_config(input, "/home/vagrant/head");
        assert_eq!(
            output,
            "[projects.\"/home/vagrant/head\"]\ntrust_level = \"trusted\"\nfoo = \"bar\"\n"
        );
    }

    #[test]
    fn resume_target_from_args_detects_thread_and_last() {
        assert_eq!(
            resume_target_from_args(&os_args(&["resume", "019e-test"])),
            Some(ResumeTarget::Thread("019e-test".to_string()))
        );
        assert_eq!(
            resume_target_from_args(&os_args(&["resume", "--last"])),
            Some(ResumeTarget::Last)
        );
        assert_eq!(
            resume_target_from_args(&os_args(&["resume"])),
            Some(ResumeTarget::Last)
        );
        assert_eq!(resume_target_from_args(&os_args(&["hello"])), None);
    }

    #[test]
    fn replace_resume_last_with_thread_keeps_other_args() {
        let args = replace_resume_last_with_thread(
            &os_args(&["--model", "gpt-5.5", "resume", "--last"]),
            "019e-thread",
        )
        .unwrap();
        assert_eq!(
            string_args(args),
            vec!["--model", "gpt-5.5", "resume", "019e-thread"]
        );

        let args = replace_resume_last_with_thread(&os_args(&["resume"]), "019e-thread").unwrap();
        assert_eq!(string_args(args), vec!["resume", "019e-thread"]);
    }

    #[test]
    fn resume_args_for_keeps_explicit_thread() {
        let args = resume_args_for(&os_args(&["resume", "019e-thread"]), Some("other-thread"));
        assert_eq!(string_args(args), vec!["resume", "019e-thread"]);
    }

    #[test]
    fn resume_args_for_uses_preferred_thread_for_plain_yolo() {
        let args = resume_args_for(&os_args(&[]), Some("019e-thread"));
        assert_eq!(string_args(args), vec!["resume", "019e-thread"]);
    }

    #[test]
    fn resume_args_for_preserves_options_with_preferred_thread() {
        let args = resume_args_for(&os_args(&["--model", "gpt-5.5"]), Some("019e-thread"));
        assert_eq!(
            string_args(args),
            vec!["--model", "gpt-5.5", "resume", "019e-thread"]
        );
    }

    #[test]
    fn resume_args_for_replaces_last_with_preferred_thread() {
        let args = resume_args_for(
            &os_args(&["--model", "gpt-5.5", "resume", "--last"]),
            Some("019e-thread"),
        );
        assert_eq!(
            string_args(args),
            vec!["--model", "gpt-5.5", "resume", "019e-thread"]
        );
    }

    #[test]
    fn resume_args_for_falls_back_to_last_without_preferred_thread() {
        let args = resume_args_for(&os_args(&["--model", "gpt-5.5"]), None);
        assert_eq!(string_args(args), vec!["resume", "--last"]);
    }

    #[test]
    fn preserve_resume_settings_args_adds_client_launch_settings() {
        let settings = ClientResumeSettings {
            thread_id: Some("019e-thread".to_string()),
            model: Some("gpt-5.5".to_string()),
            service_tier: Some("default".to_string()),
            reasoning_effort: Some("medium".to_string()),
        };
        let args = preserve_resume_settings_args(os_args(&["resume", "019e-thread"]), &settings);
        assert_eq!(
            string_args(args),
            vec![
                "-c",
                "model=\"gpt-5.5\"",
                "-c",
                "service_tier=\"default\"",
                "-c",
                "model_reasoning_effort=\"medium\"",
                "resume",
                "019e-thread"
            ]
        );
    }

    #[test]
    fn preserve_resume_settings_args_keeps_explicit_launch_settings() {
        let settings = ClientResumeSettings {
            thread_id: Some("019e-thread".to_string()),
            model: Some("gpt-5.6".to_string()),
            service_tier: Some("priority".to_string()),
            reasoning_effort: Some("high".to_string()),
        };
        let args = preserve_resume_settings_args(
            os_args(&[
                "--model",
                "gpt-5.5",
                "-c",
                "service_tier=default",
                "-c",
                "model_reasoning_effort=medium",
                "resume",
                "019e-thread",
            ]),
            &settings,
        );
        assert_eq!(
            string_args(args),
            vec![
                "--model",
                "gpt-5.5",
                "-c",
                "service_tier=default",
                "-c",
                "model_reasoning_effort=medium",
                "resume",
                "019e-thread"
            ]
        );
    }

    #[test]
    fn latest_resume_candidate_for_cwd_ignores_other_cwd() {
        let older = UNIX_EPOCH + Duration::from_secs(10);
        let newer = UNIX_EPOCH + Duration::from_secs(20);
        let candidates = vec![
            SessionCandidate {
                path: PathBuf::from("/tmp/head.jsonl"),
                modified: newer,
                id: "head-thread".to_string(),
                cwd: Some("/home/vagrant/head".to_string()),
            },
            SessionCandidate {
                path: PathBuf::from("/tmp/websh-old.jsonl"),
                modified: older,
                id: "websh-old".to_string(),
                cwd: Some("/home/vagrant/websh".to_string()),
            },
            SessionCandidate {
                path: PathBuf::from("/tmp/websh-new.jsonl"),
                modified: newer,
                id: "websh-new".to_string(),
                cwd: Some("/home/vagrant/websh".to_string()),
            },
        ];
        let candidate =
            latest_resume_candidate_for_cwd_from(candidates, "/home/vagrant/websh", |_| false)
                .unwrap();
        assert_eq!(candidate.id, "websh-new");
    }

    #[test]
    fn latest_resume_candidate_for_cwd_refuses_running_latest() {
        let older = UNIX_EPOCH + Duration::from_secs(10);
        let newer = UNIX_EPOCH + Duration::from_secs(20);
        let candidates = vec![
            SessionCandidate {
                path: PathBuf::from("/tmp/websh-old.jsonl"),
                modified: older,
                id: "websh-old".to_string(),
                cwd: Some("/home/vagrant/websh".to_string()),
            },
            SessionCandidate {
                path: PathBuf::from("/tmp/websh-running.jsonl"),
                modified: newer,
                id: "websh-running".to_string(),
                cwd: Some("/home/vagrant/websh".to_string()),
            },
        ];
        assert!(
            latest_resume_candidate_for_cwd_from(candidates, "/home/vagrant/websh", |candidate| {
                candidate.id == "websh-running"
            })
            .is_none()
        );
    }

    #[test]
    fn session_meta_from_path_reads_id_and_cwd() {
        let path = env::temp_dir().join(format!(
            "yolo-session-meta-test-{}-{}.jsonl",
            std::process::id(),
            now_millis()
        ));
        fs::write(
            &path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"019e-meta\",\"cwd\":\"/home/vagrant/websh\"}}\n",
        )
        .unwrap();
        let (id, cwd) = session_meta_from_path(&path);
        assert_eq!(id.as_deref(), Some("019e-meta"));
        assert_eq!(cwd.as_deref(), Some("/home/vagrant/websh"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn process_thread_id_detects_codex_resume_arg() {
        let process = ProcInfo {
            pid: 1,
            ppid: 0,
            comm: "codex".to_string(),
            cmdline: vec![
                "codex".to_string(),
                "--remote".to_string(),
                "unix:///tmp/codex.sock".to_string(),
                "resume".to_string(),
                "019e-thread".to_string(),
            ],
            cwd: Some("/home/vagrant/websh".to_string()),
        };
        assert_eq!(process_thread_id(&process), Some("019e-thread"));
    }

    #[test]
    fn scanned_codex_args_preserve_managed_remote() {
        let remote = remote_from_codex_args(&[
            "codex".to_string(),
            "--remote".to_string(),
            "unix:///run/user/1000/yolo/client-proxies/client.sock".to_string(),
        ]);
        assert_eq!(
            remote,
            "unix:///run/user/1000/yolo/client-proxies/client.sock"
        );
        assert_eq!(
            client_id_from_managed_proxy_remote(&remote).as_deref(),
            Some("client")
        );
    }

    #[test]
    fn app_server_pid_detection_collapses_node_native_pair() {
        let socket = "/run/user/1000/yolo/app-server/codex-app-server.sock";
        let processes = vec![
            ProcInfo {
                pid: 10,
                ppid: 1,
                comm: "node".to_string(),
                cmdline: vec![
                    "node".to_string(),
                    "/home/vagrant/.local/share/yolo/codex-npm/bin/codex".to_string(),
                    "app-server".to_string(),
                    "--listen".to_string(),
                    format!("unix://{socket}"),
                ],
                cwd: None,
            },
            ProcInfo {
                pid: 11,
                ppid: 10,
                comm: "codex".to_string(),
                cmdline: vec![
                    "/vendor/codex".to_string(),
                    "app-server".to_string(),
                    "--listen".to_string(),
                    format!("unix://{socket}"),
                ],
                cwd: None,
            },
        ];
        assert_eq!(top_level_app_server_pids(&processes, socket), vec![10]);
    }

    #[test]
    fn app_server_pid_detection_keeps_real_duplicate_roots() {
        let socket = "/run/user/1000/yolo/app-server/codex-app-server.sock";
        let processes = vec![
            ProcInfo {
                pid: 10,
                ppid: 1,
                comm: "node".to_string(),
                cmdline: vec![
                    "node".to_string(),
                    "codex".to_string(),
                    "app-server".to_string(),
                    format!("unix://{socket}"),
                ],
                cwd: None,
            },
            ProcInfo {
                pid: 20,
                ppid: 1,
                comm: "node".to_string(),
                cmdline: vec![
                    "node".to_string(),
                    "codex".to_string(),
                    "app-server".to_string(),
                    format!("unix://{socket}"),
                ],
                cwd: None,
            },
        ];
        assert_eq!(top_level_app_server_pids(&processes, socket), vec![10, 20]);
    }

    #[test]
    fn next_self_heal_backoff_doubles_until_cap() {
        assert_eq!(
            next_self_heal_backoff(Duration::from_secs(2)),
            Duration::from_secs(4)
        );
        assert_eq!(
            next_self_heal_backoff(Duration::from_secs(45)),
            APP_SERVER_SELF_HEAL_MAX_BACKOFF
        );
        assert_eq!(
            next_self_heal_backoff(APP_SERVER_SELF_HEAL_MAX_BACKOFF),
            APP_SERVER_SELF_HEAL_MAX_BACKOFF
        );
    }

    #[test]
    fn clear_conflicting_inferred_thread_ids_keeps_explicit_owner() {
        let mut state = test_state(vec![
            test_client(
                "explicit",
                &["resume", "thread-a"],
                "/home/vagrant/head",
                Some("thread-a"),
            ),
            test_client("inferred", &[], "/home/vagrant/head", Some("thread-a")),
        ]);

        clear_conflicting_inferred_thread_ids(&mut state);

        assert_eq!(
            state.clients["explicit"].thread_id.as_deref(),
            Some("thread-a")
        );
        assert_eq!(state.clients["inferred"].thread_id, None);
        assert_eq!(state.clients["inferred"].codex_status, None);
    }

    #[test]
    fn clear_conflicting_inferred_thread_ids_repairs_wrong_explicit_client() {
        let mut state = test_state(vec![test_client(
            "explicit",
            &["resume", "thread-real"],
            "/home/vagrant/head",
            Some("thread-wrong"),
        )]);

        clear_conflicting_inferred_thread_ids(&mut state);

        assert_eq!(
            state.clients["explicit"].thread_id.as_deref(),
            Some("thread-real")
        );
        assert_eq!(state.clients["explicit"].codex_status, None);
    }

    #[test]
    fn clear_conflicting_inferred_thread_ids_clears_all_unverified_owners() {
        let mut state = test_state(vec![
            test_client("first", &[], "/home/vagrant/head", Some("thread-a")),
            test_client("second", &[], "/home/vagrant/head", Some("thread-a")),
            test_client("unique", &[], "/home/vagrant/websh", Some("thread-b")),
        ]);

        clear_conflicting_inferred_thread_ids(&mut state);

        assert_eq!(state.clients["first"].thread_id, None);
        assert_eq!(state.clients["second"].thread_id, None);
        assert_eq!(state.clients["unique"].thread_id, None);
    }

    #[test]
    fn unique_active_thread_rebinds_one_legacy_client_without_a_resume_arg() {
        let mut state = test_state(vec![test_client("legacy", &[], "/home/vagrant/head", None)]);
        let snapshot = vec![AppThreadSnapshot {
            id: "thread-active".to_string(),
            cwd: "/home/vagrant/head".to_string(),
            status: "active".to_string(),
            active_flags: Vec::new(),
            model: Some("gpt-5.6-sol".to_string()),
            service_tier: Some("default".to_string()),
            reasoning_effort: Some("low".to_string()),
        }];

        bind_unique_active_legacy_clients(&mut state, &snapshot);

        assert_eq!(
            state.clients["legacy"].thread_id.as_deref(),
            Some("thread-active")
        );
        assert_eq!(
            state.clients["legacy"].thread_id_source,
            "legacy_active_unique"
        );
    }

    #[test]
    fn thread_settings_override_stale_launch_settings_after_resume() {
        let state = Arc::new(Mutex::new(test_state(vec![test_client(
            "resumed",
            &[
                "-c",
                "model=\"gpt-5.6-luna\"",
                "-c",
                "service_tier=default",
                "-c",
                "model_reasoning_effort=xhigh",
                "resume",
                "thread-max",
            ],
            "/home/vagrant/head",
            Some("thread-max"),
        )])));
        apply_thread_snapshot(
            &state,
            &[AppThreadSnapshot {
                id: "thread-max".to_string(),
                cwd: "/home/vagrant/head".to_string(),
                status: "idle".to_string(),
                active_flags: Vec::new(),
                model: Some("gpt-5.6-luna".to_string()),
                service_tier: Some("default".to_string()),
                reasoning_effort: Some("max".to_string()),
            }],
        );

        let state = state.lock().unwrap();
        let client = &state.clients["resumed"];
        assert_eq!(client.model.as_deref(), Some("gpt-5.6-luna"));
        assert_eq!(client.service_tier.as_deref(), Some("default"));
        assert_eq!(client.reasoning_effort.as_deref(), Some("max"));
        assert!(!client.fast);
    }

    #[test]
    fn legacy_client_stays_unresolved_when_multiple_active_threads_match_its_cwd() {
        let mut state = test_state(vec![test_client("legacy", &[], "/home/vagrant/head", None)]);
        let snapshot = ["thread-a", "thread-b"]
            .into_iter()
            .map(|id| AppThreadSnapshot {
                id: id.to_string(),
                cwd: "/home/vagrant/head".to_string(),
                status: "active".to_string(),
                active_flags: Vec::new(),
                model: None,
                service_tier: None,
                reasoning_effort: None,
            })
            .collect::<Vec<_>>();

        bind_unique_active_legacy_clients(&mut state, &snapshot);

        assert_eq!(state.clients["legacy"].thread_id, None);
    }

    #[test]
    fn websocket_resume_request_binds_the_proxy_client_to_its_thread() {
        let (event_tx, event_rx) = mpsc::channel();
        let tracker = Arc::new(Mutex::new(ThreadBindingTracker {
            pending_create_request_ids: BTreeSet::new(),
            current_thread_id: None,
            event_tx,
        }));

        observe_client_app_server_request(
            &tracker,
            &json!({
                "id": 7,
                "method": "thread/resume",
                "params": { "threadId": "thread-resumed" }
            }),
        );

        assert!(matches!(
            event_rx.recv_timeout(Duration::from_millis(50)),
            Ok(ClientEvent::ThreadBound(thread_id)) if thread_id == "thread-resumed"
        ));
    }

    #[test]
    fn websocket_thread_start_response_binds_the_proxy_client_to_created_thread() {
        let (event_tx, event_rx) = mpsc::channel();
        let tracker = Arc::new(Mutex::new(ThreadBindingTracker {
            pending_create_request_ids: BTreeSet::new(),
            current_thread_id: None,
            event_tx,
        }));

        observe_client_app_server_request(
            &tracker,
            &json!({ "id": 8, "method": "thread/start", "params": {} }),
        );
        observe_app_server_response(
            &tracker,
            &json!({
                "id": 8,
                "result": { "thread": { "id": "thread-created" } }
            }),
        );

        assert!(matches!(
            event_rx.recv_timeout(Duration::from_millis(50)),
            Ok(ClientEvent::ThreadBound(thread_id)) if thread_id == "thread-created"
        ));
    }

    #[test]
    fn websocket_thread_started_notification_binds_the_proxy_client() {
        let (event_tx, event_rx) = mpsc::channel();
        let tracker = Arc::new(Mutex::new(ThreadBindingTracker {
            pending_create_request_ids: BTreeSet::new(),
            current_thread_id: None,
            event_tx,
        }));

        observe_app_server_response(
            &tracker,
            &json!({
                "method": "thread/started",
                "params": { "thread": { "id": "thread-notified" } }
            }),
        );

        assert!(matches!(
            event_rx.recv_timeout(Duration::from_millis(50)),
            Ok(ClientEvent::ThreadBound(thread_id)) if thread_id == "thread-notified"
        ));
    }

    #[test]
    fn websocket_string_request_id_binds_thread_start_response() {
        let (event_tx, event_rx) = mpsc::channel();
        let tracker = Arc::new(Mutex::new(ThreadBindingTracker {
            pending_create_request_ids: BTreeSet::new(),
            current_thread_id: None,
            event_tx,
        }));

        observe_client_app_server_request(
            &tracker,
            &json!({ "id": "create-9", "method": "thread/start", "params": {} }),
        );
        observe_app_server_response(
            &tracker,
            &json!({
                "id": "create-9",
                "result": { "thread": { "id": "thread-string-id" } }
            }),
        );

        assert!(matches!(
            event_rx.recv_timeout(Duration::from_millis(50)),
            Ok(ClientEvent::ThreadBound(thread_id)) if thread_id == "thread-string-id"
        ));
    }

    #[test]
    fn thread_started_binds_one_unresolved_managed_client_for_cwd() {
        let mut state = test_state(vec![test_client(
            "new-client",
            &[],
            "/home/vagrant/head",
            None,
        )]);
        state.clients.get_mut("new-client").unwrap().remote =
            "unix:///run/user/1000/yolo/app-server/codex-app-server.sock".to_string();
        let state = Arc::new(Mutex::new(state));

        bind_thread_started_to_unique_managed_client(
            &state,
            &json!({
                "method": "thread/started",
                "params": {
                    "thread": { "id": "thread-new", "cwd": "/home/vagrant/head" }
                }
            }),
        );

        let state = state.lock().unwrap();
        assert_eq!(
            state.clients["new-client"].thread_id.as_deref(),
            Some("thread-new")
        );
        assert_eq!(
            state.clients["new-client"].thread_id_source,
            "app_server_started"
        );
    }

    #[test]
    fn thread_started_does_not_guess_between_managed_clients() {
        let mut first = test_client("first", &[], "/home/vagrant/head", None);
        first.remote = "unix:///run/user/1000/yolo/app-server/codex-app-server.sock".to_string();
        let mut second = test_client("second", &[], "/home/vagrant/head", None);
        second.remote = "unix:///run/user/1000/yolo/app-server/codex-app-server.sock".to_string();
        let state = Arc::new(Mutex::new(test_state(vec![first, second])));

        bind_thread_started_to_unique_managed_client(
            &state,
            &json!({
                "method": "thread/started",
                "params": {
                    "thread": { "id": "thread-ambiguous", "cwd": "/home/vagrant/head" }
                }
            }),
        );

        let state = state.lock().unwrap();
        assert!(
            state
                .clients
                .values()
                .all(|client| client.thread_id.is_none())
        );
    }

    #[test]
    fn rewrite_session_meta_cwd_updates_turn_contexts() {
        let path = env::temp_dir().join(format!(
            "yolo-session-cwd-test-{}-{}.jsonl",
            std::process::id(),
            now_millis()
        ));
        fs::write(
            &path,
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"cwd\":\"/home/vagrant\"}}\n",
                "{\"type\":\"turn_context\",\"payload\":{\"cwd\":\"/home/vagrant\",\"workspace_roots\":[\"/home/vagrant/websh\"]}}\n",
                "{\"type\":\"turn_context\",\"payload\":{\"cwd\":\"/home/vagrant\"}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"cwd\":\"/home/vagrant\"}}\n"
            ),
        )
        .unwrap();

        rewrite_session_meta_cwd(&path, "/home/vagrant/websh").unwrap();
        let output = fs::read_to_string(&path).unwrap();
        let rows = output
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            rows[0]["payload"]["cwd"].as_str(),
            Some("/home/vagrant/websh")
        );
        assert_eq!(
            rows[1]["payload"]["cwd"].as_str(),
            Some("/home/vagrant/websh")
        );
        assert_eq!(
            rows[1]["payload"]["workspace_roots"],
            json!(["/home/vagrant/websh"])
        );
        assert_eq!(
            rows[2]["payload"]["cwd"].as_str(),
            Some("/home/vagrant/websh")
        );
        assert_eq!(
            rows[2]["payload"]["workspace_roots"],
            json!(["/home/vagrant/websh"])
        );
        assert_eq!(rows[3]["payload"]["cwd"].as_str(), Some("/home/vagrant"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn rewrite_session_meta_cwd_repairs_stale_read_only_context_messages() {
        let path = env::temp_dir().join(format!(
            "yolo-session-permission-test-{}-{}.jsonl",
            std::process::id(),
            now_millis()
        ));
        fs::write(
            &path,
            concat!(
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"developer\",\"content\":[{\"type\":\"input_text\",\"text\":\"<permissions instructions>\\nFilesystem sandboxing defines which files can be read or written. `sandbox_mode` is `read-only`: The sandbox only permits reading files.\\n# Escalation Requests\\nProvide the `sandbox_permissions` parameter with the value `require_escalated`.\\n</permissions instructions>\"}]}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"<environment_context>\\n  <filesystem><workspace_roots><root>/home/vagrant/head</root></workspace_roots><permission_profile type=\\\"managed\\\"><file_system type=\\\"restricted\\\"><entry access=\\\"read\\\"><special>:root</special></entry></file_system></permission_profile></filesystem>\\n</environment_context>\"}]}}\n"
            ),
        )
        .unwrap();

        rewrite_session_meta_cwd(&path, "/home/vagrant/head").unwrap();
        let output = fs::read_to_string(&path).unwrap();

        assert!(output.contains("sandbox_mode` is `danger-full-access`"));
        assert!(output.contains("Approval policy is currently never"));
        assert!(output.contains("permission_profile type=\\\"disabled\\\""));
        assert!(!output.contains("read-only"));
        assert!(!output.contains("require_escalated"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn session_id_from_path_reads_session_meta() {
        let path = env::temp_dir().join(format!(
            "rollout-2026-06-07T00-00-00-019etest-from-name.jsonl"
        ));
        fs::write(
            &path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"019etest-from-meta\",\"cwd\":\"/tmp\"}}\n",
        )
        .unwrap();

        assert_eq!(
            session_id_from_path(&path),
            Some("019etest-from-meta".to_string())
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn sqlite_quote_escapes_single_quotes() {
        assert_eq!(sqlite_quote("/tmp/it's"), "'/tmp/it''s'");
    }

    #[test]
    fn ensure_json_ok_rejects_explicit_false() {
        let value = json!({
            "ok": false,
            "error": "timed out waiting for selected Codex clients"
        });

        assert_eq!(
            ensure_json_ok(&value).unwrap_err(),
            "timed out waiting for selected Codex clients"
        );
        assert!(ensure_json_ok(&json!({"ok": true})).is_ok());
        assert!(ensure_json_ok(&json!({"clients": []})).is_ok());
    }

    #[test]
    fn slave_command_deserializes_configure_request() {
        let command = serde_json::from_value::<SlaveCommand>(json!({
            "id": "cmd-test",
            "action": "configure-clients",
            "configure": {
                "all": true,
                "model": "gpt-5.5",
                "reasoning_effort": "medium",
                "fast": false,
                "timeout_secs": 5
            }
        }))
        .unwrap();

        assert_eq!(command.action, "configure-clients");
        let configure = command.configure.unwrap();
        assert!(configure.all);
        assert_eq!(configure.model.as_deref(), Some("gpt-5.5"));
        assert_eq!(configure.reasoning_effort.as_deref(), Some("medium"));
        assert_eq!(configure.fast, Some(false));
        assert_eq!(configure.timeout_secs, Some(5));

        let turns_command = serde_json::from_value::<SlaveCommand>(json!({
            "id": "cmd-turns",
            "action": "turns",
            "thread_id": "thread-1",
            "limit": 3
        }))
        .unwrap();
        assert_eq!(turns_command.thread_id.as_deref(), Some("thread-1"));
        assert_eq!(turns_command.limit, Some(3));
    }

    #[test]
    fn codex_ui_status_parses_low_and_fast_footer_tokens() {
        let low = extract_codex_ui_status("gpt-5.6-sol low · Context 32% left").unwrap();
        assert_eq!(low.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(low.effort.as_deref(), Some("low"));
        assert_eq!(low.fast, Some(false));

        let fast = extract_codex_ui_status("gpt-5.5 xhigh fast · ~/repo").unwrap();
        assert_eq!(fast.effort.as_deref(), Some("xhigh"));
        assert_eq!(fast.fast, Some(true));
        assert!(extract_codex_ui_status("example: gpt-5.6-sol / medium / normal").is_none());
    }
}

fn wait_for_resume_generation_advance(seen_resume_generation: &AtomicU64) -> bool {
    let start = SystemTime::now();
    while start.elapsed().unwrap_or_default() < RESUME_GENERATION_GRACE {
        if resume_generation_advanced(seen_resume_generation) {
            eprintln!(
                "yolo: Codex child exited during app-server restart; resuming via Phoenix mode"
            );
            return true;
        }
        thread::sleep(Duration::from_millis(200));
    }
    false
}

fn should_reexec_after_codex_exit(
    success: bool,
    original_args: &[OsString],
    seen_resume_generation: &AtomicU64,
) -> bool {
    if resume_generation_advanced(seen_resume_generation) {
        return true;
    }
    if success {
        return false;
    }
    if wait_for_resume_generation_advance(seen_resume_generation) {
        return true;
    }
    if resume_target_from_args(original_args).is_some() && wait_for_app_server_reconnect() {
        eprintln!("yolo: Codex child lost the app-server transport; app-server is back, resuming");
        return true;
    }
    false
}

fn wait_for_app_server_reconnect() -> bool {
    let Ok(paths) = runtime_paths() else {
        return false;
    };
    let start = SystemTime::now();
    while start.elapsed().unwrap_or_default() < RESUME_GENERATION_GRACE {
        if paths.api_socket.exists()
            && paths.app_server_socket.exists()
            && api_get_json("/status").is_ok()
            && AppServerRpcClient::connect(&paths.app_server_socket).is_ok()
        {
            return true;
        }
        thread::sleep(Duration::from_millis(200));
    }
    false
}

fn resume_generation_advanced(seen_resume_generation: &AtomicU64) -> bool {
    let seen = seen_resume_generation.load(Ordering::SeqCst);
    if let Ok(value) = api_get_json("/status")
        && let Some(generation) = restart_generation_from_status(&value)
        && generation > seen
    {
        seen_resume_generation.store(generation, Ordering::SeqCst);
        return true;
    }
    false
}

fn running_duplicate_thread_client(thread_id: &str, current_pid: u32) -> Option<String> {
    if let Some(existing) = running_duplicate_thread_process(thread_id, current_pid) {
        return Some(existing);
    }
    let value = api_get_json("/clients").ok()?;
    let clients = value.get("clients")?.as_array()?;
    for client in clients {
        let client_thread_id = client.get("thread_id").and_then(Value::as_str);
        if client_thread_id != Some(thread_id) {
            continue;
        }
        if client.get("status").and_then(Value::as_str) != Some("running") {
            continue;
        }
        let yolo_pid = client.get("yolo_pid").and_then(Value::as_u64)? as u32;
        if yolo_pid == current_pid || !pid_is_alive(yolo_pid) {
            continue;
        }
        let id = client
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let cwd = client.get("cwd").and_then(Value::as_str).unwrap_or("");
        return Some(format!("{id} pid={yolo_pid} cwd={cwd}"));
    }
    None
}

fn running_duplicate_thread_process(thread_id: &str, current_pid: u32) -> Option<String> {
    let processes = read_process_table().ok()?;
    let current_children = child_pids_recursive(current_pid)
        .into_iter()
        .collect::<BTreeSet<_>>();
    for process in processes {
        if process.pid == current_pid || current_children.contains(&process.pid) {
            continue;
        }
        if process_thread_id(&process) != Some(thread_id) {
            continue;
        }
        let cwd = process.cwd.as_deref().unwrap_or_default();
        let label = if is_yolo_process(&process) {
            "yolo"
        } else {
            "codex"
        };
        return Some(format!(
            "{label} pid={} cwd={} args={}",
            process.pid,
            cwd,
            process.cmdline.join(" ")
        ));
    }
    None
}

fn process_thread_id(process: &ProcInfo) -> Option<&str> {
    process.cmdline.windows(2).find_map(|window| {
        let first = window[0].as_str();
        let second = window[1].as_str();
        (first == "resume" && !second.starts_with('-')).then_some(second)
    })
}

fn run_upgrade_resume(mut args: Vec<OsString>) {
    if args.is_empty() {
        args.push(OsString::from("--last"));
    }
    if let Err(err) = upgrade_codex_cli() {
        eprintln!("yolo upgrade-resume: {err}");
        std::process::exit(1);
    }
    if let Err(err) = restart_server_for_upgrade() {
        eprintln!("yolo upgrade-resume: failed to restart yolo server: {err}");
        std::process::exit(1);
    }
    let mut client_args = Vec::with_capacity(args.len() + 1);
    client_args.push(OsString::from("resume"));
    client_args.extend(args);
    run_client(client_args);
}

fn run_upgrade_resume_all() -> Result<(), String> {
    ensure_server()?;
    let mut request = serde_json::Map::new();
    if let Ok(thread_id) = env::var("CODEX_THREAD_ID")
        && !thread_id.trim().is_empty()
    {
        request.insert("ignore_thread_id".to_string(), Value::String(thread_id));
    } else if let Ok(cwd) = env::current_dir() {
        request.insert(
            "ignore_cwd".to_string(),
            Value::String(cwd.to_string_lossy().to_string()),
        );
    }
    let value = api_post_json("/upgrade-resume-all", &Value::Object(request))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|err| err.to_string())?
    );
    Ok(())
}

fn run_external_codex_upgrade_resume(args: Vec<OsString>) -> Result<(), String> {
    let mut codex_version: Option<String> = None;
    let mut include_busy = false;
    let mut update_system = false;
    let mut dry_run = false;
    let mut defer_busy = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        let arg = arg.to_string_lossy().to_string();
        match arg.as_str() {
            "--codex-version" | "--version" => {
                let value = iter
                    .next()
                    .ok_or_else(|| format!("{arg} requires a value"))?
                    .to_string_lossy()
                    .to_string();
                codex_version = Some(value);
            }
            "--include-busy" => include_busy = true,
            "--system" => update_system = true,
            "--dry-run" => dry_run = true,
            "--defer-busy" => defer_busy = true,
            _ => {
                return Err(format!(
                    "unknown external-codex-upgrade-resume argument: {arg}"
                ));
            }
        }
    }
    let package = codex_version
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("@openai/codex@{}", value.trim()))
        .unwrap_or_else(|| CODEX_PACKAGE.to_string());
    let script = r###"
import json, os, re, shlex, subprocess, sys
from pathlib import Path

package = os.environ["YOLO_EXTERNAL_CODEX_PACKAGE"]
include_busy = os.environ.get("YOLO_EXTERNAL_INCLUDE_BUSY") == "1"
defer_busy = os.environ.get("YOLO_EXTERNAL_DEFER_BUSY") == "1"
update_system = os.environ.get("YOLO_EXTERNAL_UPDATE_SYSTEM") == "1"
dry_run = os.environ.get("YOLO_EXTERNAL_DRY_RUN") == "1"
home = Path.home()

def run(cmd, *, check=True):
    print("+", " ".join(shlex.quote(str(x)) for x in cmd), flush=True)
    if dry_run:
        return subprocess.CompletedProcess(cmd, 0, "", "")
    return subprocess.run(cmd, check=check, text=True)

run(["npm", "install", "--global", "--prefix", str(home / ".npm-global"), package])
if update_system:
    run(["sudo", "npm", "install", "--global", "--prefix", "/usr/local", package])

def output(cmd):
    return subprocess.run(cmd, text=True, capture_output=True, check=False).stdout

def session_ids_by_cwd():
    found = {}
    root = home / ".codex" / "sessions"
    if not root.exists():
        return found
    rows = []
    for path in root.rglob("*.jsonl"):
        try:
            stat = path.stat()
        except OSError:
            continue
        match = re.search(r"(019e[0-9a-fA-F-]+)", path.name)
        if not match:
            continue
        cwd_value = None
        try:
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                head = "".join([next(handle, "") for _ in range(120)])
        except OSError:
            continue
        for pattern in [r'"cwd"\s*:\s*"([^"]+)"', r'"workdir"\s*:\s*"([^"]+)"', r'"directory"\s*:\s*"([^"]+)"']:
            cwd_match = re.search(pattern, head)
            if cwd_match:
                cwd_value = cwd_match.group(1)
                break
        if cwd_value:
            rows.append((stat.st_mtime, cwd_value, match.group(1)))
    by_cwd = {}
    for mtime, cwd_value, thread_id in sorted(rows, reverse=True):
        by_cwd.setdefault(cwd_value, []).append(thread_id)
    for cwd_value, thread_ids in by_cwd.items():
        uniq = []
        for thread_id in thread_ids:
            if thread_id not in uniq:
                uniq.append(thread_id)
        if len(uniq) == 1:
            found[cwd_value] = uniq[0]
    return found

pane_raw = output([
    "tmux", "list-panes", "-a", "-F",
    "#{session_name}:#{window_index}.#{pane_index}\t#{session_name}\t#{window_name}\t#{pane_id}\t#{pane_pid}\t#{pane_current_command}\t#{pane_tty}\t#{pane_current_path}"
])
targets = []
deferred = []
cwd_thread_ids = session_ids_by_cwd()
for line in pane_raw.splitlines():
    parts = line.split("\t")
    if len(parts) != 8:
        continue
    key, session_name, window_name, pane_id, pane_pid, pane_cmd, pane_tty, cwd = parts
    if not pane_tty:
        continue
    tty_name = pane_tty[5:] if pane_tty.startswith("/dev/") else pane_tty
    ps = output(["ps", "-t", tty_name, "-o", "pid=,ppid=,comm=,args="])
    if re.search(r"\byolo\b", ps):
        continue
    if "codex" not in ps:
        continue
    node_lines = [raw.strip() for raw in ps.splitlines() if " codex " in f" {raw} " or "/codex" in raw]
    if not node_lines:
        continue
    capture = output(["tmux", "capture-pane", "-p", "-t", key, "-S", "-80"])
    thread_id = None
    for raw in node_lines:
        tokens = shlex.split(raw, posix=True)
        for idx, token in enumerate(tokens):
            if token == "resume":
                for value in tokens[idx + 1:]:
                    if not value.startswith("-"):
                        thread_id = value
                        break
            if thread_id:
                break
        if thread_id:
            break
    if not thread_id:
        match = re.search(r"Session:\s+([0-9a-fA-F-]{20,})", capture)
        if match:
            thread_id = match.group(1)
    if not thread_id:
        thread_id = cwd_thread_ids.get(cwd)
    if not thread_id:
        print(json.dumps({"pane": key, "action": "skip", "reason": "thread_id_missing", "cwd": cwd}), flush=True)
        continue
    tail_capture = "\n".join(capture.splitlines()[-12:])
    busy = any(marker in tail_capture for marker in [
        "Working (", "Waiting for background terminal", "\u25e6 Waiting", "\u2022 Running", "background terminals running"
    ])
    if busy and not include_busy:
        if defer_busy:
            deferred.append({"pane": key, "session": session_name, "window_name": window_name, "thread_id": thread_id, "cwd": cwd})
            print(json.dumps({"pane": key, "action": "defer", "reason": "busy", "thread_id": thread_id, "cwd": cwd}), flush=True)
        else:
            print(json.dumps({"pane": key, "action": "skip", "reason": "busy", "thread_id": thread_id, "cwd": cwd}), flush=True)
        continue
    targets.append({"pane": key, "session": session_name, "window_name": window_name, "thread_id": thread_id, "cwd": cwd})

for target in targets:
    cwd = target["cwd"] or str(home)
    thread_id = target["thread_id"]
    command = "export PATH=\"$HOME/.cargo/bin:$HOME/.npm-global/bin:$PATH\"; yolo resume " + shlex.quote(thread_id) + "; exec \"$SHELL\" -l"
    window_name = "yolo-" + (target.get("window_name") or "codex")
    print(json.dumps({"pane": target["pane"], "action": "new-window", "thread_id": thread_id, "cwd": cwd}), flush=True)
    run(["tmux", "new-window", "-d", "-t", target["session"], "-n", window_name, "-c", cwd, os.environ.get("SHELL", "/bin/sh") + " -lc " + shlex.quote(command)])

for target in deferred:
    wait_script = r'''
import os, shlex, subprocess, time
pane = os.environ["YOLO_DEFER_PANE"]
session = os.environ["YOLO_DEFER_SESSION"]
window_name = os.environ["YOLO_DEFER_WINDOW_NAME"]
cwd = os.environ["YOLO_DEFER_CWD"]
thread_id = os.environ["YOLO_DEFER_THREAD_ID"]
shell = os.environ.get("SHELL", "/bin/sh")
markers = ["Working (", "Waiting for background terminal", "\u25e6 Waiting", "\u2022 Running", "background terminals running"]
def output(cmd):
    return subprocess.run(cmd, text=True, capture_output=True, check=False).stdout
deadline = time.time() + 6 * 60 * 60
while time.time() < deadline:
    capture = output(["tmux", "capture-pane", "-p", "-t", pane, "-S", "-20"])
    tail = "\n".join(capture.splitlines()[-12:])
    if not any(marker in tail for marker in markers):
        command = "export PATH=\"$HOME/.cargo/bin:$HOME/.npm-global/bin:$PATH\"; yolo resume " + shlex.quote(thread_id) + "; exec \"$SHELL\" -l"
        subprocess.run(["tmux", "new-window", "-d", "-t", session, "-n", "yolo-" + window_name, "-c", cwd, shell + " -lc " + shlex.quote(command)], check=False)
        raise SystemExit(0)
    time.sleep(10)
raise SystemExit(2)
'''
    env = os.environ.copy()
    env.update({
        "YOLO_DEFER_PANE": target["pane"],
        "YOLO_DEFER_SESSION": target["session"],
        "YOLO_DEFER_WINDOW_NAME": target.get("window_name") or "codex",
        "YOLO_DEFER_CWD": target["cwd"] or str(home),
        "YOLO_DEFER_THREAD_ID": target["thread_id"],
    })
    if dry_run:
        print("+ defer", target["pane"], target["thread_id"], flush=True)
    else:
        subprocess.Popen(["python3", "-c", wait_script], env=env, start_new_session=True,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

print(json.dumps({"ok": True, "targets": targets, "deferred": deferred, "count": len(targets)}), flush=True)
"###;
    let mut command = Command::new("python3");
    command
        .arg("-c")
        .arg(script)
        .env("YOLO_EXTERNAL_CODEX_PACKAGE", package)
        .env(
            "YOLO_EXTERNAL_INCLUDE_BUSY",
            if include_busy { "1" } else { "0" },
        )
        .env(
            "YOLO_EXTERNAL_DEFER_BUSY",
            if defer_busy { "1" } else { "0" },
        )
        .env(
            "YOLO_EXTERNAL_UPDATE_SYSTEM",
            if update_system { "1" } else { "0" },
        )
        .env("YOLO_EXTERNAL_DRY_RUN", if dry_run { "1" } else { "0" })
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let status = command
        .status()
        .map_err(|err| format!("spawn external codex upgrade helper: {err}"))?;
    if !status.success() {
        return Err(format_exit_status("external codex upgrade helper", status));
    }
    Ok(())
}

fn run_configure(args: Vec<OsString>) -> Result<(), String> {
    ensure_server()?;
    let request = parse_configure_args(args)?;
    let value = api_post_json(
        "/clients/configure",
        &serde_json::to_value(&request).map_err(|err| err.to_string())?,
    )?;
    print_pretty_json(&value)?;
    ensure_json_ok(&value)
}

fn run_refresh_resume(args: Vec<OsString>) -> Result<(), String> {
    ensure_server()?;
    let request = parse_refresh_resume_args(args)?;
    let value = api_post_json(
        "/clients/refresh-resume",
        &serde_json::to_value(&request).map_err(|err| err.to_string())?,
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|err| err.to_string())?
    );
    Ok(())
}

fn print_pretty_json(value: &Value) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|err| err.to_string())?
    );
    Ok(())
}

fn ensure_json_ok(value: &Value) -> Result<(), String> {
    if value.get("ok").and_then(Value::as_bool) != Some(false) {
        return Ok(());
    }
    let message = value
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("server returned ok=false");
    Err(message.to_string())
}

fn run_refresh_permissions(args: Vec<OsString>) -> Result<(), String> {
    ensure_server()?;
    let request = parse_refresh_resume_args(args)?;
    let value = api_post_json(
        "/clients/refresh-permissions",
        &serde_json::to_value(&request).map_err(|err| err.to_string())?,
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|err| err.to_string())?
    );
    Ok(())
}

fn parse_refresh_resume_args(args: Vec<OsString>) -> Result<RefreshResumeRequest, String> {
    let mut request = RefreshResumeRequest::default();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        let arg = arg.to_string_lossy().to_string();
        let mut value_for = |name: &str| -> Result<String, String> {
            iter.next()
                .map(|value| value.to_string_lossy().to_string())
                .ok_or_else(|| format!("{name} requires a value"))
        };
        match arg.as_str() {
            "--all" => request.all = true,
            "--client" | "--client-id" => request.client_id = Some(value_for(&arg)?),
            "--thread" | "--thread-id" => request.thread_id = Some(value_for(&arg)?),
            "--cwd" => request.cwd = Some(value_for(&arg)?),
            _ => return Err(format!("unknown refresh-resume argument: {arg}")),
        }
    }
    if !request.all
        && request.client_id.is_none()
        && request.thread_id.is_none()
        && request.cwd.is_none()
    {
        return Err("refresh-resume requires --all, --client, --thread, or --cwd".to_string());
    }
    Ok(request)
}

fn parse_configure_args(args: Vec<OsString>) -> Result<ConfigureClientsRequest, String> {
    let mut request = ConfigureClientsRequest {
        client_id: None,
        thread_id: None,
        cwd: None,
        all: false,
        model: None,
        fast: None,
        reasoning_effort: None,
        timeout_secs: None,
        queue: false,
    };
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        let arg = arg.to_string_lossy().to_string();
        let mut value_for = |name: &str| -> Result<String, String> {
            iter.next()
                .map(|value| value.to_string_lossy().to_string())
                .ok_or_else(|| format!("{name} requires a value"))
        };
        match arg.as_str() {
            "--all" => request.all = true,
            "--client" | "--client-id" => request.client_id = Some(value_for(&arg)?),
            "--thread" | "--thread-id" => request.thread_id = Some(value_for(&arg)?),
            "--cwd" => request.cwd = Some(value_for(&arg)?),
            "--model" => request.model = Some(value_for(&arg)?),
            "--effort" | "--reasoning-effort" => {
                request.reasoning_effort = Some(value_for(&arg)?);
            }
            "--fast" => request.fast = Some(parse_boolish(&value_for(&arg)?)?),
            "--fast-on" => request.fast = Some(true),
            "--fast-off" => request.fast = Some(false),
            "--timeout-secs" => {
                request.timeout_secs = Some(
                    value_for(&arg)?
                        .parse::<u64>()
                        .map_err(|err| format!("invalid --timeout-secs: {err}"))?,
                );
            }
            _ => return Err(format!("unknown set argument: {arg}")),
        }
    }
    if request.model.is_none() && request.fast.is_none() && request.reasoning_effort.is_none() {
        return Err("set requires --model, --fast, or --effort".to_string());
    }
    if !request.all
        && request.client_id.is_none()
        && request.thread_id.is_none()
        && request.cwd.is_none()
    {
        return Err("set requires --all, --client, --thread, or --cwd".to_string());
    }
    Ok(request)
}

fn parse_boolish(value: &str) -> Result<bool, String> {
    match value {
        "1" | "true" | "on" | "yes" | "fast" | "priority" => Ok(true),
        "0" | "false" | "off" | "no" | "default" => Ok(false),
        _ => Err(format!("invalid boolean value: {value}")),
    }
}

fn upgrade_codex_cli() -> Result<(), String> {
    upgrade_codex_cli_version(None)
}

fn upgrade_codex_cli_version(version: Option<&str>) -> Result<(), String> {
    if let Ok(command) = env::var("YOLO_CODEX_UPGRADE_COMMAND") {
        eprintln!("yolo: upgrading Codex CLI with override command: {command}");
        let status = Command::new("sh")
            .arg("-lc")
            .arg(&command)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .map_err(|err| format!("spawn upgrade command: {err}"))?;
        if !status.success() {
            return Err(format_exit_status("upgrade command", status));
        }
        return Ok(());
    }

    let prefix = managed_codex_prefix();
    let package = version
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("@openai/codex@{}", value.trim()))
        .unwrap_or_else(|| CODEX_PACKAGE.to_string());
    fs::create_dir_all(&prefix)
        .map_err(|err| format!("create managed Codex prefix {}: {err}", prefix.display()))?;
    eprintln!(
        "yolo: upgrading Codex CLI package {package} into user-writable prefix {}",
        prefix.display()
    );
    let status = Command::new("npm")
        .arg("install")
        .arg("--global")
        .arg("--prefix")
        .arg(&prefix)
        .arg(package)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|err| format!("spawn npm: {err}"))?;
    if !status.success() {
        return Err(format_exit_status("npm managed install", status));
    }
    let bin = managed_codex_bin();
    if !bin.exists() {
        return Err(format!(
            "managed Codex install completed but {} was not found",
            bin.display()
        ));
    }
    eprintln!("yolo: managed Codex CLI is {}", bin.display());
    Ok(())
}

fn format_exit_status(label: &str, status: std::process::ExitStatus) -> String {
    format!(
        "{label} exited with {}",
        status
            .code()
            .map(|code| code.to_string())
            .unwrap_or_else(|| "signal".to_string())
    )
}

fn restart_server_for_upgrade() -> Result<(), String> {
    if api_get_json("/status").is_ok() {
        eprintln!("yolo: restarting yolo server so app-server uses upgraded Codex");
        let _ = api_post_json("/shutdown", &json!({}));
        wait_for_server_stopped(Duration::from_secs(5))?;
    }
    ensure_server()
}

fn wait_for_server_stopped(timeout: Duration) -> Result<(), String> {
    let start = SystemTime::now();
    while start.elapsed().unwrap_or_default() < timeout {
        if api_get_json("/status").is_err() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err("server did not stop after shutdown request".to_string())
}

fn terminate_pid_tree(pid: u32, timeout: Duration) {
    let mut pids = child_pids_recursive(pid);
    pids.push(pid);
    pids.sort_unstable();
    pids.dedup();
    for pid in pids.iter().rev() {
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status();
    }
    let start = SystemTime::now();
    while start.elapsed().unwrap_or_default() < timeout {
        if !pids.iter().any(|pid| pid_is_alive(*pid)) {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    for pid in pids.iter().rev() {
        let _ = Command::new("kill")
            .arg("-KILL")
            .arg(pid.to_string())
            .status();
    }
}

fn child_pids_recursive(pid: u32) -> Vec<u32> {
    let mut result = Vec::new();
    let output = Command::new("pgrep")
        .arg("-P")
        .arg(pid.to_string())
        .output();
    let Ok(output) = output else {
        return result;
    };
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Ok(child_pid) = line.trim().parse::<u32>() else {
            continue;
        };
        result.extend(child_pids_recursive(child_pid));
        result.push(child_pid);
    }
    result
}

fn pid_is_alive(pid: u32) -> bool {
    let proc_stat = PathBuf::from(format!("/proc/{pid}/stat"));
    if let Ok(stat) = fs::read_to_string(proc_stat)
        && let Some(after_comm) = stat.rsplit_once(") ")
        && after_comm
            .1
            .split_whitespace()
            .next()
            .is_some_and(|state| state == "Z")
    {
        return false;
    }
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn resume_args_for(args: &[OsString], preferred_thread_id: Option<&str>) -> Vec<OsString> {
    if thread_id_from_args(args).is_some() {
        return args.to_vec();
    }
    if let Some(thread_id) = preferred_thread_id.filter(|value| !value.trim().is_empty()) {
        if resume_target_from_args(args) == Some(ResumeTarget::Last) {
            if let Ok(args) = replace_resume_last_with_thread(args, thread_id) {
                return args;
            }
        }
        let mut out = args.to_vec();
        out.push(OsString::from("resume"));
        out.push(OsString::from(thread_id));
        return out;
    }
    vec![OsString::from("resume"), OsString::from("--last")]
}

fn reexec_client_for_resume(original_args: &[OsString], client_id: &str) -> ! {
    let resume_settings = current_client_resume_settings(client_id);
    let resume_args = resume_args_for(original_args, resume_settings.thread_id.as_deref());
    let resume_args = preserve_resume_settings_args(resume_args, &resume_settings);
    eprintln!(
        "yolo: re-executing client after app-server restart with args: {}",
        resume_args
            .iter()
            .map(|arg| arg.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ")
    );
    let mut errors = Vec::new();
    for exe in yolo_reexec_candidates() {
        let err = Command::new(&exe).args(&resume_args).exec();
        errors.push(format!("{}: {err}", exe.display()));
    }
    eprintln!("yolo: failed to re-execute client: {}", errors.join("; "));
    std::process::exit(127);
}

fn preserve_resume_settings_args(
    args: Vec<OsString>,
    settings: &ClientResumeSettings,
) -> Vec<OsString> {
    let string_args = args
        .iter()
        .map(|arg| arg.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    let launch_config = parse_codex_launch_config(&string_args);
    let mut config_args = Vec::new();
    if launch_config.model.is_none()
        && let Some(model) = settings.model.as_deref().filter(|value| !value.is_empty())
    {
        config_args.push(codex_config_os_arg("model", model));
    }
    if launch_config.service_tier.is_none()
        && let Some(service_tier) = settings
            .service_tier
            .as_deref()
            .filter(|value| !value.is_empty())
    {
        config_args.push(codex_config_os_arg("service_tier", service_tier));
    }
    if launch_config.reasoning_effort.is_none()
        && let Some(effort) = settings
            .reasoning_effort
            .as_deref()
            .filter(|value| !value.is_empty())
    {
        config_args.push(codex_config_os_arg("model_reasoning_effort", effort));
    }
    if config_args.is_empty() {
        return args;
    }
    prepend_codex_config_args(args, config_args)
}

fn codex_config_os_arg(key: &str, value: &str) -> OsString {
    OsString::from(format!("{key}=\"{}\"", toml_basic_string_escape(value)))
}

fn prepend_codex_config_args(args: Vec<OsString>, config_args: Vec<OsString>) -> Vec<OsString> {
    let mut out = Vec::with_capacity(args.len() + config_args.len() * 2);
    for config_arg in config_args {
        out.push(OsString::from("-c"));
        out.push(config_arg);
    }
    out.extend(args);
    out
}

fn current_client_resume_settings(client_id: &str) -> ClientResumeSettings {
    let Ok(value) = api_get_json("/clients") else {
        return ClientResumeSettings::default();
    };
    let Some(clients) = value.get("clients").and_then(Value::as_array) else {
        return ClientResumeSettings::default();
    };
    let Some(client) = clients
        .iter()
        .find(|client| client.get("id").and_then(Value::as_str) == Some(client_id))
    else {
        return ClientResumeSettings::default();
    };
    let thread_id = nonempty_json_string(client, "thread_id");
    let model = nonempty_json_string(client, "model");
    let mut service_tier = nonempty_json_string(client, "service_tier").map(normalize_service_tier);
    if service_tier.is_none() {
        service_tier = client
            .get("fast")
            .and_then(Value::as_bool)
            .map(|fast| if fast { "priority" } else { "default" }.to_string());
    }
    let reasoning_effort = nonempty_json_string(client, "reasoning_effort");
    ClientResumeSettings {
        thread_id,
        model,
        service_tier,
        reasoning_effort,
    }
}

fn nonempty_json_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn yolo_reexec_candidates() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(value) = env::var("YOLO_REEXEC_BIN")
        && !value.trim().is_empty()
    {
        out.push(PathBuf::from(value));
    }
    if let Some(path_exe) = find_executable_in_path("yolo") {
        out.push(path_exe);
    }
    if let Ok(exe) = env::current_exe()
        && !exe.to_string_lossy().contains("(deleted)")
    {
        out.push(exe);
    }
    out.push(PathBuf::from("yolo"));

    let mut seen = BTreeSet::new();
    out.into_iter()
        .filter(|path| seen.insert(path.display().to_string()))
        .collect()
}

fn find_executable_in_path(name: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    for dir in env::split_paths(&path) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn thread_id_from_args(args: &[OsString]) -> Option<String> {
    let resume_idx = args
        .iter()
        .position(|arg| matches!(arg.to_str(), Some("resume")))?;
    args.iter()
        .skip(resume_idx + 1)
        .filter_map(|arg| arg.to_str())
        .find(|arg| !arg.starts_with('-'))
        .map(ToString::to_string)
}

fn thread_id_from_args_strs(args: &[String]) -> Option<String> {
    let resume_idx = args.iter().position(|arg| arg == "resume")?;
    args.iter()
        .skip(resume_idx + 1)
        .find(|arg| !arg.starts_with('-'))
        .cloned()
}

fn current_restart_generation() -> u64 {
    api_get_json("/status")
        .ok()
        .and_then(|value| restart_generation_from_status(&value))
        .unwrap_or(0)
}

fn restart_generation_from_status(value: &Value) -> Option<u64> {
    let resume_generation = value
        .get("resume_generation")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let app_server_generation = value
        .get("app_server_generation")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    Some(resume_generation.max(app_server_generation))
}

fn ensure_server() -> Result<(), String> {
    let paths = runtime_paths()?;
    if api_get_json("/status").is_ok() {
        return wait_for_app_server_ready(&paths, APP_SERVER_READY_TIMEOUT);
    }
    if let Some(pid) = running_yolo_server_pid(&paths) {
        return Err(format!(
            "yolo server pid {pid} is running but {} is not reachable",
            paths.api_socket.display()
        ));
    }
    spawn_server_daemon(&[])?;
    wait_for_server_ready(&paths, APP_SERVER_READY_TIMEOUT)
}

fn running_yolo_server_pid(paths: &RuntimePaths) -> Option<u32> {
    read_server_pid(paths).filter(|pid| pid_is_alive(*pid) && pid_is_yolo_server(*pid))
}

fn read_server_pid(paths: &RuntimePaths) -> Option<u32> {
    fs::read_to_string(&paths.pid_file)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
}

fn pid_is_yolo_server(pid: u32) -> bool {
    let cmdline = read_proc_cmdline(PathBuf::from(format!("/proc/{pid}/cmdline")));
    cmdline.first().is_some_and(|arg| {
        Path::new(arg).file_name().and_then(|name| name.to_str()) == Some("yolo")
    }) && cmdline.iter().skip(1).any(|arg| arg == "server")
}

fn wait_for_server_ready(paths: &RuntimePaths, timeout: Duration) -> Result<(), String> {
    let start = SystemTime::now();
    while start.elapsed().unwrap_or_default() < timeout {
        if paths.api_socket.exists()
            && api_get_json("/status").is_ok()
            && wait_for_app_server_ready(paths, Duration::from_millis(100)).is_ok()
        {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(format!(
        "server did not become ready at {}",
        paths.api_socket.display()
    ))
}

fn wait_for_app_server_ready(paths: &RuntimePaths, timeout: Duration) -> Result<(), String> {
    let start = SystemTime::now();
    while start.elapsed().unwrap_or_default() < timeout {
        if paths.app_server_socket.exists()
            && AppServerRpcClient::connect(&paths.app_server_socket).is_ok()
        {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(format!(
        "app-server did not become ready at {}",
        paths.app_server_socket.display()
    ))
}

fn print_status() -> Result<(), String> {
    let value = api_get_json("/clients")?;
    let text = serde_json::to_string_pretty(&value).map_err(|err| err.to_string())?;
    println!("{text}");
    Ok(())
}

fn print_turns(args: Vec<OsString>) -> Result<(), String> {
    let mut thread_id = None;
    let mut limit = 100usize;
    let mut history = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        let arg = arg.to_string_lossy();
        if arg == "--thread" || arg == "--thread-id" {
            thread_id = Some(
                iter.next()
                    .ok_or_else(|| format!("{arg} requires a thread id"))?
                    .to_string_lossy()
                    .to_string(),
            );
        } else if let Some(value) = arg.strip_prefix("--thread=") {
            thread_id = Some(value.to_string());
        } else if let Some(value) = arg.strip_prefix("--thread-id=") {
            thread_id = Some(value.to_string());
        } else if arg == "--limit" {
            limit = iter
                .next()
                .ok_or_else(|| String::from("--limit requires a number"))?
                .to_string_lossy()
                .parse::<usize>()
                .map_err(|err| format!("invalid --limit: {err}"))?;
        } else if let Some(value) = arg.strip_prefix("--limit=") {
            limit = value
                .parse::<usize>()
                .map_err(|err| format!("invalid --limit: {err}"))?;
        } else if arg == "--history" {
            history = true;
        } else {
            return Err(format!("unknown argument: {arg}"));
        }
    }
    if history && thread_id.is_none() {
        return Err(String::from("--history requires --thread THREAD_ID"));
    }
    let mut path = if history {
        String::from("/turns/history?")
    } else {
        format!("/turns?limit={}", limit.clamp(1, MAX_TELEMETRY_TURNS))
    };
    if let Some(thread_id) = thread_id.filter(|value| !value.trim().is_empty()) {
        if history {
            path.push_str("thread_id=");
        } else {
            path.push_str("&thread_id=");
        }
        path.push_str(&thread_id);
        if history {
            path.push_str("&limit=");
            path.push_str(&limit.clamp(1, MAX_TELEMETRY_TURNS).to_string());
        }
    }
    let value = api_get_json(&path)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|err| err.to_string())?
    );
    Ok(())
}

fn stop_server() -> Result<(), String> {
    let value = api_post_json("/shutdown", &json!({}))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|err| err.to_string())?
    );
    Ok(())
}

fn federation_listen_addr(args: &[OsString]) -> Option<String> {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "--federation-listen" || arg == "--master-listen" {
            return iter.next().map(|value| value.to_string_lossy().to_string());
        }
        if let Some(value) = arg.to_string_lossy().strip_prefix("--federation-listen=") {
            return Some(value.to_string());
        }
    }
    env::var("YOLO_FEDERATION_LISTEN").ok()
}

fn spawn_federation_listener(
    state: Arc<Mutex<ServerState>>,
    paths: RuntimePaths,
    addr: String,
) -> Result<(), String> {
    let listener = TcpListener::bind(&addr).map_err(|err| format!("bind {addr}: {err}"))?;
    eprintln!("yolo federation API listening on http://{addr}");
    thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let state = Arc::clone(&state);
                    let paths = paths.clone();
                    thread::spawn(move || handle_federation_connection(stream, state, paths));
                }
                Err(err) => eprintln!("yolo federation: accept failed: {err}"),
            }
        }
    });
    Ok(())
}

fn handle_federation_connection(
    mut stream: TcpStream,
    state: Arc<Mutex<ServerState>>,
    paths: RuntimePaths,
) {
    let request = match read_http_request(&mut stream) {
        Ok(request) => request,
        Err(err) => {
            let _ = stream
                .write_all(json_response(400, &json!({"ok": false, "error": err})).as_bytes());
            return;
        }
    };
    let (method, path, headers, body) = request;
    if method == "GET"
        && path == "/federation/slaves/stream"
        && headers
            .get("upgrade")
            .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
    {
        match websocket_upgrade_response(&headers) {
            Ok(response) => {
                if stream.write_all(response.as_bytes()).is_ok() {
                    handle_federation_push_connection(stream, state);
                }
            }
            Err(err) => {
                let _ = stream
                    .write_all(json_response(400, &json!({"ok": false, "error": err})).as_bytes());
            }
        }
        return;
    }
    if method == "GET"
        && path == "/federation/events"
        && headers
            .get("upgrade")
            .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
    {
        match websocket_upgrade_response(&headers) {
            Ok(response) => {
                if stream.write_all(response.as_bytes()).is_ok() {
                    handle_status_event_connection(stream, state);
                }
            }
            Err(err) => {
                let _ = stream
                    .write_all(json_response(400, &json!({"ok": false, "error": err})).as_bytes());
            }
        }
        return;
    }
    let response = handle_federation_request(&method, &path, &headers, &body, state, paths);
    let _ = stream.write_all(response.as_bytes());
}

fn websocket_upgrade_response(headers: &BTreeMap<String, String>) -> Result<String, String> {
    let key = headers
        .get("sec-websocket-key")
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "missing sec-websocket-key".to_string())?;
    let mut hasher = Sha1::new();
    hasher.update(key.trim().as_bytes());
    hasher.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let accept = BASE64_STANDARD.encode(hasher.finalize());
    Ok(format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    ))
}

fn register_status_event_subscriber(
    state: &Arc<Mutex<ServerState>>,
) -> Result<(u64, mpsc::Receiver<Value>), String> {
    let (sender, receiver) = mpsc::channel::<Value>();
    let mut state = state
        .lock()
        .map_err(|_| "server state lock poisoned".to_string())?;
    state.next_status_event_id = state.next_status_event_id.saturating_add(1);
    let subscriber_id = state.next_status_event_id;
    state.status_event_senders.insert(subscriber_id, sender);
    Ok((subscriber_id, receiver))
}

fn unregister_status_event_subscriber(state: &Arc<Mutex<ServerState>>, subscriber_id: u64) {
    if let Ok(mut state) = state.lock() {
        state.status_event_senders.remove(&subscriber_id);
    }
}

fn publish_status_event(state: &Arc<Mutex<ServerState>>, reason: &str) {
    let event = json!({
        "event": "status",
        "reason": reason,
        "at": now_millis(),
    });
    let senders = {
        let Ok(state) = state.lock() else {
            return;
        };
        state
            .status_event_senders
            .iter()
            .map(|(id, sender)| (*id, sender.clone()))
            .collect::<Vec<_>>()
    };
    let mut stale = Vec::new();
    for (id, sender) in senders {
        if sender.send(event.clone()).is_err() {
            stale.push(id);
        }
    }
    if !stale.is_empty()
        && let Ok(mut state) = state.lock()
    {
        for id in stale {
            state.status_event_senders.remove(&id);
        }
    }
}

fn handle_status_event_connection(mut stream: TcpStream, state: Arc<Mutex<ServerState>>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(35)));
    let Ok((subscriber_id, receiver)) = register_status_event_subscriber(&state) else {
        return;
    };
    let _ = websocket_send_text_unmasked(
        &mut stream,
        &json!({"event":"ready","version":VERSION}).to_string(),
    );
    let _ = websocket_send_text_unmasked(
        &mut stream,
        &json!({"event":"status","reason":"initial","at":now_millis()}).to_string(),
    );
    let mut writer_stream = match stream.try_clone() {
        Ok(stream) => stream,
        Err(_) => {
            unregister_status_event_subscriber(&state, subscriber_id);
            return;
        }
    };
    let writer = thread::spawn(move || {
        while let Ok(event) = receiver.recv() {
            if websocket_send_text_unmasked(&mut writer_stream, &event.to_string()).is_err() {
                break;
            }
        }
    });
    loop {
        match websocket_read_text(&mut stream) {
            Ok(_) => {}
            Err(err) if err.contains("timed out") || err.contains("WouldBlock") => {}
            Err(_) => break,
        }
    }
    unregister_status_event_subscriber(&state, subscriber_id);
    let _ = writer.join();
}

fn handle_federation_push_connection(mut stream: TcpStream, state: Arc<Mutex<ServerState>>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(35)));
    let hello = match websocket_read_text(&mut stream)
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
    {
        Some(value) if value.get("event").and_then(Value::as_str) == Some("hello") => value,
        _ => return,
    };
    let Some(slave_id) = hello
        .get("slave_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|id| !id.is_empty())
    else {
        return;
    };
    let slave_id = slave_id.to_string();
    let (sender, receiver) = mpsc::channel::<Value>();
    let pending = {
        let Ok(mut state) = state.lock() else {
            return;
        };
        state
            .federation_push_senders
            .insert(slave_id.clone(), sender.clone());
        let now = now_secs();
        let slave = state
            .slaves
            .entry(slave_id.clone())
            .or_insert_with(|| SlaveInfo {
                id: slave_id.clone(),
                host: None,
                version: String::new(),
                pid: 0,
                last_seen_at: now,
                status: "online".to_string(),
                commands: Vec::new(),
                latest_status: None,
            });
        slave.host = hello
            .get("host")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        slave.version = hello
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        slave.pid = hello.get("pid").and_then(Value::as_u64).unwrap_or_default() as u32;
        slave.last_seen_at = now;
        slave.status = "online".to_string();
        let mut pending = Vec::new();
        for record in &mut slave.commands {
            if record.status == "pending" {
                record.status = "running".to_string();
                record.started_at = Some(now);
                pending.push(record.command.clone());
            }
        }
        pending
    };
    let _ = websocket_send_text_unmasked(
        &mut stream,
        &json!({"event":"hello_ack","slave_id":slave_id}).to_string(),
    );
    for command in pending {
        let _ = sender.send(json!({"event":"command","command":command}));
    }
    let mut writer_stream = match stream.try_clone() {
        Ok(stream) => stream,
        Err(_) => return,
    };
    let writer = thread::spawn(move || {
        while let Ok(event) = receiver.recv() {
            if websocket_send_text_unmasked(&mut writer_stream, &event.to_string()).is_err() {
                break;
            }
        }
    });
    loop {
        let Ok(text) = websocket_read_text(&mut stream) else {
            break;
        };
        let Ok(value) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        match value.get("event").and_then(Value::as_str) {
            Some("result") => {
                if let Ok(result) = serde_json::from_value::<SlaveResultRequest>(value.clone()) {
                    record_slave_result(&state, result);
                }
            }
            Some("status") => {
                if let Ok(mut state) = state.lock() {
                    if let Some(slave) = state.slaves.get_mut(&slave_id) {
                        slave.latest_status = value.get("status").cloned();
                        slave.last_seen_at = now_secs();
                    }
                }
                publish_status_event(&state, "slave-status");
            }
            Some("heartbeat") => {
                if let Ok(mut state) = state.lock() {
                    if let Some(slave) = state.slaves.get_mut(&slave_id) {
                        slave.last_seen_at = now_secs();
                        slave.status = "online".to_string();
                    }
                }
            }
            _ => {}
        }
    }
    if let Ok(mut state) = state.lock() {
        state.federation_push_senders.remove(&slave_id);
        if let Some(slave) = state.slaves.get_mut(&slave_id) {
            slave.status = "offline".to_string();
        }
    }
    drop(sender);
    let _ = writer.join();
}

fn handle_federation_request(
    method: &str,
    path: &str,
    _headers: &BTreeMap<String, String>,
    body: &str,
    state: Arc<Mutex<ServerState>>,
    paths: RuntimePaths,
) -> String {
    match (method, path) {
        ("GET", "/federation/slaves") => {
            let slaves = state
                .lock()
                .map(|state| state.slaves.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            json_response(200, &json!({"ok": true, "slaves": slaves}))
        }
        ("POST", "/federation/slaves/poll") => {
            let request = match serde_json::from_str::<SlavePollRequest>(body) {
                Ok(request) => request,
                Err(err) => {
                    return json_response(400, &json!({"ok": false, "error": err.to_string()}));
                }
            };
            let command = poll_slave_command(&state, request);
            json_response(200, &json!({"ok": true, "command": command}))
        }
        ("POST", "/federation/slaves/result") => {
            let request = match serde_json::from_str::<SlaveResultRequest>(body) {
                Ok(request) => request,
                Err(err) => {
                    return json_response(400, &json!({"ok": false, "error": err.to_string()}));
                }
            };
            record_slave_result(&state, request);
            json_response(200, &json!({"ok": true}))
        }
        ("POST", path)
            if path.starts_with("/federation/slaves/") && path.ends_with("/commands") =>
        {
            let slave_id = path
                .trim_start_matches("/federation/slaves/")
                .trim_end_matches("/commands")
                .trim_matches('/');
            let mut command = match serde_json::from_str::<SlaveCommand>(body) {
                Ok(command) => command,
                Err(err) => {
                    return json_response(400, &json!({"ok": false, "error": err.to_string()}));
                }
            };
            if command.id.trim().is_empty() {
                command.id = format!("cmd-{}", now_millis());
            }
            let record = enqueue_slave_command(&state, slave_id, command);
            json_response(200, &json!({"ok": true, "command": record}))
        }
        ("POST", "/federation/local/upgrade-resume-all") => {
            let request = serde_json::from_str::<UpgradeResumeAllRequest>(body).unwrap_or_default();
            let version = request.codex_version.as_deref();
            match run_codex_upgrade_resume_all_local(Arc::clone(&state), &paths, version, &request)
            {
                Ok(value) => json_response(200, &value),
                Err(err) => json_response(500, &json!({"ok": false, "error": err})),
            }
        }
        _ => json_response(404, &json!({"ok": false, "error": "not found"})),
    }
}

fn poll_slave_command(
    state: &Arc<Mutex<ServerState>>,
    request: SlavePollRequest,
) -> Option<SlaveCommand> {
    let now = now_secs();
    let Ok(mut state) = state.lock() else {
        return None;
    };
    let slave = state
        .slaves
        .entry(request.slave_id.clone())
        .or_insert_with(|| SlaveInfo {
            id: request.slave_id.clone(),
            host: request.host.clone(),
            version: request.version.clone(),
            pid: request.pid,
            last_seen_at: now,
            status: "online".to_string(),
            commands: Vec::new(),
            latest_status: None,
        });
    slave.host = request.host;
    slave.version = request.version;
    slave.pid = request.pid;
    slave.last_seen_at = now;
    slave.status = request.status.unwrap_or_else(|| "online".to_string());
    for record in &mut slave.commands {
        if record.status == "pending" {
            record.status = "running".to_string();
            record.started_at = Some(now);
            return Some(record.command.clone());
        }
    }
    None
}

fn enqueue_slave_command(
    state: &Arc<Mutex<ServerState>>,
    slave_id: &str,
    command: SlaveCommand,
) -> SlaveCommandRecord {
    let now = now_secs();
    let mut record = SlaveCommandRecord {
        command,
        status: "pending".to_string(),
        created_at: now,
        started_at: None,
        finished_at: None,
        result: None,
    };
    let mut push_sender = None;
    if let Ok(mut state) = state.lock() {
        let existing_push_sender = state.federation_push_senders.get(slave_id).cloned();
        let slave = state
            .slaves
            .entry(slave_id.to_string())
            .or_insert_with(|| SlaveInfo {
                id: slave_id.to_string(),
                host: None,
                version: String::new(),
                pid: 0,
                last_seen_at: 0,
                status: "unknown".to_string(),
                commands: Vec::new(),
                latest_status: None,
            });
        if let Some(sender) = existing_push_sender {
            record.status = "running".to_string();
            record.started_at = Some(now);
            push_sender = Some(sender);
        }
        slave.commands.push(record.clone());
    }
    if let Some(sender) = push_sender {
        let _ = sender.send(json!({"event":"command","command":record.command}));
    }
    record
}

fn record_slave_result(state: &Arc<Mutex<ServerState>>, request: SlaveResultRequest) {
    let now = now_secs();
    let Ok(mut state) = state.lock() else {
        return;
    };
    let Some(slave) = state.slaves.get_mut(&request.slave_id) else {
        return;
    };
    slave.last_seen_at = now;
    for record in &mut slave.commands {
        if record.command.id == request.command_id {
            record.status = if request.ok { "done" } else { "failed" }.to_string();
            record.finished_at = Some(now);
            record.result = Some(request.result);
            return;
        }
    }
}

fn federation_master_socket(base_url: &str) -> Result<(String, u16), String> {
    let authority = base_url
        .trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "YOLO_MASTER_URL has no host".to_string())?;
    if let Some((host, port)) = authority.rsplit_once(':')
        && let Ok(port) = port.parse::<u16>()
    {
        return Ok((host.trim_matches(['[', ']']).to_string(), port));
    }
    Ok((authority.to_string(), 80))
}

fn connect_federation_push(
    master_url: &str,
    slave_id: &str,
    version: &str,
    bearer_token: Option<&str>,
) -> Result<TcpStream, String> {
    let (host, port) = federation_master_socket(master_url)?;
    let mut addresses = (host.as_str(), port)
        .to_socket_addrs()
        .map_err(|err| format!("resolve federation master: {err}"))?;
    let address = addresses
        .next()
        .ok_or_else(|| "federation master has no addresses".to_string())?;
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))
        .map_err(|err| format!("connect federation push: {err}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(35)))
        .map_err(|err| format!("set federation push read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|err| format!("set federation push write timeout: {err}"))?;
    let mut request = format!(
        "GET /federation/slaves/stream HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: eW9sby1mZWRlcmF0aW9uLXB1c2g=\r\nSec-WebSocket-Version: 13\r\n"
    );
    if let Some(token) = bearer_token.filter(|token| !token.trim().is_empty()) {
        request.push_str(&format!("Authorization: Bearer {}\r\n", token.trim()));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|err| format!("write federation push handshake: {err}"))?;
    let response = read_http_headers(&mut stream)?;
    if !response.starts_with("HTTP/1.1 101") && !response.starts_with("HTTP/1.0 101") {
        return Err(format!(
            "federation push handshake failed: {}",
            response.lines().next().unwrap_or_default()
        ));
    }
    websocket_send_text(
        &mut stream,
        &json!({
            "event": "hello",
            "slave_id": slave_id,
            "version": version,
            "pid": std::process::id(),
            "host": hostname(),
        })
        .to_string(),
    )?;
    let hello_ack = websocket_read_text(&mut stream)?;
    let ack = serde_json::from_str::<Value>(&hello_ack)
        .map_err(|err| format!("decode federation push hello_ack: {err}"))?;
    if ack.get("event").and_then(Value::as_str) != Some("hello_ack") {
        return Err("federation push master did not acknowledge hello".to_string());
    }
    Ok(stream)
}

fn run_federation_push_session(
    state: Arc<Mutex<ServerState>>,
    paths: RuntimePaths,
    master_url: &str,
    bearer_token: Option<&str>,
    slave_id: &str,
) -> Result<(), String> {
    let mut stream = connect_federation_push(master_url, slave_id, VERSION, bearer_token)?;
    loop {
        let text = match websocket_read_text(&mut stream) {
            Ok(text) => text,
            Err(err) if err.contains("timed out waiting") => {
                websocket_send_text(
                    &mut stream,
                    &json!({"event":"heartbeat","slave_id":slave_id,"at":now_millis()}).to_string(),
                )?;
                continue;
            }
            Err(err) => return Err(err),
        };
        let value = serde_json::from_str::<Value>(&text)
            .map_err(|err| format!("decode federation push event: {err}"))?;
        if value.get("event").and_then(Value::as_str) != Some("command") {
            continue;
        }
        let Some(command_value) = value.get("command") else {
            continue;
        };
        let command = serde_json::from_value::<SlaveCommand>(command_value.clone())
            .map_err(|err| format!("decode federation push command: {err}"))?;
        let command_id = command.id.clone();
        let result = execute_slave_command(
            Arc::clone(&state),
            &paths,
            master_url,
            bearer_token,
            slave_id,
            &command,
        );
        websocket_send_text(
            &mut stream,
            &json!({
                "event": "result",
                "slave_id": slave_id,
                "command_id": command_id,
                "ok": result.get("ok").and_then(Value::as_bool).unwrap_or(false),
                "result": result,
            })
            .to_string(),
        )?;
        let status = server_info(&state, &paths);
        websocket_send_text(
            &mut stream,
            &json!({
                "event": "status",
                "slave_id": slave_id,
                "status": status,
            })
            .to_string(),
        )?;
    }
}

fn spawn_slave_connector_if_configured(state: Arc<Mutex<ServerState>>, paths: RuntimePaths) {
    let Ok(master_url) = env::var("YOLO_MASTER_URL") else {
        return;
    };
    let Ok(slave_id) = env::var("YOLO_SLAVE_ID") else {
        eprintln!("yolo slave connector disabled: YOLO_SLAVE_ID is missing");
        return;
    };
    let bearer_token = env::var("YOLO_MASTER_BEARER_TOKEN")
        .ok()
        .or_else(|| env::var("YOLO_AGENT_GATE_TOKEN").ok())
        .or_else(|| env::var("YOLO_SLAVE_TOKEN").ok());
    thread::spawn(move || {
        let mut pending_result: Option<SlaveResultRequest> = None;
        loop {
            match run_federation_push_session(
                Arc::clone(&state),
                paths.clone(),
                &master_url,
                bearer_token.as_deref(),
                &slave_id,
            ) {
                Ok(()) => continue,
                Err(err) => eprintln!(
                    "yolo slave connector: push unavailable: {err}; using polling fallback"
                ),
            }
            if let Some(result) = pending_result.take() {
                let _ = federation_post_json(
                    &master_url,
                    "/federation/slaves/result",
                    bearer_token.as_deref(),
                    &serde_json::to_value(result).unwrap_or_else(|_| json!({})),
                );
            }
            let poll = SlavePollRequest {
                slave_id: slave_id.clone(),
                version: VERSION.to_string(),
                pid: std::process::id(),
                host: hostname(),
                status: Some("online".to_string()),
            };
            match federation_post_json(
                &master_url,
                "/federation/slaves/poll",
                bearer_token.as_deref(),
                &serde_json::to_value(&poll).unwrap_or_else(|_| json!({})),
            ) {
                Ok(value) => {
                    if let Some(command) = value.get("command")
                        && !command.is_null()
                    {
                        match serde_json::from_value::<SlaveCommand>(command.clone()) {
                            Ok(command) => {
                                let result = execute_slave_command(
                                    Arc::clone(&state),
                                    &paths,
                                    &master_url,
                                    bearer_token.as_deref(),
                                    &slave_id,
                                    &command,
                                );
                                pending_result = Some(SlaveResultRequest {
                                    slave_id: slave_id.clone(),
                                    command_id: command.id,
                                    ok: result.get("ok").and_then(Value::as_bool).unwrap_or(false),
                                    result,
                                });
                            }
                            Err(err) => {
                                eprintln!("yolo slave connector: invalid command: {err}");
                            }
                        }
                    }
                }
                Err(err) => eprintln!("yolo slave connector: poll failed: {err}"),
            }
            thread::sleep(FEDERATION_POLL_INTERVAL);
        }
    });
}

fn execute_slave_command(
    state: Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
    master_url: &str,
    bearer_token: Option<&str>,
    slave_id: &str,
    command: &SlaveCommand,
) -> Value {
    match command.action.as_str() {
        "codex-upgrade-resume" | "upgrade-codex" | "upgrade-resume-all" => {
            let request = UpgradeResumeAllRequest {
                codex_version: command.codex_version.clone(),
                ..UpgradeResumeAllRequest::default()
            };
            match run_codex_upgrade_resume_all_local(
                state,
                paths,
                command.codex_version.as_deref(),
                &request,
            ) {
                Ok(value) => value,
                Err(err) => json!({"ok": false, "error": err}),
            }
        }
        "yolo-upgrade" | "upgrade-yolo" => {
            match run_yolo_upgrade_resume_local(Arc::clone(&state), paths, command) {
                Ok(value) => {
                    let result = SlaveResultRequest {
                        slave_id: slave_id.to_string(),
                        command_id: command.id.clone(),
                        ok: true,
                        result: value.clone(),
                    };
                    let _ = federation_post_json(
                        master_url,
                        "/federation/slaves/result",
                        bearer_token,
                        &serde_json::to_value(result).unwrap_or_else(|_| json!({})),
                    );
                    value
                }
                Err(err) => json!({"ok": false, "error": err}),
            }
        }
        "configure-clients" | "clients-configure" | "configure" | "set" => {
            let Some(request) = command.configure.clone() else {
                return json!({
                    "ok": false,
                    "error": "configure-clients command requires a configure request"
                });
            };
            match configure_clients(state, paths, request) {
                Ok(value) => value,
                Err(err) => json!({"ok": false, "error": err}),
            }
        }
        "refresh-permissions" | "permissions-refresh" | "refresh-yolo-permissions" => {
            let request = command.configure.clone().map_or_else(
                || ConfigureClientsRequest {
                    all: true,
                    ..ConfigureClientsRequest::default()
                },
                |configure| ConfigureClientsRequest {
                    all: configure.all,
                    client_id: configure.client_id,
                    thread_id: configure.thread_id,
                    cwd: configure.cwd,
                    ..ConfigureClientsRequest::default()
                },
            );
            let request = RefreshResumeRequest {
                all: request.all,
                client_id: request.client_id,
                thread_id: request.thread_id,
                cwd: request.cwd,
            };
            match refresh_resume_permissions_clients(state, paths, request) {
                Ok(value) => value,
                Err(err) => json!({"ok": false, "error": err}),
            }
        }
        "turns" | "thread-turns" => {
            let Some(thread_id) = command
                .thread_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                return json!({
                    "ok": false,
                    "error": "turns command requires a thread_id"
                });
            };
            let limit = command.limit.unwrap_or(20).clamp(1, MAX_TELEMETRY_TURNS);
            match app_server_thread_history(paths, thread_id, limit) {
                Ok(turns) => json!({
                    "ok": true,
                    "thread_id": thread_id,
                    "turns": turns,
                }),
                Err(err) => json!({"ok": false, "error": err}),
            }
        }
        "status" | "clients" | "local-status" | "local-clients" => {
            let info = server_info(&state, paths);
            json!({
                "ok": true,
                "status": info,
                "clients": info.clients,
                "tmux_panes": info.tmux_panes,
            })
        }
        _ => {
            json!({"ok": false, "error": format!("unknown slave command action: {}", command.action)})
        }
    }
}

fn run_codex_upgrade_resume_all_local(
    state: Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
    codex_version: Option<&str>,
    request: &UpgradeResumeAllRequest,
) -> Result<Value, String> {
    wait_for_clients_idle(Arc::clone(&state), paths, request)
        .and_then(|_| upgrade_codex_cli_version(codex_version))
        .and_then(|_| restart_tracked_app_server(Arc::clone(&state), paths.clone()))
        .and_then(|app_server_pid| {
            if let Ok(mut state) = state.lock() {
                state.resume_generation = state.resume_generation.saturating_add(1);
                let generation = state.resume_generation;
                return Ok(json!({
                    "ok": true,
                    "app_server_pid": app_server_pid,
                    "app_server_generation": state.app_server_generation,
                    "resume_generation": generation,
                    "codex_version": codex_version,
                    "clients": state.clients.len()
                }));
            }
            Err("server state lock poisoned".to_string())
        })
}

fn refresh_resume_clients(
    state: Arc<Mutex<ServerState>>,
    request: RefreshResumeRequest,
) -> Result<Value, String> {
    let clients = {
        let state = state
            .lock()
            .map_err(|_| "server state lock poisoned".to_string())?;
        state
            .clients
            .values()
            .filter(|client| client.status == "running")
            .filter(|client| refresh_resume_request_matches(&request, client))
            .cloned()
            .collect::<Vec<_>>()
    };
    if clients.is_empty() {
        return Ok(json!({"ok": true, "matched": 0, "resume_generation": null}));
    }

    let mut repaired = Vec::new();
    let mut errors = Vec::new();
    for client in &clients {
        let Some(thread_id) = client.thread_id.as_deref() else {
            continue;
        };
        match repair_resume_thread_id(thread_id, &client.cwd) {
            Ok(()) => repaired.push(json!({
                "client_id": client.id,
                "thread_id": thread_id,
                "cwd": client.cwd
            })),
            Err(err) => errors.push(json!({
                "client_id": client.id,
                "thread_id": thread_id,
                "cwd": client.cwd,
                "error": err
            })),
        }
    }
    if !errors.is_empty() {
        return Err(format!("failed to repair resume contexts: {errors:?}"));
    }

    let generation = {
        let mut state = state
            .lock()
            .map_err(|_| "server state lock poisoned".to_string())?;
        state.resume_generation = state.resume_generation.saturating_add(1);
        state.resume_generation
    };
    Ok(json!({
        "ok": true,
        "matched": clients.len(),
        "repaired": repaired,
        "resume_generation": generation
    }))
}

fn refresh_resume_permissions_clients(
    state: Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
    request: RefreshResumeRequest,
) -> Result<Value, String> {
    let clients = {
        let state = state
            .lock()
            .map_err(|_| "server state lock poisoned".to_string())?;
        state
            .clients
            .values()
            .filter(|client| client.status == "running")
            .filter(|client| refresh_resume_request_matches(&request, client))
            .cloned()
            .collect::<Vec<_>>()
    };
    if clients.is_empty() {
        return Ok(json!({"ok": true, "matched": 0, "updated": []}));
    }

    let mut updated = Vec::new();
    let mut updated_thread_ids = BTreeSet::new();
    let mut skipped = Vec::new();
    let mut errors = Vec::new();
    for client in &clients {
        let Some(thread_id) = client.thread_id.as_deref() else {
            skipped.push(json!({
                "client_id": client.id,
                "cwd": client.cwd,
                "reason": "client has no thread_id"
            }));
            continue;
        };
        if let Err(err) = repair_resume_thread_id(thread_id, &client.cwd) {
            errors.push(json!({
                "client_id": client.id,
                "thread_id": thread_id,
                "cwd": client.cwd,
                "stage": "repair_resume_context",
                "error": err
            }));
            continue;
        }
        match update_app_server_resume_thread_settings(
            &paths.app_server_socket,
            thread_id,
            &client.cwd,
        ) {
            Ok(()) => {
                note_client_permissions_update(&state, &client.id);
                publish_status_event(&state, "client-permissions-updated");
                updated_thread_ids.insert(thread_id.to_string());
                updated.push(json!({
                    "client_id": client.id,
                    "thread_id": thread_id,
                    "cwd": client.cwd
                }));
            }
            Err(err) => errors.push(json!({
                "client_id": client.id,
                "thread_id": thread_id,
                "cwd": client.cwd,
                "error": err
            })),
        }
    }
    let loaded_threads = app_server_thread_snapshot(paths, None).unwrap_or_else(|err| {
        errors.push(json!({
            "stage": "app_server_snapshot",
            "error": err
        }));
        Vec::new()
    });
    let mut updated_loaded_threads = Vec::new();
    for thread in loaded_threads {
        if updated_thread_ids.contains(&thread.id) {
            continue;
        }
        if !refresh_resume_request_matches_thread(&request, &thread) {
            continue;
        }
        match update_app_server_resume_thread_settings(
            &paths.app_server_socket,
            &thread.id,
            &thread.cwd,
        ) {
            Ok(()) => {
                updated_thread_ids.insert(thread.id.clone());
                updated_loaded_threads.push(json!({
                    "thread_id": thread.id,
                    "cwd": thread.cwd,
                    "status": thread.status
                }));
            }
            Err(err) => errors.push(json!({
                "thread_id": thread.id,
                "cwd": thread.cwd,
                "stage": "loaded_thread_settings_update",
                "error": err
            })),
        }
    }
    if !errors.is_empty() {
        return Err(format!("failed to refresh yolo permissions: {errors:?}"));
    }

    Ok(json!({
        "ok": true,
        "matched": clients.len(),
        "updated": updated,
        "updated_loaded_threads": updated_loaded_threads,
        "skipped": skipped
    }))
}

fn refresh_resume_request_matches_thread(
    request: &RefreshResumeRequest,
    thread: &AppThreadSnapshot,
) -> bool {
    request.all
        || request
            .thread_id
            .as_deref()
            .is_some_and(|value| value == thread.id)
        || request
            .cwd
            .as_deref()
            .is_some_and(|value| value == thread.cwd)
}

fn refresh_resume_request_matches(request: &RefreshResumeRequest, client: &ClientInfo) -> bool {
    request.all
        || request
            .client_id
            .as_deref()
            .is_some_and(|value| value == client.id)
        || request
            .thread_id
            .as_deref()
            .is_some_and(|value| client.thread_id.as_deref() == Some(value))
        || request
            .cwd
            .as_deref()
            .is_some_and(|value| value == client.cwd)
}

fn run_yolo_upgrade_resume_local(
    state: Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
    command: &SlaveCommand,
) -> Result<Value, String> {
    let request = UpgradeResumeAllRequest::default();
    wait_for_clients_idle(Arc::clone(&state), paths, &request)?;
    let mut value = upgrade_yolo(command)?;
    let generation = {
        let mut state = state
            .lock()
            .map_err(|_| "server state lock poisoned".to_string())?;
        state.resume_generation = state.resume_generation.saturating_add(1);
        state.resume_generation
    };
    if let Some(object) = value.as_object_mut() {
        object.insert("resume_generation".to_string(), Value::from(generation));
        object.insert("client_reexec_scheduled".to_string(), Value::Bool(true));
        object.insert("server_restart_required".to_string(), Value::Bool(true));
        object.insert(
            "restart_policy".to_string(),
            Value::String("clients_reexec_in_place_after_idle_server_restart_deferred".to_string()),
        );
    }
    Ok(value)
}

fn upgrade_yolo(command: &SlaveCommand) -> Result<Value, String> {
    let shell_command = if let Some(command) = command.command.as_ref() {
        command.clone()
    } else if let Ok(command) = env::var("YOLO_SELF_UPGRADE_COMMAND") {
        command
    } else if let Some(version) = command.yolo_version.as_ref() {
        let tag = if version.starts_with('v') {
            version.clone()
        } else {
            format!("v{version}")
        };
        format!("cargo install --git https://github.com/genki/yolo --tag {tag} --force")
    } else {
        "cargo install --git https://github.com/genki/yolo --branch main --force".to_string()
    };
    eprintln!("yolo: self-upgrade command: {shell_command}");
    let status = Command::new("sh")
        .arg("-lc")
        .arg(&shell_command)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|err| format!("spawn yolo self-upgrade command: {err}"))?;
    if !status.success() {
        return Err(format_exit_status("yolo self-upgrade command", status));
    }
    Ok(json!({
        "ok": true,
        "restart_scheduled": false,
        "restart_required": true,
        "restart_policy": "deferred_to_avoid_resetting_active_codex_clients",
        "yolo_version": command.yolo_version,
    }))
}

fn handle_api_connection(
    mut stream: UnixStream,
    state: Arc<Mutex<ServerState>>,
    paths: RuntimePaths,
) {
    let response = match read_http_request(&mut stream) {
        Ok((method, path, _headers, body)) => {
            handle_api_request(&method, &path, &body, state, paths, &mut stream)
        }
        Err(err) => json_response(400, &json!({"ok": false, "error": err})),
    };

    let _ = stream.write_all(response.as_bytes());
}

fn handle_api_request(
    method: &str,
    path: &str,
    body: &str,
    state: Arc<Mutex<ServerState>>,
    paths: RuntimePaths,
    stream: &mut UnixStream,
) -> String {
    let (route, query) = path.split_once('?').unwrap_or((path, ""));
    match (method, route) {
        ("GET", "/status") => {
            let info = server_info(&state, &paths);
            json_response(200, &info)
        }
        ("GET", "/clients") => {
            let info = server_info(&state, &paths);
            json_response(200, &info)
        }
        ("GET", "/agents") | ("GET", "/subagents") => {
            let snapshot = state
                .lock()
                .map(|state| state.telemetry.snapshot())
                .unwrap_or_else(|_| TelemetrySnapshot {
                    generated_at: now_secs(),
                    summary: TelemetrySummary::default(),
                    agents: Vec::new(),
                    tool_calls: Vec::new(),
                    hook_runs: Vec::new(),
                });
            let agents = if route == "/subagents" {
                snapshot
                    .agents
                    .into_iter()
                    .filter(|agent| agent.is_subagent)
                    .collect::<Vec<_>>()
            } else {
                snapshot.agents
            };
            json_response(
                200,
                &json!({
                    "ok": true,
                    "generated_at": snapshot.generated_at,
                    "summary": snapshot.summary,
                    "agents": agents
                }),
            )
        }
        ("GET", "/telemetry") => {
            let snapshot = state
                .lock()
                .map(|state| state.telemetry.snapshot())
                .unwrap_or_else(|_| TelemetrySnapshot {
                    generated_at: now_secs(),
                    summary: TelemetrySummary::default(),
                    agents: Vec::new(),
                    tool_calls: Vec::new(),
                    hook_runs: Vec::new(),
                });
            json_response(200, &json!({"ok": true, "telemetry": snapshot}))
        }
        ("GET", "/turns") => {
            let thread_id = query_parameter(query, "thread_id");
            let limit = query_parameter(query, "limit")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(100)
                .clamp(1, MAX_TELEMETRY_TURNS);
            let snapshot = state
                .lock()
                .map(|state| state.telemetry.turns_snapshot(thread_id.as_deref(), limit))
                .unwrap_or_else(|_| TurnArchiveSnapshot {
                    generated_at: now_secs(),
                    turns: Vec::new(),
                });
            json_response(
                200,
                &json!({
                    "ok": true,
                    "generated_at": snapshot.generated_at,
                    "turns": snapshot.turns,
                }),
            )
        }
        ("GET", "/turns/history") => {
            let Some(thread_id) = query_parameter(query, "thread_id")
                .filter(|thread_id| !thread_id.trim().is_empty())
            else {
                return json_response(400, &json!({"ok": false, "error": "thread_id is required"}));
            };
            let limit = query_parameter(query, "limit")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(20)
                .clamp(1, MAX_TELEMETRY_TURNS);
            match app_server_thread_history(&paths, &thread_id, limit) {
                Ok(turns) => {
                    if let Ok(mut state) = state.lock() {
                        state.telemetry.merge_turn_infos(turns.clone());
                        persist_turn_archive(&paths.turn_archive, &state.telemetry);
                    }
                    json_response(
                        200,
                        &json!({"ok": true, "thread_id": thread_id, "turns": turns}),
                    )
                }
                Err(err) => json_response(500, &json!({"ok": false, "error": err})),
            }
        }
        ("POST", "/turns/input") => {
            let parsed = serde_json::from_str::<Value>(body);
            match parsed {
                Ok(value) => {
                    let thread_id = value
                        .get("thread_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let turn_id = value.get("turn_id").and_then(Value::as_str);
                    let prompt = value
                        .get("prompt")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if thread_id.trim().is_empty() || prompt.trim().is_empty() {
                        return json_response(
                            400,
                            &json!({"ok": false, "error": "thread_id and prompt are required"}),
                        );
                    }
                    if let Ok(mut state) = state.lock() {
                        state
                            .telemetry
                            .record_turn_input(thread_id, turn_id, prompt);
                        persist_turn_archive(&paths.turn_archive, &state.telemetry);
                    }
                    json_response(202, &json!({"ok": true, "captured": true}))
                }
                Err(err) => json_response(400, &json!({"ok": false, "error": err.to_string()})),
            }
        }
        ("POST", "/clients/register") => match serde_json::from_str::<ClientInfo>(body) {
            Ok(client) => {
                if let Ok(mut state) = state.lock() {
                    let client_id = client.id.clone();
                    let yolo_pid = client.yolo_pid;
                    state
                        .clients
                        .retain(|id, existing| id == &client_id || existing.yolo_pid != yolo_pid);
                    state.clients.insert(client.id.clone(), client);
                }
                json_response(200, &json!({"ok": true}))
            }
            Err(err) => json_response(400, &json!({"ok": false, "error": err.to_string()})),
        },
        ("POST", "/clients/heartbeat") => {
            let parsed = serde_json::from_str::<Value>(body);
            match parsed {
                Ok(value) => {
                    let mut resume_generation = 0;
                    let mut app_server_generation = 0;
                    if let Some(id) = value.get("id").and_then(Value::as_str)
                        && let Ok(mut state) = state.lock()
                    {
                        resume_generation = state.resume_generation;
                        app_server_generation = state.app_server_generation;
                        if let Some(client) = state.clients.get_mut(id) {
                            client.updated_at = value
                                .get("updated_at")
                                .and_then(Value::as_u64)
                                .unwrap_or_else(now_secs);
                            if let Some(model) = value.get("model").and_then(Value::as_str) {
                                client.model = Some(model.to_string());
                            }
                            if let Some(service_tier) =
                                value.get("service_tier").and_then(Value::as_str)
                            {
                                client.service_tier = Some(service_tier.to_string());
                                client.fast = is_fast_tier(client.service_tier.as_deref());
                            }
                            if let Some(reasoning_effort) =
                                value.get("reasoning_effort").and_then(Value::as_str)
                            {
                                client.reasoning_effort = Some(reasoning_effort.to_string());
                            }
                            if let Some(fast) = value.get("fast").and_then(Value::as_bool) {
                                client.fast = fast;
                            }
                            client.status = value
                                .get("status")
                                .and_then(Value::as_str)
                                .unwrap_or("running")
                                .to_string();
                        }
                    }
                    json_response(
                        200,
                        &json!({
                            "ok": true,
                            "app_server_generation": app_server_generation,
                            "resume_generation": resume_generation
                        }),
                    )
                }
                Err(err) => json_response(400, &json!({"ok": false, "error": err.to_string()})),
            }
        }
        ("POST", "/clients/finish") => match serde_json::from_str::<ClientInfo>(body) {
            Ok(client) => {
                if let Ok(mut state) = state.lock() {
                    state.clients.insert(client.id.clone(), client);
                }
                json_response(200, &json!({"ok": true}))
            }
            Err(err) => json_response(400, &json!({"ok": false, "error": err.to_string()})),
        },
        ("POST", "/clients/configure") => {
            match serde_json::from_str::<ConfigureClientsRequest>(body) {
                Ok(request) if request.queue => {
                    let job_id = format!("configure-{}", now_millis());
                    let worker_state = Arc::clone(&state);
                    let worker_paths = paths.clone();
                    let worker_request = request;
                    let worker_job_id = job_id.clone();
                    thread::spawn(move || {
                        match configure_clients(worker_state, &worker_paths, worker_request) {
                            Ok(value) => eprintln!(
                                "yolo configure job {worker_job_id} completed: {}",
                                value
                                    .get("updated")
                                    .and_then(Value::as_array)
                                    .map_or(0, Vec::len)
                            ),
                            Err(err) => {
                                eprintln!("yolo configure job {worker_job_id} failed: {err}")
                            }
                        }
                    });
                    json_response(202, &json!({"ok": true, "queued": true, "job_id": job_id}))
                }
                Ok(request) => match configure_clients(Arc::clone(&state), &paths, request) {
                    Ok(value) => json_response(200, &value),
                    Err(err) => json_response(500, &json!({"ok": false, "error": err})),
                },
                Err(err) => json_response(400, &json!({"ok": false, "error": err.to_string()})),
            }
        }
        ("POST", "/clients/refresh-resume") => {
            match serde_json::from_str::<RefreshResumeRequest>(body) {
                Ok(request) => match refresh_resume_clients(Arc::clone(&state), request) {
                    Ok(value) => json_response(200, &value),
                    Err(err) => json_response(500, &json!({"ok": false, "error": err})),
                },
                Err(err) => json_response(400, &json!({"ok": false, "error": err.to_string()})),
            }
        }
        ("POST", "/clients/refresh-permissions") => {
            match serde_json::from_str::<RefreshResumeRequest>(body) {
                Ok(request) => {
                    match refresh_resume_permissions_clients(Arc::clone(&state), &paths, request) {
                        Ok(value) => json_response(200, &value),
                        Err(err) => json_response(500, &json!({"ok": false, "error": err})),
                    }
                }
                Err(err) => json_response(400, &json!({"ok": false, "error": err.to_string()})),
            }
        }
        ("POST", "/upgrade-resume-all") => {
            let request = serde_json::from_str::<UpgradeResumeAllRequest>(body).unwrap_or_default();
            let version = request.codex_version.as_deref();
            let result =
                run_codex_upgrade_resume_all_local(Arc::clone(&state), &paths, version, &request);
            match result {
                Ok(value) => json_response(200, &value),
                Err(err) => json_response(500, &json!({"ok": false, "error": err})),
            }
        }
        ("POST", "/app-server/restart") => {
            let parsed = serde_json::from_str::<Value>(body);
            match parsed {
                Ok(value) => {
                    let cwd = value.get("cwd").and_then(Value::as_str).map(PathBuf::from);
                    if let Some(invalid_cwd) = cwd
                        .as_deref()
                        .filter(|cwd| !cwd.is_absolute() || !cwd.is_dir())
                    {
                        json_response(
                            400,
                            &json!({"ok": false, "error": format!("invalid cwd: {}", invalid_cwd.display())}),
                        )
                    } else {
                        let (app_server_generation, resume_generation, app_server_pid) = state
                            .lock()
                            .map(|state| {
                                (
                                    state.app_server_generation,
                                    state.resume_generation,
                                    state.app_server_pid,
                                )
                            })
                            .unwrap_or_default();
                        let restart_state = Arc::clone(&state);
                        let restart_paths = paths.clone();
                        thread::spawn(move || {
                            thread::sleep(Duration::from_millis(50));
                            if let Err(err) = restart_tracked_app_server_with_cwd(
                                restart_state,
                                restart_paths,
                                cwd,
                            ) {
                                eprintln!("yolo server: app-server restart failed: {err}");
                            }
                        });
                        json_response(
                            202,
                            &json!({
                                "ok": true,
                                "restart_scheduled": true,
                                "app_server_pid": app_server_pid,
                                "app_server_generation": app_server_generation,
                                "resume_generation": resume_generation
                            }),
                        )
                    }
                }
                Err(err) => json_response(400, &json!({"ok": false, "error": err.to_string()})),
            }
        }
        ("POST", "/shutdown") => {
            if let Ok(state) = state.lock()
                && let Some(pid) = state.app_server_pid
            {
                terminate_pid_tree(pid, Duration::from_secs(5));
            }
            let _ = stream.write_all(json_response(200, &json!({"ok": true})).as_bytes());
            std::process::exit(0);
        }
        _ => json_response(404, &json!({"ok": false, "error": "not found"})),
    }
}

fn spawn_thread_status_monitor(state: Arc<Mutex<ServerState>>, paths: RuntimePaths) {
    thread::spawn(move || {
        let mut heal_backoff = THREAD_MONITOR_INTERVAL;
        loop {
            let listener_started = Instant::now();
            if let Err(err) = run_thread_status_event_listener(&state, &paths) {
                eprintln!("yolo server: Codex app-server status listener stopped: {err}");
                if listener_started.elapsed() >= APP_SERVER_SELF_HEAL_STABLE_AFTER {
                    heal_backoff = THREAD_MONITOR_INTERVAL;
                }
                thread::sleep(THREAD_MONITOR_INTERVAL);
                match heal_missing_app_server_after_listener_error(Arc::clone(&state), &paths) {
                    Ok(
                        AppServerSelfHeal::AlreadyReachable | AppServerSelfHeal::AdoptedExisting,
                    ) => {}
                    Ok(AppServerSelfHeal::SpawnedReplacement) => {
                        eprintln!(
                            "yolo server: Codex app-server replacement spawned; gating next rapid self-heal for {:?}",
                            heal_backoff
                        );
                        thread::sleep(heal_backoff);
                        heal_backoff = next_self_heal_backoff(heal_backoff);
                    }
                    Err(heal_err) => {
                        eprintln!(
                            "yolo server: Codex app-server self-heal failed: {heal_err}; retrying in {:?}",
                            heal_backoff
                        );
                        thread::sleep(heal_backoff);
                        heal_backoff = next_self_heal_backoff(heal_backoff);
                    }
                }
            }
        }
    });
}

enum AppServerSelfHeal {
    AlreadyReachable,
    AdoptedExisting,
    SpawnedReplacement,
}

fn heal_missing_app_server_after_listener_error(
    state: Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
) -> Result<AppServerSelfHeal, String> {
    if paths.app_server_socket.exists()
        && AppServerRpcClient::connect(&paths.app_server_socket).is_ok()
    {
        return Ok(AppServerSelfHeal::AlreadyReachable);
    }
    let existing_pids = find_app_server_pids(paths);
    if let Some(pid) = existing_pids.first().copied() {
        if let Ok(mut state) = state.lock() {
            state.app_server_pid = Some(pid);
        }
        return Ok(AppServerSelfHeal::AdoptedExisting);
    }
    eprintln!("yolo server: restarting missing Codex app-server after listener disconnect");
    spawn_tracked_app_server(state, paths.clone()).map(|_| AppServerSelfHeal::SpawnedReplacement)
}

fn next_self_heal_backoff(current: Duration) -> Duration {
    current
        .saturating_mul(2)
        .min(APP_SERVER_SELF_HEAL_MAX_BACKOFF)
}

fn run_thread_status_event_listener(
    state: &Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
) -> Result<(), String> {
    let mut client = AppServerRpcClient::connect(&paths.app_server_socket)?;
    client.initialize()?;

    let mut subscribed_thread_ids = BTreeSet::new();
    loop {
        scan_existing_yolo_clients(state);
        subscribe_running_client_threads(state, &mut client, &mut subscribed_thread_ids)?;
        client.set_read_timeout(Some(Duration::from_secs(1)))?;
        match client.read_message_value() {
            Ok(value) => observe_app_server_message(state, &value, paths),
            Err(err) if is_app_server_read_timeout(&err) => {}
            Err(err) => return Err(err),
        }
    }
}

fn observe_app_server_message(
    state: &Arc<Mutex<ServerState>>,
    value: &Value,
    paths: &RuntimePaths,
) {
    bind_thread_started_to_unique_managed_client(state, value);
    if let Some(snapshot) = parse_app_server_thread_response(value) {
        apply_single_thread_snapshot(state, &snapshot);
    }
    if let Some(update) = parse_app_server_status_notification(value) {
        apply_thread_status_update(state, &update);
    }
    let changed = if let Ok(mut state) = state.lock() {
        state.telemetry.record_app_server_event(value)
    } else {
        false
    };
    if changed && let Ok(state) = state.lock() {
        persist_turn_archive(&paths.turn_archive, &state.telemetry);
    }
}

fn bind_thread_started_to_unique_managed_client(state: &Arc<Mutex<ServerState>>, value: &Value) {
    if value.get("method").and_then(Value::as_str) != Some("thread/started") {
        return;
    }
    let Some(thread) = value.get("params").and_then(|params| params.get("thread")) else {
        return;
    };
    let Some(thread_id) = thread
        .get("id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    else {
        return;
    };
    let Some(cwd) = thread
        .get("cwd")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    else {
        return;
    };
    let Ok(mut state) = state.lock() else {
        return;
    };
    if state.clients.values().any(|client| {
        client_thread_id_is_authoritative(client) && client.thread_id.as_deref() == Some(thread_id)
    }) {
        return;
    }
    let candidates = state
        .clients
        .values()
        .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
        .filter(|client| !client_thread_id_is_authoritative(client))
        // Only clients launched through a managed app-server proxy are safe
        // to associate from a contemporaneous thread/started notification.
        .filter(|client| client_uses_managed_proxy(client) && client.cwd == cwd)
        .map(|client| client.id.clone())
        .collect::<Vec<_>>();
    if candidates.len() != 1 {
        return;
    }
    let client_id = &candidates[0];
    let Some(client) = state.clients.get_mut(client_id) else {
        return;
    };
    client.thread_id = Some(thread_id.to_string());
    client.thread_id_source = "app_server_started".to_string();
    client.updated_at = now_secs();
    eprintln!(
        "yolo server: bound managed client {} to newly started thread {}",
        client.id, thread_id
    );
}

fn client_uses_managed_proxy(client: &ClientInfo) -> bool {
    if !client.remote.trim().is_empty() {
        return true;
    }
    let Some(codex_pid) = client.codex_pid else {
        return false;
    };
    read_proc_cmdline(PathBuf::from(format!("/proc/{codex_pid}/cmdline")))
        .iter()
        .any(|arg| arg.contains("/yolo/client-proxies/") || arg.contains("/yolo/client-proxies"))
}

fn subscribe_running_client_threads(
    state: &Arc<Mutex<ServerState>>,
    client: &mut AppServerRpcClient,
    subscribed_thread_ids: &mut BTreeSet<String>,
) -> Result<(), String> {
    let mut target_thread_ids = known_running_client_thread_ids(state);
    target_thread_ids.extend(known_running_agent_thread_ids(state));
    for thread_id in target_thread_ids {
        if subscribed_thread_ids.contains(&thread_id) {
            continue;
        }
        client.send_request(
            "thread/resume",
            json!({
                "threadId": thread_id,
                "excludeTurns": true
            }),
        )?;
        subscribed_thread_ids.insert(thread_id);
    }
    Ok(())
}

fn known_running_client_thread_ids(state: &Arc<Mutex<ServerState>>) -> BTreeSet<String> {
    let Ok(state) = state.lock() else {
        return BTreeSet::new();
    };
    state
        .clients
        .values()
        .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
        .filter_map(|client| client.thread_id.as_deref())
        .map(str::trim)
        .filter(|thread_id| !thread_id.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn known_running_agent_thread_ids(state: &Arc<Mutex<ServerState>>) -> BTreeSet<String> {
    let Ok(state) = state.lock() else {
        return BTreeSet::new();
    };
    state
        .telemetry
        .threads
        .values()
        .filter(|thread| is_active_agent_status(&thread.status))
        .map(|thread| thread.thread_id.clone())
        .collect()
}

fn is_app_server_read_timeout(err: &str) -> bool {
    err.contains("WouldBlock")
        || err.contains("TimedOut")
        || err.contains("timed out")
        || err.contains("Resource temporarily unavailable")
}

fn scan_existing_yolo_clients(state: &Arc<Mutex<ServerState>>) {
    let Ok(processes) = read_process_table() else {
        return;
    };
    let now = now_secs();
    let current_pid = std::process::id();
    let mut child_codex_by_parent: BTreeMap<u32, (u32, String)> = BTreeMap::new();
    let mut live_client_pids: BTreeSet<u32> = BTreeSet::new();
    for process in &processes {
        if process.cmdline.iter().any(|arg| arg.contains("codex"))
            && process
                .cmdline
                .iter()
                .any(|arg| arg.contains("--remote") || arg.contains("codex-app-server.sock"))
        {
            child_codex_by_parent.insert(
                process.ppid,
                (process.pid, remote_from_codex_args(&process.cmdline)),
            );
        }
        if process.pid != current_pid
            && is_yolo_process(process)
            && is_yolo_client_args(&process.cmdline.iter().skip(1).cloned().collect::<Vec<_>>())
        {
            live_client_pids.insert(process.pid);
        }
    }

    let Ok(mut state) = state.lock() else {
        return;
    };
    for client in state.clients.values_mut() {
        if client.status == "running" && !live_client_pids.contains(&client.yolo_pid) {
            client.status = "exited".to_string();
            client.ended_at = Some(now);
            client.updated_at = now;
        }
    }
    for process in processes {
        if process.pid == current_pid || !is_yolo_process(&process) {
            continue;
        }
        let args = process.cmdline.iter().skip(1).cloned().collect::<Vec<_>>();
        if !is_yolo_client_args(&args) {
            continue;
        }
        let remote = child_codex_by_parent
            .get(&process.pid)
            .map(|(_, remote)| remote.clone())
            .unwrap_or_default();
        let id = client_id_from_managed_proxy_remote(&remote)
            .unwrap_or_else(|| format!("{}-scanned", process.pid));
        if state
            .clients
            .values()
            .any(|client| client.yolo_pid == process.pid && client.status == "running")
        {
            continue;
        }
        let cfg = read_codex_config();
        let launch_cfg = parse_codex_launch_config(&args);
        let service_tier = launch_cfg.service_tier.clone();
        state.clients.insert(
            id.clone(),
            ClientInfo {
                id,
                yolo_pid: process.pid,
                codex_pid: child_codex_by_parent.get(&process.pid).map(|(pid, _)| *pid),
                cwd: process.cwd.unwrap_or_else(|| String::from("")),
                args: args.clone(),
                remote,
                model: launch_cfg.model.or(cfg.model),
                service_tier: service_tier.clone(),
                reasoning_effort: launch_cfg.reasoning_effort,
                fast: is_fast_tier(service_tier.as_deref()),
                thread_id: thread_id_from_args_strs(&args),
                thread_id_source: if thread_id_from_args_strs(&args).is_some() {
                    "resume_arg".to_string()
                } else {
                    "unresolved".to_string()
                },
                started_at: now,
                updated_at: now,
                ended_at: None,
                exit_code: None,
                status: "running".to_string(),
                codex_status: None,
                codex_active_flags: Vec::new(),
                codex_status_updated_at: None,
                settings_updated_at: None,
            },
        );
    }
}

fn remote_from_codex_args(args: &[String]) -> String {
    args.iter()
        .enumerate()
        .find_map(|(index, arg)| {
            if arg == "--remote" {
                return args.get(index + 1).cloned();
            }
            arg.strip_prefix("--remote=").map(ToString::to_string)
        })
        .unwrap_or_default()
}

fn client_id_from_managed_proxy_remote(remote: &str) -> Option<String> {
    let socket_path = remote.strip_prefix("unix://")?;
    let path = Path::new(socket_path);
    if path.parent()?.file_name()?.to_str()? != CLIENT_PROXY_DIR_NAME {
        return None;
    }
    let client_id = path.file_stem()?.to_str()?;
    if client_id.is_empty()
        || !client_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return None;
    }
    Some(client_id.to_string())
}

fn spawn_initial_app_server_thread_snapshot(state: Arc<Mutex<ServerState>>, paths: RuntimePaths) {
    thread::spawn(move || match app_server_thread_snapshot(&paths, None) {
        Ok(snapshot) => apply_thread_snapshot(&state, &snapshot),
        Err(err) => eprintln!("yolo server: initial app-server thread snapshot failed: {err}"),
    });
}

fn spawn_agent_telemetry_snapshot_monitor(state: Arc<Mutex<ServerState>>, paths: RuntimePaths) {
    thread::spawn(move || {
        loop {
            match app_server_agent_thread_inventory(&paths) {
                Ok(threads) => {
                    if let Ok(mut state) = state.lock() {
                        for thread in threads {
                            state.telemetry.record_thread_value(&thread);
                        }
                    }
                }
                Err(err) => eprintln!("yolo server: agent telemetry inventory failed: {err}"),
            }
            thread::sleep(APP_SERVER_TELEMETRY_REFRESH_INTERVAL);
        }
    });
}

fn app_server_agent_thread_inventory(paths: &RuntimePaths) -> Result<Vec<Value>, String> {
    let _rpc_lease = acquire_app_server_rpc(AppServerRpcPriority::Background);
    let mut client = AppServerRpcClient::connect(&paths.app_server_socket)?;
    client.initialize()?;
    let mut cursor: Option<String> = None;
    let mut threads = Vec::new();
    for _ in 0..16 {
        let mut params = json!({
            "limit": 200,
            "sortKey": "updated_at",
            "useStateDbOnly": true,
            "sourceKinds": [
                "cli",
                "vscode",
                "exec",
                "appServer",
                "subAgent",
                "subAgentReview",
                "subAgentCompact",
                "subAgentThreadSpawn",
                "subAgentOther",
                "unknown"
            ]
        });
        if let Some(cursor_value) = cursor.as_deref() {
            params["cursor"] = Value::String(cursor_value.to_string());
        }
        let result = client.request("thread/list", params)?;
        let data = result
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("thread/list missing data: {result}"))?;
        threads.extend(data.iter().cloned());
        cursor = result
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        if cursor.is_none() {
            break;
        }
    }
    Ok(threads)
}

#[derive(Debug)]
struct ProcInfo {
    pid: u32,
    ppid: u32,
    comm: String,
    cmdline: Vec<String>,
    cwd: Option<String>,
}

fn read_process_table() -> Result<Vec<ProcInfo>, String> {
    let mut out = Vec::new();
    let entries = fs::read_dir("/proc").map_err(|err| format!("read /proc: {err}"))?;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
        let dir = entry.path();
        let cmdline = read_proc_cmdline(dir.join("cmdline"));
        let comm = fs::read_to_string(dir.join("comm"))
            .unwrap_or_default()
            .trim()
            .to_string();
        let ppid = read_proc_ppid(dir.join("stat")).unwrap_or(0);
        let cwd = fs::read_link(dir.join("cwd"))
            .ok()
            .map(|path| path.display().to_string());
        out.push(ProcInfo {
            pid,
            ppid,
            comm,
            cmdline,
            cwd,
        });
    }
    Ok(out)
}

fn read_proc_cmdline(path: PathBuf) -> Vec<String> {
    fs::read(path)
        .unwrap_or_default()
        .split(|byte| *byte == 0)
        .filter(|part| !part.is_empty())
        .map(|part| String::from_utf8_lossy(part).to_string())
        .collect()
}

fn read_proc_ppid(path: PathBuf) -> Option<u32> {
    let stat = fs::read_to_string(path).ok()?;
    let right = stat.rsplit_once(')')?.1.trim();
    right.split_whitespace().nth(1)?.parse().ok()
}

fn is_yolo_process(process: &ProcInfo) -> bool {
    if process.comm == "yolo" {
        return true;
    }
    process
        .cmdline
        .first()
        .and_then(|arg| Path::new(arg).file_name())
        .and_then(|name| name.to_str())
        == Some("yolo")
}

fn is_yolo_client_args(args: &[String]) -> bool {
    match args.first().map(String::as_str) {
        None => true,
        Some("client") | Some("resume") => true,
        Some("server" | "status" | "clients" | "stop" | "set" | "configure" | "codex") => false,
        Some("upgrade-resume" | "resume-upgrade" | "upgrade-and-resume") => false,
        Some("upgrade-resume-all" | "resume-all-upgrade") => false,
        Some("external-codex-upgrade-resume" | "upgrade-external-codex") => false,
        Some("refresh-resume" | "resume-refresh") => false,
        Some("refresh-permissions" | "permissions-refresh") => false,
        Some(arg) if arg.starts_with('-') => true,
        Some(_) => true,
    }
}

fn find_app_server_pid(paths: &RuntimePaths) -> Option<u32> {
    find_app_server_pids(paths).into_iter().next()
}

fn find_app_server_pids(paths: &RuntimePaths) -> Vec<u32> {
    let needle = paths.app_server_socket.display().to_string();
    let processes = read_process_table().unwrap_or_default();
    top_level_app_server_pids(&processes, &needle)
}

fn top_level_app_server_pids(processes: &[ProcInfo], socket_needle: &str) -> Vec<u32> {
    let app_server_pids = processes
        .iter()
        .filter(|process| is_app_server_process(process, socket_needle))
        .map(|process| process.pid)
        .collect::<BTreeSet<_>>();
    let mut pids: Vec<u32> = processes
        .iter()
        .filter(|process| is_app_server_process(process, socket_needle))
        .filter(|process| !app_server_pids.contains(&process.ppid))
        .map(|process| process.pid)
        .collect();
    pids.sort_unstable();
    pids.dedup();
    pids
}

fn is_app_server_process(process: &ProcInfo, socket_needle: &str) -> bool {
    process.cmdline.iter().any(|arg| arg == "app-server")
        && process
            .cmdline
            .iter()
            .any(|arg| arg.contains(socket_needle))
}

fn terminate_app_servers_for_socket(paths: &RuntimePaths, timeout: Duration) {
    for pid in find_app_server_pids(paths) {
        terminate_pid_tree(pid, timeout);
    }
}

fn apply_thread_snapshot(state: &Arc<Mutex<ServerState>>, snapshot: &[AppThreadSnapshot]) {
    let now = now_secs();
    let Ok(mut state) = state.lock() else {
        return;
    };
    bind_unique_active_legacy_clients(&mut state, snapshot);
    clear_conflicting_inferred_thread_ids(&mut state);

    for client in state.clients.values_mut() {
        if !matches!(client.status.as_str(), "running" | "restarting") {
            continue;
        }

        let matched = client
            .thread_id
            .as_deref()
            .filter(|_| client_thread_id_is_authoritative(client))
            .and_then(|thread_id| snapshot.iter().find(|thread| thread.id == thread_id));

        let Some(thread) = matched else {
            client.codex_status = None;
            client.codex_active_flags.clear();
            client.codex_status_updated_at = Some(now);
            continue;
        };

        client.thread_id = Some(thread.id.clone());
        client.codex_status = Some(thread.status.clone());
        client.codex_active_flags = thread.active_flags.clone();
        client.codex_status_updated_at = Some(now);
        let launch_cfg = if client.settings_updated_at.is_some() {
            CodexLaunchConfig::default()
        } else {
            parse_codex_launch_config(&client.args)
        };
        // A resumed thread may outlive the yolo process and carry a newer
        // setting than the launch command line. Treat app-server state as the
        // source of truth and use launch arguments only as a fallback.
        if let Some(model) = thread.model.clone().or_else(|| launch_cfg.model) {
            client.model = Some(model);
        }
        if let Some(service_tier) = thread
            .service_tier
            .clone()
            .or_else(|| launch_cfg.service_tier)
        {
            client.service_tier = Some(service_tier);
            client.fast = is_fast_tier(client.service_tier.as_deref());
        }
        if let Some(reasoning_effort) = thread
            .reasoning_effort
            .clone()
            .or_else(|| launch_cfg.reasoning_effort)
        {
            client.reasoning_effort = Some(reasoning_effort);
        }
    }
}

fn apply_single_thread_snapshot(state: &Arc<Mutex<ServerState>>, thread: &AppThreadSnapshot) {
    let now = now_secs();
    let Ok(mut state) = state.lock() else {
        return;
    };
    clear_conflicting_inferred_thread_ids(&mut state);

    for client in state.clients.values_mut() {
        if !matches!(client.status.as_str(), "running" | "restarting") {
            continue;
        }

        let matched = client_thread_id_is_authoritative(client)
            && client.thread_id.as_deref() == Some(thread.id.as_str());
        if !matched {
            continue;
        }

        client.thread_id = Some(thread.id.clone());
        client.codex_status = Some(thread.status.clone());
        client.codex_active_flags = thread.active_flags.clone();
        client.codex_status_updated_at = Some(now);
        client.updated_at = now;
        let launch_cfg = if client.settings_updated_at.is_some() {
            CodexLaunchConfig::default()
        } else {
            parse_codex_launch_config(&client.args)
        };
        // A resumed thread may outlive the yolo process and carry a newer
        // setting than the launch command line. Treat app-server state as the
        // source of truth and use launch arguments only as a fallback.
        if let Some(model) = thread.model.clone().or_else(|| launch_cfg.model) {
            client.model = Some(model);
        }
        if let Some(service_tier) = thread
            .service_tier
            .clone()
            .or_else(|| launch_cfg.service_tier)
        {
            client.service_tier = Some(service_tier);
            client.fast = is_fast_tier(client.service_tier.as_deref());
        }
        if let Some(reasoning_effort) = thread
            .reasoning_effort
            .clone()
            .or_else(|| launch_cfg.reasoning_effort)
        {
            client.reasoning_effort = Some(reasoning_effort);
        }
    }
}

fn clear_conflicting_inferred_thread_ids(state: &mut ServerState) {
    for client in state.clients.values_mut() {
        if !matches!(client.status.as_str(), "running" | "restarting") {
            continue;
        }
        let explicit_for_client = thread_id_from_args_strs(&client.args);
        if let Some(explicit_thread_id) = explicit_for_client {
            if client.thread_id.as_deref() != Some(explicit_thread_id.as_str()) {
                client.thread_id = Some(explicit_thread_id);
                client.thread_id_source = "resume_arg".to_string();
                clear_client_codex_thread_status(client);
            }
            continue;
        }

        if !client_thread_id_is_authoritative(client) {
            client.thread_id = None;
            client.thread_id_source = "unresolved".to_string();
            clear_client_codex_thread_status(client);
        }
    }
}

fn client_thread_id_is_authoritative(client: &ClientInfo) -> bool {
    matches!(
        client.thread_id_source.as_str(),
        "resume_arg" | "proxy" | "app_server_started" | "legacy_active_unique"
    ) && client
        .thread_id
        .as_deref()
        .is_some_and(|thread_id| !thread_id.trim().is_empty())
}

fn bind_unique_active_legacy_clients(state: &mut ServerState, snapshot: &[AppThreadSnapshot]) {
    let claimed = state
        .clients
        .values()
        .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
        .filter(|client| client_thread_id_is_authoritative(client))
        .filter_map(|client| client.thread_id.clone())
        .collect::<BTreeSet<_>>();

    let unresolved_by_cwd = state
        .clients
        .values()
        .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
        .filter(|client| !client_thread_id_is_authoritative(client))
        .fold(
            BTreeMap::<String, Vec<String>>::new(),
            |mut groups, client| {
                groups
                    .entry(client.cwd.clone())
                    .or_default()
                    .push(client.id.clone());
                groups
            },
        );

    for (cwd, client_ids) in unresolved_by_cwd {
        if client_ids.len() != 1 {
            continue;
        }
        let candidates = snapshot
            .iter()
            .filter(|thread| thread.cwd == cwd && thread.status == "active")
            .filter(|thread| !claimed.contains(&thread.id))
            .collect::<Vec<_>>();
        if candidates.len() != 1 {
            continue;
        }
        let Some(client) = state.clients.get_mut(&client_ids[0]) else {
            continue;
        };
        client.thread_id = Some(candidates[0].id.clone());
        client.thread_id_source = "legacy_active_unique".to_string();
        client.codex_status = Some(candidates[0].status.clone());
        client.codex_active_flags = candidates[0].active_flags.clone();
        client.codex_status_updated_at = Some(now_secs());
        client.updated_at = now_secs();
        eprintln!(
            "yolo server: rebound legacy client {} to unique active thread {}",
            client.id, candidates[0].id
        );
    }
}

fn clear_client_codex_thread_status(client: &mut ClientInfo) {
    client.codex_status = None;
    client.codex_active_flags.clear();
    client.codex_status_updated_at = Some(now_secs());
}

fn apply_thread_status_update(state: &Arc<Mutex<ServerState>>, update: &AppThreadStatusUpdate) {
    let now = now_secs();
    let Ok(mut state) = state.lock() else {
        return;
    };
    for client in state.clients.values_mut() {
        if !matches!(client.status.as_str(), "running" | "restarting") {
            continue;
        }
        if client.thread_id.as_deref() != Some(update.thread_id.as_str()) {
            continue;
        }
        client.codex_status = Some(update.status.clone());
        client.codex_active_flags = update.active_flags.clone();
        client.codex_status_updated_at = Some(now);
        client.updated_at = now;
    }
}

fn parse_app_server_thread_response(value: &Value) -> Option<AppThreadSnapshot> {
    let result = value.get("result")?;
    let thread = result.get("thread")?;
    let mut snapshot = parse_app_thread_snapshot(thread)?;
    apply_app_thread_settings(&mut snapshot, result);
    Some(snapshot)
}

fn parse_app_server_status_notification(value: &Value) -> Option<AppThreadStatusUpdate> {
    let method = value.get("method")?.as_str()?;
    let params = value.get("params")?;
    match method {
        "thread/status/changed" => {
            let thread_id = params.get("threadId")?.as_str()?.to_string();
            let (status, active_flags) = parse_thread_status_value(params.get("status")?)?;
            Some(AppThreadStatusUpdate {
                thread_id,
                status,
                active_flags,
            })
        }
        "turn/started" => Some(AppThreadStatusUpdate {
            thread_id: params.get("threadId")?.as_str()?.to_string(),
            status: "active".to_string(),
            active_flags: Vec::new(),
        }),
        "turn/completed" => Some(AppThreadStatusUpdate {
            thread_id: params.get("threadId")?.as_str()?.to_string(),
            status: "idle".to_string(),
            active_flags: Vec::new(),
        }),
        "thread/closed" => Some(AppThreadStatusUpdate {
            thread_id: params.get("threadId")?.as_str()?.to_string(),
            status: "notLoaded".to_string(),
            active_flags: Vec::new(),
        }),
        _ => None,
    }
}

fn parse_thread_status_value(value: &Value) -> Option<(String, Vec<String>)> {
    let status = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let active_flags = value
        .get("activeFlags")
        .and_then(Value::as_array)
        .map(|flags| {
            flags
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Some((status, active_flags))
}

fn wait_for_clients_idle(
    state: Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
    request: &UpgradeResumeAllRequest,
) -> Result<(), String> {
    let timeout = upgrade_idle_wait_timeout();
    let start = SystemTime::now();
    loop {
        let target_thread_ids = upgrade_wait_thread_ids(&state, request);
        let snapshot = app_server_thread_snapshot(paths, target_thread_ids.as_ref())?;
        apply_thread_snapshot(&state, &snapshot);
        let working_clients = working_clients_for_snapshot(&state, &snapshot, request);
        if working_clients.is_empty() {
            return Ok(());
        }
        if start.elapsed().unwrap_or_default() >= timeout {
            return Err(format!(
                "timed out waiting for Codex clients to become idle: {}",
                working_clients.join(", ")
            ));
        }
        eprintln!(
            "yolo: waiting for Codex clients to become idle before upgrade/resume: {}",
            working_clients.join(", ")
        );
        thread::sleep(UPGRADE_IDLE_POLL_INTERVAL);
    }
}

fn working_clients_for_snapshot(
    state: &Arc<Mutex<ServerState>>,
    snapshot: &[AppThreadSnapshot],
    request: &UpgradeResumeAllRequest,
) -> Vec<String> {
    let Ok(state) = state.lock() else {
        return Vec::new();
    };
    state
        .clients
        .values()
        .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
        .filter(|client| !should_ignore_upgrade_wait_client(client, request))
        .filter_map(|client| {
            let is_active = match client.thread_id.as_deref() {
                Some(thread_id) => snapshot
                    .iter()
                    .any(|thread| thread.id == thread_id && thread.status == "active"),
                None => snapshot
                    .iter()
                    .any(|thread| thread.cwd == client.cwd && thread.status == "active"),
            };
            if is_active {
                Some(format!("{} cwd={}", client.id, client.cwd))
            } else {
                None
            }
        })
        .collect()
}

fn should_ignore_upgrade_wait_client(
    client: &ClientInfo,
    request: &UpgradeResumeAllRequest,
) -> bool {
    if request.ignore_client_id.as_deref() == Some(client.id.as_str()) {
        return true;
    }
    if let Some(thread_id) = request.ignore_thread_id.as_deref()
        && client.thread_id.as_deref() == Some(thread_id)
    {
        return true;
    }
    request.ignore_cwd.as_deref() == Some(client.cwd.as_str())
}

fn upgrade_wait_thread_ids(
    state: &Arc<Mutex<ServerState>>,
    request: &UpgradeResumeAllRequest,
) -> Option<BTreeSet<String>> {
    let Ok(state) = state.lock() else {
        return None;
    };
    let mut ids = BTreeSet::new();
    let mut has_running_without_thread = false;
    for client in state.clients.values() {
        if !matches!(client.status.as_str(), "running" | "restarting") {
            continue;
        }
        if should_ignore_upgrade_wait_client(client, request) {
            continue;
        }
        if let Some(thread_id) = client.thread_id.as_deref() {
            if !thread_id.trim().is_empty() {
                ids.insert(thread_id.to_string());
            }
        } else {
            has_running_without_thread = true;
        }
    }
    if has_running_without_thread {
        return None;
    }
    Some(ids)
}

fn configure_clients(
    state: Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
    request: ConfigureClientsRequest,
) -> Result<Value, String> {
    let selected_ids = select_configure_clients(&state, &request)?;
    if selected_ids.is_empty() {
        return Err("no matching yolo clients".to_string());
    }

    let (clients, pending_clients) = selected_clients_by_thread_state(&state, &selected_ids)?;
    let mut pending = Vec::new();
    for client_id in pending_clients {
        persist_pending_client_settings(paths, &client_id, &request)?;
        note_client_settings_update(
            &state,
            &client_id,
            request.model.clone(),
            request.fast,
            request.reasoning_effort.clone(),
        );
        pending.push(json!({
            "client_id": client_id,
            "thread_id": Value::Null,
            "pending_first_turn": true,
        }));
    }
    if !pending.is_empty() {
        publish_status_event(&state, "client-settings-pending");
    }
    if clients.is_empty() {
        return Ok(json!({
            "ok": true,
            "updated": [],
            "pending": pending,
            "model": request.model,
            "fast": request.fast,
            "reasoning_effort": request.reasoning_effort,
        }));
    }
    // `thread/settings/update` is idempotent. A short-lived app-server stall
    // must not turn a valid modal action into a permanent failure, but retries
    // are bounded and never applied to a missing thread.
    let mut last_error = None;
    for attempt in 0..APP_SERVER_CONFIGURE_MAX_ATTEMPTS {
        match configure_clients_once(&state, paths, &request, &clients) {
            Ok(mut value) => {
                value["pending"] = Value::Array(pending);
                return Ok(value);
            }
            Err(err) if is_retryable_app_server_error(&err) => {
                last_error = Some(err.clone());
                if attempt + 1 < APP_SERVER_CONFIGURE_MAX_ATTEMPTS {
                    let delay =
                        APP_SERVER_CONFIGURE_RETRY_DELAY.saturating_mul((attempt + 1) as u32);
                    eprintln!(
                        "yolo configure: app-server attempt {}/{} failed: {err}; retrying in {:?}",
                        attempt + 1,
                        APP_SERVER_CONFIGURE_MAX_ATTEMPTS,
                        delay
                    );
                    thread::sleep(delay);
                }
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_error.unwrap_or_else(|| "app-server configure failed".to_string()))
}

fn configure_clients_once(
    state: &Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
    request: &ConfigureClientsRequest,
    clients: &[(String, String)],
) -> Result<Value, String> {
    // Background inventory/history/snapshot RPCs share the same app-server
    // process. Control updates get priority so a UI setting change cannot sit
    // behind telemetry work or another configuration request.
    let _rpc_lease = acquire_app_server_rpc(AppServerRpcPriority::Control);
    let mut rpc = AppServerRpcClient::connect(&paths.app_server_socket)?;
    rpc.set_rpc_timeout(configure_rpc_timeout(request));
    rpc.initialize()?;
    let mut updated = Vec::new();
    for (client_id, thread_id) in clients {
        let mut params = serde_json::Map::new();
        params.insert("threadId".to_string(), Value::String(thread_id.clone()));
        if let Some(model) = request.model.as_ref() {
            params.insert("model".to_string(), Value::String(model.clone()));
        }
        if let Some(fast) = request.fast {
            params.insert(
                "serviceTier".to_string(),
                Value::String(if fast { "priority" } else { "default" }.to_string()),
            );
        }
        if let Some(effort) = request.reasoning_effort.as_ref() {
            params.insert("effort".to_string(), Value::String(effort.clone()));
        }
        rpc.request("thread/settings/update", Value::Object(params))?;
        note_client_settings_update(
            state,
            client_id,
            request.model.clone(),
            request.fast,
            request.reasoning_effort.clone(),
        );
        publish_status_event(state, "client-settings-updated");
        updated.push(json!({
            "client_id": client_id,
            "thread_id": thread_id,
        }));
    }

    Ok(json!({
        "ok": true,
        "updated": updated,
        "model": request.model,
        "fast": request.fast,
        "reasoning_effort": request.reasoning_effort,
    }))
}

fn is_retryable_app_server_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    !lower.contains("thread not found")
        && !lower.contains("invalid")
        && (lower.contains("timed out")
            || lower.contains("resource temporarily unavailable")
            || lower.contains("connection reset")
            || lower.contains("connection refused")
            || lower.contains("websocket closed")
            || lower.contains("broken pipe")
            || lower.contains("app-server"))
}

fn configure_rpc_timeout(request: &ConfigureClientsRequest) -> Duration {
    Duration::from_secs(request.timeout_secs.unwrap_or(10).clamp(5, 30))
}

fn note_client_settings_update(
    state: &Arc<Mutex<ServerState>>,
    client_id: &str,
    model: Option<String>,
    fast: Option<bool>,
    reasoning_effort: Option<String>,
) {
    let now = now_secs();
    let Ok(mut state) = state.lock() else {
        return;
    };
    let Some(client) = state.clients.get_mut(client_id) else {
        return;
    };
    if let Some(model) = model {
        client.model = Some(model);
    }
    if let Some(fast) = fast {
        client.fast = fast;
        client.service_tier = Some(if fast { "priority" } else { "default" }.to_string());
    }
    if let Some(reasoning_effort) = reasoning_effort {
        client.reasoning_effort = Some(reasoning_effort);
    }
    client.settings_updated_at = Some(now);
    client.updated_at = now;
}

fn note_client_permissions_update(state: &Arc<Mutex<ServerState>>, client_id: &str) {
    let now = now_secs();
    let Ok(mut state) = state.lock() else {
        return;
    };
    let Some(client) = state.clients.get_mut(client_id) else {
        return;
    };
    client.settings_updated_at = Some(now);
    client.updated_at = now;
}

fn pending_client_settings_path(paths: &RuntimePaths, client_id: &str) -> Result<PathBuf, String> {
    if client_id.is_empty()
        || !client_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(format!(
            "invalid client id for pending settings: {client_id}"
        ));
    }
    Ok(paths
        .dir
        .join(CLIENT_PENDING_SETTINGS_DIR_NAME)
        .join(format!("{client_id}.json")))
}

fn persist_pending_client_settings(
    paths: &RuntimePaths,
    client_id: &str,
    request: &ConfigureClientsRequest,
) -> Result<(), String> {
    let path = pending_client_settings_path(paths, client_id)?;
    let parent = path
        .parent()
        .ok_or_else(|| format!("pending settings path has no parent: {}", path.display()))?;
    fs::create_dir_all(parent).map_err(|err| {
        format!(
            "create pending settings directory {}: {err}",
            parent.display()
        )
    })?;
    let _ = fs::set_permissions(parent, fs::Permissions::from_mode(0o700));
    let settings = PendingClientSettings {
        model: request.model.clone(),
        fast: request.fast,
        reasoning_effort: request.reasoning_effort.clone(),
    };
    let contents = serde_json::to_vec(&settings)
        .map_err(|err| format!("encode pending settings for {client_id}: {err}"))?;
    let temporary =
        path.with_extension(format!("json.{}.{}.tmp", std::process::id(), now_millis()));
    fs::write(&temporary, contents)
        .map_err(|err| format!("write pending settings {}: {err}", temporary.display()))?;
    let _ = fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600));
    fs::rename(&temporary, &path).map_err(|err| {
        let _ = fs::remove_file(&temporary);
        format!("replace pending settings {}: {err}", path.display())
    })
}

fn apply_pending_settings_to_client_info(
    client: &mut ClientInfo,
    settings: &PendingClientSettings,
) {
    if let Some(model) = settings.model.as_ref() {
        client.model = Some(model.clone());
    }
    if let Some(effort) = settings.reasoning_effort.as_ref() {
        client.reasoning_effort = Some(effort.clone());
    }
    if let Some(fast) = settings.fast {
        client.fast = fast;
        client.service_tier = Some(if fast { "priority" } else { "default" }.to_string());
    }
    client.settings_updated_at = Some(now_secs());
    client.updated_at = now_secs();
}

fn sync_applied_pending_settings(client_id: &str, settings: &PendingClientSettings) {
    let body = json!({
        "client_id": client_id,
        "model": settings.model,
        "reasoning_effort": settings.reasoning_effort,
        "fast": settings.fast,
        "timeout_secs": 10,
        "queue": false,
    });
    if let Err(err) = api_post_json("/clients/configure", &body) {
        eprintln!("yolo: sync applied first-turn settings for {client_id}: {err}");
    }
}

fn apply_pending_settings_to_turn_start(
    value: &mut Value,
    path: &Path,
) -> Option<PendingClientSettings> {
    if value.get("method").and_then(Value::as_str) != Some("turn/start") {
        return None;
    }
    let Ok(contents) = fs::read(path) else {
        return None;
    };
    let Ok(settings) = serde_json::from_slice::<PendingClientSettings>(&contents) else {
        return None;
    };
    let Some(params) = value.get_mut("params").and_then(Value::as_object_mut) else {
        return None;
    };
    if let Some(model) = settings
        .model
        .as_ref()
        .filter(|model| !model.trim().is_empty())
    {
        params.insert("model".to_string(), Value::String(model.clone()));
    }
    if let Some(effort) = settings
        .reasoning_effort
        .as_ref()
        .filter(|effort| !effort.trim().is_empty())
    {
        params.insert("effort".to_string(), Value::String(effort.clone()));
    }
    if let Some(fast) = settings.fast {
        params.insert(
            "serviceTier".to_string(),
            Value::String(if fast { "priority" } else { "default" }.to_string()),
        );
    }
    Some(settings)
}

fn select_configure_clients(
    state: &Arc<Mutex<ServerState>>,
    request: &ConfigureClientsRequest,
) -> Result<BTreeSet<String>, String> {
    let state = state
        .lock()
        .map_err(|_| "server state lock poisoned".to_string())?;
    let mut ids = BTreeSet::new();
    for client in state.clients.values() {
        if !matches!(client.status.as_str(), "running" | "restarting") {
            continue;
        }
        let matched = request.all
            || request.client_id.as_deref() == Some(client.id.as_str())
            || request
                .thread_id
                .as_deref()
                .is_some_and(|thread_id| client.thread_id.as_deref() == Some(thread_id))
            || request.cwd.as_deref() == Some(client.cwd.as_str());
        if matched {
            ids.insert(client.id.clone());
        }
    }
    Ok(ids)
}

fn selected_clients_by_thread_state(
    state: &Arc<Mutex<ServerState>>,
    selected_ids: &BTreeSet<String>,
) -> Result<(Vec<(String, String)>, Vec<String>), String> {
    let state = state
        .lock()
        .map_err(|_| "server state lock poisoned".to_string())?;
    let mut with_threads = Vec::new();
    let mut pending = Vec::new();
    for id in selected_ids {
        let client = state
            .clients
            .get(id)
            .ok_or_else(|| format!("selected client disappeared: {id}"))?;
        if let Some(thread_id) = client
            .thread_id
            .as_deref()
            .filter(|thread_id| !thread_id.trim().is_empty())
        {
            with_threads.push((id.clone(), thread_id.to_string()));
        } else if client_uses_managed_proxy(client) {
            pending.push(id.clone());
        } else {
            return Err(format!(
                "client {id} has no app-server thread id and is not using a managed proxy"
            ));
        }
    }
    Ok((with_threads, pending))
}

fn upgrade_idle_wait_timeout() -> Duration {
    env::var("YOLO_UPGRADE_IDLE_WAIT_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_UPGRADE_IDLE_WAIT_TIMEOUT)
}

fn server_info(state: &Arc<Mutex<ServerState>>, paths: &RuntimePaths) -> ServerInfo {
    let state = state.lock().expect("server state poisoned");
    let _started_at = state.started_at;
    ServerInfo {
        version: VERSION.to_string(),
        pid: std::process::id(),
        app_server_pid: state.app_server_pid,
        app_server_generation: state.app_server_generation,
        resume_generation: state.resume_generation,
        api_socket: paths.api_socket.display().to_string(),
        app_server_socket: paths.app_server_socket.display().to_string(),
        clients: state.clients.values().cloned().collect(),
        slaves: state.slaves.values().cloned().collect(),
        tmux_panes: collect_tmux_panes(),
        telemetry_summary: state.telemetry.summary(),
    }
}

fn collect_tmux_panes() -> Vec<TmuxPaneInfo> {
    let socket_name = env::var("YOLO_TMUX_SOCKET")
        .or_else(|_| env::var("WEBSH_TMUX_SOCKET_NAME"))
        .unwrap_or_else(|_| "websh".to_string());
    let output = Command::new("tmux")
        .args([
            "-L",
            &socket_name,
            "list-panes",
            "-a",
            "-F",
            "#{session_name}\t#{window_index}\t#{pane_index}\t#{pane_pid}\t#{pane_tty}\t#{pane_current_path}\t#{pane_current_command}",
        ])
        .output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .filter_map(|line| parse_tmux_pane_line(line, &socket_name))
        .collect()
}

fn parse_tmux_pane_line(line: &str, socket_name: &str) -> Option<TmuxPaneInfo> {
    let mut parts = line.split('\t');
    let session_name = nonempty_string(parts.next());
    let window_index = parts.next().and_then(|value| value.parse::<u32>().ok());
    let pane_index = parts.next().and_then(|value| value.parse::<u32>().ok());
    let pane_pid = parts.next().and_then(|value| value.parse::<u32>().ok());
    let pane_tty = nonempty_string(parts.next());
    let cwd = nonempty_string(parts.next());
    let command = nonempty_string(parts.next());
    let yolo_pid = pane_tty.as_deref().and_then(yolo_pid_for_tty);
    let codex_ui_status = if matches!(command.as_deref(), Some("yolo" | "codex")) {
        session_name
            .as_ref()
            .zip(window_index)
            .zip(pane_index)
            .and_then(|((session, window), pane)| {
                capture_codex_ui_status(socket_name, session, window, pane)
            })
    } else {
        None
    };
    Some(TmuxPaneInfo {
        session_name,
        window_index,
        pane_index,
        pane_pid,
        yolo_pid,
        cwd,
        command,
        codex_ui_status,
    })
}

fn yolo_pid_for_tty(tty: &str) -> Option<u32> {
    let tty = tty.strip_prefix("/dev/").unwrap_or(tty);
    let output = Command::new("ps")
        .args(["-t", tty, "-o", "pid=,comm="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| {
            let mut fields = line.split_whitespace();
            let pid = fields.next()?.parse::<u32>().ok()?;
            (fields.next()? == "yolo").then_some(pid)
        })
}

fn nonempty_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn capture_codex_ui_status(
    socket_name: &str,
    session_name: &str,
    window_index: u32,
    pane_index: u32,
) -> Option<CodexUiStatus> {
    let target = format!("{session_name}:{window_index}.{pane_index}");
    let output = Command::new("tmux")
        .args([
            "-L",
            socket_name,
            "capture-pane",
            "-p",
            "-J",
            "-t",
            &target,
            "-S",
            "-20",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    extract_codex_ui_status(&text)
}

fn extract_codex_ui_status(text: &str) -> Option<CodexUiStatus> {
    for line in text.lines().rev() {
        if !line.contains('·') && !line.contains('•') {
            continue;
        }
        let words = line.split_whitespace().collect::<Vec<_>>();
        for (index, model) in words.iter().enumerate() {
            let model = model.trim();
            if !model.starts_with("gpt-")
                || !model
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '.'))
            {
                continue;
            }
            let status_words = words[index + 1..]
                .iter()
                .map(|word| word.trim_matches(|ch: char| matches!(ch, ',' | '.' | '、' | '。')))
                .take_while(|word| !matches!(*word, "·" | "•" | "|" | "context" | "Context"))
                .map(str::to_ascii_lowercase)
                .take(5)
                .collect::<Vec<_>>();
            if !matches!(
                status_words.first().map(String::as_str),
                Some("low" | "medium" | "high" | "xhigh" | "max" | "ultra" | "default" | "fast")
            ) {
                continue;
            }
            let effort = status_words.iter().find(|word| {
                matches!(
                    word.as_str(),
                    "low" | "medium" | "high" | "xhigh" | "max" | "ultra" | "default"
                )
            });
            let fast = status_words.iter().any(|word| word == "fast");
            return Some(CodexUiStatus {
                model: Some(model.to_string()),
                effort: effort.filter(|value| value.as_str() != "default").cloned(),
                fast: Some(fast),
            });
        }
    }
    None
}

fn app_server_thread_snapshot(
    paths: &RuntimePaths,
    target_thread_ids: Option<&BTreeSet<String>>,
) -> Result<Vec<AppThreadSnapshot>, String> {
    let _rpc_lease = acquire_app_server_rpc(AppServerRpcPriority::Background);
    let mut client = AppServerRpcClient::connect(&paths.app_server_socket)?;
    client.initialize()?;
    let loaded = client.request(
        "thread/loaded/list",
        json!({
            "limit": 200
        }),
    )?;
    let thread_ids = loaded
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("thread/loaded/list missing data: {loaded}"))?;

    let mut threads = Vec::new();
    for thread_id in thread_ids {
        let Some(thread_id) = thread_id.as_str() else {
            continue;
        };
        if let Some(targets) = target_thread_ids
            && !targets.is_empty()
            && !targets.contains(thread_id)
        {
            continue;
        }
        let response = match client.request(
            "thread/resume",
            json!({
                "threadId": thread_id,
                "excludeTurns": true
            }),
        ) {
            Ok(response) => response,
            Err(_) => client.request(
                "thread/read",
                json!({
                    "threadId": thread_id,
                    "includeTurns": false
                }),
            )?,
        };
        if let Some(thread) = response.get("thread") {
            if let Some(mut snapshot) = parse_app_thread_snapshot(thread) {
                apply_app_thread_settings(&mut snapshot, &response);
                threads.push(snapshot);
            }
        }
    }
    Ok(threads)
}

fn app_server_thread_history(
    paths: &RuntimePaths,
    thread_id: &str,
    limit: usize,
) -> Result<Vec<TurnInfo>, String> {
    let _rpc_lease = acquire_app_server_rpc(AppServerRpcPriority::Background);
    let mut client = AppServerRpcClient::connect(&paths.app_server_socket)?;
    client.initialize()?;
    let response = client.request(
        "thread/read",
        json!({
            "threadId": thread_id,
            "includeTurns": true
        }),
    )?;
    let thread = response
        .get("thread")
        .ok_or_else(|| format!("thread/read missing thread: {response}"))?;
    Ok(parse_thread_history(thread, limit))
}

fn parse_thread_history(thread: &Value, limit: usize) -> Vec<TurnInfo> {
    let Some(turns) = thread.get("turns").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut output = turns
        .iter()
        .rev()
        .take(limit.clamp(1, MAX_TELEMETRY_TURNS))
        .filter_map(|turn| {
            let turn_id = turn.get("id").and_then(Value::as_str)?.trim();
            if turn_id.is_empty() {
                return None;
            }
            let thread_id = thread.get("id").and_then(Value::as_str)?.trim();
            if thread_id.is_empty() {
                return None;
            }
            let mut prompt = None;
            let mut last_assistant = None;
            let mut final_report = None;
            if let Some(items) = turn.get("items").and_then(Value::as_array) {
                for item in items {
                    let Some(text) = extract_message_text(item) else {
                        continue;
                    };
                    if is_user_message_item(item) {
                        append_turn_text(&mut prompt, &text);
                    } else if is_assistant_message_item(item) {
                        last_assistant = Some(text.clone());
                        if item.get("phase").and_then(Value::as_str) == Some("final_answer") {
                            final_report = Some(text);
                        }
                    }
                }
            }
            let started_at_ms = turn_timestamp_ms(turn, "startedAt", "startedAtMs");
            let completed_at_ms = turn_timestamp_ms(turn, "completedAt", "completedAtMs");
            let updated_at = completed_at_ms
                .or(started_at_ms)
                .map(|value| value / 1000)
                .unwrap_or_else(now_secs);
            let status = turn
                .get("status")
                .and_then(|status| {
                    status
                        .as_str()
                        .or_else(|| status.get("type").and_then(Value::as_str))
                })
                .unwrap_or("unknown")
                .to_string();
            Some(TurnInfo {
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                status,
                started_at_ms,
                completed_at_ms,
                prompt,
                report: final_report.or(last_assistant),
                updated_at,
            })
        })
        .collect::<Vec<_>>();
    output.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.turn_id.cmp(&right.turn_id))
    });
    output
}

fn turn_timestamp_ms(turn: &Value, seconds_key: &str, millis_key: &str) -> Option<u64> {
    turn.get(millis_key)
        .or_else(|| turn.get(seconds_key))
        .and_then(Value::as_u64)
        .map(|value| {
            if value < 10_000_000_000 {
                value.saturating_mul(1000)
            } else {
                value
            }
        })
}

fn append_turn_text(target: &mut Option<String>, text: &str) {
    if text.trim().is_empty() {
        return;
    }
    if let Some(existing) = target.as_mut() {
        existing.push('\n');
        existing.push_str(text.trim());
        *existing = bounded_turn_text(existing);
    } else {
        *target = Some(bounded_turn_text(text));
    }
}

fn update_app_server_resume_thread_settings(
    socket: &Path,
    thread_id: &str,
    cwd: &str,
) -> Result<(), String> {
    let _rpc_lease = acquire_app_server_rpc(AppServerRpcPriority::Control);
    let mut client = AppServerRpcClient::connect(socket)?;
    client.initialize()?;
    client.request(
        "thread/settings/update",
        json!({
            "threadId": thread_id,
            "cwd": cwd,
            "runtimeWorkspaceRoots": [cwd],
            "approvalPolicy": "never",
            "approvalsReviewer": "user",
            "sandboxPolicy": {
                "type": YOLO_APP_SERVER_SANDBOX_POLICY
            }
        }),
    )?;
    Ok(())
}

fn apply_app_thread_settings(snapshot: &mut AppThreadSnapshot, response: &Value) {
    if let Some(model) = response.get("model").and_then(Value::as_str) {
        snapshot.model = Some(model.to_string());
    }
    if let Some(service_tier) = response.get("serviceTier").and_then(Value::as_str) {
        snapshot.service_tier = Some(normalize_service_tier(service_tier.to_string()));
    }
    if let Some(reasoning_effort) = response.get("reasoningEffort").and_then(Value::as_str) {
        snapshot.reasoning_effort = Some(reasoning_effort.to_string());
    }
}

fn parse_app_thread_snapshot(thread: &Value) -> Option<AppThreadSnapshot> {
    let id = thread.get("id")?.as_str()?.to_string();
    let cwd = thread.get("cwd")?.as_str()?.to_string();
    let status_value = thread.get("status")?;
    let status = status_value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let active_flags = status_value
        .get("activeFlags")
        .and_then(Value::as_array)
        .map(|flags| {
            flags
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Some(AppThreadSnapshot {
        id,
        cwd,
        status,
        active_flags,
        model: None,
        service_tier: None,
        reasoning_effort: None,
    })
}

struct AppServerRpcClient {
    stream: UnixStream,
    next_id: u64,
    rpc_timeout: Duration,
}

impl AppServerRpcClient {
    fn connect(socket: &Path) -> Result<Self, String> {
        let mut stream =
            UnixStream::connect(socket).map_err(|err| format!("connect app-server: {err}"))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(|err| format!("set app-server read timeout: {err}"))?;
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .map_err(|err| format!("set app-server write timeout: {err}"))?;

        let request = concat!(
            "GET / HTTP/1.1\r\n",
            "Host: yolo\r\n",
            "Upgrade: websocket\r\n",
            "Connection: Upgrade\r\n",
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
            "Sec-WebSocket-Version: 13\r\n",
            "\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .map_err(|err| format!("write websocket handshake: {err}"))?;
        let headers = read_http_headers(&mut stream)?;
        if !headers.starts_with("HTTP/1.1 101") && !headers.starts_with("HTTP/1.0 101") {
            return Err(format!(
                "app-server websocket handshake failed: {}",
                headers.lines().next().unwrap_or_default()
            ));
        }

        Ok(Self {
            stream,
            next_id: 1,
            rpc_timeout: APP_SERVER_RPC_READ_RETRY_TIMEOUT,
        })
    }

    fn initialize(&mut self) -> Result<(), String> {
        let id = self.send_request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "yolo",
                    "title": "yolo",
                    "version": VERSION
                },
                "capabilities": {
                    "experimentalApi": true
                }
            }),
        )?;
        self.read_response_for(id)?;
        self.send_notification("initialized", json!({}))
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), String> {
        self.stream
            .set_read_timeout(timeout)
            .map_err(|err| format!("set app-server read timeout: {err}"))
    }

    fn set_rpc_timeout(&mut self, timeout: Duration) {
        self.rpc_timeout = timeout;
    }

    fn request(&mut self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.send_request(method, params)?;
        self.read_response_for(id)
    }

    fn send_request(&mut self, method: &str, params: Value) -> Result<u64, String> {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        websocket_send_text(
            &mut self.stream,
            &json!({
                "id": id,
                "method": method,
                "params": params
            })
            .to_string(),
        )?;
        Ok(id)
    }

    fn send_notification(&mut self, method: &str, params: Value) -> Result<(), String> {
        websocket_send_text(
            &mut self.stream,
            &json!({
                "method": method,
                "params": params
            })
            .to_string(),
        )
    }

    fn read_response_for(&mut self, id: u64) -> Result<Value, String> {
        loop {
            let value = self.read_message_value()?;
            if value.get("id").and_then(Value::as_u64) != Some(id) {
                continue;
            }
            if let Some(error) = value.get("error") {
                return Err(format!("app-server request {id} failed: {error}"));
            }
            return Ok(value.get("result").cloned().unwrap_or_else(|| json!({})));
        }
    }

    fn read_message_value(&mut self) -> Result<Value, String> {
        let message = websocket_read_text_with_timeout(&mut self.stream, self.rpc_timeout)?;
        serde_json::from_str(&message)
            .map_err(|err| format!("decode app-server message: {err}: {message}"))
    }
}

fn read_http_headers<R: Read>(stream: &mut R) -> Result<String, String> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        stream
            .read_exact(&mut byte)
            .map_err(|err| format!("read websocket handshake: {err}"))?;
        buf.push(byte[0]);
        if buf.len() > 16 * 1024 {
            return Err("websocket handshake headers too large".to_string());
        }
    }
    String::from_utf8(buf).map_err(|err| format!("decode websocket handshake: {err}"))
}

fn spawn_client_thread_proxy(
    paths: &RuntimePaths,
    client_id: &str,
    upstream_remote: &str,
    event_tx: mpsc::Sender<ClientEvent>,
) -> Result<ClientThreadProxy, String> {
    let upstream_socket = upstream_remote
        .strip_prefix("unix://")
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| format!("unsupported non-unix YOLO_REMOTE: {upstream_remote}"))?;
    if !upstream_socket.is_absolute() {
        return Err(format!(
            "YOLO_REMOTE socket path must be absolute: {}",
            upstream_socket.display()
        ));
    }

    let proxy_dir = paths.dir.join(CLIENT_PROXY_DIR_NAME);
    fs::create_dir_all(&proxy_dir).map_err(|err| {
        format!(
            "create client proxy directory {}: {err}",
            proxy_dir.display()
        )
    })?;
    let socket_path = proxy_dir.join(format!("{client_id}.sock"));
    let pending_settings_path = pending_client_settings_path(paths, client_id)?;
    let relay_pending_settings_path = pending_settings_path.clone();
    remove_socket_if_present(&socket_path)?;
    let listener = UnixListener::bind(&socket_path)
        .map_err(|err| format!("bind client proxy {}: {err}", socket_path.display()))?;

    thread::spawn(move || {
        let Ok((mut client_stream, _)) = listener.accept() else {
            return;
        };
        let Ok(mut upstream_stream) = UnixStream::connect(&upstream_socket) else {
            return;
        };

        let request = match read_http_headers(&mut client_stream) {
            Ok(value) => value,
            Err(_) => return,
        };
        if upstream_stream.write_all(request.as_bytes()).is_err() {
            return;
        }
        let response = match read_http_headers(&mut upstream_stream) {
            Ok(value) => value,
            Err(_) => return,
        };
        if client_stream.write_all(response.as_bytes()).is_err() {
            return;
        }

        let tracker = Arc::new(Mutex::new(ThreadBindingTracker {
            pending_create_request_ids: BTreeSet::new(),
            current_thread_id: None,
            event_tx,
        }));
        let Ok(mut client_read) = client_stream.try_clone() else {
            return;
        };
        let Ok(mut upstream_write) = upstream_stream.try_clone() else {
            return;
        };
        let client_tracker = Arc::clone(&tracker);
        let client_to_server = thread::spawn(move || {
            relay_client_websocket_frames(
                &mut client_read,
                &mut upstream_write,
                &client_tracker,
                &relay_pending_settings_path,
            );
        });
        relay_server_websocket_frames(&mut upstream_stream, &mut client_stream, &tracker);
        let _ = client_to_server.join();
    });

    Ok(ClientThreadProxy {
        remote: format!("unix://{}", socket_path.display()),
        socket_path,
        pending_settings_path,
    })
}

fn relay_client_websocket_frames(
    source: &mut UnixStream,
    target: &mut UnixStream,
    tracker: &Arc<Mutex<ThreadBindingTracker>>,
    pending_settings_path: &Path,
) {
    while let Ok(frame) = read_websocket_frame(source) {
        if frame.opcode != 0x1 {
            if target.write_all(&frame.raw).is_err() {
                return;
            }
            continue;
        }
        let Ok(mut value) = serde_json::from_slice::<Value>(&frame.payload) else {
            if target.write_all(&frame.raw).is_err() {
                return;
            }
            continue;
        };
        let pending_settings =
            apply_pending_settings_to_turn_start(&mut value, pending_settings_path);
        if let Some(settings) = pending_settings {
            let Ok(text) = serde_json::to_string(&value) else {
                return;
            };
            if websocket_send_text(target, &text).is_err() {
                return;
            }
            // The pending configuration is deliberately one-shot. Once the
            // first turn has been forwarded, later in-TUI model changes must
            // remain authoritative instead of being overwritten by YOLO.
            let _ = fs::remove_file(pending_settings_path);
            if let Ok(tracker) = tracker.lock() {
                let _ = tracker
                    .event_tx
                    .send(ClientEvent::PendingSettingsApplied(settings));
            }
        } else if target.write_all(&frame.raw).is_err() {
            return;
        }
        observe_client_app_server_request(tracker, &value);
    }
}

fn relay_server_websocket_frames(
    source: &mut UnixStream,
    target: &mut UnixStream,
    tracker: &Arc<Mutex<ThreadBindingTracker>>,
) {
    while let Ok(frame) = read_websocket_frame(source) {
        if target.write_all(&frame.raw).is_err() {
            return;
        }
        if frame.opcode != 0x1 {
            continue;
        }
        let Ok(value) = serde_json::from_slice::<Value>(&frame.payload) else {
            continue;
        };
        observe_app_server_response(tracker, &value);
    }
}

fn observe_client_app_server_request(tracker: &Arc<Mutex<ThreadBindingTracker>>, value: &Value) {
    let Some(method) = value.get("method").and_then(Value::as_str) else {
        return;
    };
    let params = value.get("params").unwrap_or(&Value::Null);
    let explicit_thread_id = params.get("threadId").and_then(Value::as_str);
    if matches!(
        method,
        "thread/resume" | "turn/start" | "thread/settings/update"
    ) && let Some(thread_id) = explicit_thread_id
    {
        note_tracked_thread_id(tracker, thread_id);
    }
    if method == "turn/start"
        && let Some(prompt) = extract_turn_prompt(params)
    {
        let tracked_thread_id = explicit_thread_id.map(ToString::to_string).or_else(|| {
            tracker
                .lock()
                .ok()
                .and_then(|tracker| tracker.current_thread_id.clone())
        });
        if let Some(thread_id) = tracked_thread_id {
            let turn_id = params
                .get("turnId")
                .and_then(Value::as_str)
                .or_else(|| params.get("turn_id").and_then(Value::as_str))
                .map(ToString::to_string);
            if let Ok(tracker) = tracker.lock() {
                let _ = tracker.event_tx.send(ClientEvent::TurnInput {
                    thread_id,
                    turn_id,
                    prompt,
                });
            }
        }
    }
    if matches!(method, "thread/start" | "thread/fork")
        && let Some(id) = app_server_message_id(value)
        && let Ok(mut tracker) = tracker.lock()
    {
        tracker.pending_create_request_ids.insert(id);
    }
}

fn observe_app_server_response(tracker: &Arc<Mutex<ThreadBindingTracker>>, value: &Value) {
    if value.get("method").and_then(Value::as_str) == Some("thread/started") {
        if let Some(thread_id) = value
            .get("params")
            .and_then(|params| params.get("thread"))
            .and_then(|thread| thread.get("id"))
            .and_then(Value::as_str)
        {
            note_tracked_thread_id(tracker, thread_id);
        }
        return;
    }
    let Some(id) = app_server_message_id(value) else {
        return;
    };
    let should_track = tracker
        .lock()
        .map(|mut tracker| tracker.pending_create_request_ids.remove(&id))
        .unwrap_or(false);
    if !should_track {
        return;
    }
    let Some(thread_id) = value
        .get("result")
        .and_then(|result| result.get("thread"))
        .and_then(|thread| thread.get("id"))
        .and_then(Value::as_str)
    else {
        return;
    };
    note_tracked_thread_id(tracker, thread_id);
}

fn app_server_message_id(value: &Value) -> Option<String> {
    let id = value.get("id")?;
    if !id.is_number() && !id.is_string() {
        return None;
    }
    serde_json::to_string(id).ok()
}

fn note_tracked_thread_id(tracker: &Arc<Mutex<ThreadBindingTracker>>, thread_id: &str) {
    let thread_id = thread_id.trim();
    if thread_id.is_empty() {
        return;
    }
    let Ok(mut tracker) = tracker.lock() else {
        return;
    };
    if tracker.current_thread_id.as_deref() == Some(thread_id) {
        return;
    }
    tracker.current_thread_id = Some(thread_id.to_string());
    let _ = tracker
        .event_tx
        .send(ClientEvent::ThreadBound(thread_id.to_string()));
}

fn read_websocket_frame<R: Read>(stream: &mut R) -> Result<WebsocketFrame, String> {
    let mut header = [0u8; 2];
    stream
        .read_exact(&mut header)
        .map_err(|err| format!("read websocket frame header: {err}"))?;
    let opcode = header[0] & 0x0f;
    let masked = (header[1] & 0x80) != 0;
    let mut raw = header.to_vec();
    let mut len = (header[1] & 0x7f) as u64;
    if len == 126 {
        let mut bytes = [0u8; 2];
        stream
            .read_exact(&mut bytes)
            .map_err(|err| format!("read websocket frame length: {err}"))?;
        len = u16::from_be_bytes(bytes) as u64;
        raw.extend_from_slice(&bytes);
    } else if len == 127 {
        let mut bytes = [0u8; 8];
        stream
            .read_exact(&mut bytes)
            .map_err(|err| format!("read websocket frame length: {err}"))?;
        len = u64::from_be_bytes(bytes);
        raw.extend_from_slice(&bytes);
    }
    if len > 16 * 1024 * 1024 {
        return Err("websocket frame too large".to_string());
    }
    let mask = if masked {
        let mut bytes = [0u8; 4];
        stream
            .read_exact(&mut bytes)
            .map_err(|err| format!("read websocket frame mask: {err}"))?;
        raw.extend_from_slice(&bytes);
        Some(bytes)
    } else {
        None
    };
    let mut payload = vec![0u8; len as usize];
    stream
        .read_exact(&mut payload)
        .map_err(|err| format!("read websocket frame payload: {err}"))?;
    raw.extend_from_slice(&payload);
    if let Some(mask) = mask {
        for (idx, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[idx % 4];
        }
    }
    Ok(WebsocketFrame {
        raw,
        opcode,
        payload,
    })
}

fn websocket_send_text<W: Write>(stream: &mut W, text: &str) -> Result<(), String> {
    let payload = text.as_bytes();
    let mut frame = Vec::with_capacity(payload.len() + 14);
    frame.push(0x81);
    if payload.len() < 126 {
        frame.push(0x80 | payload.len() as u8);
    } else if payload.len() <= u16::MAX as usize {
        frame.push(0x80 | 126);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        frame.push(0x80 | 127);
        frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    let mask = [0x79, 0x6f, 0x6c, 0x6f];
    frame.extend_from_slice(&mask);
    frame.extend(
        payload
            .iter()
            .enumerate()
            .map(|(idx, byte)| byte ^ mask[idx % 4]),
    );
    stream
        .write_all(&frame)
        .map_err(|err| format!("write websocket frame: {err}"))
}

fn websocket_send_text_unmasked<W: Write>(stream: &mut W, text: &str) -> Result<(), String> {
    let payload = text.as_bytes();
    let mut frame = Vec::with_capacity(payload.len() + 10);
    frame.push(0x81);
    if payload.len() < 126 {
        frame.push(payload.len() as u8);
    } else if payload.len() <= u16::MAX as usize {
        frame.push(126);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        frame.push(127);
        frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    frame.extend_from_slice(payload);
    stream
        .write_all(&frame)
        .map_err(|err| format!("write unmasked websocket frame: {err}"))
}

fn websocket_read_text<S: Read + Write>(stream: &mut S) -> Result<String, String> {
    websocket_read_text_with_timeout(stream, APP_SERVER_RPC_READ_RETRY_TIMEOUT)
}

fn websocket_read_text_with_timeout<S: Read + Write>(
    stream: &mut S,
    timeout: Duration,
) -> Result<String, String> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut header = [0u8; 2];
        read_exact_retry(stream, &mut header, "read websocket frame header", deadline)?;
        let opcode = header[0] & 0x0f;
        let masked = (header[1] & 0x80) != 0;
        let mut len = (header[1] & 0x7f) as u64;
        if len == 126 {
            let mut buf = [0u8; 2];
            read_exact_retry(stream, &mut buf, "read websocket frame length", deadline)?;
            len = u16::from_be_bytes(buf) as u64;
        } else if len == 127 {
            let mut buf = [0u8; 8];
            read_exact_retry(stream, &mut buf, "read websocket frame length", deadline)?;
            len = u64::from_be_bytes(buf);
        }
        if len > 16 * 1024 * 1024 {
            return Err("websocket frame too large".to_string());
        }
        let mask = if masked {
            let mut mask = [0u8; 4];
            read_exact_retry(stream, &mut mask, "read websocket frame mask", deadline)?;
            Some(mask)
        } else {
            None
        };
        let mut payload = vec![0u8; len as usize];
        read_exact_retry(
            stream,
            &mut payload,
            "read websocket frame payload",
            deadline,
        )?;
        if let Some(mask) = mask {
            for (idx, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[idx % 4];
            }
        }

        match opcode {
            0x1 => {
                return String::from_utf8(payload)
                    .map_err(|err| format!("decode websocket text: {err}"));
            }
            0x8 => return Err("app-server websocket closed".to_string()),
            0x9 => websocket_send_pong(stream, &payload)?,
            0xA => {}
            _ => {}
        }
    }
}

fn read_exact_retry<R: Read>(
    stream: &mut R,
    mut buf: &mut [u8],
    context: &str,
    deadline: Instant,
) -> Result<(), String> {
    while !buf.is_empty() {
        match stream.read(buf) {
            Ok(0) => return Err(format!("{context}: failed to fill whole buffer")),
            Ok(nread) => {
                let tmp = buf;
                buf = &mut tmp[nread..];
            }
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
                ) =>
            {
                if Instant::now() >= deadline {
                    return Err(format!("{context}: timed out waiting for app-server"));
                }
                thread::sleep(APP_SERVER_RPC_READ_RETRY_INTERVAL);
            }
            Err(err) => return Err(format!("{context}: {err}")),
        }
    }
    Ok(())
}

fn websocket_send_pong<W: Write>(stream: &mut W, payload: &[u8]) -> Result<(), String> {
    let mut frame = Vec::with_capacity(payload.len() + 6);
    frame.push(0x8A);
    frame.push(0x80 | payload.len() as u8);
    let mask = [0x70, 0x6f, 0x6e, 0x67];
    frame.extend_from_slice(&mask);
    frame.extend(
        payload
            .iter()
            .enumerate()
            .map(|(idx, byte)| byte ^ mask[idx % 4]),
    );
    stream
        .write_all(&frame)
        .map_err(|err| format!("write websocket pong: {err}"))
}

fn api_get_json(path: &str) -> Result<Value, String> {
    api_request("GET", path, None)
}

fn api_post_json(path: &str, body: &Value) -> Result<Value, String> {
    api_request("POST", path, Some(body))
}

fn api_request(method: &str, path: &str, body: Option<&Value>) -> Result<Value, String> {
    let paths = runtime_paths()?;
    let mut stream = UnixStream::connect(&paths.api_socket)
        .map_err(|err| format!("connect {}: {err}", paths.api_socket.display()))?;
    let body_text = match body {
        Some(body) => serde_json::to_string(body).map_err(|err| err.to_string())?,
        None => String::new(),
    };
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: yolo\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body_text.len(),
        body_text
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|err| format!("write request: {err}"))?;
    stream
        .shutdown(std::net::Shutdown::Write)
        .map_err(|err| format!("shutdown request: {err}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|err| format!("read response: {err}"))?;
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .ok_or_else(|| "invalid api response".to_string())?;
    serde_json::from_str(body).map_err(|err| format!("decode api response: {err}: {body}"))
}

fn federation_post_json(
    base_url: &str,
    path: &str,
    bearer_token: Option<&str>,
    body: &Value,
) -> Result<Value, String> {
    let url = format!(
        "{}{}",
        base_url.trim_end_matches('/'),
        if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        }
    );
    let body_text = serde_json::to_string(body).map_err(|err| err.to_string())?;
    let mut command = Command::new("curl");
    command
        .arg("-fsS")
        .arg("-X")
        .arg("POST")
        .arg("-H")
        .arg("Content-Type: application/json");
    if let Some(token) = bearer_token.filter(|token| !token.trim().is_empty()) {
        command
            .arg("-H")
            .arg(format!("Authorization: Bearer {}", token.trim()));
    }
    let output = command
        .arg("--data-binary")
        .arg(body_text)
        .arg(url)
        .output()
        .map_err(|err| format!("spawn curl: {err}"))?;
    if !output.status.success() {
        return Err(format!(
            "curl exited with {}: {}",
            output
                .status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "signal".to_string()),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let text = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(&text).map_err(|err| format!("decode federation response: {err}: {text}"))
}

fn read_http_request<R: Read>(
    stream: &mut R,
) -> Result<(String, String, BTreeMap<String, String>, String), String> {
    let mut buf = Vec::new();
    let mut tmp = [0_u8; 4096];
    let header_end = loop {
        let n = stream
            .read(&mut tmp)
            .map_err(|err| format!("read request: {err}"))?;
        if n == 0 {
            return Err("connection closed before headers".to_string());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_header_end(&buf) {
            break pos;
        }
        if buf.len() > 1024 * 1024 {
            return Err("request headers too large".to_string());
        }
    };
    let headers_bytes = &buf[..header_end];
    let mut body_bytes = buf[header_end + 4..].to_vec();
    let headers_text = String::from_utf8(headers_bytes.to_vec())
        .map_err(|err| format!("decode headers: {err}"))?;
    let mut lines = headers_text.lines();
    let request_line = lines
        .next()
        .ok_or_else(|| "missing request line".to_string())?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let mut headers = BTreeMap::new();
    for line in lines {
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    let content_length = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    while body_bytes.len() < content_length {
        let n = stream
            .read(&mut tmp)
            .map_err(|err| format!("read request body: {err}"))?;
        if n == 0 {
            break;
        }
        body_bytes.extend_from_slice(&tmp[..n]);
    }
    body_bytes.truncate(content_length);
    let body = String::from_utf8(body_bytes).map_err(|err| format!("decode body: {err}"))?;
    Ok((method, path, headers, body))
}

fn query_parameter(query: &str, wanted: &str) -> Option<String> {
    query.split('&').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        (key == wanted).then(|| value.to_string())
    })
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}

fn json_response<T: Serialize>(status: u16, body: &T) -> String {
    let body = serde_json::to_string(body).unwrap_or_else(|_| "{\"ok\":false}".to_string());
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Error",
    };
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}

fn load_turn_archive(path: &Path, telemetry: &mut AgentTelemetry) {
    let Ok(contents) = fs::read_to_string(path) else {
        return;
    };
    for line in contents.lines() {
        let Ok(info) = serde_json::from_str::<TurnInfo>(line) else {
            continue;
        };
        let record = turn_record_from_info(info);
        telemetry.turns.insert(record.key.clone(), record);
    }
    telemetry.trim_turns();
}

fn persist_turn_archive(path: &Path, telemetry: &AgentTelemetry) {
    if !turn_capture_enabled() {
        return;
    }
    let Some(parent) = path.parent() else {
        return;
    };
    if let Err(err) = fs::create_dir_all(parent) {
        eprintln!(
            "yolo: create turn archive directory {}: {err}",
            parent.display()
        );
        return;
    }
    let snapshot = telemetry.turns_snapshot(None, MAX_TELEMETRY_TURNS);
    let mut contents = String::new();
    for turn in snapshot.turns {
        let Ok(line) = serde_json::to_string(&turn) else {
            continue;
        };
        contents.push_str(&line);
        contents.push('\n');
    }
    let temporary = path.with_extension("jsonl.tmp");
    if let Err(err) = fs::write(&temporary, contents) {
        eprintln!("yolo: write turn archive {}: {err}", temporary.display());
        return;
    }
    let _ = fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600));
    if let Err(err) = fs::rename(&temporary, path) {
        eprintln!("yolo: replace turn archive {}: {err}", path.display());
        let _ = fs::remove_file(&temporary);
    }
}

#[derive(Debug)]
struct CodexConfig {
    model: Option<String>,
    service_tier: Option<String>,
}

fn read_codex_config() -> CodexConfig {
    let path = codex_config_path();
    let contents = fs::read_to_string(path).unwrap_or_default();
    CodexConfig {
        model: parse_toml_string(&contents, "model"),
        service_tier: parse_toml_string(&contents, "service_tier").map(normalize_service_tier),
    }
}

fn parse_toml_string(contents: &str, key: &str) -> Option<String> {
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') || !trimmed.starts_with(key) {
            continue;
        }
        let Some((left, right)) = trimmed.split_once('=') else {
            continue;
        };
        if left.trim() != key {
            continue;
        }
        let value = right.trim().trim_matches('"').trim();
        if !value.is_empty() {
            return Some(value.to_string());
        }
    }
    None
}

fn parse_codex_launch_config(args: &[String]) -> CodexLaunchConfig {
    let mut config = CodexLaunchConfig::default();
    let mut iter = args.iter().peekable();
    while let Some(arg) = iter.next() {
        if arg == "--model" || arg == "-m" {
            if let Some(value) = iter.next() {
                config.model = Some(value.to_string());
            }
            continue;
        }
        if let Some(value) = arg.strip_prefix("--model=") {
            config.model = Some(value.to_string());
            continue;
        }
        let item = if arg == "-c" || arg == "--config" {
            iter.next().map(String::as_str)
        } else if let Some(value) = arg.strip_prefix("--config=") {
            Some(value)
        } else {
            None
        };
        let Some(item) = item else {
            continue;
        };
        let Some((key, raw_value)) = item.split_once('=') else {
            continue;
        };
        let value = unquote_config_value(raw_value.trim());
        match key.trim() {
            "model" => config.model = Some(value),
            "service_tier" => config.service_tier = Some(normalize_service_tier(value)),
            "model_reasoning_effort" => config.reasoning_effort = Some(value),
            _ => {}
        }
    }
    config
}

fn unquote_config_value(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        if (bytes[0] == b'"' && bytes[trimmed.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[trimmed.len() - 1] == b'\'')
        {
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
}

fn normalize_service_tier(service_tier: String) -> String {
    if service_tier == "fast" {
        "priority".to_string()
    } else {
        service_tier
    }
}

fn is_fast_tier(service_tier: Option<&str>) -> bool {
    matches!(service_tier, Some("fast" | "priority"))
}

fn codex_config_path() -> PathBuf {
    env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".codex")))
        .unwrap_or_else(|| PathBuf::from(".codex"))
        .join("config.toml")
}

fn codex_executable() -> OsString {
    if let Some(codex) = env::var_os("YOLO_CODEX") {
        return codex;
    }
    let managed = managed_codex_bin();
    if managed.exists() {
        return managed.into_os_string();
    }
    OsString::from(DEFAULT_CODEX)
}

fn native_codex_executable() -> OsString {
    env::var_os("YOLO_NATIVE_CODEX").unwrap_or_else(|| OsString::from(DEFAULT_CODEX))
}

fn managed_codex_prefix() -> PathBuf {
    let base = env::var_os("YOLO_CODEX_PREFIX")
        .map(PathBuf::from)
        .or_else(|| {
            env::var_os("XDG_DATA_HOME").map(|dir| PathBuf::from(dir).join(RUNTIME_DIR_NAME))
        })
        .or_else(|| {
            env::var_os("HOME").map(|home| {
                PathBuf::from(home)
                    .join(".local/share")
                    .join(RUNTIME_DIR_NAME)
            })
        })
        .unwrap_or_else(|| PathBuf::from("/tmp").join(RUNTIME_DIR_NAME));
    if env::var_os("YOLO_CODEX_PREFIX").is_some() {
        base
    } else {
        base.join(MANAGED_CODEX_DIR_NAME)
    }
}

fn managed_codex_bin() -> PathBuf {
    managed_codex_prefix().join("bin").join("codex")
}

fn runtime_paths() -> Result<RuntimePaths, String> {
    let base = env::var_os("YOLO_RUNTIME_DIR")
        .map(PathBuf::from)
        .or_else(|| env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    let dir = if env::var_os("YOLO_RUNTIME_DIR").is_some() {
        base
    } else {
        base.join(RUNTIME_DIR_NAME)
    };
    Ok(RuntimePaths {
        api_socket: dir.join(API_SOCKET_NAME),
        app_server_socket: dir.join("app-server").join(APP_SERVER_SOCKET_NAME),
        pid_file: dir.join(PID_FILE_NAME),
        log_file: dir.join("server.log"),
        turn_archive: dir.join(TURN_ARCHIVE_FILE_NAME),
        dir,
    })
}

fn remove_socket_if_present(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("remove {}: {err}", path.display())),
    }
}

fn hostname() -> Option<String> {
    fs::read_to_string("/etc/hostname")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            Command::new("hostname")
                .output()
                .ok()
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn print_help() {
    println!(
        "\
yolo {VERSION}

Launch Codex through a yolo-managed app-server in YOLO mode with web search enabled.

Usage:
  yolo [CODEX_ARGS...]
  yolo client [CODEX_ARGS...]
  yolo codex [CODEX_ARGS...]
  yolo upgrade-resume [--last|SESSION_ID|RESUME_ARGS...]
  yolo upgrade-resume-all
  yolo set --all|--client ID|--thread THREAD_ID|--cwd DIR [--model MODEL] [--effort EFFORT] [--fast-on|--fast-off]
  yolo refresh-permissions --all|--client ID|--thread THREAD_ID|--cwd DIR
  yolo server [--daemon|--foreground] [--federation-listen ADDR]
  yolo status
  yolo turns [--thread THREAD_ID] [--limit N] [--history]
  yolo stop

Default client command:
  codex --remote unix://$YOLO_RUNTIME_DIR/app-server/codex-app-server.sock --search --dangerously-bypass-approvals-and-sandbox [CODEX_ARGS...]

The client keeps Codex stdio attached to the terminal and reports its process,
model, service_tier, fast state, and app-server thread status to the yolo
server API.

yolo codex is an emergency escape hatch for yolo server/app-server trouble. It
execs the native Codex CLI directly, passes through all following arguments,
and only adds YOLO mode flags plus cwd/resume metadata repair. It does not use
the yolo server or remote app-server.

upgrade-resume installs the latest Codex CLI into a yolo-managed
user-writable npm prefix, restarts the yolo app-server, then launches
`codex resume` through yolo. With no arguments it resumes `--last`.

upgrade-resume-all asks the running yolo server to install the latest Codex
CLI, wait for active app-server threads to become idle, restart its app-server
child, and request every live yolo client wrapper to restart its Codex child as
`codex resume` on the same terminal.

refresh-permissions reapplies YOLO-mode live settings to already-loaded resume
threads without restarting the yolo client or Codex child process.

When run from inside Codex, upgrade-resume-all uses Phoenix mode: it excludes
the caller's CODEX_THREAD_ID from the idle wait, then lets the final resume
generation revive that same session.

API:
  curl --unix-socket $XDG_RUNTIME_DIR/yolo/api.sock http://yolo/clients
  curl --unix-socket $XDG_RUNTIME_DIR/yolo/api.sock 'http://yolo/turns?limit=20'
  yolo turns --thread THREAD_ID --limit 20
  yolo turns --history --thread THREAD_ID --limit 20
  curl -X POST --unix-socket $XDG_RUNTIME_DIR/yolo/api.sock http://yolo/upgrade-resume-all

Federation:
  yolo server --daemon --federation-listen 127.0.0.1:47040
  YOLO_MASTER_URL=https://agent-gate/.../@localhost:47040 \
    YOLO_SLAVE_ID=slave YOLO_MASTER_BEARER_TOKEN=agt_... yolo server --daemon
  curl -X POST http://127.0.0.1:47040/federation/slaves/slave/commands \
    -d '{{\"action\":\"configure-clients\",\"configure\":{{\"all\":true,\"model\":\"gpt-5.5\",\"reasoning_effort\":\"medium\",\"fast\":false}}}}'

Federation authentication and HTTPS are delegated to agent-gate fine grained
tokens. yolo only serves localhost HTTP and sends the optional Bearer token to
the configured master URL.

Environment:
  YOLO_CODEX        Codex executable to run (default: codex)
  YOLO_NATIVE_CODEX Native Codex executable for `yolo codex`
  YOLO_CODEX_UPGRADE_COMMAND
                    Override upgrade command
  YOLO_CODEX_PREFIX Managed Codex npm prefix
  YOLO_REMOTE       Override app-server endpoint for the client
  YOLO_RUNTIME_DIR  Runtime dir for sockets (default: $XDG_RUNTIME_DIR/yolo or /tmp/yolo)
  YOLO_TURN_CAPTURE Enable turn prompt/report capture (default: on; set off to disable)
  YOLO_UPGRADE_IDLE_WAIT_TIMEOUT_SECS
                    Max seconds to wait for working clients before upgrade
  YOLO_FEDERATION_LISTEN
                    Default master federation listen address
  YOLO_MASTER_URL, YOLO_SLAVE_ID
                    Slave connector settings
  YOLO_MASTER_BEARER_TOKEN
                    Optional Bearer token sent to master URL
  YOLO_SELF_UPGRADE_COMMAND
                    Override remote yolo-upgrade command
"
    );
}
