use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned, pki_types::ServerName};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, ErrorKind, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
const DEFAULT_UPGRADE_IDLE_WAIT_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const UPGRADE_REEXEC_PERMIT_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const UPGRADE_REEXEC_ACTIVE_TIMEOUT_SECS: u64 = 120;
const UPGRADE_MEMORY_BASE_RESERVE_MIB: u64 = 2048;
const UPGRADE_MEMORY_PER_CLIENT_RESERVE_MIB: u64 = 256;
const UPGRADE_MIN_SWAP_FREE_MIB: u64 = 512;
// Polling remains a compatibility fallback for old/proxied masters, but it
// must not add a full second of user-visible latency to every federation
// command when the WebSocket push path is unavailable.
const FEDERATION_POLL_INTERVAL: Duration = Duration::from_millis(100);
// Federation polling is a liveness path.  A proxy/TLS peer can leave the
// child process connected forever, so this must be bounded independently of
// the normal app-server RPC timeout.
const FEDERATION_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const FEDERATION_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const APP_SERVER_RPC_READ_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const APP_SERVER_RPC_READ_RETRY_INTERVAL: Duration = Duration::from_millis(25);
const APP_SERVER_RPC_BACKGROUND_GATE_TIMEOUT: Duration = Duration::from_secs(5);
// A control update is user-visible and may legitimately wait for one
// background inventory page or another control update to finish. Five
// seconds was shorter than the existing RPC timeout and surfaced a false
// "gate busy" failure during a burst of widget settings updates.
const APP_SERVER_RPC_CONTROL_GATE_TIMEOUT: Duration = Duration::from_secs(30);
const APP_SERVER_BACKGROUND_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const API_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CLIENT_RECOVERY_RETRY_DELAY: Duration = Duration::from_secs(1);
const CLIENT_CHILD_RESTART_TIMEOUT: Duration = Duration::from_secs(5);
const CLIENT_RESTART_DEBOUNCE: Duration = Duration::from_secs(2);
const UPGRADE_REEXEC_RETRY_DELAY: Duration = Duration::from_secs(5);
const APP_SERVER_CONFIGURE_MAX_ATTEMPTS: usize = 3;
const APP_SERVER_CONFIGURE_RETRY_DELAY: Duration = Duration::from_secs(1);
const APP_SERVER_READY_TIMEOUT: Duration = Duration::from_secs(180);
// Resume policy is applied once after the client's thread/resume bootstrap
// succeeds.  Repeating settings/update while TUI bootstrap or turn/start is
// in flight can monopolize the shared app-server and turn a recoverable
// application error into a transport failure.
const RESUME_POLICY_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const APP_SERVER_STATUS_SUBSCRIPTION_GRACE: Duration = Duration::from_secs(5);
const APP_SERVER_SELF_HEAL_STABLE_AFTER: Duration = Duration::from_secs(60);
const APP_SERVER_SELF_HEAL_MAX_BACKOFF: Duration = Duration::from_secs(60);
const CLIENT_PROXY_DIR_NAME: &str = "client-proxies";
const CLIENT_PENDING_SETTINGS_DIR_NAME: &str = "client-pending-settings";
const ACTIVE_SESSIONS_FILE_NAME: &str = "active-sessions.json";
const ACTIVE_SESSIONS_FILE_VERSION: u32 = 2;
const DEFAULT_CONFIGURATION_FILE_NAME: &str = "default-configuration.json";
const TURN_ARCHIVE_FILE_NAME: &str = "turns.jsonl";
const APP_SERVER_TELEMETRY_REFRESH_INTERVAL: Duration = Duration::from_secs(15);
const APP_SERVER_TELEMETRY_MAX_PAGES: usize = 11;
const MAX_TELEMETRY_THREADS: usize = 2048;
const MAX_TELEMETRY_TOOL_CALLS: usize = 512;
const MAX_TELEMETRY_HOOK_RUNS: usize = 512;
const MAX_TELEMETRY_TURNS: usize = 512;
const MAX_TURN_TEXT_BYTES: usize = 16 * 1024;
const MAX_PENDING_TURN_INPUTS: usize = 128;
const MAX_PENDING_TURN_INPUT_AGE_SECS: u64 = 600;
const MAX_SESSION_REPAIR_LINE_BYTES: usize = 4 * 1024 * 1024;
const MAX_TURN_ARCHIVE_LINE_BYTES: usize = 4 * 1024 * 1024;
const MAX_API_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_API_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
// tmux 3.4 places every pane in a transient user scope. Apply the same
// bounded budget to a managed client scope so the client-side Codex/tools
// tree cannot bypass the yolo.service budget.
const CLIENT_SCOPE_MEMORY_HIGH: &str = "6G";
const CLIENT_SCOPE_MEMORY_MAX: &str = "8G";
const CLIENT_SCOPE_MEMORY_SWAP_MAX: &str = "1G";

static UPGRADE_RESUME_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug)]
struct RuntimePaths {
    dir: PathBuf,
    api_socket: PathBuf,
    app_server_socket: PathBuf,
    pid_file: PathBuf,
    log_file: PathBuf,
    turn_archive: PathBuf,
    active_sessions: PathBuf,
    default_configuration: PathBuf,
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
    #[serde(default)]
    fast_known: bool,
    #[serde(default)]
    settings_source: String,
    #[serde(default)]
    settings_observed_at: Option<u64>,
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
    #[serde(default)]
    settings_reconfigure_generation: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ClientResumeSettings {
    thread_id: Option<String>,
    model: Option<String>,
    service_tier: Option<String>,
    reasoning_effort: Option<String>,
    settings_source: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ActiveSessionRecord {
    client_id: String,
    cwd: String,
    args: Vec<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    service_tier: Option<String>,
    #[serde(default)]
    reasoning_effort: Option<String>,
    #[serde(default)]
    fast: bool,
    #[serde(default)]
    fast_known: bool,
    #[serde(default)]
    settings_complete: bool,
    #[serde(default)]
    settings_source: String,
    #[serde(default)]
    settings_observed_at: Option<u64>,
    #[serde(default)]
    thread_id: Option<String>,
    #[serde(default)]
    thread_id_source: String,
    started_at: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ActiveSessionsFile {
    #[serde(default = "active_sessions_file_version")]
    version: u32,
    #[serde(default)]
    saved_at: u64,
    #[serde(default)]
    sessions: Vec<ActiveSessionRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct YoloDefaultConfiguration {
    model: String,
    reasoning_effort: String,
    fast: bool,
}

fn active_sessions_file_version() -> u32 {
    ACTIVE_SESSIONS_FILE_VERSION
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
    saved_sessions: Vec<ActiveSessionRecord>,
    #[serde(default)]
    slaves: Vec<SlaveInfo>,
    #[serde(default)]
    tmux_panes: Vec<TmuxPaneInfo>,
    #[serde(default)]
    telemetry_summary: TelemetrySummary,
    #[serde(default)]
    default_configuration: Option<YoloDefaultConfiguration>,
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
    #[serde(default)]
    pane_id: Option<String>,
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
    active_sessions: BTreeMap<String, ActiveSessionRecord>,
    default_configuration: Option<YoloDefaultConfiguration>,
    slaves: BTreeMap<String, SlaveInfo>,
    telemetry: AgentTelemetry,
    #[allow(dead_code)]
    federation_push_senders: BTreeMap<String, mpsc::Sender<Value>>,
    status_event_senders: BTreeMap<u64, mpsc::Sender<Value>>,
    next_status_event_id: u64,
    upgrade_reexec_queue: VecDeque<u32>,
    upgrade_reexec_active: Option<UpgradeReexecPermit>,
}

#[derive(Clone, Debug)]
struct UpgradeReexecPermit {
    yolo_pid: u32,
    claimed_at: u64,
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

fn acquire_app_server_rpc(priority: AppServerRpcPriority) -> Result<AppServerRpcLease, String> {
    let gate = app_server_rpc_gate();
    let mut state = gate.state.lock().expect("app-server RPC gate poisoned");
    let wait_timeout = match priority {
        AppServerRpcPriority::Control => APP_SERVER_RPC_CONTROL_GATE_TIMEOUT,
        AppServerRpcPriority::Background => APP_SERVER_RPC_BACKGROUND_GATE_TIMEOUT,
    };
    let deadline = Instant::now() + wait_timeout;
    if priority == AppServerRpcPriority::Control {
        state.control_waiters = state.control_waiters.saturating_add(1);
    }
    loop {
        let blocked = state.active
            || (priority == AppServerRpcPriority::Background && state.control_waiters > 0);
        if !blocked {
            if priority == AppServerRpcPriority::Control {
                state.control_waiters = state.control_waiters.saturating_sub(1);
            }
            state.active = true;
            drop(state);
            return Ok(AppServerRpcLease { gate });
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            if priority == AppServerRpcPriority::Control {
                state.control_waiters = state.control_waiters.saturating_sub(1);
            }
            return Err(format!("app-server RPC gate busy for {:?}", wait_timeout));
        }
        let (next_state, wait_result) = gate
            .changed
            .wait_timeout(state, remaining)
            .expect("app-server RPC gate poisoned");
        state = next_state;
        if wait_result.timed_out() {
            let still_blocked = state.active
                || (priority == AppServerRpcPriority::Background && state.control_waiters > 0);
            if still_blocked {
                if priority == AppServerRpcPriority::Control {
                    state.control_waiters = state.control_waiters.saturating_sub(1);
                }
                return Err(format!("app-server RPC gate busy for {:?}", wait_timeout));
            }
        }
    }
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
    default_configuration: Option<YoloDefaultConfiguration>,
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
    /// Monotonic order for trace items across all kinds within a turn.
    /// This is intentionally process-local; archived entries carry their
    /// assigned sequence so the ordering survives a server restart.
    trace_sequence: u64,
    agent_message_phases: BTreeMap<String, String>,
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
struct TraceEntry {
    #[serde(default)]
    item_id: Option<String>,
    #[serde(default)]
    text: String,
    /// Turn-local capture order. Older archives omit this field and decode as
    /// zero, which remains supported by the client fallback ordering.
    #[serde(default)]
    sequence: u64,
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
    #[serde(default, alias = "report")]
    result: Option<String>,
    /// User-visible assistant progress (agent messages with phase=commentary).
    #[serde(default)]
    commentary: Option<String>,
    /// Individual commentary items/delta streams, in capture order.
    #[serde(default)]
    commentary_entries: Vec<TraceEntry>,
    /// Public reasoning summaries emitted by Codex, when the model provides them.
    #[serde(default)]
    reasoning_summary: Option<String>,
    /// Individual reasoning summary items/delta streams, in capture order.
    #[serde(default)]
    reasoning_summary_entries: Vec<TraceEntry>,
    /// Raw reasoning text emitted by Codex, when the model provides it.
    #[serde(default)]
    reasoning_raw: Option<String>,
    /// Individual raw reasoning items/delta streams, in capture order.
    #[serde(default)]
    reasoning_raw_entries: Vec<TraceEntry>,
    /// Codex plan snapshots/items emitted during the turn, in capture order.
    #[serde(default)]
    plan: Option<String>,
    /// Individual plan snapshots/items/delta streams, in capture order.
    #[serde(default)]
    plan_entries: Vec<TraceEntry>,
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
    #[serde(default, alias = "report")]
    result: Option<String>,
    #[serde(default)]
    commentary: Option<String>,
    #[serde(default)]
    commentary_entries: Vec<TraceEntry>,
    #[serde(default)]
    reasoning_summary: Option<String>,
    #[serde(default)]
    reasoning_summary_entries: Vec<TraceEntry>,
    #[serde(default)]
    reasoning_raw: Option<String>,
    #[serde(default)]
    reasoning_raw_entries: Vec<TraceEntry>,
    #[serde(default)]
    plan: Option<String>,
    #[serde(default)]
    plan_entries: Vec<TraceEntry>,
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
    captured_commentary_count: usize,
    captured_reasoning_summary_count: usize,
    captured_reasoning_raw_count: usize,
    #[serde(default)]
    captured_plan_count: usize,
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
    fn next_trace_sequence(&mut self) -> u64 {
        self.trace_sequence = self.trace_sequence.saturating_add(1);
        self.trace_sequence
    }

    fn observe_trace_sequence(&mut self, record: &TurnRecord) {
        self.trace_sequence = self.trace_sequence.max(max_trace_sequence(record));
    }

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
                .filter(|turn| turn.result.is_some())
                .count(),
            captured_commentary_count: self
                .turns
                .values()
                .filter(|turn| turn.commentary.is_some())
                .count(),
            captured_reasoning_summary_count: self
                .turns
                .values()
                .filter(|turn| turn.reasoning_summary.is_some())
                .count(),
            captured_reasoning_raw_count: self
                .turns
                .values()
                .filter(|turn| turn.reasoning_raw.is_some())
                .count(),
            captured_plan_count: self
                .turns
                .values()
                .filter(|turn| turn.plan.is_some())
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
            let mut record = turn_record_from_info(info);
            if let Some(existing) = self.turns.get(&record.key) {
                if record.status == "unknown" && existing.status != "unknown" {
                    record.status = existing.status.clone();
                }
                record.started_at_ms = record.started_at_ms.or(existing.started_at_ms);
                record.completed_at_ms = record.completed_at_ms.or(existing.completed_at_ms);
                record.prompt = record.prompt.or_else(|| existing.prompt.clone());
                record.result = record.result.or_else(|| existing.result.clone());
                let existing_commentary = trace_entries_with_legacy(
                    &existing.commentary_entries,
                    existing.commentary.as_ref(),
                );
                merge_trace_entries(&mut record.commentary_entries, &existing_commentary);
                record.commentary = trace_entries_text(&record.commentary_entries);
                let existing_reasoning_summary = trace_entries_with_legacy(
                    &existing.reasoning_summary_entries,
                    existing.reasoning_summary.as_ref(),
                );
                merge_trace_entries(
                    &mut record.reasoning_summary_entries,
                    &existing_reasoning_summary,
                );
                record.reasoning_summary = trace_entries_text(&record.reasoning_summary_entries);
                let existing_reasoning_raw = trace_entries_with_legacy(
                    &existing.reasoning_raw_entries,
                    existing.reasoning_raw.as_ref(),
                );
                merge_trace_entries(&mut record.reasoning_raw_entries, &existing_reasoning_raw);
                record.reasoning_raw = trace_entries_text(&record.reasoning_raw_entries);
                let existing_plan_entries =
                    trace_entries_with_legacy(&existing.plan_entries, existing.plan.as_ref());
                merge_trace_entries(&mut record.plan_entries, &existing_plan_entries);
                record.plan = trace_entries_text(&record.plan_entries);
                record.updated_at = record.updated_at.max(existing.updated_at);
            }
            self.observe_trace_sequence(&record);
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
        let sequence = self.next_trace_sequence();
        let record = self.ensure_turn(thread_id, &turn_id);
        ensure_turn_trace_entries(record);
        if is_user_message_item(item) {
            record.prompt = Some(text.clone());
        } else if is_commentary_message_item(item) {
            let item_id = item.get("id").and_then(Value::as_str);
            set_trace_entry(&mut record.commentary_entries, item_id, &text, sequence);
            sync_legacy_trace(&mut record.commentary, &record.commentary_entries);
        } else if is_assistant_message_item(item) {
            record.result = Some(text);
        }
        record.updated_at = now_secs();
        self.last_event_at = Some(now_secs());
        self.trim_turns();
    }

    fn record_reasoning_item(&mut self, thread_id: &str, turn_id: &str, item: &Value) -> bool {
        if !turn_capture_enabled() {
            return false;
        }
        let item_id = item.get("id").and_then(Value::as_str);
        let summary = item
            .get("summary")
            .and_then(extract_message_text)
            .filter(|text| !text.is_empty());
        let raw = item
            .get("content")
            .and_then(extract_message_text)
            .filter(|text| !text.is_empty());
        if summary.is_none() && raw.is_none() {
            return false;
        }
        let sequence = self.next_trace_sequence();
        let record = self.ensure_turn(thread_id, turn_id);
        ensure_turn_trace_entries(record);
        if let Some(summary) = summary {
            set_trace_entry(
                &mut record.reasoning_summary_entries,
                item_id,
                &summary,
                sequence,
            );
            sync_legacy_trace(
                &mut record.reasoning_summary,
                &record.reasoning_summary_entries,
            );
        }
        if let Some(raw) = raw {
            set_trace_entry(&mut record.reasoning_raw_entries, item_id, &raw, sequence);
            sync_legacy_trace(&mut record.reasoning_raw, &record.reasoning_raw_entries);
        }
        record.updated_at = now_secs();
        self.last_event_at = Some(now_secs());
        self.trim_turns();
        true
    }

    fn record_reasoning_delta(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        item_id: Option<&str>,
        field: TraceField,
        delta: &str,
    ) -> bool {
        if !turn_capture_enabled() || delta.trim().is_empty() {
            return false;
        }
        let sequence = self.next_trace_sequence();
        let record = self.ensure_turn(thread_id, turn_id);
        ensure_turn_trace_entries(record);
        match field {
            TraceField::Commentary => {
                append_trace_entry(&mut record.commentary_entries, item_id, delta, sequence);
                sync_legacy_trace(&mut record.commentary, &record.commentary_entries);
            }
            TraceField::ReasoningSummary => {
                append_trace_entry(
                    &mut record.reasoning_summary_entries,
                    item_id,
                    delta,
                    sequence,
                );
                sync_legacy_trace(
                    &mut record.reasoning_summary,
                    &record.reasoning_summary_entries,
                );
            }
            TraceField::ReasoningRaw => {
                append_trace_entry(&mut record.reasoning_raw_entries, item_id, delta, sequence);
                sync_legacy_trace(&mut record.reasoning_raw, &record.reasoning_raw_entries);
            }
        }
        record.updated_at = now_secs();
        self.last_event_at = Some(now_secs());
        self.trim_turns();
        true
    }

    fn record_plan_update(&mut self, params: &Value) -> bool {
        if !turn_capture_enabled() {
            return false;
        }
        let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
            return false;
        };
        let Some(turn_id) = params.get("turnId").and_then(Value::as_str) else {
            return false;
        };
        let Some(text) = format_plan_update_text(params) else {
            return false;
        };
        let sequence = self.next_trace_sequence();
        let record = self.ensure_turn(thread_id, turn_id);
        ensure_turn_trace_entries(record);
        let item_id = format!("plan-update-{}", record.plan_entries.len() + 1);
        append_trace_entry(&mut record.plan_entries, Some(&item_id), &text, sequence);
        sync_legacy_trace(&mut record.plan, &record.plan_entries);
        record.updated_at = now_secs();
        self.last_event_at = Some(now_secs());
        self.trim_turns();
        true
    }

    fn record_plan_delta(&mut self, params: &Value) -> bool {
        if !turn_capture_enabled() || params.get("delta").and_then(Value::as_str).is_none() {
            return false;
        }
        let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
            return false;
        };
        let Some(turn_id) = params.get("turnId").and_then(Value::as_str) else {
            return false;
        };
        let item_id = params.get("itemId").and_then(Value::as_str);
        let delta = params
            .get("delta")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if delta.trim().is_empty() {
            return false;
        }
        let sequence = self.next_trace_sequence();
        let record = self.ensure_turn(thread_id, turn_id);
        ensure_turn_trace_entries(record);
        append_trace_entry(&mut record.plan_entries, item_id, delta, sequence);
        sync_legacy_trace(&mut record.plan, &record.plan_entries);
        record.updated_at = now_secs();
        self.last_event_at = Some(now_secs());
        self.trim_turns();
        true
    }

    fn record_plan_item(&mut self, thread_id: &str, turn_id: &str, item: &Value) -> bool {
        if !turn_capture_enabled() {
            return false;
        }
        let Some(text) = item
            .get("text")
            .and_then(Value::as_str)
            .map(bounded_turn_text)
            .filter(|text| !text.is_empty())
        else {
            return false;
        };
        let item_id = item.get("id").and_then(Value::as_str);
        let sequence = self.next_trace_sequence();
        let record = self.ensure_turn(thread_id, turn_id);
        ensure_turn_trace_entries(record);
        set_trace_entry(&mut record.plan_entries, item_id, &text, sequence);
        sync_legacy_trace(&mut record.plan, &record.plan_entries);
        record.updated_at = now_secs();
        self.last_event_at = Some(now_secs());
        self.trim_turns();
        true
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
            if let Some(record) = self.turns.remove(&candidate) {
                let item_prefix = format!("{}:{}:", record.thread_id, record.turn_id);
                self.agent_message_phases
                    .retain(|key, _| !key.starts_with(&item_prefix));
            }
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
        self.record_agent_message_phase(thread_id, turn_id, item);
        if item_type == "collabAgentToolCall" {
            self.record_collab_agent_states(thread_id, item);
        }
        let captures_reasoning = item_type == "reasoning"
            && !turn_id.is_empty()
            && self.record_reasoning_item(thread_id, turn_id, item);
        let captures_plan = item_type == "plan"
            && !turn_id.is_empty()
            && self.record_plan_item(thread_id, turn_id, item);
        let captures_turn_text =
            is_user_message_item(item) || (completed && is_assistant_message_item(item));
        if captures_turn_text {
            self.record_turn_message(thread_id, Some(turn_id), item);
        }
        let captured_output = captures_turn_text || captures_reasoning || captures_plan;
        if !is_tracked_tool_call_type(item_type) {
            self.last_event_at = Some(now_secs());
            self.trim_turns();
            return captured_output;
        }

        let Some(item_id) = item.get("id").and_then(Value::as_str) else {
            return captured_output;
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
        captured_output
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
                    if let Some(turn_id) = app_server_turn_id(params) {
                        self.record_turn_started(
                            thread_id,
                            turn_id,
                            app_server_turn_timestamp_ms(params, "startedAt", "startedAtMs"),
                        );
                    }
                    self.record_thread_status(thread_id, "active", Vec::new());
                }
                true
            }
            "turn/completed" => {
                if let Some(thread_id) = params.get("threadId").and_then(Value::as_str) {
                    if let Some(turn_id) = app_server_turn_id(params) {
                        let status = app_server_turn_status(params).unwrap_or("completed");
                        self.record_turn_completed(
                            thread_id,
                            turn_id,
                            status,
                            app_server_turn_timestamp_ms(params, "completedAt", "completedAtMs"),
                        );
                    }
                    self.record_thread_status(thread_id, "idle", Vec::new());
                }
                true
            }
            "turn/plan/updated" => self.record_plan_update(params),
            "thread/closed" => {
                if let Some(thread_id) = params.get("threadId").and_then(Value::as_str) {
                    self.record_thread_status(thread_id, "notLoaded", Vec::new());
                }
                false
            }
            "item/started" => self.record_item_event(params, false),
            "item/completed" => self.record_item_event(params, true),
            "item/agentMessage/delta" => self.record_text_delta(params, TraceField::Commentary),
            "item/plan/delta" => self.record_plan_delta(params),
            "item/reasoning/summaryTextDelta" => {
                self.record_text_delta(params, TraceField::ReasoningSummary)
            }
            "item/reasoning/textDelta" => self.record_text_delta(params, TraceField::ReasoningRaw),
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

    fn record_text_delta(&mut self, params: &Value, field: TraceField) -> bool {
        let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
            return false;
        };
        let Some(turn_id) = params.get("turnId").and_then(Value::as_str) else {
            return false;
        };
        let item_id = params
            .get("itemId")
            .and_then(Value::as_str)
            .or_else(|| params.get("item_id").and_then(Value::as_str))
            .or_else(|| {
                params
                    .get("item")
                    .and_then(|item| item.get("id"))
                    .and_then(Value::as_str)
            });
        if matches!(field, TraceField::Commentary) {
            let explicit_phase = params.get("phase").and_then(Value::as_str).or_else(|| {
                params
                    .get("item")
                    .and_then(|item| item.get("phase"))
                    .and_then(Value::as_str)
            });
            let mapped_phase = params
                .get("itemId")
                .and_then(Value::as_str)
                .or_else(|| params.get("item_id").and_then(Value::as_str))
                .or_else(|| {
                    params
                        .get("item")
                        .and_then(|item| item.get("id"))
                        .and_then(Value::as_str)
                })
                .and_then(|item_id| {
                    self.agent_message_phases
                        .get(&agent_message_key(thread_id, turn_id, item_id))
                        .map(String::as_str)
                });
            if explicit_phase.or(mapped_phase) != Some("commentary") {
                return false;
            }
        }
        let Some(delta) = params
            .get("delta")
            .and_then(Value::as_str)
            .or_else(|| params.get("text").and_then(Value::as_str))
        else {
            return false;
        };
        self.record_reasoning_delta(thread_id, turn_id, item_id, field, delta)
    }

    fn record_agent_message_phase(&mut self, thread_id: &str, turn_id: &str, item: &Value) {
        if item.get("type").and_then(Value::as_str) != Some("agentMessage") {
            return;
        }
        let Some(item_id) = item.get("id").and_then(Value::as_str) else {
            return;
        };
        let Some(phase) = item.get("phase").and_then(Value::as_str) else {
            return;
        };
        self.agent_message_phases.insert(
            agent_message_key(thread_id, turn_id, item_id),
            phase.to_string(),
        );
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

fn agent_message_key(thread_id: &str, turn_id: &str, item_id: &str) -> String {
    format!("{thread_id}:{turn_id}:{item_id}")
}

fn turn_info(turn: &TurnRecord) -> TurnInfo {
    TurnInfo {
        thread_id: turn.thread_id.clone(),
        turn_id: turn.turn_id.clone(),
        status: if turn.status == "unknown" && turn.result.is_some() {
            "saved".to_string()
        } else {
            turn.status.clone()
        },
        started_at_ms: turn.started_at_ms,
        completed_at_ms: turn.completed_at_ms,
        prompt: turn.prompt.clone(),
        result: turn.result.clone(),
        commentary: turn.commentary.clone(),
        commentary_entries: turn.commentary_entries.clone(),
        reasoning_summary: turn.reasoning_summary.clone(),
        reasoning_summary_entries: turn.reasoning_summary_entries.clone(),
        reasoning_raw: turn.reasoning_raw.clone(),
        reasoning_raw_entries: turn.reasoning_raw_entries.clone(),
        plan: turn.plan.clone(),
        plan_entries: turn.plan_entries.clone(),
        updated_at: turn.updated_at,
    }
}

fn turn_record_from_info(info: TurnInfo) -> TurnRecord {
    let key = turn_key(&info.thread_id, &info.turn_id);
    let commentary_entries =
        trace_entries_with_legacy(&info.commentary_entries, info.commentary.as_ref());
    let reasoning_summary_entries = trace_entries_with_legacy(
        &info.reasoning_summary_entries,
        info.reasoning_summary.as_ref(),
    );
    let reasoning_raw_entries =
        trace_entries_with_legacy(&info.reasoning_raw_entries, info.reasoning_raw.as_ref());
    let plan_entries = trace_entries_with_legacy(&info.plan_entries, info.plan.as_ref());
    TurnRecord {
        key,
        thread_id: info.thread_id,
        turn_id: info.turn_id,
        status: info.status,
        started_at_ms: info.started_at_ms,
        completed_at_ms: info.completed_at_ms,
        prompt: info.prompt,
        result: info.result,
        commentary: trace_entries_text(&commentary_entries),
        commentary_entries,
        reasoning_summary: trace_entries_text(&reasoning_summary_entries),
        reasoning_summary_entries,
        reasoning_raw: trace_entries_text(&reasoning_raw_entries),
        reasoning_raw_entries,
        plan: trace_entries_text(&plan_entries),
        plan_entries,
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
            for key in [
                "content",
                "input",
                "message",
                "prompt",
                "items",
                "parts",
                "summary",
                "summaryText",
            ] {
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

fn app_server_turn_id(params: &Value) -> Option<&str> {
    params
        .get("turnId")
        .and_then(Value::as_str)
        .or_else(|| params.get("turn")?.get("id")?.as_str())
}

fn app_server_turn_timestamp_ms(params: &Value, camel_key: &str, ms_key: &str) -> Option<u64> {
    let turn = params.get("turn").unwrap_or(&Value::Null);
    params
        .get(ms_key)
        .and_then(Value::as_u64)
        .or_else(|| turn.get(ms_key).and_then(Value::as_u64))
        .or_else(|| params.get(camel_key).and_then(Value::as_u64))
        .or_else(|| turn.get(camel_key).and_then(Value::as_u64))
}

fn app_server_turn_status(params: &Value) -> Option<&str> {
    let status = params
        .get("status")
        .or_else(|| params.get("turn").and_then(|turn| turn.get("status")))?;
    status
        .as_str()
        .or_else(|| status.get("type").and_then(Value::as_str))
}

fn format_plan_update_text(params: &Value) -> Option<String> {
    let plan = params.get("plan").and_then(Value::as_array)?;
    let mut lines = vec!["Updated Plan".to_string()];
    if let Some(explanation) = params
        .get("explanation")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        lines.push(format!("  {explanation}"));
    }
    for (index, step) in plan.iter().enumerate() {
        let Some(text) = step
            .get("step")
            .and_then(Value::as_str)
            .or_else(|| step.get("text").and_then(Value::as_str))
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let status = step
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("pending")
            .to_ascii_lowercase()
            .replace(['_', '-'], "");
        let marker = match status.as_str() {
            "completed" | "complete" | "done" => "☑",
            "inprogress" | "running" | "active" => "◐",
            _ => "□",
        };
        let branch = if index + 1 == plan.len() {
            "└"
        } else {
            "├"
        };
        lines.push(format!("  {branch} {marker} {text}"));
    }
    (lines.len() > 1).then(|| bounded_turn_text(&lines.join("\n")))
}

fn is_user_message_item(item: &Value) -> bool {
    item.get("type").and_then(Value::as_str) == Some("userMessage")
        || item.get("role").and_then(Value::as_str) == Some("user")
}

#[derive(Clone, Copy)]
enum TraceField {
    Commentary,
    ReasoningSummary,
    ReasoningRaw,
}

fn is_commentary_message_item(item: &Value) -> bool {
    is_assistant_message_item(item)
        && item.get("phase").and_then(Value::as_str) == Some("commentary")
}

fn is_assistant_message_item(item: &Value) -> bool {
    matches!(
        item.get("type").and_then(Value::as_str),
        Some("agentMessage" | "assistantMessage")
    ) || item.get("role").and_then(Value::as_str) == Some("assistant")
}

fn trace_entries_with_legacy(entries: &[TraceEntry], legacy: Option<&String>) -> Vec<TraceEntry> {
    let entries = entries
        .iter()
        .filter(|entry| !entry.text.trim().is_empty())
        .cloned()
        .collect::<Vec<_>>();
    if !entries.is_empty() {
        return entries;
    }
    legacy
        .filter(|text| !text.trim().is_empty())
        .map(|text| {
            vec![TraceEntry {
                item_id: None,
                text: bounded_turn_text(text),
                sequence: 0,
            }]
        })
        .unwrap_or_default()
}

fn trace_entries_text(entries: &[TraceEntry]) -> Option<String> {
    let mut text = String::new();
    for entry in entries {
        if entry.text.trim().is_empty() {
            continue;
        }
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(&entry.text);
    }
    (!text.is_empty()).then(|| bounded_trace_text(&text))
}

fn sync_legacy_trace(target: &mut Option<String>, entries: &[TraceEntry]) {
    *target = trace_entries_text(entries);
}

fn trace_item_id_matches(entry: &TraceEntry, item_id: Option<&str>) -> bool {
    match (entry.item_id.as_deref(), item_id) {
        (Some(existing), Some(incoming)) => existing == incoming,
        (None, None) => true,
        _ => false,
    }
}

fn append_trace_entry(
    entries: &mut Vec<TraceEntry>,
    item_id: Option<&str>,
    delta: &str,
    sequence: u64,
) {
    if delta.trim().is_empty() {
        return;
    }
    if let Some(entry) = entries
        .iter_mut()
        .rev()
        .find(|entry| trace_item_id_matches(entry, item_id))
    {
        entry.text = bounded_trace_text(&format!("{}{}", entry.text, delta));
        return;
    }
    entries.push(TraceEntry {
        item_id: item_id
            .map(str::trim)
            .filter(|item_id| !item_id.is_empty())
            .map(ToString::to_string),
        text: bounded_trace_text(delta),
        sequence,
    });
}

fn set_trace_entry(
    entries: &mut Vec<TraceEntry>,
    item_id: Option<&str>,
    text: &str,
    sequence: u64,
) {
    let text = bounded_turn_text(text);
    if text.is_empty() {
        return;
    }
    let normalized_item_id = item_id
        .map(str::trim)
        .filter(|item_id| !item_id.is_empty())
        .map(ToString::to_string);
    if let Some(entry) = entries.iter_mut().find(|entry| {
        normalized_item_id.is_some() && entry.item_id.as_deref() == normalized_item_id.as_deref()
    }) {
        entry.text = text;
        return;
    }
    if let Some(entry) = entries.iter_mut().rev().find(|entry| {
        entry.item_id.is_none()
            && (entry.text == text
                || entry.text.starts_with(&text)
                || text.starts_with(entry.text.as_str()))
    }) {
        entry.item_id = normalized_item_id;
        entry.text = text;
        return;
    }
    entries.push(TraceEntry {
        item_id: normalized_item_id,
        text,
        sequence,
    });
}

fn merge_trace_entries(target: &mut Vec<TraceEntry>, incoming: &[TraceEntry]) {
    for entry in incoming {
        if entry.text.trim().is_empty() {
            continue;
        }
        let item_id = entry.item_id.as_deref();
        if let Some(existing) = target
            .iter_mut()
            .find(|existing| item_id.is_some() && existing.item_id.as_deref() == item_id)
        {
            if existing.text == entry.text || existing.text.starts_with(&entry.text) {
                continue;
            }
            if entry.text.starts_with(existing.text.as_str()) {
                existing.text = bounded_turn_text(&entry.text);
            } else {
                existing.text = bounded_trace_text(&format!("{}{}", existing.text, entry.text));
            }
            continue;
        }
        if item_id.is_none()
            && target.iter().any(|existing| {
                existing.item_id.is_none()
                    && (existing.text == entry.text
                        || existing.text.starts_with(&entry.text)
                        || entry.text.starts_with(existing.text.as_str()))
            })
        {
            continue;
        }
        target.push(TraceEntry {
            item_id: entry.item_id.clone(),
            text: bounded_trace_text(&entry.text),
            sequence: entry.sequence,
        });
    }
}

fn max_trace_sequence(record: &TurnRecord) -> u64 {
    record
        .commentary_entries
        .iter()
        .chain(record.reasoning_summary_entries.iter())
        .chain(record.reasoning_raw_entries.iter())
        .chain(record.plan_entries.iter())
        .map(|entry| entry.sequence)
        .max()
        .unwrap_or(0)
}

fn ensure_turn_trace_entries(record: &mut TurnRecord) {
    if record.commentary_entries.is_empty() {
        record.commentary_entries =
            trace_entries_with_legacy(&record.commentary_entries, record.commentary.as_ref());
    }
    if record.reasoning_summary_entries.is_empty() {
        record.reasoning_summary_entries = trace_entries_with_legacy(
            &record.reasoning_summary_entries,
            record.reasoning_summary.as_ref(),
        );
    }
    if record.reasoning_raw_entries.is_empty() {
        record.reasoning_raw_entries =
            trace_entries_with_legacy(&record.reasoning_raw_entries, record.reasoning_raw.as_ref());
    }
    if record.plan_entries.is_empty() {
        record.plan_entries = trace_entries_with_legacy(&record.plan_entries, record.plan.as_ref());
    }
}

fn bounded_trace_text(value: &str) -> String {
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
    UpgradeResumeRequested,
    SettingsReconfigureRequested,
    ProxyDisconnected {
        error: String,
    },
    ResumeBootstrapCompleted,
    ThreadBound(String),
    ThreadStatus {
        thread_id: String,
        status: String,
        active_flags: Vec<String>,
    },
    PendingSettingsApplied(PendingClientSettings),
    TurnInput {
        thread_id: String,
        turn_id: Option<String>,
        prompt: String,
    },
    CodexExited {
        generation: u64,
        result: Result<ExitStatus, String>,
    },
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
    pending_resume_request_ids: BTreeSet<String>,
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ResolveResumeLastRequest {
    #[serde(default)]
    cwd: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct PrepareResumeRequest {
    #[serde(default)]
    client_id: String,
    #[serde(default)]
    thread_id: String,
    #[serde(default)]
    cwd: String,
    #[serde(default)]
    configuration: Option<YoloDefaultConfiguration>,
}

fn settings_source_rank(source: &str) -> u8 {
    match source {
        // The yolo launch intent is the durable source for a resumed client.
        // App-server snapshots can be stale while a thread is being loaded,
        // so they must never replace a complete launch configuration.
        "configure" => 6,
        "heartbeat" => 5,
        "tmux_footer" => 4,
        "launch_args" => 3,
        "legacy" => 2,
        "app_server" => 1,
        _ => 0,
    }
}

fn client_settings_source(client: &ClientInfo) -> String {
    if !client.settings_source.trim().is_empty() {
        return client.settings_source.clone();
    }
    if client.settings_updated_at.is_some() {
        return "configure".to_string();
    }
    if client.model.is_some() || client.service_tier.is_some() || client.reasoning_effort.is_some()
    {
        return "legacy".to_string();
    }
    "unknown".to_string()
}

fn client_fast_known(client: &ClientInfo) -> bool {
    client.fast_known
        || known_fast_from_service_tier(client.service_tier.as_deref()).is_some()
        || matches!(
            client.settings_source.as_str(),
            "app_server" | "configure" | "tmux_footer"
        )
}

fn known_fast_from_service_tier(service_tier: Option<&str>) -> Option<bool> {
    match service_tier.map(str::trim) {
        Some("fast" | "priority") => Some(true),
        Some("default") => Some(false),
        _ => None,
    }
}

fn session_settings_complete(
    model: &Option<String>,
    service_tier: &Option<String>,
    reasoning_effort: &Option<String>,
    fast_known: bool,
) -> bool {
    fast_known
        && model
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        && service_tier
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        && reasoning_effort
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
}

fn active_session_record_settings_complete(record: &ActiveSessionRecord) -> bool {
    record.settings_complete
        || session_settings_complete(
            &record.model,
            &record.service_tier,
            &record.reasoning_effort,
            record.fast_known
                || known_fast_from_service_tier(record.service_tier.as_deref()).is_some(),
        )
}

fn codex_args_settings_score(args: &[String]) -> u8 {
    let settings = parse_codex_launch_config(args);
    settings.model.is_some() as u8
        + settings.service_tier.is_some() as u8
        + settings.reasoning_effort.is_some() as u8
}

fn merge_optional_setting(
    incoming: &Option<String>,
    current: &Option<String>,
    prefer_incoming: bool,
) -> Option<String> {
    if prefer_incoming {
        incoming.clone().or_else(|| current.clone())
    } else {
        current.clone().or_else(|| incoming.clone())
    }
}

fn merge_active_session_record(
    current: Option<&ActiveSessionRecord>,
    client: &ClientInfo,
) -> ActiveSessionRecord {
    let incoming = active_session_record_from_client(client);
    let Some(current) = current else {
        return incoming;
    };

    let current_fast_known = current.fast_known
        || known_fast_from_service_tier(current.service_tier.as_deref()).is_some();
    let current_complete = active_session_record_settings_complete(current);
    let incoming_rank = settings_source_rank(&incoming.settings_source);
    let current_rank = settings_source_rank(&current.settings_source);
    let incoming_observed_at = incoming.settings_observed_at.unwrap_or(0);
    let current_observed_at = current.settings_observed_at.unwrap_or(0);
    let prefer_incoming = incoming_rank > current_rank
        || (incoming_rank == current_rank && incoming_observed_at >= current_observed_at);

    let mut record = incoming.clone();
    if record.cwd.trim().is_empty() {
        record.cwd = current.cwd.clone();
    }
    if current_complete
        && codex_args_settings_score(&record.args) < codex_args_settings_score(&current.args)
    {
        record.args = current.args.clone();
    }
    record.model = merge_optional_setting(&incoming.model, &current.model, prefer_incoming);
    record.service_tier = merge_optional_setting(
        &incoming.service_tier,
        &current.service_tier,
        prefer_incoming,
    );
    record.reasoning_effort = merge_optional_setting(
        &incoming.reasoning_effort,
        &current.reasoning_effort,
        prefer_incoming,
    );

    if incoming.fast_known && (prefer_incoming || !current_fast_known) {
        record.fast = incoming.fast;
        record.fast_known = true;
    } else {
        record.fast = current.fast;
        record.fast_known = current_fast_known || incoming.fast_known;
    }
    if let Some(service_fast) = known_fast_from_service_tier(record.service_tier.as_deref()) {
        record.fast = service_fast;
        record.fast_known = true;
    }

    if current_rank > incoming_rank || incoming.settings_source.trim().is_empty() {
        record.settings_source = current.settings_source.clone();
    }
    record.settings_observed_at =
        match (current.settings_observed_at, incoming.settings_observed_at) {
            (Some(current), Some(incoming)) => Some(current.max(incoming)),
            (Some(current), None) => Some(current),
            (None, Some(incoming)) => Some(incoming),
            (None, None) => None,
        };
    record.settings_complete = session_settings_complete(
        &record.model,
        &record.service_tier,
        &record.reasoning_effort,
        record.fast_known,
    );
    record.started_at = if current.started_at > 0 {
        current.started_at.min(incoming.started_at)
    } else {
        incoming.started_at
    };
    if record.thread_id.is_none() {
        record.thread_id = current.thread_id.clone();
        record.thread_id_source = current.thread_id_source.clone();
    }
    record
}

fn active_session_record_from_client(client: &ClientInfo) -> ActiveSessionRecord {
    let fast_known = client_fast_known(client);
    let settings_source = client_settings_source(client);
    let fast = known_fast_from_service_tier(client.service_tier.as_deref()).unwrap_or(client.fast);
    ActiveSessionRecord {
        client_id: client.id.clone(),
        cwd: client.cwd.clone(),
        args: client.args.clone(),
        model: client.model.clone(),
        service_tier: client.service_tier.clone(),
        reasoning_effort: client.reasoning_effort.clone(),
        fast,
        fast_known,
        settings_complete: session_settings_complete(
            &client.model,
            &client.service_tier,
            &client.reasoning_effort,
            fast_known,
        ),
        settings_source,
        settings_observed_at: client.settings_observed_at,
        thread_id: client.thread_id.clone(),
        thread_id_source: client.thread_id_source.clone(),
        started_at: client.started_at,
    }
}

fn active_session_record_matches_client(record: &ActiveSessionRecord, client: &ClientInfo) -> bool {
    record.client_id == client.id
        || record
            .thread_id
            .as_deref()
            .zip(client.thread_id.as_deref())
            .is_some_and(|(left, right)| !left.is_empty() && left == right)
}

fn remove_active_session_matches_client(
    active_sessions: &mut BTreeMap<String, ActiveSessionRecord>,
    client: &ClientInfo,
) -> bool {
    let ids = active_sessions
        .iter()
        .filter(|(_, record)| active_session_record_matches_client(record, client))
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    let changed = !ids.is_empty();
    for id in ids {
        active_sessions.remove(&id);
    }
    changed
}

fn remove_active_sessions_for_yolo_pid_except(
    active_sessions: &mut BTreeMap<String, ActiveSessionRecord>,
    yolo_pid: u32,
    keep_client: Option<&ClientInfo>,
) -> bool {
    let prefix = format!("{yolo_pid}-");
    let ids = active_sessions
        .iter()
        .filter(|(id, record)| {
            id.starts_with(&prefix)
                && keep_client
                    .is_none_or(|client| !active_session_record_matches_client(record, client))
        })
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    let changed = !ids.is_empty();
    for id in ids {
        active_sessions.remove(&id);
    }
    changed
}

fn upsert_active_session_locked(state: &mut ServerState, client: &ClientInfo) -> bool {
    if !matches!(client.status.as_str(), "running" | "restarting") {
        return remove_active_session_matches_client(&mut state.active_sessions, client);
    }

    let matching_ids = state
        .active_sessions
        .iter()
        .filter(|(_, record)| active_session_record_matches_client(record, client))
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    let current = matching_ids
        .iter()
        .filter_map(|id| state.active_sessions.get(id))
        .max_by_key(|record| {
            (
                active_session_record_settings_complete(record),
                settings_source_rank(&record.settings_source),
                record.settings_observed_at.unwrap_or(0),
                record.started_at,
            )
        });
    let record = merge_active_session_record(current, client);
    let changed = matching_ids.len() != 1 || state.active_sessions.get(&client.id) != Some(&record);
    for id in matching_ids {
        state.active_sessions.remove(&id);
    }
    state.active_sessions.insert(client.id.clone(), record);
    changed
}

fn reconcile_registered_client_process(state: &mut ServerState, client: &ClientInfo) -> bool {
    let stale_clients = state
        .clients
        .values()
        .filter(|existing| {
            if existing.id == client.id {
                return false;
            }
            existing.yolo_pid == client.yolo_pid
                || (!matches!(existing.status.as_str(), "running" | "restarting")
                    && existing
                        .thread_id
                        .as_deref()
                        .zip(client.thread_id.as_deref())
                        .is_some_and(|(left, right)| !left.is_empty() && left == right))
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut changed = false;
    changed |= stale_clients.iter().any(|existing| {
        existing.yolo_pid != client.yolo_pid
            && !matches!(existing.status.as_str(), "running" | "restarting")
            && existing
                .thread_id
                .as_deref()
                .zip(client.thread_id.as_deref())
                .is_some_and(|(left, right)| !left.is_empty() && left == right)
    });
    for stale_client in stale_clients {
        // Keep the saved record when a stale/exited client is replaced by a
        // new wrapper for the same thread. The following registration will
        // merge it into the new client identity.
        if stale_client.yolo_pid == client.yolo_pid {
            changed |=
                remove_active_session_matches_client(&mut state.active_sessions, &stale_client);
        }
    }
    let replaced_process = state
        .clients
        .values()
        .any(|existing| existing.id != client.id && existing.yolo_pid == client.yolo_pid);
    state.clients.retain(|id, existing| {
        if id == &client.id || existing.yolo_pid == client.yolo_pid {
            return id == &client.id;
        }
        let same_thread = existing
            .thread_id
            .as_deref()
            .zip(client.thread_id.as_deref())
            .is_some_and(|(left, right)| !left.is_empty() && left == right);
        matches!(existing.status.as_str(), "running" | "restarting") || !same_thread
    });
    if replaced_process {
        release_upgrade_reexec_permit_locked(state, client.yolo_pid);
    }
    changed
}

fn is_waiting_thread_status(status: &str) -> bool {
    status.eq_ignore_ascii_case("idle") || status.eq_ignore_ascii_case("waiting")
}

fn client_is_waiting_for_upgrade(client: &ClientInfo) -> bool {
    matches!(client.status.as_str(), "running" | "restarting")
        && client
            .codex_status
            .as_deref()
            .is_some_and(is_waiting_thread_status)
        && client.codex_active_flags.is_empty()
        && client.codex_status_updated_at.is_some()
}

fn prepare_upgrade_reexec_gate(
    state: &Arc<Mutex<ServerState>>,
    request: &UpgradeResumeAllRequest,
) -> usize {
    let client_ids = state
        .lock()
        .map(|state| {
            state
                .clients
                .values()
                .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
                .map(|client| client.id.clone())
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    prepare_upgrade_reexec_gate_for_client_ids(state, &client_ids, request)
}

fn prepare_upgrade_reexec_gate_for_client_ids(
    state: &Arc<Mutex<ServerState>>,
    client_ids: &BTreeSet<String>,
    request: &UpgradeResumeAllRequest,
) -> usize {
    let Ok(mut state) = state.lock() else {
        return 0;
    };
    let mut clients = state
        .clients
        .values()
        .filter(|client| client_ids.contains(&client.id))
        .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
        .collect::<Vec<_>>();
    // Keep the caller last when Phoenix mode asked us to ignore it while
    // waiting. This lets the other sessions migrate before the initiating
    // terminal is asked to re-exec.
    clients.sort_by_key(|client| {
        (
            should_ignore_upgrade_wait_client(client, request),
            client.started_at,
            client.yolo_pid,
        )
    });
    let mut seen = BTreeSet::new();
    state.upgrade_reexec_queue = clients
        .into_iter()
        .filter_map(|client| seen.insert(client.yolo_pid).then_some(client.yolo_pid))
        .collect();
    state.upgrade_reexec_active = None;
    state.upgrade_reexec_queue.len()
}

fn prune_upgrade_reexec_gate_locked(state: &mut ServerState) {
    let now = now_secs();
    if state.upgrade_reexec_active.as_ref().is_some_and(|permit| {
        now.saturating_sub(permit.claimed_at) >= UPGRADE_REEXEC_ACTIVE_TIMEOUT_SECS
    }) {
        if let Some(permit) = state.upgrade_reexec_active.take() {
            eprintln!(
                "yolo upgrade: releasing stale re-exec permit for yolo pid {}",
                permit.yolo_pid
            );
        }
    }

    while let Some(yolo_pid) = state.upgrade_reexec_queue.front().copied() {
        let present = state.clients.values().any(|client| {
            client.yolo_pid == yolo_pid
                && matches!(client.status.as_str(), "running" | "restarting")
                && now.saturating_sub(client.updated_at) < UPGRADE_REEXEC_ACTIVE_TIMEOUT_SECS
        });
        if present {
            break;
        }
        state.upgrade_reexec_queue.pop_front();
    }
}

fn release_upgrade_reexec_permit_locked(state: &mut ServerState, yolo_pid: u32) {
    if state
        .upgrade_reexec_active
        .as_ref()
        .is_some_and(|permit| permit.yolo_pid == yolo_pid)
    {
        state.upgrade_reexec_active = None;
        if state.upgrade_reexec_queue.front() == Some(&yolo_pid) {
            state.upgrade_reexec_queue.pop_front();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpgradeReexecClaimResult {
    Granted,
    GateAbsent,
    ClientUnknown,
    WaitingStateAbsent,
    NotClientTurn,
    Unknown,
}

impl UpgradeReexecClaimResult {
    fn granted(self) -> bool {
        self == Self::Granted
    }

    fn reason(self) -> &'static str {
        match self {
            Self::Granted => "granted",
            Self::GateAbsent => "gate_absent",
            Self::ClientUnknown => "client_unknown",
            Self::WaitingStateAbsent => "waiting_state_absent",
            Self::NotClientTurn => "not_client_turn",
            Self::Unknown => "unknown",
        }
    }
}

fn claim_upgrade_reexec_permit_result(
    state: &Arc<Mutex<ServerState>>,
    client_id: &str,
) -> Result<UpgradeReexecClaimResult, String> {
    let mut state = state
        .lock()
        .map_err(|_| "server state lock poisoned".to_string())?;
    prune_upgrade_reexec_gate_locked(&mut state);
    // A re-exec permit is never a generic liveness/recovery permit. It is
    // valid only while an explicit upgrade-resume gate is active.
    if state.upgrade_reexec_queue.is_empty() && state.upgrade_reexec_active.is_none() {
        return Ok(UpgradeReexecClaimResult::GateAbsent);
    }
    let Some(client) = state.clients.get(client_id) else {
        return Ok(UpgradeReexecClaimResult::ClientUnknown);
    };
    // The upgrade worker waits for the app-server thread to become idle. Keep
    // this check at the final claim point as well, so a client that became
    // active again can never be terminated/re-execed by the upgrade flow.
    if !client_is_waiting_for_upgrade(client) {
        return Ok(UpgradeReexecClaimResult::WaitingStateAbsent);
    }
    if state
        .upgrade_reexec_active
        .as_ref()
        .is_some_and(|permit| permit.yolo_pid == client.yolo_pid)
    {
        return Ok(UpgradeReexecClaimResult::Granted);
    }
    if state.upgrade_reexec_active.is_some()
        || state.upgrade_reexec_queue.front() != Some(&client.yolo_pid)
    {
        return Ok(UpgradeReexecClaimResult::NotClientTurn);
    }
    state.upgrade_reexec_active = Some(UpgradeReexecPermit {
        yolo_pid: client.yolo_pid,
        claimed_at: now_secs(),
    });
    Ok(UpgradeReexecClaimResult::Granted)
}

fn claim_upgrade_reexec_permit(
    state: &Arc<Mutex<ServerState>>,
    client_id: &str,
) -> Result<bool, String> {
    Ok(claim_upgrade_reexec_permit_result(state, client_id)?.granted())
}

fn upgrade_reexec_gate_pending(state: &Arc<Mutex<ServerState>>) -> Result<bool, String> {
    let mut state = state
        .lock()
        .map_err(|_| "server state lock poisoned".to_string())?;
    prune_upgrade_reexec_gate_locked(&mut state);
    Ok(!state.upgrade_reexec_queue.is_empty() || state.upgrade_reexec_active.is_some())
}

fn wait_for_upgrade_reexec_gate(state: &Arc<Mutex<ServerState>>) -> Result<(), String> {
    let start = SystemTime::now();
    loop {
        if !upgrade_reexec_gate_pending(state)? {
            return Ok(());
        }
        if start.elapsed().unwrap_or_default() >= UPGRADE_REEXEC_PERMIT_TIMEOUT {
            return Err("timed out waiting for serialized Codex client re-exec".to_string());
        }
        thread::sleep(UPGRADE_IDLE_POLL_INTERVAL);
    }
}

fn persist_active_sessions_snapshot(
    path: &Path,
    sessions: &BTreeMap<String, ActiveSessionRecord>,
) -> Result<(), String> {
    let Some(parent) = path.parent() else {
        return Err(format!(
            "active sessions path has no parent: {}",
            path.display()
        ));
    };
    fs::create_dir_all(parent).map_err(|err| {
        format!(
            "create active sessions directory {}: {err}",
            parent.display()
        )
    })?;
    let _ = fs::set_permissions(parent, fs::Permissions::from_mode(0o700));

    let file = ActiveSessionsFile {
        version: ACTIVE_SESSIONS_FILE_VERSION,
        saved_at: now_secs(),
        sessions: sessions.values().cloned().collect(),
    };
    let contents = serde_json::to_vec_pretty(&file)
        .map_err(|err| format!("encode active sessions {}: {err}", path.display()))?;
    let temporary =
        path.with_extension(format!("json.{}.{}.tmp", std::process::id(), now_millis()));
    let mut file = fs::File::create(&temporary)
        .map_err(|err| format!("create active sessions {}: {err}", temporary.display()))?;
    file.write_all(&contents)
        .map_err(|err| format!("write active sessions {}: {err}", temporary.display()))?;
    file.sync_all()
        .map_err(|err| format!("sync active sessions {}: {err}", temporary.display()))?;
    drop(file);
    let _ = fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600));
    if let Err(err) = fs::rename(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(format!("replace active sessions {}: {err}", path.display()));
    }
    if let Ok(directory) = fs::File::open(parent) {
        let _ = directory.sync_all();
    }
    Ok(())
}

fn persist_active_sessions(state: &Arc<Mutex<ServerState>>, paths: &RuntimePaths) {
    let Ok(state) = state.lock() else {
        return;
    };
    if let Err(err) =
        persist_active_sessions_snapshot(&paths.active_sessions, &state.active_sessions)
    {
        eprintln!("yolo: failed to persist active sessions: {err}");
    }
}

fn load_active_sessions(path: &Path) -> BTreeMap<String, ActiveSessionRecord> {
    let Ok(contents) = fs::read_to_string(path) else {
        return BTreeMap::new();
    };
    let Ok(file) = serde_json::from_str::<ActiveSessionsFile>(&contents) else {
        eprintln!(
            "yolo: ignoring invalid active sessions file {}",
            path.display()
        );
        return BTreeMap::new();
    };
    if file.version > ACTIVE_SESSIONS_FILE_VERSION {
        eprintln!(
            "yolo: active sessions file {} has unsupported version {}; ignoring",
            path.display(),
            file.version
        );
        return BTreeMap::new();
    }
    file.sessions
        .into_iter()
        .filter(|session| !session.client_id.trim().is_empty() && !session.cwd.trim().is_empty())
        .map(|mut session| {
            if !session.fast_known {
                session.fast_known = known_fast_from_service_tier(session.service_tier.as_deref())
                    .is_some()
                    || session.settings_complete;
            }
            session.settings_complete |= session_settings_complete(
                &session.model,
                &session.service_tier,
                &session.reasoning_effort,
                session.fast_known,
            );
            if let Some(service_fast) =
                known_fast_from_service_tier(session.service_tier.as_deref())
            {
                session.fast = service_fast;
                session.fast_known = true;
            }
            if session.settings_source.trim().is_empty() {
                session.settings_source = if session.settings_complete {
                    "legacy".to_string()
                } else {
                    "unknown".to_string()
                };
            }
            (session.client_id.clone(), session)
        })
        .collect()
}

fn load_yolo_default_configuration(path: &Path) -> Option<YoloDefaultConfiguration> {
    let contents = fs::read_to_string(path).ok()?;
    let configuration = serde_json::from_str::<YoloDefaultConfiguration>(&contents).ok()?;
    if configuration.model.trim().is_empty() || configuration.reasoning_effort.trim().is_empty() {
        return None;
    }
    Some(configuration)
}

fn persist_yolo_default_configuration(
    path: &Path,
    configuration: &YoloDefaultConfiguration,
) -> Result<(), String> {
    let Some(parent) = path.parent() else {
        return Err(format!(
            "default configuration path has no parent: {}",
            path.display()
        ));
    };
    fs::create_dir_all(parent).map_err(|err| {
        format!(
            "create default configuration directory {}: {err}",
            parent.display()
        )
    })?;
    let _ = fs::set_permissions(parent, fs::Permissions::from_mode(0o700));
    let contents = serde_json::to_vec_pretty(configuration)
        .map_err(|err| format!("encode default configuration {}: {err}", path.display()))?;
    let temporary =
        path.with_extension(format!("json.{}.{}.tmp", std::process::id(), now_millis()));
    let mut file = fs::File::create(&temporary).map_err(|err| {
        format!(
            "create default configuration {}: {err}",
            temporary.display()
        )
    })?;
    file.write_all(&contents)
        .map_err(|err| format!("write default configuration {}: {err}", temporary.display()))?;
    file.sync_all()
        .map_err(|err| format!("sync default configuration {}: {err}", temporary.display()))?;
    drop(file);
    let _ = fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600));
    if let Err(err) = fs::rename(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(format!(
            "replace default configuration {}: {err}",
            path.display()
        ));
    }
    if let Ok(directory) = fs::File::open(parent) {
        let _ = directory.sync_all();
    }
    Ok(())
}

fn set_yolo_default_configuration(
    state: &Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
    configuration: YoloDefaultConfiguration,
) -> Result<Value, String> {
    if configuration.model.trim().is_empty() || configuration.reasoning_effort.trim().is_empty() {
        return Err("default configuration values are required".to_string());
    }
    let changed = {
        let mut state = state
            .lock()
            .map_err(|_| "server state lock poisoned".to_string())?;
        let changed = state.default_configuration.as_ref() != Some(&configuration);
        state.default_configuration = Some(configuration.clone());
        changed
    };
    persist_yolo_default_configuration(&paths.default_configuration, &configuration)?;
    if changed {
        publish_status_event(state, "default-configuration-updated");
    }
    Ok(json!({"ok": true, "configuration": configuration}))
}

fn sync_active_sessions_for_client_ids(
    state: &Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
    client_ids: &[String],
) {
    let changed = if let Ok(mut state) = state.lock() {
        let mut changed = false;
        for client_id in client_ids {
            if let Some(client) = state.clients.get(client_id).cloned() {
                changed |= upsert_active_session_locked(&mut state, &client);
            }
        }
        changed
    } else {
        false
    };
    if changed {
        persist_active_sessions(state, paths);
    }
}

fn main() {
    let mut args = env::args_os().skip(1).collect::<Vec<_>>();
    let is_native_codex_command = args.first().and_then(|arg| arg.to_str()) == Some("codex");

    if !is_native_codex_command && args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_help();
        return;
    }
    if !is_native_codex_command && args.iter().any(|arg| arg == "--version" || arg == "-V") {
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
        Some("saved-sessions") | Some("active-sessions") => {
            if let Err(err) = print_saved_sessions() {
                eprintln!("yolo saved-sessions: {err}");
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
    let active_sessions = load_active_sessions(&paths.active_sessions);
    if !active_sessions.is_empty() {
        eprintln!(
            "yolo: loaded {} saved active sessions from {}",
            active_sessions.len(),
            paths.active_sessions.display()
        );
    }
    let default_configuration = load_yolo_default_configuration(&paths.default_configuration);
    if let Some(configuration) = &default_configuration {
        eprintln!(
            "yolo: loaded widget defaults {} / {} / {} from {}",
            configuration.model,
            configuration.reasoning_effort,
            if configuration.fast { "fast" } else { "normal" },
            paths.default_configuration.display()
        );
    }
    // A server process restart must produce a generation newer than the one
    // remembered by surviving yolo clients. A per-process counter starting at
    // zero can repeat the previous generation and leave the re-exec gate
    // permanently unobserved after a service restart.
    let server_generation = now_millis() as u64;
    let state = Arc::new(Mutex::new(ServerState {
        started_at: now_secs(),
        app_server_pid: None,
        app_server_generation: server_generation,
        // Do not turn an ordinary yolo.service/app-server restart into a
        // proactive client kill or re-exec. Only an explicit upgrade-resume
        // operation advances this generation and opens the waiting-client
        // gate.
        resume_generation: 0,
        clients: BTreeMap::new(),
        active_sessions,
        default_configuration,
        slaves: BTreeMap::new(),
        telemetry,
        federation_push_senders: BTreeMap::new(),
        status_event_senders: BTreeMap::new(),
        next_status_event_id: 0,
        upgrade_reexec_queue: VecDeque::new(),
        upgrade_reexec_active: None,
    }));
    let app_server_pid = ensure_tracked_app_server(Arc::clone(&state), paths.clone())?;
    scan_existing_yolo_clients(&state, &paths);
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

fn try_claim_upgrade_reexec_permit(client_id: &str) -> Result<UpgradeReexecClaimResult, String> {
    let value = api_post_json("/clients/reexec-claim", &json!({"client_id": client_id}))?;
    if value
        .get("granted")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(UpgradeReexecClaimResult::Granted);
    }
    Ok(match value.get("reason").and_then(Value::as_str) {
        Some("gate_absent") => UpgradeReexecClaimResult::GateAbsent,
        Some("client_unknown") => UpgradeReexecClaimResult::ClientUnknown,
        Some("waiting_state_absent") => UpgradeReexecClaimResult::WaitingStateAbsent,
        Some("not_client_turn") => UpgradeReexecClaimResult::NotClientTurn,
        _ => UpgradeReexecClaimResult::Unknown,
    })
}

fn codex_child_exit_is_user_interrupt(status: &ExitStatus) -> bool {
    #[cfg(unix)]
    {
        status.signal() == Some(2) || status.code() == Some(130)
    }
    #[cfg(not(unix))]
    {
        status.code() == Some(130)
    }
}

fn spawn_codex_child(cwd: &str, remote: &str, args: &[OsString]) -> Result<Child, String> {
    let codex = codex_executable();
    let mut command = Command::new(codex);
    command
        .current_dir(cwd)
        .arg("--remote")
        .arg(remote)
        .args(yolo_mode_cli_args());
    if resume_target_from_args(args).is_some() {
        command.arg("-c").arg("include_environment_context=false");
    }
    command
        .args(codex_args_with_cwd(args.to_vec(), cwd))
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    command
        .spawn()
        .map_err(|err| format!("failed to spawn codex: {err}"))
}

fn spawn_and_register_codex_child(
    cwd: &str,
    remote: &str,
    args: &[OsString],
    generation: u64,
    info: &mut ClientInfo,
    event_tx: &mpsc::Sender<ClientEvent>,
) -> Result<u32, String> {
    let mut child = spawn_codex_child(cwd, remote, args)?;
    let child_pid = child.id();
    info.codex_pid = Some(child_pid);
    info.updated_at = now_secs();
    info.ended_at = None;
    info.exit_code = None;
    info.status = "running".to_string();
    info.codex_status = None;
    info.codex_active_flags.clear();
    info.codex_status_updated_at = None;
    let _ = api_post_json(
        "/clients/register",
        &serde_json::to_value(&info).unwrap_or_else(|_| json!({})),
    );

    let child_event_tx = event_tx.clone();
    thread::spawn(move || {
        let result = child.wait().map_err(|err| err.to_string());
        let _ = child_event_tx.send(ClientEvent::CodexExited { generation, result });
    });
    Ok(child_pid)
}

fn wait_for_client_transport(paths: &RuntimePaths) {
    let mut announced = false;
    loop {
        if paths.api_socket.exists()
            && api_get_json("/status").is_ok()
            && wait_for_app_server_ready(paths, Duration::from_millis(250)).is_ok()
        {
            if announced {
                eprintln!("yolo: server transport recovered; resuming Codex child");
            }
            return;
        }
        if !announced {
            eprintln!("yolo: server transport unavailable; keeping client resident and retrying");
            announced = true;
        }
        thread::sleep(CLIENT_RECOVERY_RETRY_DELAY);
    }
}

fn run_client(args: Vec<OsString>) {
    apply_client_scope_memory_budget();
    if let Err(err) = ensure_server() {
        eprintln!("yolo: server is not ready: {err}; client will remain resident and retry");
    }

    let paths = loop {
        match runtime_paths() {
            Ok(paths) => break paths,
            Err(err) => {
                eprintln!("yolo: runtime paths unavailable: {err}; retrying");
                thread::sleep(CLIENT_RECOVERY_RETRY_DELAY);
            }
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
            eprintln!("yolo: {err}; continuing with the requested arguments");
            args.clone()
        }
    };
    let default_configuration = yolo_default_configuration_from_server();
    let resolved_args = strip_conflicting_yolo_options(with_yolo_session_defaults(
        resolved_args,
        default_configuration.as_ref(),
    ));
    // Keep the effective launch intent, including yolo's injected defaults,
    // for every later re-exec. Capturing the pre-default arguments here loses
    // the original model mode when a client is resumed after a restart.
    let original_args = resolved_args.clone();
    ensure_codex_project_trusted(&codex_cwd);
    let string_args = resolved_args
        .iter()
        .map(|arg| arg.to_string_lossy().to_string())
        .collect::<Vec<_>>();

    let initial_config = read_codex_config();
    let launch_config = parse_codex_launch_config(&string_args);
    let initial_service_tier = launch_config
        .service_tier
        .clone()
        .or(initial_config.service_tier);
    let initial_fast = is_fast_tier(initial_service_tier.as_deref());
    let initial_fast_known = initial_service_tier.is_some();
    let thread_id = thread_id_from_args(&resolved_args);
    let resume_thread_id = thread_id.clone();
    let resume_configuration = resume_thread_id.as_ref().and_then(|_| {
        resume_configuration_for_args(&original_args, default_configuration.as_ref())
    });
    if let Some(thread_id) = thread_id.as_deref() {
        let mut duplicate_notice = false;
        while let Some(existing) = running_duplicate_thread_client(thread_id, std::process::id()) {
            if !duplicate_notice {
                eprintln!(
                    "yolo: duplicate resume for thread {thread_id} is active in {existing}; client will remain resident and retry"
                );
                duplicate_notice = true;
            }
            thread::sleep(CLIENT_RECOVERY_RETRY_DELAY);
        }
    }
    let mut info = ClientInfo {
        id: client_id.clone(),
        yolo_pid: std::process::id(),
        codex_pid: None,
        cwd: cwd.clone(),
        args: string_args,
        remote: upstream_remote.clone(),
        model: launch_config.model.or(initial_config.model),
        service_tier: initial_service_tier,
        reasoning_effort: launch_config.reasoning_effort,
        fast: initial_fast,
        fast_known: initial_fast_known,
        settings_source: "launch_args".to_string(),
        settings_observed_at: Some(now_secs()),
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
        settings_reconfigure_generation: 0,
    };

    let heartbeat_id = client_id.clone();
    let (event_tx, event_rx) = mpsc::channel::<ClientEvent>();
    let client_proxy = match spawn_client_thread_proxy(
        &paths,
        &client_id,
        &upstream_remote,
        event_tx.clone(),
        resume_thread_id.as_deref(),
    ) {
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
    let seen_resume_generation = Arc::new(AtomicU64::new(current_resume_generation()));
    let heartbeat_seen_generation = Arc::clone(&seen_resume_generation);
    let heartbeat_seen_settings_reconfigure_generation = Arc::new(AtomicU64::new(0));
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
                    if let Some(generation) = value
                        .get("settings_reconfigure_generation")
                        .and_then(Value::as_u64)
                    {
                        let seen =
                            heartbeat_seen_settings_reconfigure_generation.load(Ordering::SeqCst);
                        if generation > seen {
                            heartbeat_seen_settings_reconfigure_generation
                                .store(generation, Ordering::SeqCst);
                            if heartbeat_event_tx
                                .send(ClientEvent::SettingsReconfigureRequested)
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                    // A normal app-server restart or transport loss must never
                    // proactively kill a terminal-bound Codex process. An
                    // upgrade generation is only a deferred request here. The
                    // client loop claims it after observing an idle thread;
                    // the server performs the final idle check as well.
                    if let Some(generation) = resume_generation_from_status(&value) {
                        let seen = heartbeat_seen_generation.load(Ordering::SeqCst);
                        if generation > seen {
                            heartbeat_seen_generation.store(generation, Ordering::SeqCst);
                            if heartbeat_event_tx
                                .send(ClientEvent::UpgradeResumeRequested)
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

    let mut active_args = resolved_args.clone();
    let mut child_generation = 0_u64;
    let mut child_pid = loop {
        match spawn_and_register_codex_child(
            &cwd,
            &remote,
            &active_args,
            child_generation,
            &mut info,
            &event_tx,
        ) {
            Ok(pid) => break pid,
            Err(err) => {
                eprintln!("yolo: {err}; client wrapper remains resident and retries");
                wait_for_client_transport(&paths);
            }
        }
    };

    let mut pending_settings_applied = None;
    let mut resume_policy_requested = false;
    let mut last_child_restart = None;
    let mut pending_upgrade_resume = false;
    let mut next_upgrade_reexec_attempt = None;
    loop {
        match event_rx.recv_timeout(CLIENT_RECOVERY_RETRY_DELAY) {
            Ok(ClientEvent::SettingsReconfigureRequested) => {
                // The server sends this only after thread/settings/update has
                // succeeded. Restart only the terminal-bound Codex child so
                // the yolo wrapper itself remains alive. The wrapper may
                // therefore continue supervising the session across settings
                // changes, app-server outages, and proxy reconnects.
                let settings = current_client_resume_settings(&client_id);
                if settings.model.is_none()
                    && settings.service_tier.is_none()
                    && settings.reasoning_effort.is_none()
                {
                    eprintln!(
                        "yolo: settings update for {client_id} had no launch settings; keeping current child"
                    );
                    continue;
                }
                active_args = override_client_settings_args(
                    if let Some(thread_id) = settings.thread_id.as_deref() {
                        resume_args_for(&original_args, Some(thread_id))
                    } else {
                        original_args.clone()
                    },
                    &settings,
                );
                child_generation = child_generation.wrapping_add(1);
                terminate_pid_tree(child_pid, CLIENT_CHILD_RESTART_TIMEOUT);
                wait_for_client_transport(&paths);
                loop {
                    match spawn_and_register_codex_child(
                        &cwd,
                        &remote,
                        &active_args,
                        child_generation,
                        &mut info,
                        &event_tx,
                    ) {
                        Ok(pid) => {
                            child_pid = pid;
                            info.args = active_args
                                .iter()
                                .map(|arg| arg.to_string_lossy().to_string())
                                .collect();
                            last_child_restart = Some(Instant::now());
                            break;
                        }
                        Err(err) => {
                            eprintln!("yolo: {err}; retrying settings child restart");
                            wait_for_client_transport(&paths);
                        }
                    }
                }
            }
            Ok(ClientEvent::UpgradeResumeRequested) => {
                // Authentication and a server-side generation are only a
                // request. Do not claim the permit, stop the child, or
                // re-exec while the local thread is still working. The
                // bottom-of-loop check retries after status events and on a
                // timeout, so a turn-completed notification is sufficient to
                // release the request without another heartbeat race.
                pending_upgrade_resume = true;
                next_upgrade_reexec_attempt = None;
            }
            Ok(ClientEvent::ProxyDisconnected { error }) => {
                eprintln!(
                    "yolo: client proxy lost the app-server transport: {error}; keeping yolo client resident"
                );
                if last_child_restart
                    .is_some_and(|started| started.elapsed() < CLIENT_RESTART_DEBOUNCE)
                {
                    continue;
                }
                child_generation = child_generation.wrapping_add(1);
                terminate_pid_tree(child_pid, CLIENT_CHILD_RESTART_TIMEOUT);
                wait_for_client_transport(&paths);
                loop {
                    match spawn_and_register_codex_child(
                        &cwd,
                        &remote,
                        &active_args,
                        child_generation,
                        &mut info,
                        &event_tx,
                    ) {
                        Ok(pid) => {
                            child_pid = pid;
                            last_child_restart = Some(Instant::now());
                            break;
                        }
                        Err(err) => {
                            eprintln!("yolo: {err}; retrying after proxy recovery");
                            wait_for_client_transport(&paths);
                        }
                    }
                }
            }
            Ok(ClientEvent::ResumeBootstrapCompleted) => {
                if !resume_policy_requested && let Some(thread_id) = resume_thread_id.as_deref() {
                    resume_policy_requested = true;
                    request_resume_policy_preparation(
                        &client_id,
                        thread_id,
                        &codex_cwd,
                        resume_configuration.as_ref(),
                    );
                }
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
            Ok(ClientEvent::ThreadStatus {
                thread_id,
                status,
                active_flags,
            }) => {
                if info.thread_id.as_deref().is_none()
                    || info.thread_id.as_deref() == Some(thread_id.as_str())
                {
                    info.thread_id = Some(thread_id);
                    if info.thread_id_source == "unresolved" {
                        info.thread_id_source = "proxy".to_string();
                    }
                    info.codex_status = Some(status);
                    info.codex_active_flags = active_flags;
                    info.codex_status_updated_at = Some(now_secs());
                    info.updated_at = now_secs();
                    let _ = api_post_json(
                        "/clients/register",
                        &serde_json::to_value(&info).unwrap_or_else(|_| json!({})),
                    );
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
                if info.thread_id.as_deref() == Some(thread_id.as_str()) {
                    info.codex_status = Some("active".to_string());
                    info.codex_active_flags.clear();
                    info.codex_status_updated_at = Some(now_secs());
                    info.updated_at = now_secs();
                }
                let _ = api_post_json(
                    "/turns/input",
                    &json!({
                        "thread_id": thread_id,
                        "turn_id": turn_id,
                        "prompt": prompt,
                    }),
                );
            }
            Ok(ClientEvent::CodexExited { generation, result }) => {
                if generation != child_generation {
                    continue;
                }
                match result {
                    Ok(status) if codex_child_exit_is_user_interrupt(&status) => {
                        info.updated_at = now_secs();
                        info.ended_at = Some(now_secs());
                        info.status = "exited".to_string();
                        info.exit_code = status.code().or(Some(130));
                        let _ = api_post_json(
                            "/clients/finish",
                            &serde_json::to_value(&info).unwrap_or_else(|_| json!({})),
                        );
                        std::process::exit(info.exit_code.unwrap_or(130));
                    }
                    Ok(status) => {
                        eprintln!(
                            "yolo: codex child exited unexpectedly with status {status:?}; restarting child"
                        );
                    }
                    Err(err) => {
                        eprintln!("yolo: failed to wait for codex: {err}; restarting child");
                    }
                }
                child_generation = child_generation.wrapping_add(1);
                wait_for_client_transport(&paths);
                loop {
                    match spawn_and_register_codex_child(
                        &cwd,
                        &remote,
                        &active_args,
                        child_generation,
                        &mut info,
                        &event_tx,
                    ) {
                        Ok(pid) => {
                            child_pid = pid;
                            last_child_restart = Some(Instant::now());
                            break;
                        }
                        Err(err) => {
                            eprintln!("yolo: {err}; retrying after child exit");
                            wait_for_client_transport(&paths);
                        }
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                eprintln!(
                    "yolo: client event channel closed unexpectedly; keeping wrapper resident"
                );
                thread::sleep(CLIENT_RECOVERY_RETRY_DELAY);
            }
        }

        if pending_upgrade_resume && client_is_waiting_for_upgrade(&info) {
            let retry_allowed = next_upgrade_reexec_attempt
                .map(|deadline| Instant::now() >= deadline)
                .unwrap_or(true);
            if retry_allowed {
                match try_claim_upgrade_reexec_permit(&client_id) {
                    Ok(UpgradeReexecClaimResult::Granted) => {
                        next_upgrade_reexec_attempt = None;
                        // The server's claim performs a second idle check. Repeat the
                        // local check immediately before terminating the child as well;
                        // a queued working/status event must never be treated as an exit
                        // authorization.
                        if client_is_waiting_for_upgrade(&info) {
                            if let Some(proxy) = client_proxy.as_ref() {
                                let _ = remove_socket_if_present(&proxy.socket_path);
                                let _ = fs::remove_file(&proxy.pending_settings_path);
                            }
                            terminate_pid_tree(child_pid, CLIENT_CHILD_RESTART_TIMEOUT);
                            reexec_client_for_resume(&original_args, &client_id);
                        }
                    }
                    Ok(
                        UpgradeReexecClaimResult::GateAbsent
                        | UpgradeReexecClaimResult::ClientUnknown,
                    ) => {
                        // The generation notification may outlive the serialized
                        // server-side gate (for example when a restart follows a
                        // timed-out migration). Do not retain a stale request and
                        // emit a claim failure on every idle transition forever.
                        pending_upgrade_resume = false;
                        next_upgrade_reexec_attempt = None;
                    }
                    Ok(
                        UpgradeReexecClaimResult::WaitingStateAbsent
                        | UpgradeReexecClaimResult::NotClientTurn,
                    ) => {
                        // A real gate may still be active for another client, or the
                        // server may have observed a newer working status. Keep the
                        // request but avoid a one-second log/HTTP retry loop.
                        next_upgrade_reexec_attempt =
                            Some(Instant::now() + UPGRADE_REEXEC_RETRY_DELAY);
                    }
                    Ok(UpgradeReexecClaimResult::Unknown) => {
                        // Keep compatibility with an older server that does not
                        // return a claim reason, but bound the retry rate.
                        next_upgrade_reexec_attempt =
                            Some(Instant::now() + UPGRADE_REEXEC_RETRY_DELAY);
                    }
                    Err(err) => {
                        next_upgrade_reexec_attempt =
                            Some(Instant::now() + UPGRADE_REEXEC_RETRY_DELAY);
                        eprintln!(
                            "yolo: cannot verify upgrade-resume gate for {client_id}; retrying: {err}"
                        );
                    }
                }
            }
        }
    }
}

fn run_native_codex_passthrough(args: Vec<OsString>) -> ! {
    apply_client_scope_memory_budget();
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
    let default_configuration = yolo_default_configuration_from_server();
    let resolved_args = strip_conflicting_yolo_options(with_yolo_session_defaults(
        resolved_args,
        default_configuration.as_ref(),
    ));
    let launch_args = codex_args_with_cwd(resolved_args.clone(), &cwd);
    let mut command = Command::new(native_codex_executable());
    command.current_dir(&cwd).args(yolo_mode_cli_args());
    if resume_target_from_args(&resolved_args).is_some() {
        command.arg("-c").arg("include_environment_context=false");
    }
    let err = command.args(&launch_args).exec();
    eprintln!("yolo codex: failed to exec native codex: {err}");
    std::process::exit(127);
}

fn apply_client_scope_memory_budget() {
    let Ok(cgroup) = fs::read_to_string("/proc/self/cgroup") else {
        return;
    };
    let Some(scope) = cgroup.lines().find_map(|line| {
        let path = line.strip_prefix("0::")?.trim();
        let unit = Path::new(path).file_name()?.to_str()?;
        (unit.starts_with("tmux-spawn-") && unit.ends_with(".scope")).then(|| unit.to_string())
    }) else {
        return;
    };

    let memory_high = format!("MemoryHigh={CLIENT_SCOPE_MEMORY_HIGH}");
    let memory_max = format!("MemoryMax={CLIENT_SCOPE_MEMORY_MAX}");
    let memory_swap_max = format!("MemorySwapMax={CLIENT_SCOPE_MEMORY_SWAP_MAX}");
    let result = Command::new("systemctl")
        .args([
            "--user",
            "set-property",
            "--runtime",
            scope.as_str(),
            memory_high.as_str(),
            memory_max.as_str(),
            memory_swap_max.as_str(),
        ])
        .output();
    match result {
        Ok(output) if output.status.success() => {}
        Ok(output) => {
            let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
            eprintln!(
                "yolo: failed to apply client scope memory budget to {scope}: {}",
                if detail.is_empty() {
                    format!("systemctl exited with {}", output.status)
                } else {
                    detail
                }
            );
        }
        Err(err) => eprintln!("yolo: failed to apply client scope memory budget to {scope}: {err}"),
    }
}

fn request_resume_policy_preparation(
    client_id: &str,
    thread_id: &str,
    cwd: &str,
    configuration: Option<&YoloDefaultConfiguration>,
) {
    // Resume policy belongs to the yolo server, which owns the app-server
    // connection. The client only submits a bounded launch intent; it never
    // scans or rewrites Codex rollout/state files on the normal launch path.
    let request = PrepareResumeRequest {
        client_id: client_id.to_string(),
        thread_id: thread_id.to_string(),
        cwd: cwd.to_string(),
        configuration: configuration.cloned(),
    };
    if let Err(err) = api_post_json(
        "/clients/prepare-resume",
        &serde_json::to_value(request).unwrap_or_else(|_| json!({})),
    ) {
        eprintln!(
            "yolo: failed to schedule server-side resume policy preparation for {thread_id}: {err}"
        );
    }
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
    let value = api_post_json("/resume/resolve-last", &json!({"cwd": cwd}))?;
    if value.get("ok").and_then(Value::as_bool) == Some(false) {
        return Err(value
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("server could not resolve resume --last")
            .to_string());
    }
    let thread_id = value
        .get("thread_id")
        .and_then(Value::as_str)
        .filter(|thread_id| !thread_id.trim().is_empty())
        .ok_or_else(|| format!("server returned no resume thread for {cwd}"))?;
    replace_resume_last_with_thread(args, thread_id)
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

fn resolve_resume_last_thread_on_server(
    state: &Arc<Mutex<ServerState>>,
    cwd: &str,
) -> Option<String> {
    let current_pid = std::process::id();
    latest_resume_candidate_for_cwd_from(session_candidates(), cwd, |candidate| {
        if running_duplicate_thread_process(&candidate.id, current_pid).is_some() {
            return true;
        }
        let Some(modified_secs) = candidate
            .modified
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_secs())
        else {
            return false;
        };
        let Ok(state) = state.lock() else {
            return false;
        };
        state.clients.values().any(|client| {
            if client.thread_id.as_deref() != Some(candidate.id.as_str()) {
                return false;
            }
            if matches!(client.status.as_str(), "running" | "restarting") {
                return true;
            }
            client.status == "exited"
                && client
                    .ended_at
                    .is_some_and(|ended_at| modified_secs > ended_at.saturating_add(5))
        })
    })
    .map(|candidate| candidate.id)
}

fn is_app_server_thread_not_found_error(err: &str, thread_id: &str) -> bool {
    err.contains(&format!("thread not found: {thread_id}"))
}

fn resume_configuration_for_args(
    args: &[OsString],
    default_configuration: Option<&YoloDefaultConfiguration>,
) -> Option<YoloDefaultConfiguration> {
    let default_configuration = default_configuration?;
    let strings = args
        .iter()
        .map(|arg| arg.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    let explicit = parse_codex_launch_config(&strings);
    Some(YoloDefaultConfiguration {
        model: explicit
            .model
            .unwrap_or_else(|| default_configuration.model.clone()),
        reasoning_effort: explicit
            .reasoning_effort
            .unwrap_or_else(|| default_configuration.reasoning_effort.clone()),
        fast: explicit
            .service_tier
            .as_deref()
            .map(|tier| is_fast_tier(Some(tier)))
            .unwrap_or(default_configuration.fast),
    })
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
    let Ok(input) = fs::File::open(path) else {
        return (id, cwd);
    };
    let reader = BufReader::new(input);
    for line in reader.lines().take(20) {
        let Ok(line) = line else {
            break;
        };
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
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

fn session_rewrite_temp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("rollout.jsonl");
    path.with_file_name(format!(
        ".{file_name}.yolo-rewrite-{}-{}.tmp",
        std::process::id(),
        now_millis()
    ))
}

fn rewrite_session_line(line: &[u8], cwd: &str) -> Result<Option<Vec<u8>>, String> {
    let has_newline = line.last() == Some(&b'\n');
    let raw = if has_newline {
        &line[..line.len().saturating_sub(1)]
    } else {
        line
    };
    let Ok(raw_text) = std::str::from_utf8(raw) else {
        return Ok(None);
    };

    if raw_text.contains("\"session_meta\"") || raw_text.contains("\"turn_context\"") {
        if let Ok(mut value) = serde_json::from_slice::<Value>(raw)
            && matches!(
                value.get("type").and_then(Value::as_str),
                Some("session_meta" | "turn_context")
            )
        {
            let is_turn_context = value.get("type").and_then(Value::as_str) == Some("turn_context");
            let mut changed = false;
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
            }
            if changed {
                let mut replacement = serde_json::to_vec(&value).map_err(|err| err.to_string())?;
                if has_newline {
                    replacement.push(b'\n');
                }
                return Ok(Some(replacement));
            }
        }
    }

    if raw_text.contains("<permissions instructions>")
        || raw_text.contains("<environment_context>")
        || raw_text.contains("sandbox_mode")
    {
        if let Ok(mut value) = serde_json::from_slice::<Value>(raw)
            && value.get("type").and_then(Value::as_str) == Some("response_item")
            && repair_resume_context_message(&mut value, cwd)
        {
            let mut replacement = serde_json::to_vec(&value).map_err(|err| err.to_string())?;
            if has_newline {
                replacement.push(b'\n');
            }
            return Ok(Some(replacement));
        }
    }

    Ok(None)
}

fn write_session_line<W: Write>(
    writer: &mut W,
    line: &[u8],
    cwd: &str,
    changed: &mut bool,
) -> Result<(), String> {
    if let Some(replacement) = rewrite_session_line(line, cwd)? {
        writer
            .write_all(&replacement)
            .map_err(|err| err.to_string())?;
        *changed = true;
    } else {
        writer.write_all(line).map_err(|err| err.to_string())?;
    }
    Ok(())
}

fn rewrite_session_meta_cwd(path: &Path, cwd: &str) -> Result<bool, String> {
    // Rollout JSONL can contain gigabytes of command output. Never materialize
    // the whole file; large individual records are copied without parsing.
    let input = fs::File::open(path).map_err(|err| err.to_string())?;
    let permissions = input
        .metadata()
        .map_err(|err| err.to_string())?
        .permissions();
    let temp_path = session_rewrite_temp_path(path);
    let temp_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .map_err(|err| err.to_string())?;
    if let Err(err) = fs::set_permissions(&temp_path, permissions) {
        let _ = fs::remove_file(&temp_path);
        return Err(err.to_string());
    }

    let mut changed = false;
    let result = (|| -> Result<(), String> {
        let mut reader = BufReader::new(input);
        let mut writer = BufWriter::new(temp_file);
        let mut line = Vec::with_capacity(8192);

        'lines: loop {
            line.clear();
            let mut saw_any = false;
            let mut oversized = false;

            loop {
                let buffer = reader.fill_buf().map_err(|err| err.to_string())?;
                if buffer.is_empty() {
                    if !saw_any {
                        break 'lines;
                    }
                    if !oversized {
                        write_session_line(&mut writer, &line, cwd, &mut changed)?;
                    }
                    break;
                }

                saw_any = true;
                let newline = buffer.iter().position(|byte| *byte == b'\n');
                let take_len = newline.map_or(buffer.len(), |offset| offset + 1);
                if !oversized
                    && line.len().saturating_add(take_len) <= MAX_SESSION_REPAIR_LINE_BYTES
                {
                    line.extend_from_slice(&buffer[..take_len]);
                } else {
                    if !oversized {
                        writer.write_all(&line).map_err(|err| err.to_string())?;
                        line.clear();
                        oversized = true;
                    }
                    writer
                        .write_all(&buffer[..take_len])
                        .map_err(|err| err.to_string())?;
                }
                reader.consume(take_len);

                if newline.is_some() {
                    if !oversized {
                        write_session_line(&mut writer, &line, cwd, &mut changed)?;
                    }
                    break;
                }
            }
        }
        writer.flush().map_err(|err| err.to_string())
    })();

    if let Err(err) = result {
        let _ = fs::remove_file(&temp_path);
        return Err(err);
    }
    if !changed {
        let _ = fs::remove_file(&temp_path);
        return Ok(false);
    }
    fs::rename(&temp_path, path).map_err(|err| {
        let _ = fs::remove_file(&temp_path);
        err.to_string()
    })?;
    Ok(true)
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
            fast_known: false,
            settings_source: "unknown".to_string(),
            settings_observed_at: None,
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
            settings_reconfigure_generation: 0,
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
            active_sessions: BTreeMap::new(),
            default_configuration: None,
            slaves: BTreeMap::new(),
            telemetry: AgentTelemetry::default(),
            federation_push_senders: BTreeMap::new(),
            status_event_senders: BTreeMap::new(),
            next_status_event_id: 0,
            upgrade_reexec_queue: VecDeque::new(),
            upgrade_reexec_active: None,
        }
    }

    #[test]
    fn upgrade_resume_waits_for_local_idle_status_before_reexec() {
        let mut client = test_client(
            "client",
            &["resume", "thread-client"],
            "/tmp/client",
            Some("thread-client"),
        );
        assert!(!client_is_waiting_for_upgrade(&client));

        client.codex_status = Some("idle".to_string());
        client.codex_status_updated_at = Some(now_secs());
        assert!(client_is_waiting_for_upgrade(&client));

        client
            .codex_active_flags
            .push("waiting_on_approval".to_string());
        assert!(!client_is_waiting_for_upgrade(&client));
    }

    #[cfg(unix)]
    #[test]
    fn only_sigint_child_exit_is_terminal() {
        assert!(codex_child_exit_is_user_interrupt(&ExitStatus::from_raw(2)));
        assert!(codex_child_exit_is_user_interrupt(&ExitStatus::from_raw(
            130 << 8
        )));
        assert!(!codex_child_exit_is_user_interrupt(&ExitStatus::from_raw(
            1 << 8
        )));
    }

    #[cfg(unix)]
    #[test]
    fn client_proxy_releases_upstream_when_child_disconnects() {
        let temp_dir = env::temp_dir().join(format!(
            "yolo-client-proxy-disconnect-{}-{}",
            std::process::id(),
            now_millis()
        ));
        fs::create_dir_all(&temp_dir).unwrap();
        let upstream_path = temp_dir.join("app-server.sock");
        let pending_settings_path = temp_dir.join("pending-settings.json");
        let upstream_listener = UnixListener::bind(&upstream_path).unwrap();
        let (upstream_closed_tx, upstream_closed_rx) = mpsc::channel();
        let upstream_thread = thread::spawn(move || {
            let (mut stream, _) = upstream_listener.accept().unwrap();
            let request = read_http_headers(&mut stream).unwrap();
            assert!(request.starts_with("GET "));
            stream
                .write_all(
                    b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
                )
                .unwrap();
            let mut byte = [0_u8; 1];
            let read = stream.read(&mut byte).unwrap();
            upstream_closed_tx.send(read).unwrap();
        });

        let (mut child_stream, proxy_stream) = UnixStream::pair().unwrap();
        let (event_tx, _event_rx) = mpsc::channel();
        let proxy_upstream_path = upstream_path.clone();
        let (proxy_done_tx, proxy_done_rx) = mpsc::channel();
        let proxy_thread = thread::spawn(move || {
            let result = run_client_proxy_connection(
                proxy_stream,
                &proxy_upstream_path,
                &event_tx,
                &pending_settings_path,
                Some("thread-test"),
            );
            proxy_done_tx.send(result).unwrap();
        });

        child_stream
            .write_all(b"GET / HTTP/1.1\r\nHost: yolo\r\nUpgrade: websocket\r\n\r\n")
            .unwrap();
        let response = read_http_headers(&mut child_stream).unwrap();
        assert!(response.starts_with("HTTP/1.1 101"));
        child_stream.shutdown(Shutdown::Both).unwrap();
        drop(child_stream);

        let proxy_result = proxy_done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("proxy relay should stop after the child disconnects");
        assert!(proxy_result.is_ok(), "proxy result: {proxy_result:?}");
        assert_eq!(
            upstream_closed_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("upstream connection should be closed"),
            0
        );
        proxy_thread.join().unwrap();
        upstream_thread.join().unwrap();
        let _ = fs::remove_file(&upstream_path);
        let _ = fs::remove_dir(&temp_dir);
    }

    #[test]
    fn upgrade_memory_headroom_scales_with_client_count() {
        let required = upgrade_memory_requirement_bytes(4);
        assert!(upgrade_memory_headroom_sufficient(
            required,
            Some(UPGRADE_MIN_SWAP_FREE_MIB * 1024 * 1024),
            4
        ));
        assert!(!upgrade_memory_headroom_sufficient(
            required - 1,
            Some(UPGRADE_MIN_SWAP_FREE_MIB * 1024 * 1024),
            4
        ));
        assert!(!upgrade_memory_headroom_sufficient(
            required,
            Some(UPGRADE_MIN_SWAP_FREE_MIB * 1024 * 1024 - 1),
            4
        ));
    }

    #[test]
    fn upgrade_reexec_gate_allows_only_one_client_at_a_time() {
        let mut first = test_client(
            "first",
            &["resume", "thread-first"],
            "/tmp/first",
            Some("thread-first"),
        );
        first.yolo_pid = 11;
        first.codex_status = Some("idle".to_string());
        first.codex_status_updated_at = Some(now_secs());
        first.updated_at = now_secs();
        let mut second = test_client(
            "second",
            &["resume", "thread-second"],
            "/tmp/second",
            Some("thread-second"),
        );
        second.yolo_pid = 22;
        second.codex_status = Some("waiting".to_string());
        second.codex_status_updated_at = Some(now_secs());
        second.updated_at = now_secs();
        let state = Arc::new(Mutex::new(test_state(vec![first, second])));
        let request = UpgradeResumeAllRequest::default();

        assert_eq!(prepare_upgrade_reexec_gate(&state, &request), 2);
        assert!(claim_upgrade_reexec_permit(&state, "first").unwrap());
        assert!(!claim_upgrade_reexec_permit(&state, "second").unwrap());
        {
            let mut state = state.lock().unwrap();
            release_upgrade_reexec_permit_locked(&mut state, 11);
        }
        assert!(claim_upgrade_reexec_permit(&state, "second").unwrap());
    }

    #[test]
    fn upgrade_reexec_gate_can_limit_migration_to_selected_clients() {
        let mut first = test_client(
            "first",
            &["resume", "thread-first"],
            "/tmp/first",
            Some("thread-first"),
        );
        first.yolo_pid = 11;
        first.codex_status = Some("idle".to_string());
        first.codex_status_updated_at = Some(now_secs());
        let mut second = test_client(
            "second",
            &["resume", "thread-second"],
            "/tmp/second",
            Some("thread-second"),
        );
        second.yolo_pid = 22;
        second.codex_status = Some("idle".to_string());
        second.codex_status_updated_at = Some(now_secs());
        second.updated_at = now_secs();
        let state = Arc::new(Mutex::new(test_state(vec![first, second])));
        let selected = BTreeSet::from(["second".to_string()]);

        assert_eq!(
            prepare_upgrade_reexec_gate_for_client_ids(
                &state,
                &selected,
                &UpgradeResumeAllRequest::default()
            ),
            1
        );
        assert!(!claim_upgrade_reexec_permit(&state, "first").unwrap());
        assert!(claim_upgrade_reexec_permit(&state, "second").unwrap());
    }

    #[test]
    fn upgrade_reexec_claim_requires_explicit_gate_and_waiting_state() {
        let mut client = test_client(
            "client",
            &["resume", "thread-client"],
            "/tmp/client",
            Some("thread-client"),
        );
        client.codex_status = Some("idle".to_string());
        client.codex_status_updated_at = Some(now_secs());
        client.updated_at = now_secs();
        let state = Arc::new(Mutex::new(test_state(vec![client])));

        // An idle client is still not allowed to re-exec unless an explicit
        // upgrade-resume operation installed the gate.
        assert!(!claim_upgrade_reexec_permit(&state, "client").unwrap());

        assert_eq!(
            prepare_upgrade_reexec_gate(&state, &UpgradeResumeAllRequest::default()),
            1
        );
        assert!(claim_upgrade_reexec_permit(&state, "client").unwrap());
    }

    #[test]
    fn upgrade_reexec_claim_reports_absent_gate_for_stale_requests() {
        let mut client = test_client(
            "client",
            &["resume", "thread-client"],
            "/tmp/client",
            Some("thread-client"),
        );
        client.codex_status = Some("idle".to_string());
        client.codex_status_updated_at = Some(now_secs());
        client.updated_at = now_secs();
        let state = Arc::new(Mutex::new(test_state(vec![client])));

        assert_eq!(
            claim_upgrade_reexec_permit_result(&state, "client").unwrap(),
            UpgradeReexecClaimResult::GateAbsent
        );
    }

    #[test]
    fn upgrade_reexec_claim_rejects_active_or_unknown_client_status() {
        for status in [None, Some("active"), Some("notLoaded")] {
            let mut client = test_client(
                "client",
                &["resume", "thread-client"],
                "/tmp/client",
                Some("thread-client"),
            );
            client.codex_status = status.map(ToString::to_string);
            client.codex_status_updated_at = Some(now_secs());
            let state = Arc::new(Mutex::new(test_state(vec![client])));
            assert_eq!(
                prepare_upgrade_reexec_gate(&state, &UpgradeResumeAllRequest::default()),
                1
            );
            assert!(!claim_upgrade_reexec_permit(&state, "client").unwrap());
        }
    }

    #[test]
    fn upgrade_idle_wait_requires_explicit_waiting_snapshot() {
        let client = test_client(
            "client",
            &["resume", "thread-client"],
            "/tmp/client",
            Some("thread-client"),
        );
        let statuses = ["active", "notLoaded", "unknown"];
        for status in statuses {
            let snapshot = vec![AppThreadSnapshot {
                id: "thread-client".to_string(),
                cwd: "/tmp/client".to_string(),
                status: status.to_string(),
                active_flags: Vec::new(),
                model: None,
                service_tier: None,
                reasoning_effort: None,
            }];
            assert!(!client_is_waiting_in_snapshot(&client, &snapshot));
        }

        let waiting_snapshot = vec![AppThreadSnapshot {
            id: "thread-client".to_string(),
            cwd: "/tmp/client".to_string(),
            status: "idle".to_string(),
            active_flags: Vec::new(),
            model: None,
            service_tier: None,
            reasoning_effort: None,
        }];
        assert!(client_is_waiting_in_snapshot(&client, &waiting_snapshot));
    }

    #[test]
    fn upgrade_idle_wait_rejects_ambiguous_or_missing_client_thread() {
        let client = test_client("client", &[], "/tmp/client", None);
        let snapshot = vec![
            AppThreadSnapshot {
                id: "thread-a".to_string(),
                cwd: "/tmp/client".to_string(),
                status: "idle".to_string(),
                active_flags: Vec::new(),
                model: None,
                service_tier: None,
                reasoning_effort: None,
            },
            AppThreadSnapshot {
                id: "thread-b".to_string(),
                cwd: "/tmp/client".to_string(),
                status: "idle".to_string(),
                active_flags: Vec::new(),
                model: None,
                service_tier: None,
                reasoning_effort: None,
            },
        ];
        assert!(!client_is_waiting_in_snapshot(&client, &snapshot));
        assert!(!client_is_waiting_in_snapshot(&client, &[]));
    }

    #[test]
    fn active_sessions_round_trip_atomically() {
        let path = std::env::temp_dir().join(format!(
            "yolo-active-sessions-test-{}-{}.json",
            std::process::id(),
            now_millis()
        ));
        let record = ActiveSessionRecord {
            client_id: "client-1".to_string(),
            cwd: "/tmp/project".to_string(),
            args: vec!["resume".to_string(), "thread-1".to_string()],
            model: Some("gpt-5.6-sol".to_string()),
            service_tier: Some("default".to_string()),
            reasoning_effort: Some("low".to_string()),
            fast: false,
            fast_known: true,
            settings_complete: true,
            settings_source: "app_server".to_string(),
            settings_observed_at: Some(43),
            thread_id: Some("thread-1".to_string()),
            thread_id_source: "resume_arg".to_string(),
            started_at: 42,
        };
        let mut sessions = BTreeMap::new();
        sessions.insert(record.client_id.clone(), record.clone());

        persist_active_sessions_snapshot(&path, &sessions).expect("persist sessions");
        let loaded = load_active_sessions(&path);
        assert_eq!(loaded.get("client-1"), Some(&record));
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );

        fs::remove_file(path).expect("remove test sessions file");
    }

    #[test]
    fn active_sessions_v1_migrates_complete_settings_metadata() {
        let path = std::env::temp_dir().join(format!(
            "yolo-active-sessions-v1-test-{}-{}.json",
            std::process::id(),
            now_millis()
        ));
        fs::write(
            &path,
            r#"{
                "version": 1,
                "saved_at": 42,
                "sessions": [{
                    "client_id": "client-v1",
                    "cwd": "/tmp/project",
                    "args": ["resume", "thread-v1"],
                    "model": "gpt-5.6-luna",
                    "service_tier": "priority",
                    "reasoning_effort": "max",
                    "fast": true,
                    "thread_id": "thread-v1",
                    "thread_id_source": "resume_arg",
                    "started_at": 42
                }]
            }"#,
        )
        .expect("write v1 sessions");

        let record = load_active_sessions(&path)
            .remove("client-v1")
            .expect("migrated v1 record");
        assert!(record.fast_known);
        assert!(record.settings_complete);
        assert_eq!(record.settings_source, "legacy");

        fs::remove_file(path).expect("remove test sessions file");
    }

    #[test]
    fn widget_default_configuration_round_trip_atomically() {
        let path = std::env::temp_dir().join(format!(
            "yolo-default-configuration-test-{}-{}.json",
            std::process::id(),
            now_millis()
        ));
        let configuration = YoloDefaultConfiguration {
            model: "gpt-5.6-luna".to_string(),
            reasoning_effort: "max".to_string(),
            fast: true,
        };
        persist_yolo_default_configuration(&path, &configuration)
            .expect("persist default configuration");
        assert_eq!(load_yolo_default_configuration(&path), Some(configuration));
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::remove_file(path).expect("remove default configuration");
    }

    #[test]
    fn active_session_upsert_replaces_old_thread_record() {
        let client = test_client(
            "client-2",
            &["resume", "thread-2"],
            "/tmp/project",
            Some("thread-2"),
        );
        let mut state = test_state(Vec::new());
        state.active_sessions.insert(
            "old-client".to_string(),
            ActiveSessionRecord {
                client_id: "old-client".to_string(),
                cwd: client.cwd.clone(),
                args: vec!["resume".to_string(), "thread-2".to_string()],
                model: None,
                service_tier: None,
                reasoning_effort: None,
                fast: false,
                fast_known: false,
                settings_complete: false,
                settings_source: "unknown".to_string(),
                settings_observed_at: None,
                thread_id: client.thread_id.clone(),
                thread_id_source: "resume_arg".to_string(),
                started_at: 1,
            },
        );

        assert!(remove_active_session_matches_client(
            &mut state.active_sessions,
            &client
        ));
        assert!(upsert_active_session_locked(&mut state, &client));
        assert!(state.active_sessions.contains_key("client-2"));
        assert!(!state.active_sessions.contains_key("old-client"));
    }

    #[test]
    fn active_session_upsert_preserves_complete_settings_from_incomplete_scan() {
        let mut saved = test_client(
            "saved-client",
            &["resume", "thread-settings", "--model", "gpt-5.6-luna"],
            "/tmp/project",
            Some("thread-settings"),
        );
        saved.model = Some("gpt-5.6-luna".to_string());
        saved.service_tier = Some("priority".to_string());
        saved.reasoning_effort = Some("max".to_string());
        saved.fast = true;
        saved.fast_known = true;
        saved.settings_source = "app_server".to_string();
        saved.settings_observed_at = Some(100);
        saved.updated_at = 100;

        let mut state = test_state(Vec::new());
        state
            .active_sessions
            .insert(saved.id.clone(), active_session_record_from_client(&saved));

        let scanned = test_client(
            "scanned-client",
            &["resume", "thread-settings"],
            "/tmp/project",
            Some("thread-settings"),
        );
        assert!(upsert_active_session_locked(&mut state, &scanned));

        let record = state
            .active_sessions
            .get("scanned-client")
            .expect("rebound session record");
        assert_eq!(record.model.as_deref(), Some("gpt-5.6-luna"));
        assert_eq!(record.service_tier.as_deref(), Some("priority"));
        assert_eq!(record.reasoning_effort.as_deref(), Some("max"));
        assert!(record.fast);
        assert!(record.fast_known);
        assert!(record.settings_complete);
        assert_eq!(record.settings_source, "app_server");
        assert!(!state.active_sessions.contains_key("saved-client"));
    }

    #[test]
    fn active_session_upsert_preserves_launch_settings_over_stale_app_server() {
        let mut client = test_client(
            "client-settings",
            &["resume", "thread-settings"],
            "/tmp/project",
            Some("thread-settings"),
        );
        client.model = Some("gpt-5.6-sol".to_string());
        client.service_tier = Some("priority".to_string());
        client.reasoning_effort = Some("max".to_string());
        client.fast = true;
        client.fast_known = true;
        client.settings_source = "launch_args".to_string();
        client.settings_observed_at = Some(100);
        let mut state = test_state(Vec::new());
        state.active_sessions.insert(
            client.id.clone(),
            active_session_record_from_client(&client),
        );

        client.model = Some("gpt-5.6-luna".to_string());
        client.service_tier = Some("default".to_string());
        client.reasoning_effort = Some("low".to_string());
        client.fast = false;
        client.settings_source = "app_server".to_string();
        client.settings_observed_at = Some(101);

        assert!(upsert_active_session_locked(&mut state, &client));
        let record = state
            .active_sessions
            .get("client-settings")
            .expect("updated session record");
        assert_eq!(record.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(record.service_tier.as_deref(), Some("priority"));
        assert_eq!(record.reasoning_effort.as_deref(), Some("max"));
        assert!(record.fast);
        assert!(record.fast_known);
        assert!(record.settings_complete);
        assert_eq!(record.settings_source, "launch_args");
    }

    #[test]
    fn active_session_record_derives_fast_from_known_service_tier() {
        let mut client = test_client(
            "client-fast",
            &["resume", "thread-fast"],
            "/tmp/project",
            Some("thread-fast"),
        );
        client.service_tier = Some("priority".to_string());
        client.fast = false;
        let record = active_session_record_from_client(&client);
        assert!(record.fast);
        assert!(record.fast_known);

        client.service_tier = Some("default".to_string());
        client.fast = true;
        let record = active_session_record_from_client(&client);
        assert!(!record.fast);
        assert!(record.fast_known);
    }

    #[test]
    fn registered_client_reconciles_scan_duplicate_and_saved_record() {
        let mut stale = test_client(
            "client-3-scanned",
            &["resume"],
            "/tmp/project",
            Some("thread-3"),
        );
        stale.yolo_pid = 303;
        let mut state = test_state(vec![stale.clone()]);
        state
            .active_sessions
            .insert(stale.id.clone(), active_session_record_from_client(&stale));

        let mut registered = test_client(
            "client-3",
            &["resume", "thread-3"],
            "/tmp/project",
            Some("thread-3"),
        );
        registered.yolo_pid = stale.yolo_pid;

        assert!(reconcile_registered_client_process(&mut state, &registered));
        assert!(!state.clients.contains_key(&stale.id));
        assert!(!state.active_sessions.contains_key(&stale.id));
    }

    #[test]
    fn registered_client_replaces_stale_same_thread_and_preserves_saved_record() {
        let mut stale = test_client(
            "client-4-stale",
            &["resume"],
            "/tmp/project",
            Some("thread-4"),
        );
        stale.yolo_pid = 404;
        stale.status = "stale".to_string();
        let mut state = test_state(vec![stale.clone()]);
        state
            .active_sessions
            .insert(stale.id.clone(), active_session_record_from_client(&stale));

        let mut registered = test_client(
            "client-4-new",
            &["resume", "thread-4"],
            "/tmp/project",
            Some("thread-4"),
        );
        registered.yolo_pid = 405;

        assert!(reconcile_registered_client_process(&mut state, &registered));
        assert!(!state.clients.contains_key(&stale.id));
        assert!(state.active_sessions.contains_key(&stale.id));

        assert!(upsert_active_session_locked(&mut state, &registered));
        assert!(!state.active_sessions.contains_key(&stale.id));
        assert!(state.active_sessions.contains_key(&registered.id));
    }

    #[test]
    fn scanned_yolo_pid_replaces_stale_saved_records() {
        let mut sessions = BTreeMap::new();
        sessions.insert(
            "303-older".to_string(),
            ActiveSessionRecord {
                client_id: "303-older".to_string(),
                cwd: "/tmp/project".to_string(),
                args: vec!["resume".to_string()],
                model: None,
                service_tier: None,
                reasoning_effort: None,
                fast: false,
                fast_known: false,
                settings_complete: false,
                settings_source: "unknown".to_string(),
                settings_observed_at: None,
                thread_id: None,
                thread_id_source: "unresolved".to_string(),
                started_at: 1,
            },
        );
        sessions.insert(
            "404-keep".to_string(),
            ActiveSessionRecord {
                client_id: "404-keep".to_string(),
                cwd: "/tmp/project".to_string(),
                args: vec![],
                model: None,
                service_tier: None,
                reasoning_effort: None,
                fast: false,
                fast_known: false,
                settings_complete: false,
                settings_source: "unknown".to_string(),
                settings_observed_at: None,
                thread_id: None,
                thread_id_source: "unresolved".to_string(),
                started_at: 1,
            },
        );

        assert!(remove_active_sessions_for_yolo_pid_except(
            &mut sessions,
            303,
            None
        ));
        assert!(!sessions.contains_key("303-older"));
        assert!(sessions.contains_key("404-keep"));
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
            "method": "item/started",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "item": {
                    "type": "agentMessage",
                    "id": "commentary-1",
                    "phase": "commentary"
                }
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/agentMessage/delta",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "itemId": "commentary-1",
                "delta": "I will "
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/agentMessage/delta",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "itemId": "commentary-1",
                "delta": "inspect the configuration."
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/completed",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "item": {
                    "type": "agentMessage",
                    "id": "commentary-1",
                    "phase": "commentary",
                    "text": "I will inspect the configuration."
                }
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/started",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "item": {
                    "type": "agentMessage",
                    "id": "assistant-1",
                    "phase": "final_answer"
                }
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/agentMessage/delta",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "itemId": "assistant-1",
                "delta": "The service was restored and verified."
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/reasoning/summaryTextDelta",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "delta": "The configuration appears "
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/reasoning/summaryTextDelta",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "delta": "to be the relevant boundary."
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/reasoning/textDelta",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "delta": "Inspect config.toml"
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/completed",
            "params": {
                "threadId": "root",
                "turnId": "turn-1",
                "item": {
                    "type": "reasoning",
                    "id": "reasoning-1",
                    "summary": [{"text": "The configuration appears to be the relevant boundary."}],
                    "content": [{"text": "Inspect config.toml"}]
                }
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
            turn.result.as_deref(),
            Some("The service was restored and verified.")
        );
        assert_eq!(
            turn.commentary.as_deref(),
            Some("I will inspect the configuration.")
        );
        assert_eq!(turn.commentary_entries.len(), 1);
        assert_eq!(
            turn.commentary_entries[0].text,
            "I will inspect the configuration."
        );
        assert_eq!(
            turn.reasoning_summary.as_deref(),
            Some("The configuration appears to be the relevant boundary.")
        );
        assert_eq!(turn.reasoning_summary_entries.len(), 1);
        assert_eq!(
            turn.reasoning_summary_entries[0].text,
            "The configuration appears to be the relevant boundary."
        );
        assert_eq!(turn.reasoning_raw.as_deref(), Some("Inspect config.toml"));
        assert_eq!(turn.reasoning_raw_entries.len(), 1);
        assert_eq!(turn.reasoning_raw_entries[0].text, "Inspect config.toml");
        assert_eq!(telemetry.summary().captured_prompt_count, 1);
        assert_eq!(telemetry.summary().captured_report_count, 1);
        assert_eq!(telemetry.summary().captured_commentary_count, 1);
        assert_eq!(telemetry.summary().captured_reasoning_summary_count, 1);
        assert_eq!(telemetry.summary().captured_reasoning_raw_count, 1);
    }

    #[test]
    fn telemetry_keeps_multiple_trace_items_without_delta_duplication() {
        let mut telemetry = AgentTelemetry::default();
        telemetry.record_app_server_event(&json!({
            "method": "turn/started",
            "params": {"threadId": "root", "turnId": "turn-multi"}
        }));
        for (item_id, delta, text) in [
            ("commentary-1", "First progress", "First progress"),
            ("commentary-2", "Second progress", "Second progress"),
        ] {
            telemetry.record_app_server_event(&json!({
                "method": "item/started",
                "params": {
                    "threadId": "root",
                    "turnId": "turn-multi",
                    "item": {"type": "agentMessage", "id": item_id, "phase": "commentary"}
                }
            }));
            telemetry.record_app_server_event(&json!({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "root",
                    "turnId": "turn-multi",
                    "itemId": item_id,
                    "delta": delta
                }
            }));
            telemetry.record_app_server_event(&json!({
                "method": "item/completed",
                "params": {
                    "threadId": "root",
                    "turnId": "turn-multi",
                    "item": {
                        "type": "agentMessage",
                        "id": item_id,
                        "phase": "commentary",
                        "text": text
                    }
                }
            }));
        }
        telemetry.record_app_server_event(&json!({
            "method": "item/reasoning/summaryTextDelta",
            "params": {
                "threadId": "root",
                "turnId": "turn-multi",
                "itemId": "reasoning-1",
                "delta": "Summary "
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/completed",
            "params": {
                "threadId": "root",
                "turnId": "turn-multi",
                "item": {
                    "type": "reasoning",
                    "id": "reasoning-1",
                    "summary": [{"text": "Summary complete"}]
                }
            }
        }));

        let turns = telemetry.turns_snapshot(Some("root"), 10).turns;
        assert_eq!(turns.len(), 1);
        let turn = &turns[0];
        assert_eq!(turn.commentary_entries.len(), 2);
        assert_eq!(
            turn.commentary_entries[0].item_id.as_deref(),
            Some("commentary-1")
        );
        assert_eq!(turn.commentary_entries[0].text, "First progress");
        assert_eq!(
            turn.commentary_entries[1].item_id.as_deref(),
            Some("commentary-2")
        );
        assert_eq!(turn.commentary_entries[1].text, "Second progress");
        assert!(
            turn.commentary_entries[0].sequence < turn.commentary_entries[1].sequence,
            "commentary entries should retain their capture order"
        );
        assert_eq!(
            turn.commentary.as_deref(),
            Some("First progress\nSecond progress")
        );
        assert_eq!(turn.reasoning_summary_entries.len(), 1);
        assert_eq!(turn.reasoning_summary_entries[0].text, "Summary complete");
        assert!(
            turn.commentary_entries[1].sequence < turn.reasoning_summary_entries[0].sequence,
            "trace sequence should span commentary and reasoning kinds"
        );
        assert_eq!(turn.reasoning_summary.as_deref(), Some("Summary complete"));
    }

    #[test]
    fn telemetry_captures_plan_updates_and_plan_items_as_separate_entries() {
        let mut telemetry = AgentTelemetry::default();
        telemetry.record_app_server_event(&json!({
            "method": "turn/started",
            "params": {"threadId": "root", "turnId": "turn-plan"}
        }));
        telemetry.record_app_server_event(&json!({
            "method": "turn/plan/updated",
            "params": {
                "threadId": "root",
                "turnId": "turn-plan",
                "explanation": null,
                "plan": [{"step": "Inspect the source", "status": "inProgress"}]
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "turn/plan/updated",
            "params": {
                "threadId": "root",
                "turnId": "turn-plan",
                "explanation": "The first check is complete.",
                "plan": [
                    {"step": "Inspect the source", "status": "completed"},
                    {"step": "Apply the fix", "status": "pending"}
                ]
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/plan/delta",
            "params": {
                "threadId": "root",
                "turnId": "turn-plan",
                "itemId": "plan-item-1",
                "delta": "Plan item "
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/plan/delta",
            "params": {
                "threadId": "root",
                "turnId": "turn-plan",
                "itemId": "plan-item-1",
                "delta": "stream"
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "item/completed",
            "params": {
                "threadId": "root",
                "turnId": "turn-plan",
                "item": {"type": "plan", "id": "plan-item-1", "text": "Final plan item"}
            }
        }));

        let turns = telemetry.turns_snapshot(Some("root"), 10).turns;
        assert_eq!(turns.len(), 1);
        let turn = &turns[0];
        assert_eq!(turn.plan_entries.len(), 3);
        assert_eq!(
            turn.plan_entries[0].item_id.as_deref(),
            Some("plan-update-1")
        );
        assert!(turn.plan_entries[0].text.contains("Updated Plan"));
        assert!(turn.plan_entries[0].text.contains("◐ Inspect the source"));
        assert_eq!(
            turn.plan_entries[1].item_id.as_deref(),
            Some("plan-update-2")
        );
        assert!(
            turn.plan_entries[1]
                .text
                .contains("The first check is complete.")
        );
        assert_eq!(turn.plan_entries[2].item_id.as_deref(), Some("plan-item-1"));
        assert_eq!(turn.plan_entries[2].text, "Final plan item");
        assert_eq!(telemetry.summary().captured_plan_count, 1);
    }

    #[test]
    fn turn_result_serialization_migrates_legacy_report_field() {
        let legacy = json!({
            "thread_id": "root",
            "turn_id": "turn-1",
            "status": "completed",
            "prompt": "Do the work",
            "report": "Work completed",
            "updated_at": 100
        });
        let turn: TurnInfo =
            serde_json::from_value(legacy).expect("legacy turn should deserialize");
        assert_eq!(turn.result.as_deref(), Some("Work completed"));
        let serialized = serde_json::to_value(turn).expect("turn should serialize");
        assert_eq!(
            serialized.get("result").and_then(Value::as_str),
            Some("Work completed")
        );
        assert!(serialized.get("report").is_none());
    }

    #[test]
    fn telemetry_accepts_nested_turn_lifecycle_notifications() {
        let mut telemetry = AgentTelemetry::default();
        telemetry.record_app_server_event(&json!({
            "method": "turn/started",
            "params": {
                "threadId": "root",
                "turn": {"id": "turn-nested", "startedAtMs": 1200}
            }
        }));
        telemetry.record_app_server_event(&json!({
            "method": "turn/completed",
            "params": {
                "threadId": "root",
                "turn": {
                    "id": "turn-nested",
                    "status": {"type": "completed"},
                    "completedAtMs": 2400
                }
            }
        }));
        let snapshot = telemetry.turns_snapshot(Some("root"), 10);
        assert_eq!(snapshot.turns.len(), 1);
        assert_eq!(snapshot.turns[0].status, "completed");
        assert_eq!(snapshot.turns[0].started_at_ms, Some(1200));
        assert_eq!(snapshot.turns[0].completed_at_ms, Some(2400));
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
                    {"type": "reasoning", "summary": [{"text": "I should inspect the diff."}], "content": [{"text": "Compare the changed files."}]},
                    {"type": "plan", "id": "plan-1", "text": "Updated Plan\n  └ □ Inspect the diff."},
                    {"type": "agentMessage", "phase": "final_answer", "text": "The change is complete."}
                ]
            }]
        });

        let turns = parse_thread_history(&history, 10);
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].prompt.as_deref(), Some("What changed?"));
        assert_eq!(turns[0].result.as_deref(), Some("The change is complete."));
        assert_eq!(turns[0].commentary.as_deref(), Some("I will inspect it."));
        assert_eq!(turns[0].commentary_entries.len(), 1);
        assert_eq!(
            turns[0].reasoning_summary.as_deref(),
            Some("I should inspect the diff.")
        );
        assert_eq!(
            turns[0].reasoning_raw.as_deref(),
            Some("Compare the changed files.")
        );
        assert_eq!(turns[0].reasoning_summary_entries.len(), 1);
        assert_eq!(turns[0].reasoning_raw_entries.len(), 1);
        assert_eq!(
            turns[0].plan.as_deref(),
            Some("Updated Plan\n  └ □ Inspect the diff.")
        );
        assert_eq!(turns[0].plan_entries.len(), 1);
        assert_eq!(turns[0].started_at_ms, Some(100_000));
        assert_eq!(turns[0].completed_at_ms, Some(110_000));
    }

    #[test]
    fn proxy_turn_start_request_captures_input_for_server() {
        let (event_tx, event_rx) = mpsc::channel();
        let tracker = Arc::new(Mutex::new(ThreadBindingTracker {
            pending_create_request_ids: BTreeSet::new(),
            pending_resume_request_ids: BTreeSet::new(),
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
    fn thread_id_configuration_takes_precedence_over_stale_client_id() {
        let state = Arc::new(Mutex::new(test_state(vec![
            test_client(
                "current-client",
                &["resume", "thread-current"],
                "/home/vagrant/head",
                Some("thread-current"),
            ),
            test_client(
                "stale-client",
                &["resume", "thread-stale"],
                "/home/vagrant/moon",
                Some("thread-stale"),
            ),
        ])));
        let request = ConfigureClientsRequest {
            client_id: Some("stale-client".to_string()),
            thread_id: Some("thread-current".to_string()),
            model: Some("gpt-5.6-luna".to_string()),
            ..ConfigureClientsRequest::default()
        };

        let selected = select_configure_clients(&state, &request).unwrap();
        assert_eq!(selected, BTreeSet::from(["current-client".to_string()]));
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
    fn websocket_close_frame_is_masked_with_normal_status() {
        let mut frame = Vec::new();
        websocket_send_close(&mut frame).unwrap();

        assert_eq!(frame.len(), 8);
        assert_eq!(frame[0], 0x88);
        assert_eq!(frame[1], 0x82);
        assert_eq!(&frame[2..6], &[0x63, 0x6c, 0x6f, 0x73]);
        assert_eq!(
            &frame[6..8],
            &[
                1000u16.to_be_bytes()[0] ^ 0x63,
                1000u16.to_be_bytes()[1] ^ 0x6c
            ]
        );
    }

    #[test]
    fn yolo_auto_approval_response_accepts_supported_approval_requests() {
        for method in [
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
            "execCommandApproval",
        ] {
            let response = yolo_auto_approval_response(&json!({
                "id": 42,
                "method": method,
                "params": {"threadId": "thread-1"}
            }))
            .expect("supported approval request");
            assert_eq!(response["id"], 42);
            assert_eq!(response["result"]["decision"], "accept");
        }
    }

    #[test]
    fn yolo_auto_approval_response_ignores_notifications_and_unknown_methods() {
        assert!(
            yolo_auto_approval_response(&json!({
                "method": "item/commandExecution/requestApproval",
                "params": {}
            }))
            .is_none()
        );
        assert!(
            yolo_auto_approval_response(&json!({
                "id": 42,
                "method": "item/permissions/requestApproval",
                "params": {}
            }))
            .is_none()
        );
        assert!(
            yolo_auto_approval_response(&json!({
                "id": 42,
                "method": "thread/started",
                "params": {}
            }))
            .is_none()
        );
    }

    #[test]
    fn federation_master_endpoint_parses_http_authority() {
        let endpoint = federation_master_endpoint("http://kagura-sandbox:47040/api").unwrap();
        assert_eq!(
            (endpoint.host, endpoint.port),
            ("kagura-sandbox".to_string(), 47040)
        );
        let endpoint = federation_master_endpoint("http://127.0.0.1").unwrap();
        assert_eq!(
            (endpoint.host, endpoint.port),
            ("127.0.0.1".to_string(), 80)
        );
    }

    #[test]
    fn federation_master_endpoint_preserves_agent_gate_tls_and_path() {
        let endpoint =
            federation_master_endpoint("https://agent-gate.example/agt_token/@localhost:47040")
                .unwrap();
        assert_eq!(endpoint.host, "agent-gate.example");
        assert_eq!(endpoint.port, 443);
        assert!(endpoint.tls);
        assert_eq!(endpoint.host_header, "agent-gate.example");
        assert_eq!(
            endpoint.websocket_path(),
            "/agt_token/@localhost:47040/federation/slaves/stream"
        );
    }

    #[test]
    fn federation_master_endpoint_uses_explicit_port_and_plain_http() {
        let endpoint = federation_master_endpoint("http://127.0.0.1:47040").unwrap();
        assert_eq!(endpoint.host, "127.0.0.1");
        assert_eq!(endpoint.port, 47040);
        assert!(!endpoint.tls);
        assert_eq!(endpoint.websocket_path(), "/federation/slaves/stream");
    }

    #[test]
    fn command_output_with_timeout_kills_stalled_process() {
        let started = Instant::now();
        let mut command = Command::new("sh");
        command.args(["-c", "sleep 2"]);

        let result = command_output_with_timeout(command, Duration::from_millis(100));

        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn tmux_pane_snapshot_preserves_pane_id() {
        let pane = parse_tmux_pane_line(
            "kagura\t1\t0\t%1\t1234\t/dev/pts/1\t/home/vagrant/kagura\tbash",
            "websh",
        )
        .expect("tmux pane should parse");

        assert_eq!(pane.pane_id.as_deref(), Some("%1"));
        assert_eq!(pane.window_index, Some(1));
        assert_eq!(pane.pane_index, Some(0));
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
    fn status_subscription_ignores_bootstrap_until_active_grace_expires() {
        let mut client = test_client(
            "resume",
            &["resume", "thread-active"],
            "/home/vagrant/head",
            Some("thread-active"),
        );
        client.codex_status = Some("active".to_string());
        client.codex_status_updated_at = Some(now_secs());
        let state = Arc::new(Mutex::new(test_state(vec![client.clone()])));
        assert!(known_active_client_thread_ids(&state).is_empty());

        client.codex_status_updated_at =
            Some(now_secs().saturating_sub(APP_SERVER_STATUS_SUBSCRIPTION_GRACE.as_secs() + 1));
        let state = Arc::new(Mutex::new(test_state(vec![client])));
        assert_eq!(
            known_active_client_thread_ids(&state),
            BTreeSet::from(["thread-active".to_string()])
        );
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
            settings_source: String::new(),
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
            settings_source: String::new(),
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
    fn resume_args_with_current_settings_replaces_stale_launch_settings_after_configure() {
        let settings = ClientResumeSettings {
            thread_id: Some("019e-thread".to_string()),
            model: Some("gpt-5.6-luna".to_string()),
            service_tier: Some("priority".to_string()),
            reasoning_effort: Some("max".to_string()),
            settings_source: "configure".to_string(),
        };
        let args = resume_args_with_current_settings(
            os_args(&[
                "-c",
                "model=gpt-5.6-sol",
                "-c",
                "service_tier=default",
                "-c",
                "model_reasoning_effort=low",
                "resume",
                "019e-thread",
            ]),
            &settings,
        );
        assert_eq!(
            string_args(args),
            vec![
                "-c",
                "model=\"gpt-5.6-luna\"",
                "-c",
                "service_tier=\"priority\"",
                "-c",
                "model_reasoning_effort=\"max\"",
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
            state: 'S',
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
    fn stopped_or_zombie_process_is_not_live_for_client_reconciliation() {
        let mut process = ProcInfo {
            pid: 1,
            ppid: 0,
            state: 'S',
            comm: "yolo".to_string(),
            cmdline: vec!["yolo".to_string(), "resume".to_string()],
            cwd: Some("/tmp/project".to_string()),
        };
        assert!(process_is_live(&process));
        process.state = 'T';
        assert!(!process_is_live(&process));
        process.state = 'Z';
        assert!(!process_is_live(&process));
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
                state: 'S',
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
                state: 'S',
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
                state: 'S',
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
                state: 'S',
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
    fn thread_snapshot_preserves_launch_settings_after_resume() {
        let state = Arc::new(Mutex::new(test_state(vec![test_client(
            "resumed",
            &[
                "-c",
                "model=\"gpt-5.6-sol\"",
                "-c",
                "service_tier=priority",
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
        assert_eq!(client.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(client.service_tier.as_deref(), Some("priority"));
        assert_eq!(client.reasoning_effort.as_deref(), Some("xhigh"));
        assert!(client.fast);
        assert_eq!(client.settings_source, "launch_args");
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
            pending_resume_request_ids: BTreeSet::new(),
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
    fn websocket_resume_success_emits_bootstrap_completion_once() {
        let (event_tx, event_rx) = mpsc::channel();
        let tracker = Arc::new(Mutex::new(ThreadBindingTracker {
            pending_create_request_ids: BTreeSet::new(),
            pending_resume_request_ids: BTreeSet::new(),
            current_thread_id: Some("thread-resumed".to_string()),
            event_tx,
        }));

        observe_client_app_server_request(
            &tracker,
            &json!({
                "id": 17,
                "method": "thread/resume",
                "params": { "threadId": "thread-resumed" }
            }),
        );
        assert!(event_rx.try_recv().is_err());

        observe_app_server_response(
            &tracker,
            &json!({"id": 17, "result": {"thread": {"id": "thread-resumed"}}}),
        );

        assert!(matches!(
            event_rx.recv_timeout(Duration::from_millis(50)),
            Ok(ClientEvent::ResumeBootstrapCompleted)
        ));
        assert!(event_rx.try_recv().is_err());
    }

    #[test]
    fn websocket_resume_failure_does_not_schedule_policy_update() {
        let (event_tx, event_rx) = mpsc::channel();
        let tracker = Arc::new(Mutex::new(ThreadBindingTracker {
            pending_create_request_ids: BTreeSet::new(),
            pending_resume_request_ids: BTreeSet::new(),
            current_thread_id: Some("thread-resumed".to_string()),
            event_tx,
        }));

        observe_client_app_server_request(
            &tracker,
            &json!({
                "id": 18,
                "method": "thread/resume",
                "params": { "threadId": "thread-resumed" }
            }),
        );
        observe_app_server_response(
            &tracker,
            &json!({"id": 18, "error": {"message": "turn/start failed in TUI"}}),
        );

        assert!(event_rx.try_recv().is_err());
    }

    #[test]
    fn websocket_thread_start_response_binds_the_proxy_client_to_created_thread() {
        let (event_tx, event_rx) = mpsc::channel();
        let tracker = Arc::new(Mutex::new(ThreadBindingTracker {
            pending_create_request_ids: BTreeSet::new(),
            pending_resume_request_ids: BTreeSet::new(),
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
    fn websocket_thread_started_notification_does_not_bind_an_uninitialized_proxy() {
        let (event_tx, event_rx) = mpsc::channel();
        let tracker = Arc::new(Mutex::new(ThreadBindingTracker {
            pending_create_request_ids: BTreeSet::new(),
            pending_resume_request_ids: BTreeSet::new(),
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

        assert!(event_rx.try_recv().is_err());
        assert_eq!(tracker.lock().unwrap().current_thread_id, None);
    }

    #[test]
    fn websocket_thread_started_notification_does_not_replace_resume_target() {
        let (event_tx, event_rx) = mpsc::channel();
        let tracker = Arc::new(Mutex::new(ThreadBindingTracker {
            pending_create_request_ids: BTreeSet::new(),
            pending_resume_request_ids: BTreeSet::new(),
            current_thread_id: Some("thread-resumed".to_string()),
            event_tx,
        }));

        observe_app_server_response(
            &tracker,
            &json!({
                "method": "thread/started",
                "params": { "thread": { "id": "thread-other" } }
            }),
        );

        assert!(event_rx.try_recv().is_err());
        assert_eq!(
            tracker.lock().unwrap().current_thread_id.as_deref(),
            Some("thread-resumed")
        );
    }

    #[test]
    fn websocket_string_request_id_binds_thread_start_response() {
        let (event_tx, event_rx) = mpsc::channel();
        let tracker = Arc::new(Mutex::new(ThreadBindingTracker {
            pending_create_request_ids: BTreeSet::new(),
            pending_resume_request_ids: BTreeSet::new(),
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
    fn recent_thread_inventory_binds_bare_client_by_launch_time() {
        let mut client = test_client("1968389-1785961698573", &[], "/home/vagrant/head", None);
        client.remote =
            "unix:///run/user/1000/yolo/client-proxies/1968389-1785961698573.sock".to_string();
        let mut state = test_state(vec![client]);
        state.telemetry.record_thread_value(&json!({
            "id": "thread-created",
            "cwd": "/home/vagrant/head",
            "createdAt": 1785961709,
            "updatedAt": 1785961709,
            "status": { "type": "idle" }
        }));
        let state = Arc::new(Mutex::new(state));

        let bound = bind_unresolved_clients_to_recent_threads(&state);

        assert_eq!(bound, vec!["1968389-1785961698573"]);
        assert_eq!(
            state.lock().unwrap().clients["1968389-1785961698573"]
                .thread_id
                .as_deref(),
            Some("thread-created")
        );
    }

    #[test]
    fn settings_override_replaces_explicit_bare_client_configuration() {
        let settings = ClientResumeSettings {
            model: Some("gpt-5.6-luna".to_string()),
            service_tier: Some("priority".to_string()),
            reasoning_effort: Some("max".to_string()),
            ..ClientResumeSettings::default()
        };
        let args = override_client_settings_args(
            os_args(&[
                "-c",
                "model=\"gpt-5.6-sol\"",
                "-c",
                "model_reasoning_effort=\"low\"",
                "-c",
                "service_tier=\"default\"",
                "--search",
            ]),
            &settings,
        );
        let parsed = parse_codex_launch_config(&string_args(args));

        assert_eq!(parsed.model.as_deref(), Some("gpt-5.6-luna"));
        assert_eq!(parsed.service_tier.as_deref(), Some("priority"));
        assert_eq!(parsed.reasoning_effort.as_deref(), Some("max"));
    }

    #[test]
    fn settings_update_schedules_reconfigure_after_server_ack() {
        let state = Arc::new(Mutex::new(test_state(vec![test_client(
            "client",
            &["resume", "thread-settings"],
            "/home/vagrant/head",
            Some("thread-settings"),
        )])));

        note_client_settings_update(
            &state,
            "client",
            Some("gpt-5.6-sol".to_string()),
            Some(true),
            Some("max".to_string()),
        );
        request_client_reconfigure(&state, "client");

        let state = state.lock().unwrap();
        let client = &state.clients["client"];
        assert_eq!(client.settings_reconfigure_generation, 1);
        assert_eq!(client.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(client.service_tier.as_deref(), Some("priority"));
        assert_eq!(client.reasoning_effort.as_deref(), Some("max"));
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
    fn rewrite_session_meta_cwd_preserves_large_rollout_records_without_loading_them() {
        let path = env::temp_dir().join(format!(
            "yolo-session-large-record-test-{}-{}.jsonl",
            std::process::id(),
            now_millis()
        ));
        let large_record = vec![b'x'; MAX_SESSION_REPAIR_LINE_BYTES + 1024];
        let mut input = br#"{"type":"session_meta","payload":{"cwd":"/tmp"}}
"#
        .to_vec();
        input.extend_from_slice(&large_record);
        input.push(b'\n');
        input.extend_from_slice(b"{\"type\":\"tail\"}\n");
        fs::write(&path, input).unwrap();

        assert!(rewrite_session_meta_cwd(&path, "/home/vagrant/websh").unwrap());
        let output = fs::read(&path).unwrap();
        let first_line = output
            .split(|byte| *byte == b'\n')
            .next()
            .expect("session metadata line");
        let first_value: Value = serde_json::from_slice(first_line).unwrap();
        assert_eq!(
            first_value["payload"]["cwd"],
            Value::String("/home/vagrant/websh".to_string())
        );
        assert!(
            output
                .windows(large_record.len())
                .any(|window| window == large_record.as_slice())
        );
        assert!(output.ends_with(b"{\"type\":\"tail\"}\n"));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn load_turn_archive_keeps_only_bounded_recent_turns() {
        let path = env::temp_dir().join(format!(
            "yolo-turn-archive-bound-test-{}-{}.jsonl",
            std::process::id(),
            now_millis()
        ));
        let mut contents = String::new();
        for index in 0..(MAX_TELEMETRY_TURNS + 32) {
            let info = TurnInfo {
                thread_id: "thread-archive".to_string(),
                turn_id: format!("turn-{index}"),
                status: "completed".to_string(),
                started_at_ms: None,
                completed_at_ms: None,
                prompt: None,
                result: None,
                commentary: None,
                commentary_entries: Vec::new(),
                reasoning_summary: None,
                reasoning_summary_entries: Vec::new(),
                reasoning_raw: None,
                reasoning_raw_entries: Vec::new(),
                plan: None,
                plan_entries: Vec::new(),
                updated_at: index as u64,
            };
            contents.push_str(&serde_json::to_string(&info).unwrap());
            contents.push('\n');
        }
        fs::write(&path, contents).unwrap();

        let mut telemetry = AgentTelemetry::default();
        load_turn_archive(&path, &mut telemetry);

        assert_eq!(telemetry.turns.len(), MAX_TELEMETRY_TURNS);
        assert!(telemetry.turns.contains_key("thread-archive:turn-543"));
        assert!(!telemetry.turns.contains_key("thread-archive:turn-0"));

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

    #[test]
    fn yolo_session_defaults_follow_widget_configuration() {
        let widget_configuration = YoloDefaultConfiguration {
            model: "gpt-5.6-luna".to_string(),
            reasoning_effort: "max".to_string(),
            fast: true,
        };
        let args = with_yolo_session_defaults(os_args(&[]), Some(&widget_configuration));
        let strings = args
            .iter()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect::<Vec<_>>();
        let config = parse_codex_launch_config(&strings);

        assert_eq!(config.model.as_deref(), Some("gpt-5.6-luna"));
        assert_eq!(config.reasoning_effort.as_deref(), Some("max"));
        assert_eq!(config.service_tier.as_deref(), Some("priority"));

        let unchanged = with_yolo_session_defaults(os_args(&[]), None);
        assert!(unchanged.is_empty());
    }

    #[test]
    fn yolo_session_defaults_preserve_explicit_mode_for_new_and_resume() {
        let widget_configuration = YoloDefaultConfiguration {
            model: "gpt-5.6-luna".to_string(),
            reasoning_effort: "max".to_string(),
            fast: true,
        };
        let explicit = with_yolo_session_defaults(
            os_args(&[
                "-m",
                "gpt-5.6-sol",
                "-c",
                "model_reasoning_effort=low",
                "-c",
                "service_tier=priority",
            ]),
            Some(&widget_configuration),
        );
        let strings = explicit
            .iter()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect::<Vec<_>>();
        let config = parse_codex_launch_config(&strings);
        assert_eq!(config.model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(config.reasoning_effort.as_deref(), Some("low"));
        assert_eq!(config.service_tier.as_deref(), Some("priority"));

        let resume = os_args(&["resume", "thread-1"]);
        let resume = with_yolo_session_defaults(resume, Some(&widget_configuration));
        let strings = resume
            .iter()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect::<Vec<_>>();
        let config = parse_codex_launch_config(&strings);
        assert_eq!(config.model.as_deref(), Some("gpt-5.6-luna"));
        assert_eq!(config.reasoning_effort.as_deref(), Some("max"));
        assert_eq!(config.service_tier.as_deref(), Some("priority"));

        let explicit_resume = with_yolo_session_defaults(
            os_args(&[
                "-c",
                "model=custom",
                "-c",
                "model_reasoning_effort=high",
                "-c",
                "service_tier=priority",
                "resume",
                "thread-1",
            ]),
            Some(&widget_configuration),
        );
        let strings = explicit_resume
            .iter()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect::<Vec<_>>();
        let config = parse_codex_launch_config(&strings);
        assert_eq!(config.model.as_deref(), Some("custom"));
        assert_eq!(config.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(config.service_tier.as_deref(), Some("priority"));
    }

    #[test]
    fn resumed_thread_settings_follow_registered_defaults_when_mode_is_implicit() {
        let defaults = YoloDefaultConfiguration {
            model: "gpt-5.6-luna".to_string(),
            reasoning_effort: "max".to_string(),
            fast: true,
        };
        let configuration =
            resume_configuration_for_args(&os_args(&["resume", "thread-1"]), Some(&defaults))
                .expect("implicit resume configuration");
        assert_eq!(configuration, defaults);

        let explicit = resume_configuration_for_args(
            &os_args(&[
                "-c",
                "model=gpt-5.6-sol",
                "-c",
                "model_reasoning_effort=low",
                "-c",
                "service_tier=default",
                "resume",
                "thread-1",
            ]),
            Some(&defaults),
        )
        .expect("explicit resume configuration");
        assert_eq!(explicit.model, "gpt-5.6-sol");
        assert_eq!(explicit.reasoning_effort, "low");
        assert!(!explicit.fast);
    }

    #[test]
    fn resumed_thread_settings_params_include_registered_mode() {
        let defaults = YoloDefaultConfiguration {
            model: "gpt-5.6-luna".to_string(),
            reasoning_effort: "max".to_string(),
            fast: true,
        };
        let params = resume_thread_settings_params("thread-1", "/tmp/project", Some(&defaults));
        assert_eq!(params["model"], "gpt-5.6-luna");
        assert_eq!(params["serviceTier"], "priority");
        assert_eq!(params["effort"], "max");
        assert_eq!(params["cwd"], "/tmp/project");
    }

    #[test]
    fn yolo_mode_args_override_conflicting_user_permission_flags() {
        let args = strip_conflicting_yolo_options(os_args(&[
            "-s",
            "read-only",
            "--ask-for-approval=on-request",
            "--dangerously-bypass-approvals-and-sandbox",
            "-c",
            "approval_policy=on-request",
            "-c",
            "sandbox_mode=workspace-write",
            "-c",
            "model=gpt-5.5",
            "resume",
            "thread-1",
        ]));
        assert_eq!(
            string_args(args),
            vec!["-c", "model=gpt-5.5", "resume", "thread-1"]
        );

        let required = string_args(yolo_mode_cli_args());
        assert!(required.contains(&"--search".to_string()));
        assert!(required.contains(&"--dangerously-bypass-approvals-and-sandbox".to_string()));
        assert!(
            !required
                .iter()
                .any(|arg| arg == "-a" || arg == "--ask-for-approval")
        );
        assert!(
            required
                .windows(2)
                .any(|pair| pair == ["-s", "danger-full-access"])
        );
        assert!(
            required
                .windows(2)
                .any(|pair| pair == ["-c", "approval_policy=\"never\""])
        );
        assert!(
            required
                .windows(2)
                .any(|pair| pair == ["-c", "check_for_update_on_startup=false"])
        );
    }

    #[test]
    fn native_codex_selection_prefers_explicit_path_then_path_then_managed() {
        let selected = select_native_codex_executable(
            Some(OsString::from("/custom/codex")),
            Some(PathBuf::from("/path/codex")),
            PathBuf::from("/managed/codex"),
        );
        assert_eq!(selected, OsString::from("/custom/codex"));

        let selected = select_native_codex_executable(
            None,
            Some(PathBuf::from("/path/codex")),
            PathBuf::from("/managed/codex"),
        );
        assert_eq!(selected, OsString::from("/path/codex"));

        let selected = select_native_codex_executable(None, None, PathBuf::from("/bin/sh"));
        assert_eq!(selected, OsString::from("/bin/sh"));
    }

    #[test]
    fn empty_native_codex_override_does_not_hide_managed_binary() {
        let selected =
            select_native_codex_executable(Some(OsString::new()), None, PathBuf::from("/bin/sh"));
        assert_eq!(selected, OsString::from("/bin/sh"));
    }
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
        if yolo_pid == current_pid || !pid_is_runnable(yolo_pid) {
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
        if !process_is_live(&process) {
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
    // The direct single-session upgrade also replaces the shared app-server.
    // Confirm the same explicit waiting condition immediately before that
    // lifecycle change; a working client must remain untouched.
    if api_get_json("/status").is_ok()
        && let Err(err) = wait_for_upgrade_resume_clients_idle()
    {
        eprintln!("yolo upgrade-resume: {err}");
        std::process::exit(1);
    }
    if let Err(err) = upgrade_codex_cli() {
        eprintln!("yolo upgrade-resume: {err}");
        std::process::exit(1);
    }
    // Package installation can take long enough for another client to start
    // a turn. Recheck before the app-server is stopped as a final barrier.
    if api_get_json("/status").is_ok()
        && let Err(err) = wait_for_upgrade_resume_clients_idle()
    {
        eprintln!("yolo upgrade-resume: {err}");
        std::process::exit(1);
    }
    // Switch every other managed wrapper to the installed yolo executable
    // before replacing the shared app-server. If this hand-off cannot be
    // authorized and completed, abort without touching the app-server.
    if api_get_json("/status").is_ok() {
        match api_post_json("/upgrade-resume-reexec", &json!({}))
            .and_then(|value| ensure_json_ok(&value).map(|_| value))
        {
            Ok(_) => {}
            Err(err) => {
                eprintln!("yolo upgrade-resume: {err}");
                std::process::exit(1);
            }
        }
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

fn wait_for_upgrade_resume_clients_idle() -> Result<(), String> {
    let timeout = upgrade_idle_wait_timeout();
    let start = SystemTime::now();
    loop {
        let value = api_post_json("/upgrade-resume-preflight", &json!({}))?;
        ensure_json_ok(&value)?;
        let working = value
            .get("working")
            .and_then(Value::as_array)
            .map(|clients| {
                clients
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if working.is_empty() {
            return Ok(());
        }
        if start.elapsed().unwrap_or_default() >= timeout {
            return Err(format!(
                "timed out waiting for Codex clients to become idle: {}",
                working.join(", ")
            ));
        }
        eprintln!(
            "yolo upgrade-resume: waiting for Codex clients to become idle: {}",
            working.join(", ")
        );
        thread::sleep(UPGRADE_IDLE_POLL_INTERVAL);
    }
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
        let _ = signal_pid(pid, "TERM");
    }
    let start = SystemTime::now();
    while start.elapsed().unwrap_or_default() < timeout {
        if !pids.iter().any(|pid| pid_is_alive(*pid)) {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    for pid in pids.iter().rev() {
        let _ = signal_pid(pid, "KILL");
    }
}

fn signal_pid(pid: &u32, signal: &str) -> bool {
    Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(pid.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
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
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
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
    let resume_args = resume_args_with_current_settings(resume_args, &resume_settings);
    eprintln!(
        "yolo: re-executing waiting client for authorized upgrade-resume with args: {}",
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

fn override_client_settings_args(
    args: Vec<OsString>,
    settings: &ClientResumeSettings,
) -> Vec<OsString> {
    let args = strip_client_setting_args(args);
    let mut config_args = Vec::new();
    if let Some(model) = settings.model.as_deref().filter(|value| !value.is_empty()) {
        config_args.push(codex_config_os_arg("model", model));
    }
    if let Some(service_tier) = settings
        .service_tier
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        config_args.push(codex_config_os_arg("service_tier", service_tier));
    }
    if let Some(effort) = settings
        .reasoning_effort
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        config_args.push(codex_config_os_arg("model_reasoning_effort", effort));
    }
    prepend_codex_config_args(args, config_args)
}

fn strip_client_setting_args(args: Vec<OsString>) -> Vec<OsString> {
    let mut output = Vec::with_capacity(args.len());
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        let text = arg.to_string_lossy();
        if matches!(text.as_ref(), "--model" | "-m") {
            let _ = iter.next();
            continue;
        }
        if text == "--config" || text == "-c" {
            let Some(value) = iter.next() else {
                output.push(arg);
                continue;
            };
            if is_client_setting_config(&value.to_string_lossy()) {
                continue;
            }
            output.push(arg);
            output.push(value);
            continue;
        }
        if let Some(value) = text.strip_prefix("--model=")
            && !value.is_empty()
        {
            continue;
        }
        if let Some(value) = text.strip_prefix("--config=")
            && is_client_setting_config(value)
        {
            continue;
        }
        output.push(arg);
    }
    output
}

fn is_client_setting_config(value: &str) -> bool {
    matches!(
        value.split_once('=').map(|(key, _)| key.trim()),
        Some("model" | "service_tier" | "model_reasoning_effort")
    )
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

fn resume_args_with_current_settings(
    args: Vec<OsString>,
    settings: &ClientResumeSettings,
) -> Vec<OsString> {
    if settings.settings_source == "configure" {
        override_client_settings_args(args, settings)
    } else {
        preserve_resume_settings_args(args, settings)
    }
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
    let settings_source = nonempty_json_string(client, "settings_source").unwrap_or_default();
    ClientResumeSettings {
        thread_id,
        model,
        service_tier,
        reasoning_effort,
        settings_source,
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

fn current_resume_generation() -> u64 {
    api_get_json("/status")
        .ok()
        .and_then(|value| resume_generation_from_status(&value))
        .unwrap_or(0)
}

fn resume_generation_from_status(value: &Value) -> Option<u64> {
    value.get("resume_generation").and_then(Value::as_u64)
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

fn print_saved_sessions() -> Result<(), String> {
    let value = api_get_json("/saved-sessions")?;
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct FederationMasterEndpoint {
    host: String,
    port: u16,
    tls: bool,
    host_header: String,
    path_prefix: String,
}

impl FederationMasterEndpoint {
    fn websocket_path(&self) -> String {
        format!(
            "{}/federation/slaves/stream",
            self.path_prefix.trim_end_matches('/')
        )
    }
}

fn federation_master_endpoint(base_url: &str) -> Result<FederationMasterEndpoint, String> {
    let value = base_url.trim();
    let (tls, authority_and_path) = if let Some(value) = value.strip_prefix("https://") {
        (true, value)
    } else if let Some(value) = value.strip_prefix("http://") {
        (false, value)
    } else {
        (false, value)
    };
    let (authority, raw_path) = authority_and_path
        .split_once('/')
        .map_or((authority_and_path, ""), |(authority, path)| {
            (authority, path)
        });
    let authority = authority
        .split('?')
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "YOLO_MASTER_URL has no host".to_string())?;

    let (host, port) = if authority.starts_with('[') {
        let end = authority
            .find(']')
            .ok_or_else(|| "YOLO_MASTER_URL has an invalid IPv6 host".to_string())?;
        let host = authority[1..end].to_string();
        let port = authority
            .get(end + 1..)
            .and_then(|suffix| suffix.strip_prefix(':'))
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(if tls { 443 } else { 80 });
        (host, port)
    } else if let Some((host, port)) = authority.rsplit_once(':')
        && let Ok(port) = port.parse::<u16>()
        && !host.is_empty()
    {
        (host.to_string(), port)
    } else {
        (authority.to_string(), if tls { 443 } else { 80 })
    };

    let host_header = if authority.starts_with('[') {
        authority.to_string()
    } else {
        authority.to_string()
    };
    let path_prefix = if raw_path.is_empty() {
        String::new()
    } else {
        let path = raw_path.split('?').next().unwrap_or_default();
        format!("/{}", path.trim_matches('/'))
    };
    Ok(FederationMasterEndpoint {
        host,
        port,
        tls,
        host_header,
        path_prefix,
    })
}

enum FederationPushStream {
    Plain(TcpStream),
    Tls(StreamOwned<ClientConnection, TcpStream>),
}

impl Read for FederationPushStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buf),
            Self::Tls(stream) => stream.read(buf),
        }
    }
}

impl Write for FederationPushStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buf),
            Self::Tls(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

fn connect_federation_push(
    master_url: &str,
    slave_id: &str,
    version: &str,
    bearer_token: Option<&str>,
) -> Result<FederationPushStream, String> {
    let endpoint = federation_master_endpoint(master_url)?;
    let mut addresses = (endpoint.host.as_str(), endpoint.port)
        .to_socket_addrs()
        .map_err(|err| format!("resolve federation master: {err}"))?;
    let address = addresses
        .next()
        .ok_or_else(|| "federation master has no addresses".to_string())?;
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))
        .map_err(|err| format!("connect federation push: {err}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(35)))
        .map_err(|err| format!("set federation push read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|err| format!("set federation push write timeout: {err}"))?;
    let mut stream = if endpoint.tls {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let server_name = ServerName::try_from(endpoint.host.clone())
            .map_err(|err| format!("invalid federation TLS server name: {err}"))?;
        let connection = ClientConnection::new(std::sync::Arc::new(config), server_name)
            .map_err(|err| format!("create federation TLS connection: {err}"))?;
        FederationPushStream::Tls(StreamOwned::new(connection, stream))
    } else {
        FederationPushStream::Plain(stream)
    };
    let mut request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: eW9sby1mZWRlcmF0aW9uLXB1c2g=\r\nSec-WebSocket-Version: 13\r\n",
        endpoint.websocket_path(),
        endpoint.host_header,
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
                                let completed = SlaveResultRequest {
                                    slave_id: slave_id.clone(),
                                    command_id: command.id,
                                    ok: result.get("ok").and_then(Value::as_bool).unwrap_or(false),
                                    result,
                                };
                                if federation_post_json(
                                    &master_url,
                                    "/federation/slaves/result",
                                    bearer_token.as_deref(),
                                    &serde_json::to_value(&completed).unwrap_or_else(|_| json!({})),
                                )
                                .is_err()
                                {
                                    pending_result = Some(completed);
                                }
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
        "set-defaults" | "set-default-configuration" => {
            let Some(configuration) = command.default_configuration.clone() else {
                return json!({
                    "ok": false,
                    "error": "set-defaults command requires a default_configuration"
                });
            };
            match set_yolo_default_configuration(&state, paths, configuration) {
                Ok(value) => value,
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
            let snapshot = state
                .lock()
                .map(|state| state.telemetry.turns_snapshot(Some(thread_id), limit))
                .unwrap_or_else(|_| TurnArchiveSnapshot {
                    generated_at: now_secs(),
                    turns: Vec::new(),
                });
            json!({
                "ok": true,
                "thread_id": thread_id,
                "generated_at": snapshot.generated_at,
                "turns": snapshot.turns,
            })
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
    if UPGRADE_RESUME_IN_PROGRESS
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Err("a Codex upgrade-resume job is already running".to_string());
    }
    let target_client_ids = upgrade_target_client_ids(&state, request);
    let target_client_count = target_client_ids.len();
    let client_count = state
        .lock()
        .map(|state| {
            state
                .clients
                .values()
                .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
                .count()
        })
        .unwrap_or(target_client_count);
    if let Err(err) = ensure_upgrade_memory_headroom(client_count.max(1)) {
        UPGRADE_RESUME_IN_PROGRESS.store(false, Ordering::SeqCst);
        return Err(err);
    }

    let job_id = format!("upgrade-resume-{}", now_millis());
    let worker_state = Arc::clone(&state);
    let worker_paths = paths.clone();
    let worker_job_id = job_id.clone();
    let worker_codex_version = codex_version.map(ToString::to_string);
    let worker_request = request.clone();
    thread::spawn(move || {
        let result = run_incremental_codex_upgrade_resume_worker(
            worker_state,
            worker_paths,
            target_client_ids,
            worker_codex_version.as_deref(),
            worker_request,
        );
        match result {
            Ok((app_server_pid, app_server_generation, resume_generation)) => eprintln!(
                "yolo upgrade job {worker_job_id} completed: app-server pid={app_server_pid:?} generation={app_server_generation} resume_generation={resume_generation}"
            ),
            Err(err) => eprintln!("yolo upgrade job {worker_job_id} failed: {err}"),
        }
        UPGRADE_RESUME_IN_PROGRESS.store(false, Ordering::SeqCst);
    });

    Ok(json!({
        "ok": true,
        "queued": true,
        "job_id": job_id,
        "codex_version": codex_version,
        "clients": target_client_count,
        "resume_mode": "idle_then_serialized_reexec"
    }))
}

fn upgrade_memory_requirement_bytes(client_count: usize) -> u64 {
    let clients = client_count.min(64) as u64;
    (UPGRADE_MEMORY_BASE_RESERVE_MIB
        .saturating_add(clients.saturating_mul(UPGRADE_MEMORY_PER_CLIENT_RESERVE_MIB)))
    .saturating_mul(1024 * 1024)
}

fn upgrade_memory_headroom_sufficient(
    available_bytes: u64,
    swap_free_bytes: Option<u64>,
    client_count: usize,
) -> bool {
    if available_bytes < upgrade_memory_requirement_bytes(client_count) {
        return false;
    }
    swap_free_bytes.is_none_or(|bytes| {
        bytes == 0 || bytes >= UPGRADE_MIN_SWAP_FREE_MIB.saturating_mul(1024 * 1024)
    })
}

fn meminfo_value_bytes(contents: &str, key: &str) -> Option<u64> {
    contents.lines().find_map(|line| {
        let (name, rest) = line.split_once(':')?;
        if name != key {
            return None;
        }
        let value = rest.split_whitespace().next()?.parse::<u64>().ok()?;
        Some(value.saturating_mul(1024))
    })
}

fn ensure_upgrade_memory_headroom(client_count: usize) -> Result<(), String> {
    let Ok(contents) = fs::read_to_string("/proc/meminfo") else {
        return Ok(());
    };
    let Some(available_bytes) = meminfo_value_bytes(&contents, "MemAvailable") else {
        return Ok(());
    };
    let swap_total = meminfo_value_bytes(&contents, "SwapTotal").unwrap_or(0);
    let swap_free = meminfo_value_bytes(&contents, "SwapFree");
    let effective_swap_free = (swap_total > 0).then_some(swap_free.unwrap_or(0));
    if upgrade_memory_headroom_sufficient(available_bytes, effective_swap_free, client_count) {
        return Ok(());
    }
    let required = upgrade_memory_requirement_bytes(client_count) / (1024 * 1024);
    let available = available_bytes / (1024 * 1024);
    let swap = effective_swap_free.map(|bytes| bytes / (1024 * 1024));
    Err(format!(
        "insufficient memory headroom for upgrade-resume: available={available}MiB required={required}MiB swap_free={swap:?}MiB"
    ))
}

fn upgrade_target_client_ids(
    state: &Arc<Mutex<ServerState>>,
    request: &UpgradeResumeAllRequest,
) -> BTreeSet<String> {
    let Ok(state) = state.lock() else {
        return BTreeSet::new();
    };
    state
        .clients
        .values()
        .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
        .filter(|client| !should_ignore_upgrade_wait_client(client, request))
        .map(|client| client.id.clone())
        .collect()
}

fn upgrade_thread_ids_for_clients(
    state: &Arc<Mutex<ServerState>>,
    client_ids: &BTreeSet<String>,
) -> Option<BTreeSet<String>> {
    let Ok(state) = state.lock() else {
        return None;
    };
    let mut thread_ids = BTreeSet::new();
    let mut has_unbound_client = false;
    for client_id in client_ids {
        let Some(client) = state.clients.get(client_id) else {
            continue;
        };
        if !matches!(client.status.as_str(), "running" | "restarting") {
            continue;
        }
        if let Some(thread_id) = client.thread_id.as_deref() {
            if !thread_id.trim().is_empty() {
                thread_ids.insert(thread_id.to_string());
            }
        } else {
            has_unbound_client = true;
        }
    }
    if has_unbound_client {
        None
    } else {
        Some(thread_ids)
    }
}

fn matching_app_thread<'a>(
    client: &ClientInfo,
    snapshot: &'a [AppThreadSnapshot],
) -> Option<&'a AppThreadSnapshot> {
    if let Some(thread_id) = client.thread_id.as_deref() {
        return snapshot.iter().find(|thread| thread.id == thread_id);
    }

    // An unbound legacy client may be matched by cwd only when that cwd has
    // exactly one app-server thread. Ambiguous or missing ownership is
    // deliberately treated as non-waiting by the callers below.
    let mut matches = snapshot.iter().filter(|thread| thread.cwd == client.cwd);
    let thread = matches.next()?;
    matches.next().is_none().then_some(thread)
}

fn app_thread_is_waiting(thread: &AppThreadSnapshot) -> bool {
    is_waiting_thread_status(&thread.status) && thread.active_flags.is_empty()
}

fn client_is_waiting_in_snapshot(client: &ClientInfo, snapshot: &[AppThreadSnapshot]) -> bool {
    matching_app_thread(client, snapshot).is_some_and(app_thread_is_waiting)
}

fn working_upgrade_clients(
    state: &Arc<Mutex<ServerState>>,
    client_ids: &BTreeSet<String>,
    snapshot: &[AppThreadSnapshot],
) -> Vec<String> {
    let Ok(state) = state.lock() else {
        return Vec::new();
    };
    state
        .clients
        .values()
        .filter(|client| client_ids.contains(&client.id))
        .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
        // Upgrade-resume is allowed to proceed only after every target has
        // an explicit idle/waiting status. Unknown, missing, notLoaded, and
        // active states remain blocking so they can never be terminated by a
        // stale or incomplete snapshot.
        .filter(|client| !client_is_waiting_in_snapshot(client, snapshot))
        .map(|client| format!("{} cwd={}", client.id, client.cwd))
        .collect()
}

fn run_incremental_codex_upgrade_resume_worker(
    state: Arc<Mutex<ServerState>>,
    paths: RuntimePaths,
    target_client_ids: BTreeSet<String>,
    codex_version: Option<&str>,
    request: UpgradeResumeAllRequest,
) -> Result<(Option<u32>, u64, u64), String> {
    // The federation caller only waits for this worker to be queued. The
    // server that owns these clients performs the idle wait, package install,
    // app-server restart, and serialized client re-exec locally.
    let timeout = upgrade_idle_wait_timeout();
    let start = SystemTime::now();

    loop {
        let thread_ids = upgrade_thread_ids_for_clients(&state, &target_client_ids);
        let snapshot = match app_server_thread_snapshot(&paths, thread_ids.as_ref()) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                if start.elapsed().unwrap_or_default() >= timeout {
                    return Err(format!(
                        "timed out waiting for Codex clients to become idle; app-server status remained unavailable: {err}"
                    ));
                }
                eprintln!(
                    "yolo upgrade: app-server status unavailable; keeping clients alive and retrying: {err}"
                );
                thread::sleep(UPGRADE_IDLE_POLL_INTERVAL);
                continue;
            }
        };
        apply_thread_snapshot(&state, &snapshot);

        let working_clients = working_upgrade_clients(&state, &target_client_ids, &snapshot);
        if working_clients.is_empty() {
            // Do not mutate the managed Codex installation while a target
            // client is still working. The upgrade-resume operation begins
            // only after the explicit waiting condition above is satisfied.
            upgrade_codex_cli_version(codex_version)?;
            // Migrate the client wrappers while the old app-server is still
            // reachable. Replacing the app-server first lets an old wrapper
            // observe EOF, and old binaries may terminate before they can
            // receive the authorized upgrade-resume generation. The gate is
            // limited to the target set so a Phoenix caller that was
            // intentionally excluded from the idle wait is not terminated by
            // this pre-restart migration.
            let gate_count =
                prepare_upgrade_reexec_gate_for_client_ids(&state, &target_client_ids, &request);
            eprintln!(
                "yolo upgrade: prepared serialized pre-restart re-exec gate for {gate_count} clients"
            );
            let mut state_guard = state
                .lock()
                .map_err(|_| "server state lock poisoned".to_string())?;
            state_guard.resume_generation = state_guard.resume_generation.saturating_add(1);
            let resume_generation = state_guard.resume_generation;
            drop(state_guard);
            if let Err(err) = wait_for_upgrade_reexec_gate(&state) {
                if let Ok(mut state) = state.lock() {
                    state.upgrade_reexec_queue.clear();
                    state.upgrade_reexec_active = None;
                }
                return Err(err);
            }
            // Only after every target wrapper has switched to the installed
            // yolo executable may the shared app-server be replaced. Current
            // wrappers then reconnect their Codex child on the new generation;
            // legacy wrappers have already been migrated before seeing EOF.
            let app_server_pid = match restart_tracked_app_server(Arc::clone(&state), paths.clone())
            {
                Ok(pid) => pid,
                Err(err) => return Err(err),
            };
            let state_guard = state
                .lock()
                .map_err(|_| "server state lock poisoned".to_string())?;
            return Ok((
                Some(app_server_pid),
                state_guard.app_server_generation,
                resume_generation,
            ));
        }

        if start.elapsed().unwrap_or_default() >= timeout {
            return Err(format!(
                "timed out waiting for Codex clients to become idle: {}",
                working_clients.join(", ")
            ));
        }
        eprintln!(
            "yolo upgrade: waiting only for still-working clients before app-server resume: {}",
            working_clients.join(", ")
        );
        thread::sleep(UPGRADE_IDLE_POLL_INTERVAL);
    }
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
            None,
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
            None,
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
    let gate_count = prepare_upgrade_reexec_gate(&state, &request);
    eprintln!("yolo upgrade: prepared serialized re-exec gate for {gate_count} clients");
    let generation = {
        let mut state = state
            .lock()
            .map_err(|_| "server state lock poisoned".to_string())?;
        state.resume_generation = state.resume_generation.saturating_add(1);
        state.resume_generation
    };
    if let Some(object) = value.as_object_mut() {
        object.insert("resume_generation".to_string(), Value::from(generation));
        object.insert(
            "client_reexec_scheduled".to_string(),
            Value::Bool(gate_count > 0),
        );
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
        ("GET", "/defaults") | ("GET", "/default-configuration") => {
            let configuration = state
                .lock()
                .ok()
                .and_then(|state| state.default_configuration.clone());
            json_response(
                200,
                &json!({
                    "ok": true,
                    "configuration": configuration,
                }),
            )
        }
        ("POST", "/defaults") | ("POST", "/default-configuration") => {
            let configuration = match serde_json::from_str::<YoloDefaultConfiguration>(body) {
                Ok(configuration)
                    if !configuration.model.trim().is_empty()
                        && !configuration.reasoning_effort.trim().is_empty() =>
                {
                    configuration
                }
                Ok(_) => {
                    return json_response(
                        400,
                        &json!({"ok": false, "error": "default configuration values are required"}),
                    );
                }
                Err(err) => {
                    return json_response(400, &json!({"ok": false, "error": err.to_string()}));
                }
            };
            let changed = if let Ok(mut state) = state.lock() {
                let changed = state.default_configuration.as_ref() != Some(&configuration);
                state.default_configuration = Some(configuration.clone());
                changed
            } else {
                return json_response(
                    500,
                    &json!({"ok": false, "error": "server state lock poisoned"}),
                );
            };
            if let Err(err) =
                persist_yolo_default_configuration(&paths.default_configuration, &configuration)
            {
                return json_response(500, &json!({"ok": false, "error": err}));
            }
            if changed {
                publish_status_event(&state, "default-configuration-updated");
            }
            json_response(200, &json!({"ok": true, "configuration": configuration}))
        }
        ("GET", "/saved-sessions") | ("GET", "/active-sessions") => {
            let sessions = state
                .lock()
                .map(|state| state.active_sessions.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            json_response(
                200,
                &json!({
                    "ok": true,
                    "generated_at": now_secs(),
                    "sessions": sessions,
                }),
            )
        }
        ("POST", "/resume/resolve-last") => {
            let request = match serde_json::from_str::<ResolveResumeLastRequest>(body) {
                Ok(request) if !request.cwd.trim().is_empty() => request,
                Ok(_) => {
                    return json_response(400, &json!({"ok": false, "error": "cwd is required"}));
                }
                Err(err) => {
                    return json_response(400, &json!({"ok": false, "error": err.to_string()}));
                }
            };
            match resolve_resume_last_thread_on_server(&state, &request.cwd) {
                Some(thread_id) => json_response(
                    200,
                    &json!({
                        "ok": true,
                        "cwd": request.cwd,
                        "thread_id": thread_id,
                    }),
                ),
                None => json_response(
                    409,
                    &json!({
                        "ok": false,
                        "error": format!(
                            "refusing resume --last for {}: no non-running Codex session with matching cwd",
                            request.cwd
                        ),
                    }),
                ),
            }
        }
        ("POST", "/clients/prepare-resume") => {
            let request = match serde_json::from_str::<PrepareResumeRequest>(body) {
                Ok(request)
                    if !request.thread_id.trim().is_empty() && !request.cwd.trim().is_empty() =>
                {
                    request
                }
                Ok(_) => {
                    return json_response(
                        400,
                        &json!({"ok": false, "error": "thread_id and cwd are required"}),
                    );
                }
                Err(err) => {
                    return json_response(400, &json!({"ok": false, "error": err.to_string()}));
                }
            };
            if !Path::new(&request.cwd).is_absolute() {
                return json_response(400, &json!({"ok": false, "error": "cwd must be absolute"}));
            }
            spawn_server_resume_policy_preparer(paths.clone(), request.clone());
            json_response(
                202,
                &json!({
                    "ok": true,
                    "scheduled": true,
                    "client_id": request.client_id,
                    "thread_id": request.thread_id,
                }),
            )
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
                        for client in state.clients.values_mut() {
                            if matches!(client.status.as_str(), "running" | "restarting")
                                && client.thread_id.as_deref() == Some(thread_id)
                            {
                                client.codex_status = Some("active".to_string());
                                client.codex_active_flags.clear();
                                client.codex_status_updated_at = Some(now_secs());
                                client.updated_at = now_secs();
                            }
                        }
                        persist_turn_archive(&paths.turn_archive, &state.telemetry);
                    }
                    json_response(202, &json!({"ok": true, "captured": true}))
                }
                Err(err) => json_response(400, &json!({"ok": false, "error": err.to_string()})),
            }
        }
        ("POST", "/clients/register") => match serde_json::from_str::<ClientInfo>(body) {
            Ok(client) => {
                let changed = if let Ok(mut state) = state.lock() {
                    let changed = reconcile_registered_client_process(&mut state, &client);
                    let changed = upsert_active_session_locked(&mut state, &client) || changed;
                    state.clients.insert(client.id.clone(), client);
                    changed
                } else {
                    false
                };
                if changed {
                    persist_active_sessions(&state, &paths);
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
                    let mut settings_reconfigure_generation = 0;
                    let mut active_sessions_changed = false;
                    if let Some(id) = value.get("id").and_then(Value::as_str)
                        && let Ok(mut state) = state.lock()
                    {
                        resume_generation = state.resume_generation;
                        app_server_generation = state.app_server_generation;
                        if let Some(client) = state.clients.get_mut(id) {
                            let heartbeat_now = now_secs();
                            let mut settings_changed = false;
                            let mut fast_known = false;
                            settings_reconfigure_generation =
                                client.settings_reconfigure_generation;
                            client.updated_at = value
                                .get("updated_at")
                                .and_then(Value::as_u64)
                                .unwrap_or(heartbeat_now);
                            if let Some(model) = value.get("model").and_then(Value::as_str) {
                                client.model = Some(model.to_string());
                                settings_changed = true;
                            }
                            if let Some(service_tier) =
                                value.get("service_tier").and_then(Value::as_str)
                            {
                                client.service_tier = Some(service_tier.to_string());
                                client.fast = is_fast_tier(client.service_tier.as_deref());
                                settings_changed = true;
                                fast_known = true;
                            }
                            if let Some(reasoning_effort) =
                                value.get("reasoning_effort").and_then(Value::as_str)
                            {
                                client.reasoning_effort = Some(reasoning_effort.to_string());
                                settings_changed = true;
                            }
                            if let Some(fast) = value.get("fast").and_then(Value::as_bool) {
                                client.fast = fast;
                                settings_changed = true;
                                fast_known = true;
                            }
                            if settings_changed {
                                client.settings_source = "heartbeat".to_string();
                                client.settings_observed_at = Some(heartbeat_now);
                                client.fast_known |= fast_known;
                            }
                            client.status = value
                                .get("status")
                                .and_then(Value::as_str)
                                .unwrap_or("running")
                                .to_string();
                            if settings_changed {
                                if let Some(client) = state.clients.get(id).cloned() {
                                    active_sessions_changed |=
                                        upsert_active_session_locked(&mut state, &client);
                                }
                            }
                        }
                    }
                    if active_sessions_changed {
                        persist_active_sessions(&state, &paths);
                    }
                    json_response(
                        200,
                        &json!({
                            "ok": true,
                            "app_server_generation": app_server_generation,
                            "resume_generation": resume_generation,
                            "settings_reconfigure_generation": settings_reconfigure_generation
                        }),
                    )
                }
                Err(err) => json_response(400, &json!({"ok": false, "error": err.to_string()})),
            }
        }
        ("POST", "/clients/reexec-claim") => {
            let client_id = serde_json::from_str::<Value>(body).ok().and_then(|value| {
                value
                    .get("client_id")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            });
            let Some(client_id) = client_id.filter(|id| !id.trim().is_empty()) else {
                return json_response(400, &json!({"ok": false, "error": "client_id is required"}));
            };
            match claim_upgrade_reexec_permit_result(&state, &client_id) {
                Ok(result) => json_response(
                    200,
                    &json!({
                        "ok": true,
                        "granted": result.granted(),
                        "reason": result.reason()
                    }),
                ),
                Err(err) => json_response(500, &json!({"ok": false, "error": err})),
            }
        }
        ("POST", "/clients/finish") => match serde_json::from_str::<ClientInfo>(body) {
            Ok(client) => {
                let changed = if let Ok(mut state) = state.lock() {
                    state.clients.insert(client.id.clone(), client.clone());
                    remove_active_session_matches_client(&mut state.active_sessions, &client)
                } else {
                    false
                };
                if changed {
                    persist_active_sessions(&state, &paths);
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
        ("POST", "/upgrade-resume-preflight") => {
            let request = serde_json::from_str::<UpgradeResumeAllRequest>(body).unwrap_or_default();
            match upgrade_resume_preflight(Arc::clone(&state), &paths, &request) {
                Ok(value) => json_response(200, &value),
                Err(err) => json_response(500, &json!({"ok": false, "error": err})),
            }
        }
        ("POST", "/upgrade-resume-reexec") => {
            let request = serde_json::from_str::<UpgradeResumeAllRequest>(body).unwrap_or_default();
            match run_upgrade_resume_reexec_local(Arc::clone(&state), &paths, &request) {
                Ok(value) => json_response(200, &value),
                Err(err) => json_response(500, &json!({"ok": false, "error": err})),
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
        scan_existing_yolo_clients(state, paths);
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
    bind_unresolved_clients_to_recent_threads(state);
    apply_pending_client_settings_for_bound_clients(state, paths);
    if changed && let Ok(state) = state.lock() {
        persist_turn_archive(&paths.turn_archive, &state.telemetry);
    }
    let client_ids = state
        .lock()
        .map(|state| {
            state
                .clients
                .values()
                .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
                .map(|client| client.id.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    sync_active_sessions_for_client_ids(state, paths, &client_ids);
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
    let thread_created_at = thread
        .get("createdAt")
        .or_else(|| thread.get("created_at"))
        .and_then(Value::as_u64);
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
        .map(|client| (client.id.clone(), managed_client_start_secs(client)))
        .collect::<Vec<_>>();
    let client_id = if candidates.len() == 1 {
        candidates[0].0.clone()
    } else {
        let Some(thread_created_at) = thread_created_at else {
            return;
        };
        let mut ranked = candidates
            .into_iter()
            .filter_map(|(client_id, started_at)| {
                let started_at = started_at?;
                let distance = started_at.abs_diff(thread_created_at);
                (distance <= 120).then_some((distance, client_id))
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| left.cmp(right));
        let Some((distance, client_id)) = ranked.first().cloned() else {
            return;
        };
        if ranked
            .get(1)
            .is_some_and(|(other_distance, _)| *other_distance == distance)
        {
            return;
        }
        client_id
    };
    let Some(client) = state.clients.get_mut(&client_id) else {
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

fn managed_client_start_secs(client: &ClientInfo) -> Option<u64> {
    let (_, millis) = client.id.rsplit_once('-')?;
    let millis = millis.parse::<u64>().ok()?;
    (millis >= 1_000_000_000_000).then_some(millis / 1000)
}

fn bind_unresolved_clients_to_recent_threads(state: &Arc<Mutex<ServerState>>) -> Vec<String> {
    let Ok(mut state) = state.lock() else {
        return Vec::new();
    };
    let clients = state
        .clients
        .values()
        .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
        .filter(|client| !client_thread_id_is_authoritative(client))
        .filter(|client| client_uses_managed_proxy(client))
        .filter_map(|client| {
            Some((
                client.id.clone(),
                client.cwd.clone(),
                managed_client_start_secs(client)?,
            ))
        })
        .collect::<Vec<_>>();
    let threads = state
        .telemetry
        .threads
        .values()
        .filter(|thread| thread.parent_thread_id.is_none())
        .filter_map(|thread| {
            Some((
                thread.thread_id.clone(),
                thread.cwd.clone()?,
                thread.created_at?,
            ))
        })
        .collect::<Vec<_>>();

    let mut pairs = clients
        .iter()
        .flat_map(|(client_id, client_cwd, client_started_at)| {
            threads
                .iter()
                .filter_map(move |(thread_id, thread_cwd, thread_created_at)| {
                    if client_cwd != thread_cwd {
                        return None;
                    }
                    let distance = client_started_at.abs_diff(*thread_created_at);
                    (distance <= 120).then_some((distance, client_id.clone(), thread_id.clone()))
                })
        })
        .collect::<Vec<_>>();
    pairs.sort();

    let mut assigned_clients = BTreeSet::new();
    let mut assigned_threads = BTreeSet::new();
    let mut bound = Vec::new();
    for (_, client_id, thread_id) in pairs {
        if !assigned_clients.insert(client_id.clone())
            || !assigned_threads.insert(thread_id.clone())
        {
            continue;
        }
        let Some(client) = state.clients.get_mut(&client_id) else {
            continue;
        };
        if client_thread_id_is_authoritative(client) {
            continue;
        }
        client.thread_id = Some(thread_id.clone());
        client.thread_id_source = "app_server_started".to_string();
        client.updated_at = now_secs();
        eprintln!(
            "yolo server: bound managed client {} to recent thread {} by launch time",
            client.id, thread_id
        );
        bound.push(client_id);
    }
    bound
}

fn apply_pending_client_settings_for_bound_clients(
    state: &Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
) {
    let client_ids = state
        .lock()
        .map(|state| {
            state
                .clients
                .values()
                .filter(|client| {
                    matches!(client.status.as_str(), "running" | "restarting")
                        && client
                            .thread_id
                            .as_deref()
                            .is_some_and(|thread_id| !thread_id.trim().is_empty())
                })
                .filter_map(|client| {
                    let path = pending_client_settings_path(paths, &client.id).ok()?;
                    path.exists().then_some(client.id.clone())
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    for client_id in client_ids {
        let Ok(path) = pending_client_settings_path(paths, &client_id) else {
            continue;
        };
        let Ok(contents) = fs::read(&path) else {
            continue;
        };
        let Ok(settings) = serde_json::from_slice::<PendingClientSettings>(&contents) else {
            continue;
        };
        let request = ConfigureClientsRequest {
            client_id: Some(client_id.clone()),
            model: settings.model.clone(),
            fast: settings.fast,
            reasoning_effort: settings.reasoning_effort.clone(),
            timeout_secs: Some(10),
            ..ConfigureClientsRequest::default()
        };
        match configure_clients(Arc::clone(state), paths, request) {
            Ok(value)
                if value
                    .get("updated")
                    .and_then(Value::as_array)
                    .is_some_and(|updated| !updated.is_empty()) =>
            {
                let _ = fs::remove_file(path);
                eprintln!(
                    "yolo server: applied pending settings immediately after binding client {client_id}"
                );
            }
            Ok(_) => {}
            Err(err) => {
                eprintln!("yolo server: pending settings apply failed for {client_id}: {err}")
            }
        }
    }
}

fn subscribe_running_client_threads(
    state: &Arc<Mutex<ServerState>>,
    client: &mut AppServerRpcClient,
    subscribed_thread_ids: &mut BTreeSet<String>,
) -> Result<(), String> {
    // Do not make a fresh app-server load for every saved client at service
    // startup. The native client proxy already owns its resume connection;
    // subscribe telemetry only after that client has sent a turn and its
    // short bootstrap grace period has elapsed.
    let mut target_thread_ids = known_active_client_thread_ids(state);
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

fn known_active_client_thread_ids(state: &Arc<Mutex<ServerState>>) -> BTreeSet<String> {
    let now = now_secs();
    let Ok(state) = state.lock() else {
        return BTreeSet::new();
    };
    state
        .clients
        .values()
        .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
        .filter(|client| {
            client
                .codex_status
                .as_deref()
                .map(str::to_ascii_lowercase)
                .is_some_and(|status| {
                    matches!(
                        status.as_str(),
                        "active" | "working" | "running" | "inprogress"
                    )
                })
        })
        .filter(|client| {
            client.codex_status_updated_at.is_some_and(|updated_at| {
                now.saturating_sub(updated_at) >= APP_SERVER_STATUS_SUBSCRIPTION_GRACE.as_secs()
            })
        })
        .filter_map(|client| client.thread_id.as_deref())
        .map(str::trim)
        .filter(|thread_id| !thread_id.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn known_running_client_ids(state: &Arc<Mutex<ServerState>>) -> Vec<String> {
    let Ok(state) = state.lock() else {
        return Vec::new();
    };
    state
        .clients
        .values()
        .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
        .map(|client| client.id.clone())
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

fn scan_existing_yolo_clients(state: &Arc<Mutex<ServerState>>, paths: &RuntimePaths) {
    let Ok(processes) = read_process_table() else {
        return;
    };
    let now = now_secs();
    let current_pid = std::process::id();
    let mut child_codex_by_parent: BTreeMap<u32, (u32, String, Option<String>)> = BTreeMap::new();
    let mut live_client_pids: BTreeSet<u32> = BTreeSet::new();
    for process in &processes {
        if process_is_live(process)
            && process.cmdline.iter().any(|arg| arg.contains("codex"))
            && process
                .cmdline
                .iter()
                .any(|arg| arg.contains("--remote") || arg.contains("codex-app-server.sock"))
        {
            child_codex_by_parent.insert(
                process.ppid,
                (
                    process.pid,
                    remote_from_codex_args(&process.cmdline),
                    process_thread_id(process).map(ToString::to_string),
                ),
            );
        }
        if process.pid != current_pid
            && process_is_live(process)
            && is_yolo_process(process)
            && is_yolo_client_args(&process.cmdline.iter().skip(1).cloned().collect::<Vec<_>>())
        {
            live_client_pids.insert(process.pid);
        }
    }

    let Ok(mut state_guard) = state.lock() else {
        return;
    };
    let mut tmux_panes = None;
    let mut persisted_sessions_changed = false;
    let stale_client_ids = state_guard
        .clients
        .values()
        .filter(|client| client.status == "running" && !live_client_pids.contains(&client.yolo_pid))
        .map(|client| client.id.clone())
        .collect::<Vec<_>>();
    for client_id in stale_client_ids {
        // An unexpected process loss is a stale client, not proof that the
        // saved thread should be discarded. Ensure the latest client record
        // is persisted as a recovery candidate before changing its lifecycle
        // status, then keep stale clients out of upgrade/preflight targets.
        let Some(client_snapshot) = state_guard.clients.get(&client_id).cloned() else {
            continue;
        };
        persisted_sessions_changed |=
            upsert_active_session_locked(&mut state_guard, &client_snapshot);
        if let Some(client) = state_guard.clients.get_mut(&client_id) {
            client.status = "stale".to_string();
            client.ended_at = Some(now);
            client.codex_status = None;
            client.codex_active_flags.clear();
            client.codex_status_updated_at = None;
            client.updated_at = now;
            eprintln!(
                "yolo server: marked missing or stopped client {} stale; preserving saved session",
                client.id
            );
        }
    }
    for process in processes {
        if process.pid == current_pid || !process_is_live(&process) || !is_yolo_process(&process) {
            continue;
        }
        let args = process.cmdline.iter().skip(1).cloned().collect::<Vec<_>>();
        if !is_yolo_client_args(&args) {
            continue;
        }
        let remote = child_codex_by_parent
            .get(&process.pid)
            .map(|(_, remote, _)| remote.clone())
            .unwrap_or_default();
        let yolo_thread_id = thread_id_from_args_strs(&args);
        let codex_thread_id = child_codex_by_parent
            .get(&process.pid)
            .and_then(|(_, _, thread_id)| thread_id.clone());
        let thread_id = yolo_thread_id.clone().or(codex_thread_id.clone());
        let id = client_id_from_managed_proxy_remote(&remote)
            .unwrap_or_else(|| format!("{}-scanned", process.pid));
        if state_guard
            .clients
            .values()
            .any(|client| client.yolo_pid == process.pid && client.status == "running")
        {
            continue;
        }
        let cfg = read_codex_config();
        let launch_cfg = parse_codex_launch_config(&args);
        let ui_status = tmux_panes
            .get_or_insert_with(collect_tmux_panes)
            .iter()
            .find(|pane| pane.yolo_pid == Some(process.pid))
            .and_then(|pane| pane.codex_ui_status.clone());
        let ui_service_tier = ui_status.as_ref().and_then(|status| {
            status
                .fast
                .map(|fast| if fast { "priority" } else { "default" }.to_string())
        });
        let model = ui_status
            .as_ref()
            .and_then(|status| status.model.clone())
            .or(launch_cfg.model)
            .or(cfg.model);
        let service_tier = ui_service_tier
            .or(launch_cfg.service_tier)
            .or(cfg.service_tier);
        let reasoning_effort = ui_status
            .as_ref()
            .and_then(|status| status.effort.clone())
            .or(launch_cfg.reasoning_effort);
        let fast = ui_status
            .as_ref()
            .and_then(|status| status.fast)
            .or_else(|| service_tier.as_deref().map(|tier| is_fast_tier(Some(tier))))
            .unwrap_or(false);
        let fast_known =
            ui_status.as_ref().and_then(|status| status.fast).is_some() || service_tier.is_some();
        let has_session_settings =
            model.is_some() || service_tier.is_some() || reasoning_effort.is_some();
        let client = ClientInfo {
            id: id.clone(),
            yolo_pid: process.pid,
            codex_pid: child_codex_by_parent
                .get(&process.pid)
                .map(|(pid, _, _)| *pid),
            cwd: process.cwd.unwrap_or_else(|| String::from("")),
            args: args.clone(),
            remote,
            model,
            service_tier: service_tier.clone(),
            reasoning_effort,
            fast,
            fast_known,
            settings_source: if ui_status.is_some() {
                "tmux_footer".to_string()
            } else if has_session_settings {
                "launch_args".to_string()
            } else {
                "unknown".to_string()
            },
            settings_observed_at: has_session_settings.then_some(now),
            thread_id,
            thread_id_source: if yolo_thread_id.is_some() || codex_thread_id.is_some() {
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
            settings_reconfigure_generation: 0,
        };
        persisted_sessions_changed |= remove_active_sessions_for_yolo_pid_except(
            &mut state_guard.active_sessions,
            process.pid,
            Some(&client),
        );
        persisted_sessions_changed |= upsert_active_session_locked(&mut state_guard, &client);
        state_guard.clients.insert(id, client);
    }
    drop(state_guard);
    if persisted_sessions_changed {
        persist_active_sessions(state, paths);
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
    // Startup must never enumerate or resume every saved rollout. A single
    // large thread/read can monopolize the app-server and delay a live TUI's
    // own thread/resume or turn/start. Thread status is populated lazily by
    // the status listener after a client actually starts a turn.
    let client_ids = known_running_client_ids(&state);
    sync_active_sessions_for_client_ids(&state, &paths, &client_ids);
}

fn spawn_agent_telemetry_snapshot_monitor(state: Arc<Mutex<ServerState>>, paths: RuntimePaths) {
    thread::spawn(move || {
        loop {
            // Let native client resumes complete before the first background
            // inventory request. Telemetry is intentionally eventual; it
            // must not win the app-server startup race.
            thread::sleep(APP_SERVER_TELEMETRY_REFRESH_INTERVAL);
            match app_server_agent_thread_inventory(&paths) {
                Ok(threads) => {
                    if let Ok(mut state) = state.lock() {
                        for thread in threads {
                            state.telemetry.record_thread_value(&thread);
                        }
                    }
                    bind_unresolved_clients_to_recent_threads(&state);
                    apply_pending_client_settings_for_bound_clients(&state, &paths);
                }
                Err(err) => eprintln!("yolo server: agent telemetry inventory failed: {err}"),
            }
        }
    });
}

fn app_server_agent_thread_inventory(paths: &RuntimePaths) -> Result<Vec<Value>, String> {
    let mut cursor: Option<String> = None;
    let mut threads = Vec::new();
    for _ in 0..APP_SERVER_TELEMETRY_MAX_PAGES {
        let (page, next_cursor) = app_server_agent_thread_inventory_page(paths, cursor.as_deref())?;
        threads.extend(page);
        cursor = next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    Ok(threads)
}

fn app_server_agent_thread_inventory_page(
    paths: &RuntimePaths,
    cursor: Option<&str>,
) -> Result<(Vec<Value>, Option<String>), String> {
    // Do not hold the global RPC gate across all pagination pages. A large
    // state DB inventory can take tens of seconds and used to make every
    // foreground settings update report "app-server RPC gate busy".
    let _rpc_lease = acquire_app_server_rpc(AppServerRpcPriority::Background)?;
    let mut client = AppServerRpcClient::connect(&paths.app_server_socket)?;
    client.set_rpc_timeout(APP_SERVER_BACKGROUND_RPC_TIMEOUT);
    client.initialize()?;
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
    if let Some(cursor) = cursor {
        params["cursor"] = Value::String(cursor.to_string());
    }
    let result = client.request("thread/list", params)?;
    let data = result
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("thread/list missing data: {result}"))?
        .to_vec();
    let next_cursor = result
        .get("nextCursor")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    Ok((data, next_cursor))
}

#[derive(Debug)]
struct ProcInfo {
    pid: u32,
    ppid: u32,
    state: char,
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
        let stat_path = dir.join("stat");
        let ppid = read_proc_ppid(stat_path.clone()).unwrap_or(0);
        let state = read_proc_state(stat_path).unwrap_or('?');
        let cwd = fs::read_link(dir.join("cwd"))
            .ok()
            .map(|path| path.display().to_string());
        out.push(ProcInfo {
            pid,
            ppid,
            state,
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

fn read_proc_state(path: PathBuf) -> Option<char> {
    let stat = fs::read_to_string(path).ok()?;
    let right = stat.rsplit_once(')')?.1.trim();
    right.split_whitespace().next()?.chars().next()
}

fn process_is_live(process: &ProcInfo) -> bool {
    !matches!(process.state, 'T' | 't' | 'Z' | 'X' | 'x')
}

fn pid_is_runnable(pid: u32) -> bool {
    let state = read_proc_state(PathBuf::from(format!("/proc/{pid}/stat")));
    state.is_some_and(|state| !matches!(state, 'T' | 't' | 'Z' | 'X' | 'x')) && pid_is_alive(pid)
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

fn apply_thread_settings_to_client(client: &mut ClientInfo, thread: &AppThreadSnapshot, now: u64) {
    let app_server_settings_observed = thread.model.is_some()
        || thread.service_tier.is_some()
        || thread.reasoning_effort.is_some();
    let app_server_fast_observed = thread.service_tier.is_some();
    let preserve_client_settings = client.settings_updated_at.is_some()
        || matches!(
            client.settings_source.as_str(),
            "configure" | "heartbeat" | "tmux_footer"
        );
    let launch_cfg = if preserve_client_settings {
        CodexLaunchConfig::default()
    } else {
        parse_codex_launch_config(&client.args)
    };

    // A thread snapshot can describe the settings that were last persisted by
    // app-server, while the just-restarted client is still being prepared.
    // The yolo launch intent (or an explicit live update) is authoritative for
    // this managed client; app-server fills only settings absent locally.
    let local_model = if preserve_client_settings {
        client.model.clone()
    } else {
        launch_cfg.model
    };
    let local_service_tier = if preserve_client_settings {
        client.service_tier.clone()
    } else {
        launch_cfg.service_tier
    };
    let local_reasoning_effort = if preserve_client_settings {
        client.reasoning_effort.clone()
    } else {
        launch_cfg.reasoning_effort
    };
    let local_settings_observed =
        local_model.is_some() || local_service_tier.is_some() || local_reasoning_effort.is_some();

    if let Some(model) = local_model.or_else(|| thread.model.clone()) {
        client.model = Some(model);
    }
    if let Some(service_tier) = local_service_tier.or_else(|| thread.service_tier.clone()) {
        client.service_tier = Some(service_tier);
        client.fast = is_fast_tier(client.service_tier.as_deref());
        client.fast_known = true;
    }
    if let Some(reasoning_effort) =
        local_reasoning_effort.or_else(|| thread.reasoning_effort.clone())
    {
        client.reasoning_effort = Some(reasoning_effort);
    }

    if local_settings_observed {
        if !matches!(
            client.settings_source.as_str(),
            "configure" | "heartbeat" | "tmux_footer"
        ) {
            client.settings_source = "launch_args".to_string();
        }
        client.settings_observed_at = Some(now);
    } else if app_server_settings_observed {
        client.settings_source = "app_server".to_string();
        client.settings_observed_at = Some(now);
        client.fast_known |= app_server_fast_observed;
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
        client.updated_at = now;
        apply_thread_settings_to_client(client, thread, now);
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
        apply_thread_settings_to_client(client, thread, now);
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
        let snapshot = match app_server_thread_snapshot(paths, target_thread_ids.as_ref()) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                if start.elapsed().unwrap_or_default() >= timeout {
                    return Err(format!(
                        "timed out waiting for Codex clients to become idle; app-server status remained unavailable: {err}"
                    ));
                }
                eprintln!(
                    "yolo: app-server status unavailable; keeping clients alive and retrying: {err}"
                );
                thread::sleep(UPGRADE_IDLE_POLL_INTERVAL);
                continue;
            }
        };
        apply_thread_snapshot(&state, &snapshot);
        let client_ids = known_running_client_ids(&state);
        sync_active_sessions_for_client_ids(&state, paths, &client_ids);
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

fn upgrade_resume_preflight(
    state: Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
    request: &UpgradeResumeAllRequest,
) -> Result<Value, String> {
    // Reconcile /proc before using the last heartbeat as an upgrade gate. A
    // stopped wrapper must be stale, not an idle live client that can block
    // migration indefinitely.
    scan_existing_yolo_clients(&state, paths);
    let target_thread_ids = upgrade_wait_thread_ids(&state, request);
    let snapshot = match app_server_thread_snapshot(paths, target_thread_ids.as_ref()) {
        Ok(snapshot) => snapshot,
        Err(err) => {
            // A timeout is not evidence of waiting. Return a blocking status
            // so direct upgrade-resume callers can poll until the app-server
            // reports an explicit state instead of aborting or proceeding.
            return Ok(json!({
                "ok": true,
                "waiting": false,
                "working": [format!("app-server status unavailable: {err}")],
                "status_error": err,
            }));
        }
    };
    apply_thread_snapshot(&state, &snapshot);
    let working = working_clients_for_snapshot(&state, &snapshot, request);
    Ok(json!({
        "ok": true,
        "waiting": working.is_empty(),
        "working": working,
    }))
}

fn run_upgrade_resume_reexec_local(
    state: Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
    request: &UpgradeResumeAllRequest,
) -> Result<Value, String> {
    if UPGRADE_RESUME_IN_PROGRESS
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Err("a Codex upgrade-resume job is already running".to_string());
    }

    let result = (|| {
        // This endpoint is the safe hand-off point for a yolo binary update:
        // wait for explicit app-server idle state, then re-exec wrappers while
        // the current app-server is still reachable. It intentionally does
        // not restart the app-server itself.
        scan_existing_yolo_clients(&state, paths);
        wait_for_clients_idle(Arc::clone(&state), paths, request)?;
        let target_client_ids = upgrade_target_client_ids(&state, request);
        let gate_count =
            prepare_upgrade_reexec_gate_for_client_ids(&state, &target_client_ids, request);
        let generation = {
            let mut state = state
                .lock()
                .map_err(|_| "server state lock poisoned".to_string())?;
            state.resume_generation = state.resume_generation.saturating_add(1);
            state.resume_generation
        };
        if let Err(err) = wait_for_upgrade_reexec_gate(&state) {
            if let Ok(mut state) = state.lock() {
                state.upgrade_reexec_queue.clear();
                state.upgrade_reexec_active = None;
            }
            return Err(err);
        }
        Ok(json!({
            "ok": true,
            "client_reexec_scheduled": gate_count > 0,
            "clients": target_client_ids.len(),
            "reexecuted": gate_count,
            "resume_generation": generation,
            "app_server_restart_required": false,
        }))
    })();
    UPGRADE_RESUME_IN_PROGRESS.store(false, Ordering::SeqCst);
    result
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
            if !client_is_waiting_in_snapshot(client, snapshot) {
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
    // Pending clients have no loaded app-server thread yet. Their durable
    // launch intent is safe to record above; the first turn will consume it.
    // Live clients are deliberately not updated here: their metadata becomes
    // authoritative only after thread/settings/update acknowledges success.
    // The live update then advances the settings generation, and the client
    // re-execs after the ACK so the terminal CLI reflects the new mode.
    if clients.is_empty() {
        let client_ids = selected_ids.iter().cloned().collect::<Vec<_>>();
        sync_active_sessions_for_client_ids(&state, paths, &client_ids);
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
                let client_ids = selected_ids.iter().cloned().collect::<Vec<_>>();
                sync_active_sessions_for_client_ids(&state, paths, &client_ids);
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
    let _rpc_lease = acquire_app_server_rpc(AppServerRpcPriority::Control)?;
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
        // Commit the in-memory and durable settings only after the live RPC
        // succeeded. Otherwise a timeout/reset would make /status advertise a
        // configuration that Codex never accepted.
        note_client_settings_update(
            state,
            client_id,
            request.model.clone(),
            request.fast,
            request.reasoning_effort.clone(),
        );
        // Advance the generation only after the app-server acknowledged the
        // update. The heartbeat then asks the terminal-bound client to re-exec
        // with the server's authoritative settings without racing this RPC.
        request_client_reconfigure(state, client_id);
        sync_active_sessions_for_client_ids(state, paths, &[client_id.clone()]);
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
    let fast_known_update = fast.is_some();
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
    client.settings_source = "configure".to_string();
    client.settings_observed_at = Some(now);
    client.fast_known |= fast_known_update;
    client.settings_updated_at = Some(now);
    client.updated_at = now;
}

fn request_client_reconfigure(state: &Arc<Mutex<ServerState>>, client_id: &str) {
    let Ok(mut state) = state.lock() else {
        return;
    };
    let Some(client) = state.clients.get_mut(client_id) else {
        return;
    };
    client.settings_reconfigure_generation =
        client.settings_reconfigure_generation.saturating_add(1);
    client.updated_at = now_secs();
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
    let now = now_secs();
    client.settings_source = "configure".to_string();
    client.settings_observed_at = Some(now);
    client.fast_known |= settings.fast.is_some();
    client.settings_updated_at = Some(now);
    client.updated_at = now;
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
        if configure_client_matches(client, request) {
            ids.insert(client.id.clone());
        }
    }
    Ok(ids)
}

fn configure_client_matches(client: &ClientInfo, request: &ConfigureClientsRequest) -> bool {
    if !matches!(client.status.as_str(), "running" | "restarting") {
        return false;
    }
    if request.all {
        return true;
    }

    // A thread ID is stable across a client re-exec and is the identity used
    // by the app-server RPC. If it is present, never broaden the match with a
    // stale client ID or cwd from an older modal snapshot.
    if let Some(thread_id) = request
        .thread_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return client.thread_id.as_deref().map(str::trim) == Some(thread_id);
    }
    if let Some(client_id) = request
        .client_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return client.id.trim() == client_id;
    }
    request
        .cwd
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some_and(|cwd| client.cwd.trim() == cwd)
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
        saved_sessions: state.active_sessions.values().cloned().collect(),
        default_configuration: state.default_configuration.clone(),
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
            "#{session_name}\t#{window_index}\t#{pane_index}\t#{pane_id}\t#{pane_pid}\t#{pane_tty}\t#{pane_current_path}\t#{pane_current_command}",
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
    let pane_id = nonempty_string(parts.next());
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
        pane_id,
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
    if let Some(targets) = target_thread_ids {
        // A preflight may inspect many live sessions. Keep each resume/read
        // RPC behind its own short gate lease so a settings update can run
        // between sessions instead of waiting for the whole batch.
        let mut threads = Vec::new();
        for thread_id in targets {
            if let Some(thread) = app_server_thread_snapshot_one(paths, thread_id)? {
                threads.push(thread);
            }
        }
        return Ok(threads);
    }

    let _rpc_lease = acquire_app_server_rpc(AppServerRpcPriority::Background)?;
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
        .ok_or_else(|| format!("thread/loaded/list missing data: {loaded}"))?
        .iter()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    let mut threads = Vec::new();
    for thread_id in thread_ids {
        let response = match client.request(
            "thread/resume",
            json!({
                "threadId": &thread_id,
                "excludeTurns": true
            }),
        ) {
            Ok(response) => response,
            Err(_) => client.request(
                "thread/read",
                json!({
                    "threadId": &thread_id,
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

fn app_server_thread_snapshot_one(
    paths: &RuntimePaths,
    thread_id: &str,
) -> Result<Option<AppThreadSnapshot>, String> {
    let _rpc_lease = acquire_app_server_rpc(AppServerRpcPriority::Background)?;
    let mut client = AppServerRpcClient::connect(&paths.app_server_socket)?;
    client.set_rpc_timeout(APP_SERVER_BACKGROUND_RPC_TIMEOUT);
    client.initialize()?;
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
    let Some(thread) = response.get("thread") else {
        return Ok(None);
    };
    let Some(mut snapshot) = parse_app_thread_snapshot(thread) else {
        return Ok(None);
    };
    apply_app_thread_settings(&mut snapshot, &response);
    Ok(Some(snapshot))
}

fn app_server_thread_history(
    paths: &RuntimePaths,
    thread_id: &str,
    limit: usize,
) -> Result<Vec<TurnInfo>, String> {
    let _rpc_lease = acquire_app_server_rpc(AppServerRpcPriority::Background)?;
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
            let mut commentary_entries = Vec::new();
            let mut reasoning_summary_entries = Vec::new();
            let mut reasoning_raw_entries = Vec::new();
            let mut plan_entries = Vec::new();
            if let Some(items) = turn.get("items").and_then(Value::as_array) {
                for (item_index, item) in items.iter().enumerate() {
                    // Thread history preserves the original item order, so
                    // use it as the sequence when rebuilding a turn from the
                    // app-server. Multiple fields on one reasoning item share
                    // the same sequence and remain adjacent in the UI.
                    let sequence = (item_index as u64).saturating_add(1);
                    if is_user_message_item(item) {
                        if let Some(text) = extract_message_text(item) {
                            append_turn_text(&mut prompt, &text);
                        }
                    } else if is_assistant_message_item(item) {
                        if let Some(text) = extract_message_text(item) {
                            if is_commentary_message_item(item) {
                                let item_id = item.get("id").and_then(Value::as_str);
                                set_trace_entry(&mut commentary_entries, item_id, &text, sequence);
                            } else {
                                last_assistant = Some(text.clone());
                                if item.get("phase").and_then(Value::as_str) == Some("final_answer")
                                {
                                    final_report = Some(text);
                                }
                            }
                        }
                    } else if item.get("type").and_then(Value::as_str) == Some("reasoning") {
                        let item_id = item.get("id").and_then(Value::as_str);
                        if let Some(text) = item.get("summary").and_then(extract_message_text) {
                            set_trace_entry(
                                &mut reasoning_summary_entries,
                                item_id,
                                &text,
                                sequence,
                            );
                        }
                        if let Some(text) = item.get("content").and_then(extract_message_text) {
                            set_trace_entry(&mut reasoning_raw_entries, item_id, &text, sequence);
                        }
                    } else if item.get("type").and_then(Value::as_str) == Some("plan") {
                        let item_id = item.get("id").and_then(Value::as_str);
                        if let Some(text) = item.get("text").and_then(Value::as_str) {
                            set_trace_entry(&mut plan_entries, item_id, text, sequence);
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
                result: final_report.or(last_assistant),
                commentary: trace_entries_text(&commentary_entries),
                commentary_entries,
                reasoning_summary: trace_entries_text(&reasoning_summary_entries),
                reasoning_summary_entries,
                reasoning_raw: trace_entries_text(&reasoning_raw_entries),
                reasoning_raw_entries,
                plan: trace_entries_text(&plan_entries),
                plan_entries,
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
    configuration: Option<&YoloDefaultConfiguration>,
) -> Result<(), String> {
    let _rpc_lease = acquire_app_server_rpc(AppServerRpcPriority::Control)?;
    let mut client = AppServerRpcClient::connect(socket)?;
    client.set_rpc_timeout(RESUME_POLICY_RPC_TIMEOUT);
    client.initialize()?;
    client.request(
        "thread/settings/update",
        resume_thread_settings_params(thread_id, cwd, configuration),
    )?;
    Ok(())
}

fn spawn_server_resume_policy_preparer(paths: RuntimePaths, request: PrepareResumeRequest) {
    thread::spawn(move || {
        match update_app_server_resume_thread_settings(
            &paths.app_server_socket,
            &request.thread_id,
            &request.cwd,
            request.configuration.as_ref(),
        ) {
            Ok(()) => eprintln!(
                "yolo server: prepared resume policy for client {} thread {}",
                request.client_id, request.thread_id
            ),
            Err(err) if is_app_server_thread_not_found_error(&err, &request.thread_id) => {
                eprintln!(
                    "yolo server: resume policy skipped because Codex thread {} is not loaded: {err}",
                    request.thread_id
                );
            }
            Err(err) => eprintln!(
                "yolo server: resume policy preparation failed for thread {} (best effort): {err}",
                request.thread_id
            ),
        }
    });
}

fn resume_thread_settings_params(
    thread_id: &str,
    cwd: &str,
    configuration: Option<&YoloDefaultConfiguration>,
) -> Value {
    let mut params = serde_json::Map::from_iter([
        ("threadId".to_string(), Value::String(thread_id.to_string())),
        ("cwd".to_string(), Value::String(cwd.to_string())),
        ("runtimeWorkspaceRoots".to_string(), json!([cwd])),
        (
            "approvalPolicy".to_string(),
            Value::String("never".to_string()),
        ),
        (
            "approvalsReviewer".to_string(),
            Value::String("user".to_string()),
        ),
        (
            "sandboxPolicy".to_string(),
            json!({"type": YOLO_APP_SERVER_SANDBOX_POLICY}),
        ),
    ]);
    if let Some(configuration) = configuration {
        params.insert(
            "model".to_string(),
            Value::String(configuration.model.clone()),
        );
        params.insert(
            "serviceTier".to_string(),
            Value::String(if configuration.fast {
                "priority".to_string()
            } else {
                "default".to_string()
            }),
        );
        params.insert(
            "effort".to_string(),
            Value::String(configuration.reasoning_effort.clone()),
        );
    }
    Value::Object(params)
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

impl Drop for AppServerRpcClient {
    fn drop(&mut self) {
        // The telemetry and control paths use short-lived WebSocket clients.
        // Dropping the UnixStream directly sends EOF, which makes the
        // app-server report `Connection reset without closing handshake` and
        // needlessly exercises its transport error path.  A best-effort close
        // also gives the server a chance to release the connection cleanly.
        let _ = websocket_send_close(&mut self.stream);
    }
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
        // App-server notifications may continue while the requested RPC is
        // being computed. Use one deadline for the whole request; resetting
        // the timeout for every unrelated notification turns a busy server
        // into an effectively unbounded wait.
        let deadline = Instant::now() + self.rpc_timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(format!(
                    "app-server request {id} timed out waiting for response"
                ));
            }
            let message = websocket_read_text_with_timeout(&mut self.stream, remaining)?;
            let value: Value = serde_json::from_str(&message)
                .map_err(|err| format!("decode app-server message: {err}: {message}"))?;
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
    initial_thread_id: Option<&str>,
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
    let initial_thread_id = initial_thread_id
        .filter(|thread_id| !thread_id.trim().is_empty())
        .map(ToString::to_string);
    remove_socket_if_present(&socket_path)?;
    let listener = UnixListener::bind(&socket_path)
        .map_err(|err| format!("bind client proxy {}: {err}", socket_path.display()))?;
    let listener_event_tx = event_tx.clone();
    thread::spawn(move || {
        loop {
            let (client_stream, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) => {
                    eprintln!("yolo client proxy: accept failed: {error}");
                    return;
                }
            };
            if let Err(error) = run_client_proxy_connection(
                client_stream,
                &upstream_socket,
                &listener_event_tx,
                &relay_pending_settings_path,
                initial_thread_id.as_deref(),
            ) {
                if listener_event_tx
                    .send(ClientEvent::ProxyDisconnected { error })
                    .is_err()
                {
                    return;
                }
            }
        }
    });

    Ok(ClientThreadProxy {
        remote: format!("unix://{}", socket_path.display()),
        socket_path,
        pending_settings_path,
    })
}

fn run_client_proxy_connection(
    mut client_stream: UnixStream,
    upstream_socket: &Path,
    event_tx: &mpsc::Sender<ClientEvent>,
    pending_settings_path: &Path,
    initial_thread_id: Option<&str>,
) -> Result<(), String> {
    let request = read_http_headers(&mut client_stream)
        .map_err(|error| format!("read client websocket handshake: {error}"))?;
    let mut upstream_stream = UnixStream::connect(upstream_socket)
        .map_err(|error| format!("connect app-server: {error}"))?;
    upstream_stream
        .write_all(request.as_bytes())
        .map_err(|error| format!("write app-server websocket handshake: {error}"))?;
    let response = read_http_headers(&mut upstream_stream)
        .map_err(|error| format!("read app-server websocket handshake: {error}"))?;
    if !response.starts_with("HTTP/1.1 101") && !response.starts_with("HTTP/1.0 101") {
        return Err(format!(
            "app-server websocket handshake failed: {}",
            response.lines().next().unwrap_or_default()
        ));
    }
    client_stream
        .write_all(response.as_bytes())
        .map_err(|error| format!("write client websocket handshake: {error}"))?;

    let tracker = Arc::new(Mutex::new(ThreadBindingTracker {
        pending_create_request_ids: BTreeSet::new(),
        pending_resume_request_ids: BTreeSet::new(),
        // A resume argument is authoritative from the moment the proxy
        // connects. The app-server may broadcast thread/started for other
        // loaded threads on this socket; those notifications must not
        // rebind this client away from its requested resume target.
        current_thread_id: initial_thread_id.map(ToString::to_string),
        event_tx: event_tx.clone(),
    }));
    let mut client_read = client_stream
        .try_clone()
        .map_err(|error| format!("clone client websocket: {error}"))?;
    let mut upstream_write = upstream_stream
        .try_clone()
        .map_err(|error| format!("clone app-server websocket: {error}"))?;
    let client_relay_finished = Arc::new(AtomicBool::new(false));
    let client_tracker = Arc::clone(&tracker);
    let client_relay_finished_for_relay = Arc::clone(&client_relay_finished);
    let pending_settings_path = pending_settings_path.to_path_buf();
    let client_to_server = thread::spawn(move || {
        relay_client_websocket_frames(
            &mut client_read,
            &mut upstream_write,
            &client_tracker,
            &pending_settings_path,
            &client_relay_finished_for_relay,
        );
        // The child side can disappear while the shared app-server remains
        // quiet. Closing the cloned upstream socket wakes the blocking
        // server-to-client relay so this listener can accept the replacement
        // child instead of accumulating reconnects in the socket backlog.
        client_relay_finished_for_relay.store(true, Ordering::SeqCst);
        let _ = upstream_write.shutdown(Shutdown::Both);
    });
    let server_result = relay_server_websocket_frames(
        &mut upstream_stream,
        &mut client_stream,
        &tracker,
        &client_relay_finished,
    );
    // A broken app-server connection must only tear down this child
    // connection. The listener remains bound and accepts the next Codex
    // child after the wrapper has waited for server recovery.
    let _ = client_stream.shutdown(Shutdown::Both);
    let _ = upstream_stream.shutdown(Shutdown::Both);
    let _ = client_to_server.join();

    match server_result {
        Ok(()) => Ok(()),
        Err(_error) if client_relay_finished.load(Ordering::SeqCst) => Ok(()),
        Err(error) => Err(error),
    }
}

fn relay_client_websocket_frames(
    source: &mut UnixStream,
    target: &mut UnixStream,
    tracker: &Arc<Mutex<ThreadBindingTracker>>,
    pending_settings_path: &Path,
    client_relay_finished: &Arc<AtomicBool>,
) {
    while let Ok(frame) = read_websocket_frame(source) {
        if frame.opcode == 0x8 {
            client_relay_finished.store(true, Ordering::SeqCst);
        }
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
    client_relay_finished: &Arc<AtomicBool>,
) -> Result<(), String> {
    loop {
        let frame = match read_websocket_frame(source) {
            Ok(frame) => frame,
            Err(error) => {
                if client_relay_finished.load(Ordering::SeqCst) {
                    return Ok(());
                }
                return Err(format!("app-server websocket relay: {error}"));
            }
        };
        if frame.opcode == 0x8 {
            // Do not forward an app-server close frame to Codex. The wrapper
            // owns recovery and will close/restart only the child connection,
            // keeping the yolo client process resident.
            return Err("app-server sent a websocket close frame".to_string());
        }
        if frame.opcode != 0x1 {
            if target.write_all(&frame.raw).is_err() {
                return Ok(());
            }
            continue;
        }
        let Ok(value) = serde_json::from_slice::<Value>(&frame.payload) else {
            if target.write_all(&frame.raw).is_err() {
                return Ok(());
            }
            continue;
        };
        if let Some(response) = yolo_auto_approval_response(&value) {
            if websocket_send_text(source, &response.to_string()).is_err() {
                return Ok(());
            }
            eprintln!(
                "yolo client proxy: auto-approved app-server request {}",
                value
                    .get("method")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            );
            continue;
        }
        if let Some(update) = parse_app_server_status_notification(&value)
            && let Ok(tracker) = tracker.lock()
            && (tracker.current_thread_id.as_deref().is_none()
                || tracker.current_thread_id.as_deref() == Some(update.thread_id.as_str()))
        {
            let _ = tracker.event_tx.send(ClientEvent::ThreadStatus {
                thread_id: update.thread_id,
                status: update.status,
                active_flags: update.active_flags,
            });
        }
        if target.write_all(&frame.raw).is_err() {
            return Ok(());
        }
        observe_app_server_response(tracker, &value);
    }
}

fn yolo_auto_approval_response(value: &Value) -> Option<Value> {
    let method = value.get("method").and_then(Value::as_str)?;
    if !matches!(
        method,
        "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "execCommandApproval"
    ) {
        return None;
    }
    let id = value.get("id")?;
    if !id.is_number() && !id.is_string() {
        return None;
    }
    Some(json!({
        "id": id,
        "result": {"decision": "accept"}
    }))
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
    if method == "thread/resume"
        && let Some(id) = app_server_message_id(value)
        && let Ok(mut tracker) = tracker.lock()
    {
        tracker.pending_resume_request_ids.insert(id);
    }
}

fn observe_app_server_response(tracker: &Arc<Mutex<ThreadBindingTracker>>, value: &Value) {
    if value.get("method").and_then(Value::as_str) == Some("thread/started") {
        // This is an unsolicited server notification.  It can describe a
        // thread created by another client sharing the app-server socket, so
        // it must never establish this proxy's thread binding.  Bindings are
        // learned only from this client's resume/start requests and their
        // correlated responses (or from the explicit resume argument).
        return;
    }
    let Some(id) = app_server_message_id(value) else {
        return;
    };
    let (should_track, resume_succeeded, event_tx) = match tracker.lock() {
        Ok(mut tracker) => {
            let should_track = tracker.pending_create_request_ids.remove(&id);
            let resume_succeeded =
                tracker.pending_resume_request_ids.remove(&id) && value.get("error").is_none();
            (
                should_track,
                resume_succeeded,
                Some(tracker.event_tx.clone()),
            )
        }
        Err(_) => (false, false, None),
    };
    if resume_succeeded && let Some(event_tx) = event_tx {
        let _ = event_tx.send(ClientEvent::ResumeBootstrapCompleted);
    }
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

fn websocket_send_close<W: Write>(stream: &mut W) -> Result<(), String> {
    let payload = 1000u16.to_be_bytes();
    let mask = [0x63, 0x6c, 0x6f, 0x73];
    let mut frame = Vec::with_capacity(8);
    frame.push(0x88);
    frame.push(0x80 | payload.len() as u8);
    frame.extend_from_slice(&mask);
    frame.extend(
        payload
            .iter()
            .enumerate()
            .map(|(idx, byte)| byte ^ mask[idx % 4]),
    );
    stream
        .write_all(&frame)
        .map_err(|err| format!("write websocket close frame: {err}"))
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
    if body_text.len() > MAX_API_REQUEST_BODY_BYTES {
        return Err(format!(
            "api request body too large: {} bytes",
            body_text.len()
        ));
    }
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: yolo\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body_text.len(),
        body_text
    );
    stream
        .set_read_timeout(Some(API_REQUEST_TIMEOUT))
        .map_err(|err| format!("set api read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(API_REQUEST_TIMEOUT))
        .map_err(|err| format!("set api write timeout: {err}"))?;
    stream
        .write_all(request.as_bytes())
        .map_err(|err| format!("write request: {err}"))?;
    stream
        .shutdown(std::net::Shutdown::Write)
        .map_err(|err| format!("shutdown request: {err}"))?;
    let mut response_bytes = Vec::new();
    stream
        .take((MAX_API_RESPONSE_BYTES + 1) as u64)
        .read_to_end(&mut response_bytes)
        .map_err(|err| format!("read response: {err}"))?;
    if response_bytes.len() > MAX_API_RESPONSE_BYTES {
        return Err(format!(
            "api response too large: more than {MAX_API_RESPONSE_BYTES} bytes"
        ));
    }
    let response = String::from_utf8(response_bytes)
        .map_err(|err| format!("decode api response text: {err}"))?;
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
    let connect_timeout = FEDERATION_CONNECT_TIMEOUT.as_secs().max(1).to_string();
    let request_timeout = FEDERATION_HTTP_TIMEOUT.as_secs().max(1).to_string();
    command
        .arg("--connect-timeout")
        .arg(connect_timeout)
        .arg("--max-time")
        .arg(request_timeout)
        .arg("--data-binary")
        .arg(body_text)
        .arg(url);
    let output = command_output_with_timeout(command, FEDERATION_HTTP_TIMEOUT)
        .map_err(|err| format!("federation curl: {err}"))?;
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

fn command_output_with_timeout(mut command: Command, timeout: Duration) -> Result<Output, String> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("spawn command: {err}"))?;
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                return child
                    .wait_with_output()
                    .map_err(|err| format!("collect command output: {err}"));
            }
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("command timed out after {}s", timeout.as_secs()));
            }
            Ok(None) => thread::sleep(Duration::from_millis(50)),
            Err(err) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("wait for command: {err}"));
            }
        }
    }
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
    if content_length > MAX_API_REQUEST_BODY_BYTES {
        return Err(format!("request body too large: {content_length} bytes"));
    }
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
    let Ok(input) = fs::File::open(path) else {
        return;
    };
    let mut reader = BufReader::new(input);
    let mut line = Vec::with_capacity(8192);

    'records: loop {
        line.clear();
        let mut saw_any = false;
        let mut oversized = false;

        loop {
            let Ok(buffer) = reader.fill_buf() else {
                break 'records;
            };
            if buffer.is_empty() {
                if !saw_any {
                    break 'records;
                }
                break;
            }

            saw_any = true;
            let newline = buffer.iter().position(|byte| *byte == b'\n');
            let take_len = newline.map_or(buffer.len(), |offset| offset + 1);
            if !oversized && line.len().saturating_add(take_len) <= MAX_TURN_ARCHIVE_LINE_BYTES {
                line.extend_from_slice(&buffer[..take_len]);
            } else {
                oversized = true;
            }
            reader.consume(take_len);

            if newline.is_some() {
                break;
            }
        }

        if oversized || line.is_empty() {
            continue;
        }
        let Ok(info) = serde_json::from_slice::<TurnInfo>(&line) else {
            continue;
        };
        let record = turn_record_from_info(info);
        telemetry.observe_trace_sequence(&record);
        telemetry.turns.insert(record.key.clone(), record);
        telemetry.trim_turns();
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

fn yolo_default_configuration_from_server() -> Option<YoloDefaultConfiguration> {
    let value = api_get_json("/defaults").ok()?;
    let configuration = value.get("configuration")?.clone();
    let configuration = serde_json::from_value::<YoloDefaultConfiguration>(configuration).ok()?;
    if configuration.model.trim().is_empty() || configuration.reasoning_effort.trim().is_empty() {
        return None;
    }
    Some(configuration)
}

fn with_yolo_session_defaults(
    args: Vec<OsString>,
    configuration: Option<&YoloDefaultConfiguration>,
) -> Vec<OsString> {
    let strings = args
        .iter()
        .map(|arg| arg.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    let config = parse_codex_launch_config(&strings);
    let mut defaults = Vec::new();
    let Some(configuration) = configuration else {
        return args;
    };
    if config.model.is_none() {
        defaults.extend([
            OsString::from("-c"),
            codex_config_os_arg("model", &configuration.model),
        ]);
    }
    if config.reasoning_effort.is_none() {
        defaults.extend([
            OsString::from("-c"),
            codex_config_os_arg("model_reasoning_effort", &configuration.reasoning_effort),
        ]);
    }
    if config.service_tier.is_none() {
        defaults.extend([
            OsString::from("-c"),
            codex_config_os_arg(
                "service_tier",
                if configuration.fast {
                    "priority"
                } else {
                    "default"
                },
            ),
        ]);
    }
    defaults.extend(args);
    defaults
}

fn yolo_mode_cli_args() -> Vec<OsString> {
    vec![
        OsString::from("--search"),
        OsString::from("--dangerously-bypass-approvals-and-sandbox"),
        OsString::from("-s"),
        OsString::from("danger-full-access"),
        OsString::from("-c"),
        OsString::from("approval_policy=\"never\""),
        OsString::from("-c"),
        OsString::from("sandbox_mode=\"danger-full-access\""),
        // YOLO clients must stay non-interactive when a newer Codex CLI is
        // available. Codex updates are managed explicitly by yolo's
        // `upgrade-resume`/`upgrade-resume-all` flows.
        OsString::from("-c"),
        OsString::from("check_for_update_on_startup=false"),
    ]
}

fn strip_conflicting_yolo_options(args: Vec<OsString>) -> Vec<OsString> {
    let mut output = Vec::with_capacity(args.len());
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        let text = arg.to_string_lossy();
        if matches!(
            text.as_ref(),
            "-s" | "--sandbox" | "-a" | "--ask-for-approval"
        ) {
            let _ = iter.next();
            continue;
        }
        if text.starts_with("--sandbox=") || text.starts_with("--ask-for-approval=") {
            continue;
        }
        if text == "--dangerously-bypass-approvals-and-sandbox" {
            continue;
        }
        if text == "-c" || text == "--config" {
            let Some(value) = iter.next() else {
                output.push(arg);
                continue;
            };
            if is_conflicting_yolo_config(&value.to_string_lossy()) {
                continue;
            }
            output.push(arg);
            output.push(value);
            continue;
        }
        if text
            .strip_prefix("--config=")
            .is_some_and(is_conflicting_yolo_config)
        {
            continue;
        }
        output.push(arg);
    }
    output
}

fn is_conflicting_yolo_config(value: &str) -> bool {
    let Some((key, _)) = value.split_once('=') else {
        return false;
    };
    matches!(
        key.trim(),
        "approval_policy" | "approval_mode" | "sandbox_mode" | "sandbox_policy"
    )
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
    select_native_codex_executable(
        env::var_os("YOLO_NATIVE_CODEX"),
        find_executable_in_path(DEFAULT_CODEX),
        managed_codex_bin(),
    )
}

fn select_native_codex_executable(
    explicit: Option<OsString>,
    path_executable: Option<PathBuf>,
    managed: PathBuf,
) -> OsString {
    if let Some(explicit) = explicit.filter(|value| !value.is_empty()) {
        return explicit;
    }
    if let Some(path_executable) = path_executable {
        return path_executable.into_os_string();
    }
    if managed.is_file() {
        return managed.into_os_string();
    }
    OsString::from(DEFAULT_CODEX)
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

fn persistent_state_dir() -> PathBuf {
    env::var_os("YOLO_STATE_DIR")
        .map(PathBuf::from)
        .or_else(|| {
            env::var_os("XDG_STATE_HOME").map(|dir| PathBuf::from(dir).join(RUNTIME_DIR_NAME))
        })
        .or_else(|| {
            env::var_os("HOME").map(|home| {
                PathBuf::from(home)
                    .join(".local")
                    .join("state")
                    .join(RUNTIME_DIR_NAME)
            })
        })
        .unwrap_or_else(|| PathBuf::from("/tmp").join(RUNTIME_DIR_NAME).join("state"))
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
        active_sessions: persistent_state_dir().join(ACTIVE_SESSIONS_FILE_NAME),
        default_configuration: persistent_state_dir().join(DEFAULT_CONFIGURATION_FILE_NAME),
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
  yolo saved-sessions
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
user-writable npm prefix, migrates live yolo wrappers through the authorized
idle gate, restarts the yolo app-server, then launches `codex resume` through
yolo. With no arguments it resumes `--last`.

upgrade-resume-all asks the running yolo server to install the latest Codex
CLI, wait for active app-server threads to become idle, migrate live yolo
wrappers while the current app-server is reachable, restart its app-server
child, and request every live yolo client wrapper to restart its Codex child as
`codex resume` on the same terminal.

upgrade-resume-reexec waits for idle clients and migrates their yolo wrappers
without restarting the app-server; use it after a manual yolo binary install
and before restarting yolo.service.

refresh-permissions reapplies YOLO-mode live settings to already-loaded resume
threads without restarting the yolo client or Codex child process.

When run from inside Codex, upgrade-resume-all uses Phoenix mode: it excludes
the caller's CODEX_THREAD_ID from the idle wait, then lets the final resume
generation revive that same session.

API:
curl --unix-socket $XDG_RUNTIME_DIR/yolo/api.sock http://yolo/clients
  yolo saved-sessions
  curl --unix-socket $XDG_RUNTIME_DIR/yolo/api.sock 'http://yolo/turns?limit=20'
  yolo turns --thread THREAD_ID --limit 20
  yolo turns --history --thread THREAD_ID --limit 20
  curl -X POST --unix-socket $XDG_RUNTIME_DIR/yolo/api.sock http://yolo/upgrade-resume-reexec
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
  YOLO_STATE_DIR    Persistent state dir (default: $XDG_STATE_HOME/yolo or ~/.local/state/yolo)
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
