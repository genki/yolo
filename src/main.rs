use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
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

#[derive(Clone, Debug)]
struct RuntimePaths {
    dir: PathBuf,
    api_socket: PathBuf,
    app_server_socket: PathBuf,
    pid_file: PathBuf,
    log_file: PathBuf,
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
}

#[derive(Debug)]
struct ServerState {
    started_at: u64,
    app_server_pid: Option<u32>,
    app_server_generation: u64,
    resume_generation: u64,
    clients: BTreeMap<String, ClientInfo>,
    slaves: BTreeMap<String, SlaveInfo>,
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

enum ClientEvent {
    RestartRequested,
    CodexExited(Result<ExitStatus, String>),
}

#[derive(Debug, Default)]
struct CodexLaunchConfig {
    model: Option<String>,
    service_tier: Option<String>,
    reasoning_effort: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
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
    remove_socket_if_present(&paths.api_socket)?;
    fs::write(&paths.pid_file, std::process::id().to_string())
        .map_err(|err| format!("write pid file: {err}"))?;

    let state = Arc::new(Mutex::new(ServerState {
        started_at: now_secs(),
        app_server_pid: None,
        app_server_generation: 0,
        resume_generation: 0,
        clients: BTreeMap::new(),
        slaves: BTreeMap::new(),
    }));
    let app_server_pid = ensure_tracked_app_server(Arc::clone(&state), paths.clone())?;
    scan_existing_yolo_clients(&state);
    spawn_thread_status_monitor(Arc::clone(&state), paths.clone());
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
    let exe = env::current_exe().map_err(|err| format!("current exe: {err}"))?;
    let paths = runtime_paths()?;
    fs::create_dir_all(&paths.dir).map_err(|err| format!("create runtime dir: {err}"))?;
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
        .arg(exe)
        .args(foreground_args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log2))
        .spawn()
        .map_err(|err| format!("spawn yolo server: {err}"))?;
    println!("started yolo server pid {}", child.id());
    wait_for_server_ready(&paths, Duration::from_secs(10))
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

    wait_for_app_server_ready(&paths, Duration::from_secs(10))?;

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

    wait_for_app_server_ready(&paths, Duration::from_secs(10))?;

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
    let remote = env::var("YOLO_REMOTE")
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
        remote: remote.clone(),
        model: initial_config.model,
        service_tier: initial_service_tier,
        reasoning_effort: None,
        fast: initial_fast,
        thread_id,
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

    let child_event_tx = event_tx.clone();
    thread::spawn(move || {
        let result = child.wait().map_err(|err| err.to_string());
        let _ = child_event_tx.send(ClientEvent::CodexExited(result));
    });
    drop(event_tx);

    loop {
        match event_rx.recv() {
            Ok(ClientEvent::RestartRequested) => {
                terminate_pid_tree(child_pid, Duration::from_secs(5));
                reexec_client_for_resume(&original_args);
            }
            Ok(ClientEvent::CodexExited(Ok(status))) => {
                if should_reexec_after_codex_exit(
                    status.success(),
                    &original_args,
                    &seen_resume_generation,
                ) {
                    reexec_client_for_resume(&original_args);
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
        eprintln!("yolo: failed to update loaded Codex thread settings for {thread_id}: {err}");
    }
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
        loop {
            let target = ResumeTarget::Thread(thread_id.clone());
            if let Some(path) = session_path_for_resume_target(&target) {
                let modified = fs::metadata(&path).and_then(|meta| meta.modified()).ok();
                if modified.is_some() && modified != last_modified {
                    if let Err(err) = rewrite_session_meta_cwd(&path, &cwd) {
                        eprintln!(
                            "yolo: failed to repair Codex rollout context for {}: {err}",
                            path.display()
                        );
                    }
                    last_modified = fs::metadata(&path)
                        .and_then(|meta| meta.modified())
                        .ok()
                        .or(modified);
                }
            }
            thread::sleep(Duration::from_secs(2));
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

fn rewrite_session_meta_cwd(path: &Path, cwd: &str) -> Result<(), String> {
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
    Ok(())
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
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|err| err.to_string())?
    );
    Ok(())
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

fn resume_args_for(args: &[OsString]) -> Vec<OsString> {
    if thread_id_from_args(args).is_some() {
        return args.to_vec();
    }
    vec![OsString::from("resume"), OsString::from("--last")]
}

fn reexec_client_for_resume(original_args: &[OsString]) -> ! {
    let resume_args = resume_args_for(original_args);
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
        return wait_for_app_server_ready(&paths, Duration::from_secs(10));
    }
    spawn_server_daemon(&[])?;
    wait_for_server_ready(&paths, Duration::from_secs(10))
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
    let response = match read_http_request(&mut stream) {
        Ok((method, path, headers, body)) => {
            handle_federation_request(&method, &path, &headers, &body, state, paths)
        }
        Err(err) => json_response(400, &json!({"ok": false, "error": err})),
    };
    let _ = stream.write_all(response.as_bytes());
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
    let record = SlaveCommandRecord {
        command,
        status: "pending".to_string(),
        created_at: now,
        started_at: None,
        finished_at: None,
        result: None,
    };
    if let Ok(mut state) = state.lock() {
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
            });
        slave.commands.push(record.clone());
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
    match (method, path) {
        ("GET", "/status") => {
            let info = server_info(&state, &paths);
            json_response(200, &info)
        }
        ("GET", "/clients") => {
            let info = server_info(&state, &paths);
            json_response(200, &info)
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
                Ok(request) => {
                    match configure_clients_when_idle(Arc::clone(&state), &paths, request) {
                        Ok(value) => json_response(200, &value),
                        Err(err) => json_response(500, &json!({"ok": false, "error": err})),
                    }
                }
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
        loop {
            if let Err(err) = run_thread_status_event_listener(&state, &paths) {
                eprintln!("yolo server: Codex app-server status listener stopped: {err}");
                thread::sleep(THREAD_MONITOR_INTERVAL);
            }
        }
    });
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
            Ok(value) => {
                if let Some(snapshot) = parse_app_server_thread_response(&value) {
                    apply_single_thread_snapshot(state, &snapshot);
                } else if let Some(update) = parse_app_server_status_notification(&value) {
                    apply_thread_status_update(state, &update);
                }
            }
            Err(err) if is_app_server_read_timeout(&err) => {}
            Err(err) => return Err(err),
        }
    }
}

fn subscribe_running_client_threads(
    state: &Arc<Mutex<ServerState>>,
    client: &mut AppServerRpcClient,
    subscribed_thread_ids: &mut BTreeSet<String>,
) -> Result<(), String> {
    let target_thread_ids = known_running_client_thread_ids(state);
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
    let mut child_codex_by_parent: BTreeMap<u32, u32> = BTreeMap::new();
    let mut live_client_pids: BTreeSet<u32> = BTreeSet::new();
    for process in &processes {
        if process.cmdline.iter().any(|arg| arg.contains("codex"))
            && process
                .cmdline
                .iter()
                .any(|arg| arg.contains("--remote") || arg.contains("codex-app-server.sock"))
        {
            child_codex_by_parent.insert(process.ppid, process.pid);
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
        let id = format!("{}-scanned", process.pid);
        if state
            .clients
            .values()
            .any(|client| client.yolo_pid == process.pid && client.status == "running")
        {
            continue;
        }
        let cfg = read_codex_config();
        let launch_cfg = parse_codex_launch_config(&args);
        let service_tier = launch_cfg
            .service_tier
            .clone()
            .or_else(|| cfg.service_tier.clone());
        state.clients.insert(
            id.clone(),
            ClientInfo {
                id,
                yolo_pid: process.pid,
                codex_pid: child_codex_by_parent.get(&process.pid).copied(),
                cwd: process.cwd.unwrap_or_else(|| String::from("")),
                args: args.clone(),
                remote: String::new(),
                model: launch_cfg.model.or(cfg.model),
                service_tier: service_tier.clone(),
                reasoning_effort: launch_cfg.reasoning_effort,
                fast: is_fast_tier(service_tier.as_deref()),
                thread_id: thread_id_from_args_strs(&args),
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

    for client in state.clients.values_mut() {
        if !matches!(client.status.as_str(), "running" | "restarting") {
            continue;
        }

        let matched = match client.thread_id.as_deref() {
            Some(thread_id) => snapshot.iter().find(|thread| thread.id == thread_id),
            None => snapshot
                .iter()
                .find(|thread| thread.cwd == client.cwd && thread.status == "active")
                .or_else(|| snapshot.iter().find(|thread| thread.cwd == client.cwd)),
        };

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
        if let Some(model) = launch_cfg.model.or_else(|| thread.model.clone()) {
            client.model = Some(model);
        }
        if let Some(service_tier) = launch_cfg
            .service_tier
            .or_else(|| thread.service_tier.clone())
        {
            client.service_tier = Some(service_tier);
            client.fast = is_fast_tier(client.service_tier.as_deref());
        }
        if let Some(reasoning_effort) = launch_cfg
            .reasoning_effort
            .or_else(|| thread.reasoning_effort.clone())
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

    for client in state.clients.values_mut() {
        if !matches!(client.status.as_str(), "running" | "restarting") {
            continue;
        }

        let matched = match client.thread_id.as_deref() {
            Some(thread_id) => thread.id == thread_id,
            None => thread.cwd == client.cwd && thread.status == "active",
        };
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
        if let Some(model) = launch_cfg.model.or_else(|| thread.model.clone()) {
            client.model = Some(model);
        }
        if let Some(service_tier) = launch_cfg
            .service_tier
            .or_else(|| thread.service_tier.clone())
        {
            client.service_tier = Some(service_tier);
            client.fast = is_fast_tier(client.service_tier.as_deref());
        }
        if let Some(reasoning_effort) = launch_cfg
            .reasoning_effort
            .or_else(|| thread.reasoning_effort.clone())
        {
            client.reasoning_effort = Some(reasoning_effort);
        }
    }
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

fn configure_clients_when_idle(
    state: Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
    request: ConfigureClientsRequest,
) -> Result<Value, String> {
    let timeout = request
        .timeout_secs
        .map(Duration::from_secs)
        .unwrap_or_else(upgrade_idle_wait_timeout);
    let selected_ids = select_configure_clients(&state, &request)?;
    if selected_ids.is_empty() {
        return Err("no matching yolo clients".to_string());
    }

    let start = SystemTime::now();
    loop {
        let target_thread_ids = selected_thread_ids_for_snapshot(&state, &selected_ids);
        let snapshot = app_server_thread_snapshot(paths, target_thread_ids.as_ref())?;
        apply_thread_snapshot(&state, &snapshot);
        let working = working_selected_clients_for_snapshot(&state, &snapshot, &selected_ids);
        if working.is_empty() {
            break;
        }
        if start.elapsed().unwrap_or_default() >= timeout {
            return Err(format!(
                "timed out waiting for selected Codex clients to become idle: {}",
                working.join(", ")
            ));
        }
        eprintln!(
            "yolo: waiting for selected Codex clients to become idle before settings update: {}",
            working.join(", ")
        );
        thread::sleep(UPGRADE_IDLE_POLL_INTERVAL);
    }

    let clients = selected_clients_with_threads(&state, &selected_ids)?;
    let mut rpc = AppServerRpcClient::connect(&paths.app_server_socket)?;
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
            &state,
            &client_id,
            request.model.clone(),
            request.fast,
            request.reasoning_effort.clone(),
        );
        updated.push(json!({
            "client_id": client_id,
            "thread_id": thread_id,
        }));
    }

    let target_thread_ids = selected_thread_ids_for_snapshot(&state, &selected_ids);
    let snapshot = app_server_thread_snapshot(paths, target_thread_ids.as_ref())?;
    apply_thread_snapshot(&state, &snapshot);
    Ok(json!({
        "ok": true,
        "updated": updated,
        "model": request.model,
        "fast": request.fast,
        "reasoning_effort": request.reasoning_effort,
    }))
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
            || request.thread_id.as_deref() == client.thread_id.as_deref()
            || request.cwd.as_deref() == Some(client.cwd.as_str());
        if matched {
            ids.insert(client.id.clone());
        }
    }
    Ok(ids)
}

fn selected_clients_with_threads(
    state: &Arc<Mutex<ServerState>>,
    selected_ids: &BTreeSet<String>,
) -> Result<Vec<(String, String)>, String> {
    let state = state
        .lock()
        .map_err(|_| "server state lock poisoned".to_string())?;
    selected_ids
        .iter()
        .map(|id| {
            let client = state
                .clients
                .get(id)
                .ok_or_else(|| format!("selected client disappeared: {id}"))?;
            let thread_id = client
                .thread_id
                .clone()
                .ok_or_else(|| format!("client {id} has no app-server thread id yet"))?;
            Ok((id.clone(), thread_id))
        })
        .collect()
}

fn selected_thread_ids_for_snapshot(
    state: &Arc<Mutex<ServerState>>,
    selected_ids: &BTreeSet<String>,
) -> Option<BTreeSet<String>> {
    let Ok(state) = state.lock() else {
        return None;
    };
    let mut ids = BTreeSet::new();
    let mut has_selected_without_thread = false;
    for id in selected_ids {
        let Some(client) = state.clients.get(id) else {
            continue;
        };
        if let Some(thread_id) = client.thread_id.as_deref() {
            if !thread_id.trim().is_empty() {
                ids.insert(thread_id.to_string());
            }
        } else {
            has_selected_without_thread = true;
        }
    }
    if has_selected_without_thread {
        return None;
    }
    Some(ids)
}

fn working_selected_clients_for_snapshot(
    state: &Arc<Mutex<ServerState>>,
    snapshot: &[AppThreadSnapshot],
    selected_ids: &BTreeSet<String>,
) -> Vec<String> {
    let Ok(state) = state.lock() else {
        return Vec::new();
    };
    state
        .clients
        .values()
        .filter(|client| selected_ids.contains(&client.id))
        .filter(|client| matches!(client.status.as_str(), "running" | "restarting"))
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
    }
}

fn app_server_thread_snapshot(
    paths: &RuntimePaths,
    target_thread_ids: Option<&BTreeSet<String>>,
) -> Result<Vec<AppThreadSnapshot>, String> {
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

fn update_app_server_resume_thread_settings(
    socket: &Path,
    thread_id: &str,
    cwd: &str,
) -> Result<(), String> {
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

        Ok(Self { stream, next_id: 1 })
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
        let message = websocket_read_text(&mut self.stream)?;
        serde_json::from_str(&message)
            .map_err(|err| format!("decode app-server message: {err}: {message}"))
    }
}

fn read_http_headers(stream: &mut UnixStream) -> Result<String, String> {
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

fn websocket_send_text(stream: &mut UnixStream, text: &str) -> Result<(), String> {
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

fn websocket_read_text(stream: &mut UnixStream) -> Result<String, String> {
    let deadline = Instant::now() + APP_SERVER_RPC_READ_RETRY_TIMEOUT;
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

fn read_exact_retry(
    stream: &mut UnixStream,
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

fn websocket_send_pong(stream: &mut UnixStream, payload: &[u8]) -> Result<(), String> {
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
  yolo server [--daemon|--foreground] [--federation-listen ADDR]
  yolo status
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

When run from inside Codex, upgrade-resume-all uses Phoenix mode: it excludes
the caller's CODEX_THREAD_ID from the idle wait, then lets the final resume
generation revive that same session.

API:
  curl --unix-socket $XDG_RUNTIME_DIR/yolo/api.sock http://yolo/clients
  curl -X POST --unix-socket $XDG_RUNTIME_DIR/yolo/api.sock http://yolo/upgrade-resume-all

Federation:
  yolo server --daemon --federation-listen 127.0.0.1:47040
  YOLO_MASTER_URL=https://agent-gate/.../@localhost:47040 \
    YOLO_SLAVE_ID=slave YOLO_MASTER_BEARER_TOKEN=agt_... yolo server --daemon

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
