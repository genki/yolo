use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
const FEDERATION_POLL_INTERVAL: Duration = Duration::from_secs(5);

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
        Some("set") | Some("configure") => {
            args.remove(0);
            if let Err(err) = run_configure(args) {
                eprintln!("yolo set: {err}");
                std::process::exit(1);
            }
        }
        Some("client") => {
            args.remove(0);
            run_client(args);
        }
        _ => run_client(args),
    }
}

fn run_server(args: Vec<OsString>) -> Result<(), String> {
    let daemon = args.iter().any(|arg| arg == "--daemon");
    let foreground = args.iter().any(|arg| arg == "--foreground");
    if daemon && !foreground {
        return spawn_server_daemon();
    }

    let paths = runtime_paths()?;
    fs::create_dir_all(&paths.dir).map_err(|err| format!("create runtime dir: {err}"))?;
    remove_socket_if_present(&paths.api_socket)?;
    fs::write(&paths.pid_file, std::process::id().to_string())
        .map_err(|err| format!("write pid file: {err}"))?;

    let state = Arc::new(Mutex::new(ServerState {
        started_at: now_secs(),
        app_server_pid: None,
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

fn spawn_server_daemon() -> Result<(), String> {
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
    let child = Command::new("setsid")
        .arg(exe)
        .arg("server")
        .arg("--foreground")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log2))
        .spawn()
        .map_err(|err| format!("spawn yolo server: {err}"))?;
    println!("started yolo server pid {}", child.id());
    wait_for_server_ready(&paths, Duration::from_secs(10))
}

fn spawn_app_server(paths: &RuntimePaths) -> Result<Child, String> {
    let codex = codex_executable();
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.log_file)
        .map_err(|err| format!("open {}: {err}", paths.log_file.display()))?;
    let log2 = log
        .try_clone()
        .map_err(|err| format!("clone app-server log: {err}"))?;
    Command::new(codex)
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
    let mut app_server = spawn_app_server(&paths)?;
    let pid = app_server.id();
    if let Ok(mut state) = state.lock() {
        state.app_server_pid = Some(pid);
    }

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
    if paths.app_server_socket.exists()
        && AppServerRpcClient::connect(&paths.app_server_socket).is_ok()
    {
        let pid = find_app_server_pid(&paths);
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
    let old_pid = state.lock().ok().and_then(|state| state.app_server_pid);
    if let Some(pid) = old_pid {
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status();
        wait_for_pid_exit(pid, Duration::from_secs(5));
    }
    spawn_tracked_app_server(state, paths)
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
    let original_args = args.clone();
    let mut launch_args = args;
    let string_args = original_args
        .iter()
        .map(|arg| arg.to_string_lossy().to_string())
        .collect::<Vec<_>>();

    let initial_config = read_codex_config();
    let initial_service_tier = initial_config.service_tier.clone();
    let initial_fast = is_fast_tier(initial_service_tier.as_deref());
    let mut info = ClientInfo {
        id: client_id.clone(),
        yolo_pid: std::process::id(),
        codex_pid: None,
        cwd,
        args: string_args,
        remote: remote.clone(),
        model: initial_config.model,
        service_tier: initial_service_tier,
        reasoning_effort: None,
        fast: initial_fast,
        thread_id: thread_id_from_args(&original_args),
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
    let restart_requested = Arc::new(AtomicBool::new(false));
    let heartbeat_restart_requested = Arc::clone(&restart_requested);
    let seen_resume_generation = Arc::new(AtomicU64::new(current_resume_generation()));
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
                    if let Some(generation) = value.get("resume_generation").and_then(Value::as_u64)
                    {
                        let seen = heartbeat_seen_generation.load(Ordering::SeqCst);
                        if generation > seen {
                            heartbeat_seen_generation.store(generation, Ordering::SeqCst);
                            heartbeat_restart_requested.store(true, Ordering::SeqCst);
                        }
                    }
                }
                Err(_) => continue,
            }
        }
    });

    loop {
        let codex = codex_executable();
        let mut command = Command::new(codex);
        command
            .arg("--remote")
            .arg(&remote)
            .arg("--search")
            .arg("--dangerously-bypass-approvals-and-sandbox")
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

        info.codex_pid = Some(child.id());
        info.updated_at = now_secs();
        info.ended_at = None;
        info.exit_code = None;
        info.status = "running".to_string();
        let _ = api_post_json(
            "/clients/register",
            &serde_json::to_value(&info).unwrap_or_else(|_| json!({})),
        );

        loop {
            if restart_requested.swap(false, Ordering::SeqCst) {
                terminate_child(&mut child);
                break;
            }
            match child.try_wait() {
                Ok(Some(status)) => {
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
                Ok(None) => thread::sleep(Duration::from_millis(200)),
                Err(err) => {
                    eprintln!("yolo: failed to wait for codex: {err}");
                    std::process::exit(1);
                }
            }
        }

        launch_args = resume_args_for(&original_args);
        info.args = launch_args
            .iter()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect();
        info.thread_id = thread_id_from_args(&launch_args);
        info.status = "restarting".to_string();
        info.updated_at = now_secs();
        let _ = api_post_json(
            "/clients/register",
            &serde_json::to_value(&info).unwrap_or_else(|_| json!({})),
        );
        thread::sleep(Duration::from_millis(300));
    }
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
    let value = api_post_json("/upgrade-resume-all", &json!({}))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|err| err.to_string())?
    );
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

fn wait_for_pid_exit(pid: u32, timeout: Duration) {
    let start = SystemTime::now();
    while start.elapsed().unwrap_or_default() < timeout {
        let alive = Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .status()
            .map(|status| status.success())
            .unwrap_or(false);
        if !alive {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    let _ = Command::new("kill")
        .arg("-KILL")
        .arg(pid.to_string())
        .status();
}

fn terminate_child(child: &mut Child) {
    let pid = child.id();
    let _ = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status();
    let start = SystemTime::now();
    while start.elapsed().unwrap_or_default() < Duration::from_secs(5) {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => thread::sleep(Duration::from_millis(100)),
            Err(_) => return,
        }
    }
    let _ = child.kill();
    let _ = child.wait();
}

fn resume_args_for(args: &[OsString]) -> Vec<OsString> {
    if thread_id_from_args(args).is_some() {
        return args.to_vec();
    }
    vec![OsString::from("resume"), OsString::from("--last")]
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
        .and_then(|value| value.get("resume_generation").and_then(Value::as_u64))
        .unwrap_or(0)
}

fn ensure_server() -> Result<(), String> {
    let paths = runtime_paths()?;
    if api_get_json("/status").is_ok() {
        return Ok(());
    }
    spawn_server_daemon()?;
    wait_for_server_ready(&paths, Duration::from_secs(10))
}

fn wait_for_server_ready(paths: &RuntimePaths, timeout: Duration) -> Result<(), String> {
    let start = SystemTime::now();
    while start.elapsed().unwrap_or_default() < timeout {
        if paths.api_socket.exists()
            && paths.app_server_socket.exists()
            && api_get_json("/status").is_ok()
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
    if federation_admin_token().is_none() {
        return Err("YOLO_FEDERATION_ADMIN_TOKEN is required for federation listener".to_string());
    }
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
    headers: &BTreeMap<String, String>,
    body: &str,
    state: Arc<Mutex<ServerState>>,
    paths: RuntimePaths,
) -> String {
    match (method, path) {
        ("GET", "/federation/slaves") => {
            if !is_admin_request(headers) {
                return json_response(401, &json!({"ok": false, "error": "unauthorized"}));
            }
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
            if !is_slave_request(headers, &request.slave_id) {
                return json_response(401, &json!({"ok": false, "error": "unauthorized"}));
            }
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
            if !is_slave_request(headers, &request.slave_id) {
                return json_response(401, &json!({"ok": false, "error": "unauthorized"}));
            }
            record_slave_result(&state, request);
            json_response(200, &json!({"ok": true}))
        }
        ("POST", path)
            if path.starts_with("/federation/slaves/") && path.ends_with("/commands") =>
        {
            if !is_admin_request(headers) {
                return json_response(401, &json!({"ok": false, "error": "unauthorized"}));
            }
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
            if !is_admin_request(headers) {
                return json_response(401, &json!({"ok": false, "error": "unauthorized"}));
            }
            let request = serde_json::from_str::<Value>(body).unwrap_or_else(|_| json!({}));
            let version = request.get("codex_version").and_then(Value::as_str);
            match run_codex_upgrade_resume_all_local(Arc::clone(&state), &paths, version) {
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
    let Ok(token) = env::var("YOLO_SLAVE_TOKEN") else {
        eprintln!("yolo slave connector disabled: YOLO_SLAVE_TOKEN is missing");
        return;
    };
    thread::spawn(move || {
        let mut pending_result: Option<SlaveResultRequest> = None;
        loop {
            if let Some(result) = pending_result.take() {
                let _ = federation_post_json(
                    &master_url,
                    "/federation/slaves/result",
                    &token,
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
                &token,
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
                                    &token,
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
    token: &str,
    slave_id: &str,
    command: &SlaveCommand,
) -> Value {
    match command.action.as_str() {
        "codex-upgrade-resume" | "upgrade-codex" | "upgrade-resume-all" => {
            match run_codex_upgrade_resume_all_local(state, paths, command.codex_version.as_deref())
            {
                Ok(value) => value,
                Err(err) => json!({"ok": false, "error": err}),
            }
        }
        "yolo-upgrade" | "upgrade-yolo" => match upgrade_yolo(command) {
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
                    token,
                    &serde_json::to_value(result).unwrap_or_else(|_| json!({})),
                );
                schedule_yolo_server_restart();
                value
            }
            Err(err) => json!({"ok": false, "error": err}),
        },
        _ => {
            json!({"ok": false, "error": format!("unknown slave command action: {}", command.action)})
        }
    }
}

fn run_codex_upgrade_resume_all_local(
    state: Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
    codex_version: Option<&str>,
) -> Result<Value, String> {
    wait_for_clients_idle(Arc::clone(&state), paths)
        .and_then(|_| upgrade_codex_cli_version(codex_version))
        .and_then(|_| restart_tracked_app_server(Arc::clone(&state), paths.clone()))
        .and_then(|app_server_pid| {
            if let Ok(mut state) = state.lock() {
                state.resume_generation = state.resume_generation.saturating_add(1);
                let generation = state.resume_generation;
                return Ok(json!({
                    "ok": true,
                    "app_server_pid": app_server_pid,
                    "resume_generation": generation,
                    "codex_version": codex_version,
                    "clients": state.clients.len()
                }));
            }
            Err("server state lock poisoned".to_string())
        })
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
        "restart_scheduled": true,
        "yolo_version": command.yolo_version,
    }))
}

fn schedule_yolo_server_restart() {
    let exe = env::current_exe().unwrap_or_else(|_| PathBuf::from("yolo"));
    thread::spawn(move || {
        thread::sleep(Duration::from_secs(3));
        let _ = Command::new(exe)
            .arg("server")
            .arg("--daemon")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        thread::sleep(Duration::from_secs(1));
        std::process::exit(0);
    });
}

fn handle_api_connection(
    mut stream: UnixStream,
    state: Arc<Mutex<ServerState>>,
    paths: RuntimePaths,
) {
    let mut request = Vec::new();
    if stream.read_to_end(&mut request).is_err() {
        return;
    }
    let request = String::from_utf8_lossy(&request);
    let mut lines = request.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();
    let body = request
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .or_else(|| request.split_once("\n\n").map(|(_, body)| body))
        .unwrap_or_default();

    let response = match (method, path) {
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
                    if let Some(id) = value.get("id").and_then(Value::as_str)
                        && let Ok(mut state) = state.lock()
                    {
                        resume_generation = state.resume_generation;
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
                        &json!({"ok": true, "resume_generation": resume_generation}),
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
        ("POST", "/upgrade-resume-all") => {
            let request = serde_json::from_str::<Value>(body).unwrap_or_else(|_| json!({}));
            let version = request.get("codex_version").and_then(Value::as_str);
            let result = run_codex_upgrade_resume_all_local(Arc::clone(&state), &paths, version);
            match result {
                Ok(value) => json_response(200, &value),
                Err(err) => json_response(500, &json!({"ok": false, "error": err})),
            }
        }
        ("POST", "/shutdown") => {
            if let Ok(state) = state.lock()
                && let Some(pid) = state.app_server_pid
            {
                let _ = Command::new("kill").arg(pid.to_string()).status();
            }
            let _ = stream.write_all(json_response(200, &json!({"ok": true})).as_bytes());
            std::process::exit(0);
        }
        _ => json_response(404, &json!({"ok": false, "error": "not found"})),
    };

    let _ = stream.write_all(response.as_bytes());
}

fn spawn_thread_status_monitor(state: Arc<Mutex<ServerState>>, paths: RuntimePaths) {
    thread::spawn(move || {
        loop {
            scan_existing_yolo_clients(&state);
            if let Ok(snapshot) = app_server_thread_snapshot(&paths) {
                apply_thread_snapshot(&state, &snapshot);
            }
            thread::sleep(THREAD_MONITOR_INTERVAL);
        }
    });
}

fn scan_existing_yolo_clients(state: &Arc<Mutex<ServerState>>) {
    let Ok(processes) = read_process_table() else {
        return;
    };
    let now = now_secs();
    let current_pid = std::process::id();
    let mut child_codex_by_parent: BTreeMap<u32, u32> = BTreeMap::new();
    for process in &processes {
        if process.cmdline.iter().any(|arg| arg.contains("codex"))
            && process
                .cmdline
                .iter()
                .any(|arg| arg.contains("--remote") || arg.contains("codex-app-server.sock"))
        {
            child_codex_by_parent.insert(process.ppid, process.pid);
        }
    }

    let Ok(mut state) = state.lock() else {
        return;
    };
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
        Some("server" | "status" | "clients" | "stop" | "set" | "configure") => false,
        Some("upgrade-resume" | "resume-upgrade" | "upgrade-and-resume") => false,
        Some("upgrade-resume-all" | "resume-all-upgrade") => false,
        Some(arg) if arg.starts_with('-') => true,
        Some(_) => true,
    }
}

fn find_app_server_pid(paths: &RuntimePaths) -> Option<u32> {
    let needle = paths.app_server_socket.display().to_string();
    read_process_table().ok()?.into_iter().find_map(|process| {
        let is_app_server = process.cmdline.iter().any(|arg| arg == "app-server")
            && process.cmdline.iter().any(|arg| arg.contains(&needle));
        is_app_server.then_some(process.pid)
    })
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

fn wait_for_clients_idle(
    state: Arc<Mutex<ServerState>>,
    paths: &RuntimePaths,
) -> Result<(), String> {
    let timeout = upgrade_idle_wait_timeout();
    let start = SystemTime::now();
    loop {
        let snapshot = app_server_thread_snapshot(paths)?;
        apply_thread_snapshot(&state, &snapshot);
        let working_clients = working_clients_for_snapshot(&state, &snapshot);
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
) -> Vec<String> {
    let Ok(state) = state.lock() else {
        return Vec::new();
    };
    state
        .clients
        .values()
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
        let snapshot = app_server_thread_snapshot(paths)?;
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

    let snapshot = app_server_thread_snapshot(paths)?;
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
        resume_generation: state.resume_generation,
        api_socket: paths.api_socket.display().to_string(),
        app_server_socket: paths.app_server_socket.display().to_string(),
        clients: state.clients.values().cloned().collect(),
        slaves: state.slaves.values().cloned().collect(),
    }
}

fn app_server_thread_snapshot(paths: &RuntimePaths) -> Result<Vec<AppThreadSnapshot>, String> {
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
            let message = websocket_read_text(&mut self.stream)?;
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
    loop {
        let mut header = [0u8; 2];
        stream
            .read_exact(&mut header)
            .map_err(|err| format!("read websocket frame header: {err}"))?;
        let opcode = header[0] & 0x0f;
        let masked = (header[1] & 0x80) != 0;
        let mut len = (header[1] & 0x7f) as u64;
        if len == 126 {
            let mut buf = [0u8; 2];
            stream
                .read_exact(&mut buf)
                .map_err(|err| format!("read websocket frame length: {err}"))?;
            len = u16::from_be_bytes(buf) as u64;
        } else if len == 127 {
            let mut buf = [0u8; 8];
            stream
                .read_exact(&mut buf)
                .map_err(|err| format!("read websocket frame length: {err}"))?;
            len = u64::from_be_bytes(buf);
        }
        if len > 16 * 1024 * 1024 {
            return Err("websocket frame too large".to_string());
        }
        let mask = if masked {
            let mut mask = [0u8; 4];
            stream
                .read_exact(&mut mask)
                .map_err(|err| format!("read websocket frame mask: {err}"))?;
            Some(mask)
        } else {
            None
        };
        let mut payload = vec![0u8; len as usize];
        stream
            .read_exact(&mut payload)
            .map_err(|err| format!("read websocket frame payload: {err}"))?;
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
    token: &str,
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
    let output = Command::new("curl")
        .arg("-fsS")
        .arg("-X")
        .arg("POST")
        .arg("-H")
        .arg("Content-Type: application/json")
        .arg("-H")
        .arg(format!("Authorization: Bearer {token}"))
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

fn is_admin_request(headers: &BTreeMap<String, String>) -> bool {
    let Some(token) = federation_admin_token() else {
        return false;
    };
    bearer_token(headers).as_deref() == Some(token.as_str())
}

fn is_slave_request(headers: &BTreeMap<String, String>, slave_id: &str) -> bool {
    let Some(token) = bearer_token(headers) else {
        return false;
    };
    federation_slave_tokens().get(slave_id) == Some(&token)
}

fn bearer_token(headers: &BTreeMap<String, String>) -> Option<String> {
    headers
        .get("authorization")
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(ToString::to_string)
}

fn federation_admin_token() -> Option<String> {
    env::var("YOLO_FEDERATION_ADMIN_TOKEN")
        .ok()
        .or_else(|| env::var("YOLO_MASTER_TOKEN").ok())
}

fn federation_slave_tokens() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Ok(value) = env::var("YOLO_FEDERATION_SLAVE_TOKENS") {
        for item in value.split(',') {
            if let Some((id, token)) = item.split_once(':')
                && !id.trim().is_empty()
                && !token.trim().is_empty()
            {
                out.insert(id.trim().to_string(), token.trim().to_string());
            }
        }
    }
    out
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
        app_server_socket: dir.join(APP_SERVER_SOCKET_NAME),
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
  yolo upgrade-resume [--last|SESSION_ID|RESUME_ARGS...]
  yolo upgrade-resume-all
  yolo server [--daemon|--foreground] [--federation-listen ADDR]
  yolo status
  yolo stop

Default client command:
  codex --remote unix://$YOLO_RUNTIME_DIR/codex-app-server.sock --search --dangerously-bypass-approvals-and-sandbox [CODEX_ARGS...]

The client keeps Codex stdio attached to the terminal and reports its process,
model, service_tier, fast state, and app-server thread status to the yolo
server API.

upgrade-resume installs the latest Codex CLI into a yolo-managed
user-writable npm prefix, restarts the yolo app-server, then launches
`codex resume` through yolo. With no arguments it resumes `--last`.

upgrade-resume-all asks the running yolo server to install the latest Codex
CLI, wait for active app-server threads to become idle, restart its app-server
child, and request every live yolo client wrapper to restart its Codex child as
`codex resume` on the same terminal.

API:
  curl --unix-socket $XDG_RUNTIME_DIR/yolo/api.sock http://yolo/clients
  curl -X POST --unix-socket $XDG_RUNTIME_DIR/yolo/api.sock http://yolo/upgrade-resume-all

Federation:
  YOLO_FEDERATION_ADMIN_TOKEN=... YOLO_FEDERATION_SLAVE_TOKENS=slave:token \
    yolo server --daemon --federation-listen 127.0.0.1:47040
  YOLO_MASTER_URL=https://agent-gate/.../@localhost:47040 \
    YOLO_SLAVE_ID=slave YOLO_SLAVE_TOKEN=token yolo server --daemon

Environment:
  YOLO_CODEX        Codex executable to run (default: codex)
  YOLO_CODEX_UPGRADE_COMMAND
                    Override upgrade command
  YOLO_CODEX_PREFIX Managed Codex npm prefix
  YOLO_REMOTE       Override app-server endpoint for the client
  YOLO_RUNTIME_DIR  Runtime dir for sockets (default: $XDG_RUNTIME_DIR/yolo or /tmp/yolo)
  YOLO_UPGRADE_IDLE_WAIT_TIMEOUT_SECS
                    Max seconds to wait for working clients before upgrade
  YOLO_FEDERATION_ADMIN_TOKEN
                    Bearer token for master admin API
  YOLO_FEDERATION_SLAVE_TOKENS
                    Comma-separated slave-id:token map
  YOLO_FEDERATION_LISTEN
                    Default master federation listen address
  YOLO_MASTER_URL, YOLO_SLAVE_ID, YOLO_SLAVE_TOKEN
                    Slave connector settings
  YOLO_SELF_UPGRADE_COMMAND
                    Override remote yolo-upgrade command
"
    );
}
