use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{Read, Write};
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
    fast: bool,
    thread_id: Option<String>,
    started_at: u64,
    updated_at: u64,
    ended_at: Option<u64>,
    exit_code: Option<i32>,
    status: String,
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
}

#[derive(Debug)]
struct ServerState {
    started_at: u64,
    app_server_pid: Option<u32>,
    resume_generation: u64,
    clients: BTreeMap<String, ClientInfo>,
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
    remove_socket_if_present(&paths.app_server_socket)?;
    fs::write(&paths.pid_file, std::process::id().to_string())
        .map_err(|err| format!("write pid file: {err}"))?;

    let state = Arc::new(Mutex::new(ServerState {
        started_at: now_secs(),
        app_server_pid: None,
        resume_generation: 0,
        clients: BTreeMap::new(),
    }));
    let app_server_pid = spawn_tracked_app_server(Arc::clone(&state), paths.clone())?;

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
    let mut info = ClientInfo {
        id: client_id.clone(),
        yolo_pid: std::process::id(),
        codex_pid: None,
        cwd,
        args: string_args,
        remote: remote.clone(),
        model: initial_config.model,
        service_tier: initial_config.service_tier.clone(),
        fast: is_fast_tier(initial_config.service_tier.as_deref()),
        thread_id: None,
        started_at: now_secs(),
        updated_at: now_secs(),
        ended_at: None,
        exit_code: None,
        status: "running".to_string(),
    };

    let heartbeat_id = client_id.clone();
    let restart_requested = Arc::new(AtomicBool::new(false));
    let heartbeat_restart_requested = Arc::clone(&restart_requested);
    let seen_resume_generation = Arc::new(AtomicU64::new(current_resume_generation()));
    let heartbeat_seen_generation = Arc::clone(&seen_resume_generation);
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(2));
            let cfg = read_codex_config();
            let body = json!({
                "id": heartbeat_id,
                "model": cfg.model,
                "service_tier": cfg.service_tier,
                "fast": is_fast_tier(cfg.service_tier.as_deref()),
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

fn upgrade_codex_cli() -> Result<(), String> {
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
    fs::create_dir_all(&prefix)
        .map_err(|err| format!("create managed Codex prefix {}: {err}", prefix.display()))?;
    eprintln!(
        "yolo: upgrading Codex CLI into user-writable prefix {}",
        prefix.display()
    );
    let status = Command::new("npm")
        .arg("install")
        .arg("--global")
        .arg("--prefix")
        .arg(&prefix)
        .arg(CODEX_PACKAGE)
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
    if matches!(args.first().and_then(|arg| arg.to_str()), Some("resume")) {
        return args.to_vec();
    }
    vec![OsString::from("resume"), OsString::from("--last")]
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
                            client.model = value
                                .get("model")
                                .and_then(Value::as_str)
                                .map(ToString::to_string);
                            client.service_tier = value
                                .get("service_tier")
                                .and_then(Value::as_str)
                                .map(ToString::to_string);
                            client.fast = value
                                .get("fast")
                                .and_then(Value::as_bool)
                                .unwrap_or_else(|| is_fast_tier(client.service_tier.as_deref()));
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
        ("POST", "/upgrade-resume-all") => {
            let result = upgrade_codex_cli()
                .and_then(|_| restart_tracked_app_server(Arc::clone(&state), paths.clone()))
                .and_then(|app_server_pid| {
                    if let Ok(mut state) = state.lock() {
                        state.resume_generation = state.resume_generation.saturating_add(1);
                        let generation = state.resume_generation;
                        return Ok((app_server_pid, generation, state.clients.len()));
                    }
                    Err("server state lock poisoned".to_string())
                });
            match result {
                Ok((app_server_pid, resume_generation, clients)) => json_response(
                    200,
                    &json!({
                        "ok": true,
                        "app_server_pid": app_server_pid,
                        "resume_generation": resume_generation,
                        "clients": clients
                    }),
                ),
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
    }
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
  yolo server [--daemon|--foreground]
  yolo status
  yolo stop

Default client command:
  codex --remote unix://$YOLO_RUNTIME_DIR/codex-app-server.sock --search --dangerously-bypass-approvals-and-sandbox [CODEX_ARGS...]

The client keeps Codex stdio attached to the terminal and reports its process,
model, service_tier, and fast state to the yolo server API.

upgrade-resume installs the latest Codex CLI into a yolo-managed
user-writable npm prefix, restarts the yolo app-server, then launches
`codex resume` through yolo. With no arguments it resumes `--last`.

upgrade-resume-all asks the running yolo server to install the latest Codex
CLI, restart its app-server child, and request every live yolo client wrapper
to restart its Codex child as `codex resume` on the same terminal.

API:
  curl --unix-socket $XDG_RUNTIME_DIR/yolo/api.sock http://yolo/clients
  curl -X POST --unix-socket $XDG_RUNTIME_DIR/yolo/api.sock http://yolo/upgrade-resume-all

Environment:
  YOLO_CODEX        Codex executable to run (default: codex)
  YOLO_CODEX_UPGRADE_COMMAND
                    Override upgrade command
  YOLO_CODEX_PREFIX Managed Codex npm prefix
  YOLO_REMOTE       Override app-server endpoint for the client
  YOLO_RUNTIME_DIR  Runtime dir for sockets (default: $XDG_RUNTIME_DIR/yolo or /tmp/yolo)
"
    );
}
