use std::env;
use std::ffi::OsString;
use std::process::Command;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_CODEX: &str = "codex";
const DEFAULT_REMOTE: &str = "unix://";
const SERVICE_NAME: &str = "codex-app-server.service";

fn main() {
    let mut user_args = env::args_os().skip(1).collect::<Vec<_>>();

    if user_args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_help();
        return;
    }
    if user_args
        .iter()
        .any(|arg| arg == "--version" || arg == "-V")
    {
        println!("yolo {VERSION}");
        return;
    }

    start_app_server_service_if_enabled();

    let codex = env::var_os("YOLO_CODEX").unwrap_or_else(|| OsString::from(DEFAULT_CODEX));
    let remote = env::var_os("YOLO_REMOTE").unwrap_or_else(|| OsString::from(DEFAULT_REMOTE));

    let mut command = Command::new(codex);
    command
        .arg("--remote")
        .arg(remote)
        .arg("--search")
        .arg("--dangerously-bypass-approvals-and-sandbox");
    command.args(user_args.drain(..));

    exec_or_exit(command);
}

fn start_app_server_service_if_enabled() {
    if env::var_os("YOLO_NO_SERVICE_START").is_some() {
        return;
    }

    let _ = Command::new("systemctl")
        .args(["--user", "start", SERVICE_NAME])
        .status();
}

#[cfg(unix)]
fn exec_or_exit(mut command: Command) -> ! {
    let err = command.exec();
    eprintln!("yolo: failed to exec codex: {err}");
    std::process::exit(127);
}

#[cfg(not(unix))]
fn exec_or_exit(mut command: Command) -> ! {
    match command.status() {
        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
        Err(err) => {
            eprintln!("yolo: failed to run codex: {err}");
            std::process::exit(127);
        }
    }
}

fn print_help() {
    println!(
        "\
yolo {VERSION}

Launch Codex through the local app-server in YOLO mode with web search enabled.

Usage:
  yolo [CODEX_ARGS...]

Runs:
  codex --remote unix:// --search --dangerously-bypass-approvals-and-sandbox [CODEX_ARGS...]

Environment:
  YOLO_CODEX             Codex executable to run (default: codex)
  YOLO_REMOTE            app-server endpoint (default: unix://)
  YOLO_NO_SERVICE_START  Do not run `systemctl --user start codex-app-server.service`

Examples:
  yolo --cd /home/vagrant/websh
  yolo resume --last
"
    );
}
