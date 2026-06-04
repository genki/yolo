#!/usr/bin/env python3
import base64
import hashlib
import json
import os
import signal
import socket
import struct
import sys
import time
from pathlib import Path


clients = []
listener = None
stopping = False


def version():
    path = os.environ.get("FAKE_CODEX_VERSION_FILE")
    if path and Path(path).exists():
        return Path(path).read_text().strip()
    return os.environ.get("FAKE_CODEX_VERSION", "0.135.0")


def log(kind, args):
    path = os.environ.get("FAKE_CODEX_RUN_LOG")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps({
            "at": time.time(),
            "kind": kind,
            "version": version(),
            "args": args,
            "pid": os.getpid(),
        }) + "\n")


def read_headers(conn):
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(1)
        if not chunk:
            raise EOFError("closed before headers")
        data += chunk
    return data.decode("utf-8", "replace")


def send_handshake(conn, headers):
    key = ""
    for line in headers.splitlines():
        if line.lower().startswith("sec-websocket-key:"):
            key = line.split(":", 1)[1].strip()
            break
    accept = base64.b64encode(hashlib.sha1(
        (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()
    ).digest()).decode()
    conn.sendall(
        b"HTTP/1.1 101 Switching Protocols\r\n"
        b"Upgrade: websocket\r\n"
        b"Connection: Upgrade\r\n"
        + f"Sec-WebSocket-Accept: {accept}\r\n\r\n".encode()
    )


def read_frame(conn):
    head = conn.recv(2)
    if len(head) < 2:
        raise EOFError("closed")
    first, second = head
    opcode = first & 0x0F
    masked = second & 0x80
    length = second & 0x7F
    if length == 126:
        length = struct.unpack("!H", conn.recv(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", conn.recv(8))[0]
    mask = conn.recv(4) if masked else b""
    payload = conn.recv(length)
    if masked:
        payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    return opcode, payload


def send_text(conn, value):
    payload = json.dumps(value).encode()
    if len(payload) < 126:
        header = bytes([0x81, len(payload)])
    elif len(payload) < 65536:
        header = bytes([0x81, 126]) + struct.pack("!H", len(payload))
    else:
        header = bytes([0x81, 127]) + struct.pack("!Q", len(payload))
    conn.sendall(header + payload)


def thread_payload():
    return {
        "id": os.environ.get("FAKE_CODEX_THREAD_ID", "019e0000-0000-7000-8000-000000000000"),
        "cwd": os.environ.get("FAKE_CODEX_CWD", os.getcwd()),
        "status": {"type": os.environ.get("FAKE_CODEX_THREAD_STATUS", "idle"), "activeFlags": []},
    }


def handle_rpc(conn):
    clients.append(conn)
    try:
        headers = read_headers(conn)
        send_handshake(conn, headers)
        while not stopping:
            opcode, payload = read_frame(conn)
            if opcode == 0x8:
                break
            if opcode != 0x1:
                continue
            msg = json.loads(payload.decode())
            req_id = msg.get("id")
            method = msg.get("method")
            if req_id is None:
                continue
            if method == "initialize":
                send_text(conn, {"id": req_id, "result": {}})
            elif method == "thread/loaded/list":
                send_text(conn, {"id": req_id, "result": {"data": [thread_payload()["id"]]}})
            elif method in ("thread/resume", "thread/read"):
                send_text(conn, {
                    "id": req_id,
                    "result": {
                        "thread": thread_payload(),
                        "model": "gpt-5.5",
                        "serviceTier": "default",
                        "reasoningEffort": "medium",
                    },
                })
            else:
                send_text(conn, {"id": req_id, "result": {}})
    except Exception:
        pass
    finally:
        try:
            clients.remove(conn)
        except ValueError:
            pass
        try:
            conn.close()
        except OSError:
            pass


def stop(_signum=None, _frame=None):
    global stopping
    stopping = True
    for conn in list(clients):
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            conn.close()
        except OSError:
            pass
    if listener:
        try:
            listener.close()
        except OSError:
            pass


def run_app_server(args):
    global listener
    listen = args[args.index("--listen") + 1]
    path = listen.removeprefix("unix://")
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(path)
    listener.listen()
    log("app-server", args)
    while not stopping:
        try:
            conn, _ = listener.accept()
        except OSError:
            break
        pid = os.fork()
        if pid == 0:
            try:
                listener.close()
            except OSError:
                pass
            handle_rpc(conn)
            os._exit(0)
        conn.close()
    stop()


def run_client(args):
    remote = args[args.index("--remote") + 1]
    path = remote.removeprefix("unix://")
    log("client", args)
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    deadline = time.time() + 10
    while True:
        try:
            conn.connect(path)
            break
        except OSError:
            if time.time() > deadline:
                raise
            time.sleep(0.1)
    conn.sendall(
        b"GET / HTTP/1.1\r\n"
        b"Host: fake-codex\r\n"
        b"Upgrade: websocket\r\n"
        b"Connection: Upgrade\r\n"
        b"Sec-WebSocket-Key: ZmFrZS1jb2RleC1jbGllbnQ=\r\n"
        b"Sec-WebSocket-Version: 13\r\n\r\n"
    )
    read_headers(conn)
    try:
        while conn.recv(1024):
            pass
    except OSError:
        pass
    return 23


def main():
    args = sys.argv[1:]
    if args[:1] == ["--version"]:
        print(f"codex-cli {version()}")
        return 0
    if args[:1] == ["app-server"]:
        run_app_server(args)
        return 0
    return run_client(args)


if __name__ == "__main__":
    raise SystemExit(main())
