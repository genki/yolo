#!/usr/bin/env python3
"""Exercise persisted handoffs through real YOLO Unix-socket APIs, without OpenAI."""
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

repo = Path(__file__).resolve().parents[1]
binary = Path(sys.argv[1]).resolve()


class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, endpoint):
        super().__init__('yolo', timeout=10)
        self.endpoint = endpoint

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.endpoint))


with tempfile.TemporaryDirectory(prefix='yolo-bg-restart-') as temporary:
    root = Path(temporary)
    runtime = root / 'runtime'
    state = root / 'state'
    home = root / 'home'
    for directory in (runtime, state, home):
        directory.mkdir()
    endpoint = runtime / 'api.sock'
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(('YOLO_', 'CODEX_', 'FAKE_CODEX_'))}
    environment.update({
        'YOLO_RUNTIME_DIR': str(runtime), 'YOLO_API_SOCKET': str(endpoint),
        'YOLO_APP_SERVER_SOCKET': str(runtime / 'app.sock'),
        'YOLO_STATE_DIR': str(state), 'CODEX_HOME': str(home),
        'YOLO_CODEX': str(repo / 'tests/fake_codex.py'),
        'YOLO_SERVER_ROLE': 'standby', 'YOLO_SERVER_SLOT': 'test',
        'YOLO_ACTIVE_GENERATION_FILE': str(root / 'active.json'),
    })
    process = None

    def api(method, route, body=None, expected=200):
        connection = UnixHTTP(endpoint)
        try:
            connection.request(method, route, None if body is None else json.dumps(body),
                               {'Content-Type': 'application/json'})
            response = connection.getresponse()
            result = json.loads(response.read())
            assert response.status == expected, (route, response.status, result)
            return result
        finally:
            connection.close()

    def start():
        global process
        with (root / 'server.log').open('ab') as log:
            process = subprocess.Popen([str(binary), 'server', '--foreground'], env=environment,
                                       stdout=log, stderr=log, start_new_session=True, cwd=root)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert process.poll() is None, 'test server exited'
            try:
                return api('GET', '/status')
            except (OSError, ValueError):
                time.sleep(0.1)
        raise AssertionError('test server did not start: ' + (root / 'server.log').read_text())

    def stop():
        global process
        if process is None:
            return
        try:
            if process.poll() is None:
                try:
                    api('POST', '/shutdown', {})
                except (OSError, ValueError):
                    pass
            process.wait(timeout=10)
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=10)
            process = None

    identity = 'yolo-bg-restart-test'
    thread_id = '019e0000-0000-7000-8000-000000000000'

    def register(client_id, status):
        now = int(time.time())
        return api('POST', '/clients/register', {
            'id': client_id, 'yolo_id': identity, 'codex_state_handoff_version': 2,
            'yolo_pid': os.getpid(), 'codex_pid': None, 'cwd': str(root),
            'args': ['resume', thread_id], 'remote': 'unix://' + str(runtime / 'client.sock'),
            'fast': False, 'thread_id': thread_id, 'status': 'running',
            'thread_id_source': 'resume_arg', 'thread_binding_state': 'bound',
            'started_at': now, 'updated_at': now, 'codex_status': status,
            'codex_status_updated_at': now, 'codex_active_flags': [],
        })

    request = {
        'yolo_ids': [identity], 'expected_threads': {identity: thread_id},
        'target_runtime_dir': str(root / 'green-runtime'),
        'target_api_socket': str(root / 'green-runtime/api.sock'),
        'target_app_server_socket': str(root / 'green-runtime/app.sock'),
        'target_codex_home': str(root / 'green-home'),
        'target_server_instance_id': 'test-green-1',
    }
    claim = {'yolo_id': identity, 'thread_id': thread_id, 'codex_state_handoff_version': 2}
    try:
        first = start()
        register('before-restart', 'active')
        assert api('POST', '/blue-green/handoff', request, expected=202)['count'] == 1
        assert api('POST', '/blue-green/handoff-claim', claim)['ready'] is False
        stop()
        second = start()
        assert second['server_instance_id'] != first['server_instance_id']
        register('after-restart', 'idle')
        resumed_claim = api('POST', '/blue-green/handoff-claim', claim)
        assert resumed_claim['ready'] is True, resumed_claim
        assert api('POST', '/blue-green/handoff', request, expected=202)['count'] == 1
        changed = dict(request, target_server_instance_id='conflicting-green')
        api('POST', '/blue-green/handoff', changed, expected=400)
        assert api('POST', '/blue-green/handoff-complete', claim)['source_client_id'] == 'after-restart'
        assert next(client for client in api('GET', '/status')['clients']
                    if client['id'] == 'after-restart')['status'] == 'handed-off'
        stop()
        start()
        register('third-process', 'idle')
        assert api('POST', '/blue-green/handoff-claim', claim)['ready'] is False
        print('BG runtime: active blocked, queue restored, idempotent retry, conflict rejected, completion persisted')
    finally:
        stop()
