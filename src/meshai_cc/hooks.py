"""Hook entrypoint: fsync to WAL, nudge the daemon, get out of the way.

Invoked by Claude Code as ``meshai-cc-hook <EventName>`` with the hook JSON
on stdin. The budget is <50ms p99 (D9 eng): one durable append and one
non-blocking socket poke; no network, no regex, no imports of the OTel
stack.

Failure posture (T7.5): default is fail-OPEN (exit 0, telemetry lost,
Claude Code unbothered). With ``fail_closed: true`` in policy.yaml a WAL
append failure exits 2, which blocks Claude Code; that is the point of
compliance mode: no evidence, no action.
"""

import json
import socket
import subprocess
import sys
from pathlib import Path

from meshai_cc import wal
from meshai_cc.config import load_policy
from meshai_cc.events import HOOK_EVENTS, make_event
from meshai_cc.lock import PidLock
from meshai_cc.paths import ensure_dirs, pid_path, socket_path, wal_dir


def nudge_daemon(root: Path | None = None) -> None:
    """Best-effort wake-up; the daemon's 1s poll is the backstop."""
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(0.05)
        s.connect(str(socket_path(root)))
        s.sendall(b"n")
        s.close()
    except OSError:
        pass


def maybe_start_daemon(root: Path | None = None) -> None:
    """SessionStart convenience: spawn the daemon if none holds the lock."""
    probe = PidLock(pid_path(root))
    try:
        probe.acquire()
    except Exception:
        return  # running (or unprobeable); either way, not our job
    probe.release()
    try:
        subprocess.Popen(  # noqa: S603; our own console script
            [sys.executable, "-m", "meshai_cc.daemon"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError:
        pass  # fail-open: WAL keeps accumulating for a later daemon


def run_hook(hook_event: str, stdin_text: str, root: Path | None = None) -> int:
    policy = load_policy(root)
    try:
        payload = json.loads(stdin_text) if stdin_text.strip() else {}
        if not isinstance(payload, dict):
            payload = {}
        ensure_dirs(root)
        event = make_event(hook_event, payload)
        wal.append_event(wal_dir(root), event["session_id"], event)
    except Exception as exc:  # noqa: BLE001
        if policy.fail_closed:
            print(
                f"meshai-cc: WAL append failed and compliance mode is on: {exc}",
                file=sys.stderr,
            )
            return 2  # blocks Claude Code (no evidence, no action)
        print(f"meshai-cc: telemetry dropped: {exc}", file=sys.stderr)
        return 0
    if hook_event == "SessionStart" and policy.auto_start_daemon:
        maybe_start_daemon(root)
    nudge_daemon(root)
    return 0


def main() -> None:  # pragma: no cover; thin argv/stdin shim over run_hook
    if len(sys.argv) != 2 or sys.argv[1] not in HOOK_EVENTS:
        print(f"usage: meshai-cc-hook {{{'|'.join(HOOK_EVENTS)}}}", file=sys.stderr)
        raise SystemExit(0)  # never break Claude Code over a wiring mistake
    raise SystemExit(run_hook(sys.argv[1], sys.stdin.read()))
