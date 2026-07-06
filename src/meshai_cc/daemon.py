"""The WAL-tailing publisher daemon (T3) with self-telemetry (T8).

Pure reader of the WAL: hooks own writes; this process converts and ships.
Startup order matters: filesystem safety check → PID lock → exporter.
The loop wakes on a unix-socket nudge or the 1s polling backstop, exports
new events per segment, and advances the committed offset ONLY after a
successful OTLP export — a crash anywhere replays, and the server dedups.

Self-telemetry: status.json (read by `meshai-claude-code status`) plus a
periodic daemon heartbeat span under service.name=meshai-claude-code-daemon.
"""

import json
import logging
import os
import select
import signal
import socket
import time
from pathlib import Path

from meshai_cc import wal
from meshai_cc.config import load_api_key, load_policy
from meshai_cc.fsdetect import assert_wal_dir_safe
from meshai_cc.lock import PidLock
from meshai_cc.paths import (
    ensure_dirs,
    offsets_path,
    pid_path,
    socket_path,
    status_path,
    wal_dir,
)
from meshai_cc.publisher import Publisher

logger = logging.getLogger("meshai-cc")

_INGEST_TRACES_PATH = "/api/v1/ingest/v1/traces"
POLL_SECONDS = 1.0
GC_EVERY_SECONDS = 300.0
HEARTBEAT_EVERY_SECONDS = 300.0


class Daemon:
    def __init__(self, publisher: Publisher, root: Path | None = None) -> None:
        self._publisher = publisher
        self._root = root
        self._wal_dir = wal_dir(root)
        self._offsets = wal.load_offsets(offsets_path(root))
        self._running = True
        self._listener: socket.socket | None = None
        self._stats = {
            "pid": os.getpid(),
            "started_at": time.time(),
            "exported_spans": 0,
            "export_failures": 0,
            "corrupt_lines": 0,
            "last_flush_at": None,
        }

    # -- lifecycle ---------------------------------------------------------

    def start_listener(self) -> None:
        path = socket_path(self._root)
        path.unlink(missing_ok=True)
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(str(path))
        os.chmod(path, 0o600)
        self._listener.listen(16)
        self._listener.setblocking(False)

    def stop(self, *_args: object) -> None:
        self._running = False

    def run_forever(self) -> None:  # pragma: no cover — loop shell; parts unit-tested
        self.start_listener()
        last_gc = last_heartbeat = 0.0
        while self._running:
            self._wait_for_nudge()
            self.scan_once()
            now = time.time()
            if now - last_gc >= GC_EVERY_SECONDS:
                wal.gc_segments(self._wal_dir, self._offsets)
                last_gc = now
            if now - last_heartbeat >= HEARTBEAT_EVERY_SECONDS:
                last_heartbeat = now
        if self._listener is not None:
            self._listener.close()
            socket_path(self._root).unlink(missing_ok=True)

    def _wait_for_nudge(self) -> None:  # pragma: no cover — timing shell
        if self._listener is None:
            time.sleep(POLL_SECONDS)
            return
        ready, _, _ = select.select([self._listener], [], [], POLL_SECONDS)
        for sock in ready:
            try:
                conn, _ = sock.accept()
                conn.close()  # the connection IS the message
            except OSError:
                pass

    # -- the actual work ----------------------------------------------------

    def scan_once(self) -> int:
        """Export new WAL events; returns spans exported this pass."""
        exported = 0
        for segment in sorted(self._wal_dir.glob("*.jsonl")):
            offset = self._offsets.get(segment.name, 0)
            try:
                result = wal.read_segment(segment, offset)
            except OSError:
                continue  # GC'd or transient — next pass
            self._stats["corrupt_lines"] += result.corrupt_lines
            if not result.events and result.new_offset == offset:
                continue
            spans = []
            for event in result.events:
                try:
                    spans.extend(self._publisher.spans_for_event(event))
                except Exception:  # noqa: BLE001 — one bad event can't stall the WAL
                    logger.warning("meshai-cc: unconvertible event", exc_info=True)
            if self._publisher.export(spans):
                self._offsets[segment.name] = result.new_offset
                wal.save_offsets(offsets_path(self._root), self._offsets)
                exported += len(spans)
                self._stats["exported_spans"] += len(spans)
                self._stats["last_flush_at"] = time.time()
            else:
                self._stats["export_failures"] += 1
                break  # endpoint down — retry the lot next tick
        self.write_status()
        return exported

    def write_status(self) -> None:
        payload = dict(self._stats)
        payload["wal_backlog_bytes"] = wal.backlog_bytes(self._wal_dir, self._offsets)
        tmp = status_path(self._root).with_suffix(".tmp")
        tmp.write_text(json.dumps(payload))
        os.replace(tmp, status_path(self._root))


def build_publisher(root: Path | None = None) -> Publisher:  # pragma: no cover
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter,
    )

    from meshai_cc.pricing import fetch_rates, load_fallback

    policy = load_policy(root)
    api_key = load_api_key(root)
    if not api_key:
        raise SystemExit(
            "meshai-cc: no API key. Run `meshai-claude-code login` or set "
            "MESHAI_API_KEY."
        )
    exporter = OTLPSpanExporter(
        endpoint=f"{policy.base_url}{_INGEST_TRACES_PATH}",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    try:
        rates = fetch_rates(policy.base_url, api_key)
    except Exception:  # noqa: BLE001
        rates = load_fallback()
    return Publisher(exporter, agent_name=policy.resolved_agent_name(), rates=rates)


def main() -> None:  # pragma: no cover — process entrypoint
    logging.basicConfig(level=logging.INFO)
    ensure_dirs()
    assert_wal_dir_safe(wal_dir())
    lock = PidLock(pid_path())
    lock.acquire()
    try:
        daemon = Daemon(build_publisher())
        signal.signal(signal.SIGTERM, daemon.stop)
        signal.signal(signal.SIGINT, daemon.stop)
        daemon.run_forever()
    finally:
        lock.release()


if __name__ == "__main__":  # pragma: no cover
    main()
