"""The WAL-tailing publisher daemon (T3) with self-telemetry (T8).

Pure reader of the WAL: hooks own writes; this process converts and ships.
Startup order matters: filesystem safety check → PID lock → exporter.
The loop wakes on a unix-socket nudge or the 1s polling backstop, exports
new events per segment in BOUNDED chunks, and advances the committed offset
ONLY after a successful OTLP export, but after EVERY successful chunk, so
a failure late in a drain never un-commits what already landed. A crash
anywhere replays, and the server dedups. An unbounded batch was the
2026-07-29 wedge: one payload over the server's body cap is rejected
forever, and the retry only grows.

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

# Chunk bounds for one export. The ingest endpoint rejects bodies over
# 10 MiB with HTTP 413, and a rejected batch is retried unchanged forever,
# so an unbounded batch is a permanent wedge (the 2026-07-29 incident).
# Bound by RAW WAL BYTES because that is the only size we can measure
# without reaching into OTel's encoders; one event can fan out to several
# spans (a Stop event also yields one usage span per assistant turn), so
# 1 MiB of WAL leaves ~10x headroom under the server cap.
EXPORT_MAX_BYTES = 1 * 1024 * 1024
EXPORT_MAX_EVENTS = 500

# A batch already backed off to ONE event that still fails this many times,
# over at least this much wall time, is treated as poison and skipped.
# Both gates must pass: the count alone is not a clock (hook nudges can
# spin the loop far faster than POLL_SECONDS), and the clock alone would
# fire on a slow loop. 10 minutes is ~5x the worst transient outage this
# stack has actually shown (App Runner roll, RDS failover, the 2026-06-12
# idle-socket drop), so an outage recovers long before anything is dropped.
POISON_MIN_FAILURES = 120
POISON_MIN_STALL_SECONDS = 600.0


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
            "consecutive_export_failures": 0,
            "skipped_events": 0,
            "corrupt_lines": 0,
            "last_flush_at": None,
        }
        # Single-slot stall tracker: (segment name, offset) currently stuck,
        # how many exports have failed there, and when it first failed.
        self._stall_key: tuple[str, int] | None = None
        self._stall_failures = 0
        self._stall_since = 0.0

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

    def run_forever(self) -> None:  # pragma: no cover; loop shell; parts unit-tested
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

    def _wait_for_nudge(self) -> None:  # pragma: no cover; timing shell
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
            drained, healthy = self._drain_segment(segment)
            exported += drained
            if not healthy:
                break  # endpoint down; retry the rest next tick
        self.write_status()
        return exported

    def _drain_segment(self, segment: Path) -> tuple[int, bool]:
        """Export a segment in bounded chunks, committing after EACH chunk.

        Returns (spans exported, endpoint looks healthy). Committing per
        chunk is what stops a failure late in the drain from replaying (and
        re-growing) the chunks that already landed.
        """
        exported = 0
        while True:
            offset = self._offsets.get(segment.name, 0)
            key = (segment.name, offset)
            # Halve the batch for every consecutive failure at this offset.
            # Span fan-out per event is not knowable up front (each Stop
            # event re-emits a usage span per transcript turn), so a fixed
            # event count cannot guarantee an acceptable body: back off to
            # find one in ~log2 ticks, down to a single event, which also
            # isolates a poison event. Any success resets to the full chunk.
            failures = self._stall_failures if self._stall_key == key else 0
            try:
                result = wal.read_segment(
                    segment,
                    offset,
                    max_events=max(1, EXPORT_MAX_EVENTS >> min(failures, 32)),
                    max_bytes=EXPORT_MAX_BYTES,
                )
            except OSError:
                return exported, True  # GC'd or transient; next pass
            self._stats["corrupt_lines"] += result.corrupt_lines
            if not result.events and result.new_offset == offset:
                return exported, True  # nothing consumable (empty or torn tail)
            spans = self._spans_for(result.events)
            if self._publisher.export(spans):
                self._commit(segment.name, result.new_offset)
                exported += len(spans)
                self._stats["exported_spans"] += len(spans)
                self._stats["last_flush_at"] = time.time()
                self._clear_stall()
                continue
            self._stats["export_failures"] += 1
            if self._note_stall(key) and len(result.events) == 1:
                logger.error(
                    "meshai-cc: dropping 1 poison WAL event after %d failed "
                    "exports over %ds (%s @ %d, %d spans); it will never be "
                    "accepted and was blocking every later event",
                    self._stall_failures,
                    int(time.time() - self._stall_since),
                    segment.name,
                    offset,
                    len(spans),
                )
                self._stats["skipped_events"] += 1
                self._commit(segment.name, result.new_offset)
                self._clear_stall()
                continue
            return exported, False

    def _spans_for(self, events: list[dict]) -> list:
        spans: list = []
        for event in events:
            try:
                spans.extend(self._publisher.spans_for_event(event))
            except Exception:  # noqa: BLE001; one bad event can't stall the WAL
                logger.warning("meshai-cc: unconvertible event", exc_info=True)
        return spans

    def _commit(self, segment_name: str, new_offset: int) -> None:
        self._offsets[segment_name] = new_offset
        wal.save_offsets(offsets_path(self._root), self._offsets)

    def _note_stall(self, key: tuple[str, int]) -> bool:
        """Record a failure at ``key``; True once both poison gates pass."""
        if self._stall_key != key:
            self._stall_key = key
            self._stall_failures = 0
            self._stall_since = time.time()
        self._stall_failures += 1
        self._stats["consecutive_export_failures"] = self._stall_failures
        return (
            self._stall_failures >= POISON_MIN_FAILURES
            and (time.time() - self._stall_since) >= POISON_MIN_STALL_SECONDS
        )

    def _clear_stall(self) -> None:
        """Any success clears the poison counter: an outage never drops."""
        self._stall_key = None
        self._stall_failures = 0
        self._stall_since = 0.0
        self._stats["consecutive_export_failures"] = 0

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


def main() -> None:  # pragma: no cover; process entrypoint
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
