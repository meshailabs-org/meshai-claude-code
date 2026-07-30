"""Publisher + daemon tests: conversion, filtering, at-least-once offsets."""

import json

import pytest
from meshai.tracer.filters import FilterConfig
from opentelemetry.sdk.trace.export import SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from meshai_cc import daemon as daemon_mod
from meshai_cc import wal
from meshai_cc.daemon import Daemon
from meshai_cc.events import make_event, root_span_id_for, usage_span_id_for
from meshai_cc.hooks import run_hook
from meshai_cc.paths import offsets_path, status_path, wal_dir
from meshai_cc.publisher import Publisher

DENY_ALL = FilterConfig()


def _publisher(exporter=None, filters=DENY_ALL, rates=None):
    return Publisher(
        exporter or InMemorySpanExporter(),
        agent_name="cc-test-agent",
        filters=filters,
        rates=rates,
    )


class FlakyExporter(InMemorySpanExporter):
    """Fails the first N export calls, then succeeds."""

    def __init__(self, failures: int) -> None:
        super().__init__()
        self._failures = failures

    def export(self, spans):
        if self._failures > 0:
            self._failures -= 1
            return SpanExportResult.FAILURE
        return super().export(spans)


class BatchRecorder(InMemorySpanExporter):
    """Records the size of every batch handed to the exporter."""

    def __init__(self) -> None:
        super().__init__()
        self.batches: list[int] = []

    def export(self, spans):
        self.batches.append(len(spans))
        return super().export(spans)


class CapExporter(InMemorySpanExporter):
    """Rejects any batch over `cap` spans: the server's 10 MB 413, in kind."""

    def __init__(self, cap: int) -> None:
        super().__init__()
        self._cap = cap
        self.rejections = 0

    def export(self, spans):
        if len(spans) > self._cap:
            self.rejections += 1
            return SpanExportResult.FAILURE
        return super().export(spans)


class FailAfterExporter(InMemorySpanExporter):
    """Accepts the first N batches, then fails everything (endpoint dies)."""

    def __init__(self, ok_batches: int) -> None:
        super().__init__()
        self._ok = ok_batches

    def export(self, spans):
        if self._ok <= 0:
            return SpanExportResult.FAILURE
        self._ok -= 1
        return super().export(spans)


class PoisonExporter(InMemorySpanExporter):
    """Rejects any batch containing one specific span, however small the
    batch: a single event whose spans can never fit under the server cap."""

    def __init__(self, poison: str) -> None:
        super().__init__()
        self._poison = poison

    def export(self, spans):
        if any(s.name.endswith(self._poison) for s in spans):
            return SpanExportResult.FAILURE
        return super().export(spans)


# --- Publisher conversion ---------------------------------------------------


def test_event_span_carries_ids_resource_and_structural_attrs():
    event = make_event("PreToolUse", {
        "session_id": "s1", "tool_name": "Bash",
        "tool_input": {"command": "ls"}, "cwd": "/repo",
    })
    (span,) = _publisher().spans_for_event(event)
    assert span.name == "tool.pre Bash"
    assert span.resource.attributes["service.name"] == "cc-test-agent"
    assert span.resource.attributes["meshai.agent.framework"] == "claude-code"
    assert span.attributes["meshai.session.id"] == "s1"
    assert span.attributes["gen_ai.tool.name"] == "Bash"
    assert format(span.context.span_id, "016x") == event["span_id"]
    assert format(span.parent.span_id, "016x") == root_span_id_for("s1")
    # deny-all: content dropped, structure kept
    assert "meshai.tool.input" not in span.attributes


def test_allowlisted_content_is_filtered_through_sdk_pipeline():
    filters = FilterConfig(allow={"Bash": frozenset({"tool_input"})})
    event = make_event("PreToolUse", {
        "session_id": "s1", "tool_name": "Bash",
        "tool_input": f"export KEY=sk-ant-{'a' * 24}",
    })
    (span,) = _publisher(filters=filters).spans_for_event(event)
    emitted = span.attributes["meshai.tool.input"]
    assert "sk-ant-" not in emitted
    assert "[REDACTED:anthropic_api_key]" in emitted


def test_stop_event_yields_usage_spans_with_deterministic_ids(tmp_path):
    transcript = tmp_path / "t.jsonl"
    transcript.write_text(json.dumps({
        "type": "assistant",
        "message": {"id": "msg_1", "model": "claude-sonnet-4-6",
                    "usage": {"input_tokens": 100, "output_tokens": 50}},
    }) + "\n")
    from decimal import Decimal
    rates = {"claude-sonnet-4-6": (Decimal("0.003"), Decimal("0.015"))}
    event = make_event("Stop", {
        "session_id": "s1", "transcript_path": str(transcript),
    })
    spans = _publisher(rates=rates).spans_for_event(event)
    assert [s.name for s in spans] == ["session.stop", "chat claude-sonnet-4-6"]
    usage = spans[1]
    assert usage.attributes["gen_ai.usage.input_tokens"] == 100
    assert usage.attributes["gen_ai.usage.output_tokens"] == 50
    assert usage.attributes["meshai.cost.estimate_usd"] == pytest.approx(0.00105)
    assert format(usage.context.span_id, "016x") == usage_span_id_for("s1", "msg_1")


# --- Daemon scan/export/offset loop ------------------------------------------


def _run_hooks(root, n=3):
    for i in range(n):
        payload = json.dumps({"session_id": "s1", "tool_name": f"T{i}"})
        assert run_hook("PreToolUse", payload, root=root) == 0


def test_scan_exports_events_and_advances_offsets(tmp_path):
    _run_hooks(tmp_path)
    exporter = InMemorySpanExporter()
    daemon = Daemon(_publisher(exporter), root=tmp_path)
    assert daemon.scan_once() == 3
    assert len(exporter.get_finished_spans()) == 3
    # Offsets persisted: a fresh daemon re-exports nothing.
    daemon2 = Daemon(_publisher(InMemorySpanExporter()), root=tmp_path)
    assert daemon2.scan_once() == 0


def test_export_failure_keeps_offsets_for_replay(tmp_path):
    _run_hooks(tmp_path, n=2)
    exporter = FlakyExporter(failures=1)
    daemon = Daemon(_publisher(exporter), root=tmp_path)
    assert daemon.scan_once() == 0  # endpoint down: nothing committed
    assert wal.load_offsets(offsets_path(tmp_path)) == {}
    assert daemon.scan_once() == 2  # recovered: full replay, same span ids
    spans = exporter.get_finished_spans()
    assert len(spans) == 2


def test_replay_after_crash_reuses_span_ids(tmp_path):
    """Crash between export and offset save → re-export with SAME ids."""
    _run_hooks(tmp_path, n=1)
    first = InMemorySpanExporter()
    daemon = Daemon(_publisher(first), root=tmp_path)
    daemon.scan_once()
    # Simulate crash-before-offset-save: wipe the offsets file.
    offsets_path(tmp_path).unlink()
    second = InMemorySpanExporter()
    replay = Daemon(_publisher(second), root=tmp_path)
    replay.scan_once()
    ids_a = {s.context.span_id for s in first.get_finished_spans()}
    ids_b = {s.context.span_id for s in second.get_finished_spans()}
    assert ids_a == ids_b  # server-side dedup makes the replay a no-op


def test_unconvertible_event_is_skipped_not_stalling(tmp_path):
    _run_hooks(tmp_path, n=1)
    segment = next(iter(wal_dir(tmp_path).glob("*.jsonl")))
    with open(segment, "ab") as f:
        f.write(wal.encode_line({"v": 1}))  # missing required fields
    _run_hooks(tmp_path, n=0)
    exporter = InMemorySpanExporter()
    daemon = Daemon(_publisher(exporter), root=tmp_path)
    daemon.scan_once()
    assert len(exporter.get_finished_spans()) == 1  # good event still ships
    # Offset moved past the bad line: it is never retried.
    assert Daemon(_publisher(InMemorySpanExporter()), root=tmp_path).scan_once() == 0


def test_status_json_written(tmp_path):
    _run_hooks(tmp_path, n=1)
    daemon = Daemon(_publisher(), root=tmp_path)
    daemon.scan_once()
    status = json.loads(status_path(tmp_path).read_text())
    assert status["exported_spans"] == 1
    assert status["wal_backlog_bytes"] == 0
    assert status["last_flush_at"] is not None
    assert status["skipped_events"] == 0
    assert status["consecutive_export_failures"] == 0


# --- Chunked export (the 413 wedge) ------------------------------------------


def _segment(root):
    return next(iter(wal_dir(root).glob("*.jsonl")))


def test_backlog_drains_across_chunks_committing_each(tmp_path, monkeypatch):
    monkeypatch.setattr(daemon_mod, "EXPORT_MAX_EVENTS", 2)
    _run_hooks(tmp_path, n=5)
    exporter = BatchRecorder()
    daemon = Daemon(_publisher(exporter), root=tmp_path)

    assert daemon.scan_once() == 5
    assert exporter.batches == [2, 2, 1]  # bounded batches, not one big one
    segment = _segment(tmp_path)
    assert wal.load_offsets(offsets_path(tmp_path)) == {
        segment.name: segment.stat().st_size
    }
    assert Daemon(_publisher(), root=tmp_path).scan_once() == 0  # nothing replayed


def test_failure_mid_drain_keeps_offsets_of_succeeded_chunks(tmp_path, monkeypatch):
    """The regression behind the incident: a late failure must not un-commit
    the chunks that already landed, or the retried payload only grows."""
    monkeypatch.setattr(daemon_mod, "EXPORT_MAX_EVENTS", 2)
    _run_hooks(tmp_path, n=5)
    dying = FailAfterExporter(ok_batches=1)
    daemon = Daemon(_publisher(dying), root=tmp_path)

    assert daemon.scan_once() == 2  # first chunk only
    committed = wal.load_offsets(offsets_path(tmp_path))
    assert committed[_segment(tmp_path).name] > 0  # NOT frozen at 0

    healthy = InMemorySpanExporter()
    assert Daemon(_publisher(healthy), root=tmp_path).scan_once() == 3
    shipped = [
        s.name for s in dying.get_finished_spans() + healthy.get_finished_spans()
    ]
    assert sorted(shipped) == sorted(f"tool.pre T{i}" for i in range(5))


def test_oversized_tail_exports_in_chunks_instead_of_wedging(tmp_path, monkeypatch):
    """The incident itself: a tail whose whole-batch export always 413s."""
    _run_hooks(tmp_path, n=10)

    # Pre-fix behaviour, reproduced by lifting the chunk bound: one batch of
    # 10 spans is over the cap, is rejected, and the offset never advances,
    # so the next tick re-reads the same (by then larger) payload forever.
    monkeypatch.setattr(daemon_mod, "EXPORT_MAX_EVENTS", 10_000)
    wedged_exporter = CapExporter(cap=4)
    wedged = Daemon(_publisher(wedged_exporter), root=tmp_path)
    assert wedged.scan_once() == 0
    assert wedged_exporter.rejections == 1
    assert wal.load_offsets(offsets_path(tmp_path)) == {}

    # With the bound in place the same backlog drains, never over the cap.
    monkeypatch.setattr(daemon_mod, "EXPORT_MAX_EVENTS", 3)
    exporter = CapExporter(cap=4)
    daemon = Daemon(_publisher(exporter), root=tmp_path)
    assert daemon.scan_once() == 10
    assert exporter.rejections == 0
    status = json.loads(status_path(tmp_path).read_text())
    assert status["wal_backlog_bytes"] == 0
    assert status["skipped_events"] == 0


def test_transient_failure_then_recovery_loses_no_events(tmp_path, monkeypatch):
    monkeypatch.setattr(daemon_mod, "EXPORT_MAX_EVENTS", 2)
    _run_hooks(tmp_path, n=5)
    exporter = FlakyExporter(failures=1)
    daemon = Daemon(_publisher(exporter), root=tmp_path)

    assert daemon.scan_once() == 0  # endpoint down: nothing committed
    assert wal.load_offsets(offsets_path(tmp_path)) == {}
    assert daemon.scan_once() == 5  # recovered: every event, exactly once
    assert sorted(s.name for s in exporter.get_finished_spans()) == sorted(
        f"tool.pre T{i}" for i in range(5)
    )


def test_sustained_outage_never_drops_an_event(tmp_path, monkeypatch):
    """A dead endpoint must never trip the poison skip, however long we
    retry: the wall-clock gate is what separates 'down' from 'poison'."""
    monkeypatch.setattr(daemon_mod, "POISON_MIN_FAILURES", 3)
    _run_hooks(tmp_path, n=3)
    exporter = FlakyExporter(failures=10_000)
    daemon = Daemon(_publisher(exporter), root=tmp_path)

    for _ in range(50):
        assert daemon.scan_once() == 0
    assert wal.load_offsets(offsets_path(tmp_path)) == {}
    status = json.loads(status_path(tmp_path).read_text())
    assert status["skipped_events"] == 0
    assert status["consecutive_export_failures"] == 50

    healthy = InMemorySpanExporter()
    assert Daemon(_publisher(healthy), root=tmp_path).scan_once() == 3


def test_repeated_failure_backs_the_batch_off_to_a_single_event(
    tmp_path, monkeypatch
):
    """Span fan-out per event is unbounded, so the acceptable batch size is
    found by halving: a cap that only a 1-event batch clears is reached in
    log2 ticks, not by luck."""
    monkeypatch.setattr(daemon_mod, "EXPORT_MAX_EVENTS", 8)
    _run_hooks(tmp_path, n=4)
    exporter = CapExporter(cap=1)
    daemon = Daemon(_publisher(exporter), root=tmp_path)

    shipped = []
    for _ in range(4):
        before = len(exporter.get_finished_spans())
        daemon.scan_once()
        shipped.append(len(exporter.get_finished_spans()) - before)
    # Batches of 8 -> 4 -> 2 -> 1 events; only the last one is accepted.
    assert shipped == [0, 0, 0, 1]

    for _ in range(20):
        daemon.scan_once()
    status = json.loads(status_path(tmp_path).read_text())
    assert len(exporter.get_finished_spans()) == 4
    assert status["skipped_events"] == 0  # nothing poison: everything shipped
    assert status["wal_backlog_bytes"] == 0


def test_poison_event_is_skipped_counted_and_unblocks_the_rest(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(daemon_mod, "EXPORT_MAX_EVENTS", 4)
    monkeypatch.setattr(daemon_mod, "POISON_MIN_FAILURES", 2)
    monkeypatch.setattr(daemon_mod, "POISON_MIN_STALL_SECONDS", 0.0)
    _run_hooks(tmp_path, n=5)
    exporter = PoisonExporter("T1")
    daemon = Daemon(_publisher(exporter), root=tmp_path)

    for _ in range(20):
        daemon.scan_once()

    assert sorted(s.name for s in exporter.get_finished_spans()) == [
        "tool.pre T0", "tool.pre T2", "tool.pre T3", "tool.pre T4",
    ]
    status = json.loads(status_path(tmp_path).read_text())
    assert status["skipped_events"] == 1  # exactly one event dropped, loudly
    assert status["wal_backlog_bytes"] == 0  # no wedge left behind
    assert status["consecutive_export_failures"] == 0
