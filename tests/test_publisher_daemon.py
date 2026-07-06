"""Publisher + daemon tests: conversion, filtering, at-least-once offsets."""

import json

import pytest
from meshai.tracer.filters import FilterConfig
from opentelemetry.sdk.trace.export import SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

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
