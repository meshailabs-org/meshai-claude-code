"""Hook entrypoint tests: durable append, fail-open/fail-closed, caps, ids."""

import json

from meshai_cc import wal
from meshai_cc.events import (
    MAX_CONTENT_BYTES,
    make_event,
    root_span_id_for,
    trace_id_for,
)
from meshai_cc.hooks import run_hook
from meshai_cc.paths import wal_dir


def _payload(**overrides) -> str:
    base = {
        "session_id": "sess-abc",
        "tool_name": "Bash",
        "tool_input": {"command": "ls -la"},
        "transcript_path": "/tmp/t.jsonl",
        "cwd": "/repo",
    }
    base.update(overrides)
    return json.dumps(base)


def _read_all_events(root):
    events = []
    for p in sorted(wal_dir(root).glob("*.jsonl")):
        events += wal.read_segment(p).events
    return events


def test_hook_appends_durable_event_and_exits_zero(tmp_path):
    assert run_hook("PreToolUse", _payload(), root=tmp_path) == 0
    (event,) = _read_all_events(tmp_path)
    assert event["type"] == "PreToolUse"
    assert event["session_id"] == "sess-abc"
    assert event["tool_name"] == "Bash"
    assert json.loads(event["tool_input"]) == {"command": "ls -la"}
    assert event["trace_id"] == trace_id_for("sess-abc")
    assert event["parent_span_id"] == root_span_id_for("sess-abc")
    assert len(event["span_id"]) == 16


def test_same_session_events_share_trace_but_not_span_ids(tmp_path):
    run_hook("PreToolUse", _payload(), root=tmp_path)
    run_hook("PostToolUse", _payload(tool_response="ok"), root=tmp_path)
    first, second = _read_all_events(tmp_path)
    assert first["trace_id"] == second["trace_id"]
    assert first["span_id"] != second["span_id"]


def test_garbage_stdin_fails_open_by_default(tmp_path):
    assert run_hook("PreToolUse", "{not json", root=tmp_path) == 0
    assert run_hook("PreToolUse", "", root=tmp_path) == 0


def test_wal_failure_fails_open_by_default(tmp_path, monkeypatch):
    def boom(*a, **k):
        raise OSError("disk full")

    monkeypatch.setattr(wal, "append_event", boom)
    assert run_hook("PreToolUse", _payload(), root=tmp_path) == 0


def test_wal_failure_blocks_when_fail_closed(tmp_path, monkeypatch):
    config = tmp_path / "meshai"
    config.mkdir(parents=True)
    (config / "policy.yaml").write_text("fail_closed: true\n")

    def boom(*a, **k):
        raise OSError("disk full")

    monkeypatch.setattr(wal, "append_event", boom)
    assert run_hook("PreToolUse", _payload(), root=tmp_path) == 2


def test_oversized_content_capped():
    event = make_event("PostToolUse", {
        "session_id": "s", "tool_response": "x" * (MAX_CONTENT_BYTES * 2),
    })
    assert len(event["tool_output"]) == MAX_CONTENT_BYTES


def test_missing_session_id_still_records():
    event = make_event("Stop", {})
    assert event["session_id"] == "unknown-session"
    assert event["trace_id"] == trace_id_for("unknown-session")
