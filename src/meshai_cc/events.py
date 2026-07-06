"""WAL event construction from Claude Code hook payloads.

IDs are deterministic where replay-safety needs them to be:
- trace_id  = sha256("meshai-cc:" + session_id)[:32] — one trace per session
- root span = sha256("meshai-cc:root:" + session_id)[:16] — children parent
  to it even though it's emitted by a different hook process
- event span_id = random 8 bytes, minted ONCE at hook time and persisted in
  the WAL, so a daemon replay re-exports the same id and the server dedups.

Content fields are capped, not filtered, here: the hook's latency budget
(<50ms p99) has no room for regex, and filtering-on-emission means an
updated filters.yaml applies to events already in the WAL. The WAL itself
is owner-only (0600/0700) local state.
"""

import hashlib
import json
import os
import time
from typing import Any

MAX_CONTENT_BYTES = 64 * 1024

HOOK_EVENTS = (
    "SessionStart",
    "UserPromptSubmit",
    "PreToolUse",
    "PostToolUse",
    "PreCompact",
    "Stop",
)


def trace_id_for(session_id: str) -> str:
    return hashlib.sha256(f"meshai-cc:{session_id}".encode()).hexdigest()[:32]


def root_span_id_for(session_id: str) -> str:
    return hashlib.sha256(f"meshai-cc:root:{session_id}".encode()).hexdigest()[:16]


def usage_span_id_for(session_id: str, message_id: str) -> str:
    return hashlib.sha256(
        f"meshai-cc:usage:{session_id}:{message_id}".encode()
    ).hexdigest()[:16]


def _cap(value: Any) -> str | None:
    if value is None:
        return None
    text = value if isinstance(value, str) else json.dumps(value, default=str)
    return text[:MAX_CONTENT_BYTES]


def make_event(hook_event: str, payload: dict) -> dict:
    """Build the durable WAL record for one hook invocation."""
    session_id = str(payload.get("session_id") or "unknown-session")
    return {
        "v": 1,
        "type": hook_event,
        "ts_ns": time.time_ns(),
        "session_id": session_id,
        "trace_id": trace_id_for(session_id),
        "parent_span_id": root_span_id_for(session_id),
        "span_id": os.urandom(8).hex(),
        "tool_name": str(payload.get("tool_name") or "") or None,
        "tool_input": _cap(payload.get("tool_input")),
        "tool_output": _cap(payload.get("tool_response")),
        "transcript_path": str(payload.get("transcript_path") or "") or None,
        "cwd": str(payload.get("cwd") or "") or None,
    }
