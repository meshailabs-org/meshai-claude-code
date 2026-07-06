"""Extract per-turn LLM usage from a Claude Code transcript JSONL.

Processed when a Stop event reaches the daemon. Streaming writes the same
assistant message id across multiple entries as usage accumulates — the
LAST entry per message id wins. Malformed lines are skipped; a transcript
is user-owned input, never trusted.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger("meshai-cc")


@dataclass(frozen=True)
class TurnUsage:
    message_id: str
    model: str
    input_tokens: int
    output_tokens: int
    cache_creation_tokens: int
    cache_read_tokens: int


def extract_usage(transcript_path: Path) -> list[TurnUsage]:
    turns: dict[str, TurnUsage] = {}
    try:
        lines = transcript_path.read_text(errors="replace").splitlines()
    except OSError:
        logger.warning("meshai-cc: transcript unreadable: %s", transcript_path)
        return []
    for line in lines:
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if not isinstance(entry, dict) or entry.get("type") != "assistant":
            continue
        message = entry.get("message") or {}
        usage = message.get("usage") or {}
        message_id = str(message.get("id") or "")
        model = str(message.get("model") or "")
        if not message_id or not model or not isinstance(usage, dict):
            continue
        try:
            turns[message_id] = TurnUsage(
                message_id=message_id,
                model=model,
                input_tokens=int(usage.get("input_tokens") or 0),
                output_tokens=int(usage.get("output_tokens") or 0),
                cache_creation_tokens=int(
                    usage.get("cache_creation_input_tokens") or 0
                ),
                cache_read_tokens=int(usage.get("cache_read_input_tokens") or 0),
            )
        except (TypeError, ValueError):
            continue
    return [t for t in turns.values() if t.input_tokens + t.output_tokens > 0]
