"""Transcript usage extraction and pricing fallback tests."""

import json
from decimal import Decimal

from meshai_cc.pricing import estimate_cost_usd, load_fallback
from meshai_cc.transcript import extract_usage


def _entry(msg_id="msg_1", model="claude-sonnet-4-6", inp=10, out=5, **usage):
    return json.dumps({
        "type": "assistant",
        "message": {
            "id": msg_id, "model": model,
            "usage": {"input_tokens": inp, "output_tokens": out, **usage},
        },
    })


def test_extracts_usage_per_assistant_turn(tmp_path):
    t = tmp_path / "t.jsonl"
    t.write_text("\n".join([
        json.dumps({"type": "user", "message": {"role": "user"}}),
        _entry("msg_1", inp=100, out=50, cache_read_input_tokens=7),
        _entry("msg_2", model="claude-haiku-4-5", inp=20, out=10),
    ]))
    turns = extract_usage(t)
    assert len(turns) == 2
    by_id = {t.message_id: t for t in turns}
    assert by_id["msg_1"].input_tokens == 100
    assert by_id["msg_1"].cache_read_tokens == 7
    assert by_id["msg_2"].model == "claude-haiku-4-5"


def test_streaming_repeats_last_entry_wins(tmp_path):
    t = tmp_path / "t.jsonl"
    t.write_text("\n".join([
        _entry("msg_1", inp=1, out=0),
        _entry("msg_1", inp=100, out=42),  # accumulated final usage
    ]))
    (turn,) = extract_usage(t)
    assert (turn.input_tokens, turn.output_tokens) == (100, 42)


def test_malformed_lines_and_zero_usage_skipped(tmp_path):
    t = tmp_path / "t.jsonl"
    t.write_text("\n".join([
        "{not json",
        json.dumps({"type": "assistant", "message": {}}),
        _entry("msg_0", inp=0, out=0),
        _entry("msg_1", inp=5, out=5),
    ]))
    turns = extract_usage(t)
    assert [t.message_id for t in turns] == ["msg_1"]


def test_missing_transcript_is_empty(tmp_path):
    assert extract_usage(tmp_path / "nope.jsonl") == []


def test_bundled_fallback_loads_and_prices():
    rates = load_fallback()
    assert "claude-sonnet-4-6" in rates
    cost = estimate_cost_usd(rates, "claude-sonnet-4-6", 1000, 1000)
    assert cost == 0.018  # 0.003 + 0.015


def test_unknown_model_has_no_estimate():
    rates = {"m": (Decimal("1"), Decimal("1"))}
    assert estimate_cost_usd(rates, "other", 10, 10) is None
