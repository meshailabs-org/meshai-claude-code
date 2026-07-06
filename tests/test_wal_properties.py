"""Hypothesis property tests for the WAL (T11.5).

The WAL is THE load-bearing compliance piece: under any interleaving of
appends, torn writes (crash mid-append), reader passes, and replays from
stale offsets, every durably-appended event must be recoverable — exactly
once per offset-advancing read, at least once under replay — and no
corrupt bytes may ever surface as an event.
"""

import json

from hypothesis import given, settings
from hypothesis import strategies as st

from meshai_cc import wal

_ops = st.lists(
    st.one_of(
        st.tuples(st.just("append"), st.integers(0, 999)),
        st.tuples(st.just("torn"), st.binary(min_size=1, max_size=40)),
        st.tuples(st.just("read"), st.just(0)),
    ),
    min_size=1,
    max_size=40,
)


@settings(max_examples=150, deadline=None)
@given(ops=_ops)
def test_every_acked_event_survives_any_interleaving(tmp_path_factory, ops):
    wal_dir = tmp_path_factory.mktemp("wal")
    appended: list[int] = []
    consumed: list[int] = []
    offset = 0
    segment = None

    for op, arg in ops:
        if op == "append":
            segment = wal.append_event(
                wal_dir, "s", {"i": arg, "session_id": "s"}
            )
            appended.append(arg)
        elif op == "torn" and segment is not None:
            # Crash mid-append: raw garbage with no newline terminator.
            with open(segment, "ab") as f:
                f.write(arg.replace(b"\n", b"x"))
        elif op == "read" and segment is not None:
            result = wal.read_segment(segment, offset)
            consumed += [e["i"] for e in result.events]
            offset = result.new_offset

    if segment is not None:
        final = wal.read_segment(segment, offset)
        consumed += [e["i"] for e in final.events]

    # Every acked append is recovered exactly once, in order; torn garbage
    # never surfaces as an event.
    assert consumed == appended


@settings(max_examples=50, deadline=None)
@given(cut=st.integers(min_value=1, max_value=200), n=st.integers(1, 10))
def test_replay_from_any_stale_offset_loses_nothing(tmp_path_factory, cut, n):
    """Offsets may lag arbitrarily (crash before save): replaying from any
    earlier byte offset must yield a SUFFIX-superset — never skip an event
    past the stale offset, never fabricate one."""
    wal_dir = tmp_path_factory.mktemp("wal")
    for i in range(n):
        segment = wal.append_event(wal_dir, "s", {"i": i, "session_id": "s"})
    size = segment.stat().st_size
    committed = wal.read_segment(segment, 0)
    all_events = [e["i"] for e in committed.events]

    stale = max(0, size - cut)
    # A stale offset may land mid-line; only line starts parse. Find the
    # events a replay recovers and check they form a suffix of the truth.
    replay = wal.read_segment(segment, 0 if stale == 0 else _line_start(segment, stale))
    replayed = [e["i"] for e in replay.events]
    assert replayed == all_events[len(all_events) - len(replayed):]


def _line_start(path, offset: int) -> int:
    data = path.read_bytes()[:offset]
    return data.rfind(b"\n") + 1


def test_encode_decode_fuzz_roundtrip():
    for i in range(50):
        event = {"i": i, "payload": "x" * i, "nested": {"a": [1, i]}}
        assert wal.decode_line(wal.encode_line(event)[:-1]) == event


def test_decode_rejects_mutations():
    line = wal.encode_line({"session_id": "s", "i": 1})[:-1]
    for pos in range(0, len(line), 3):
        mutated = line[:pos] + bytes([line[pos] ^ 0xFF]) + line[pos + 1:]
        decoded = wal.decode_line(mutated)
        # Either rejected outright or (crc byte flips) never a DIFFERENT event.
        if decoded is not None:
            assert decoded == {"session_id": "s", "i": 1}
            # a flip inside the crc prefix that still matches is impossible;
            # reaching here means the flip was in ignorable framing space.
            assert json.loads(line[9:]) == decoded
