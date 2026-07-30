"""WAL primitive tests: framing, torn writes, rotation, offsets, GC."""

import os

import pytest

from meshai_cc import wal


@pytest.fixture
def wal_dir(tmp_path):
    d = tmp_path / "wal"
    d.mkdir()
    return d


def _event(i: int) -> dict:
    return {"type": "PreToolUse", "seq": i, "session_id": "s1"}


def test_append_then_read_roundtrip(wal_dir):
    for i in range(3):
        wal.append_event(wal_dir, "s1", _event(i))
    segment = wal_dir / wal.segment_name("s1", 0)
    result = wal.read_segment(segment)
    assert [e["seq"] for e in result.events] == [0, 1, 2]
    assert result.corrupt_lines == 0
    assert result.new_offset == segment.stat().st_size


def test_read_from_offset_returns_only_new_events(wal_dir):
    wal.append_event(wal_dir, "s1", _event(0))
    segment = wal_dir / wal.segment_name("s1", 0)
    first = wal.read_segment(segment)
    wal.append_event(wal_dir, "s1", _event(1))
    second = wal.read_segment(segment, first.new_offset)
    assert [e["seq"] for e in second.events] == [1]


def test_torn_tail_is_not_consumed_and_next_append_heals_it(wal_dir):
    wal.append_event(wal_dir, "s1", _event(0))
    segment = wal_dir / wal.segment_name("s1", 0)
    # Simulate a hook killed mid-append: half a record, no newline.
    with open(segment, "ab") as f:
        f.write(b"deadbeef {\"type\":\"PostTool")
    torn = wal.read_segment(segment)
    assert [e["seq"] for e in torn.events] == [0]  # torn tail not consumed

    wal.append_event(wal_dir, "s1", _event(1))  # writer terminates the tail
    healed = wal.read_segment(segment, torn.new_offset)
    assert [e.get("seq") for e in healed.events] == [1]
    assert healed.corrupt_lines == 1  # the terminated fragment


def test_corrupt_middle_line_is_skipped_and_counted(wal_dir):
    wal.append_event(wal_dir, "s1", _event(0))
    segment = wal_dir / wal.segment_name("s1", 0)
    with open(segment, "ab") as f:
        f.write(b"00000000 {\"crc\":\"wrong\"}\n")
    wal.append_event(wal_dir, "s1", _event(1))
    result = wal.read_segment(segment)
    assert [e["seq"] for e in result.events] == [0, 1]
    assert result.corrupt_lines == 1


# --- Bounded reads (chunked drain) -------------------------------------------


def test_max_events_bounds_one_call_and_the_offset(wal_dir):
    for i in range(5):
        wal.append_event(wal_dir, "s1", _event(i))
    segment = wal_dir / wal.segment_name("s1", 0)
    line = len(wal.encode_line(_event(0)))

    first = wal.read_segment(segment, 0, max_events=2)
    assert [e["seq"] for e in first.events] == [0, 1]
    assert first.new_offset == 2 * line  # ONLY what this call consumed
    second = wal.read_segment(segment, first.new_offset, max_events=2)
    assert [e["seq"] for e in second.events] == [2, 3]
    third = wal.read_segment(segment, second.new_offset, max_events=2)
    assert [e["seq"] for e in third.events] == [4]
    assert third.new_offset == segment.stat().st_size


def test_max_bytes_cut_mid_record_stops_on_the_line_boundary(wal_dir):
    for i in range(5):
        wal.append_event(wal_dir, "s1", _event(i))
    segment = wal_dir / wal.segment_name("s1", 0)
    line = len(wal.encode_line(_event(0)))

    result = wal.read_segment(segment, 0, max_bytes=2 * line + 5)
    assert [e["seq"] for e in result.events] == [0, 1]
    assert result.new_offset == 2 * line  # the cut record is re-read, not lost
    rest = wal.read_segment(segment, result.new_offset)
    assert [e["seq"] for e in rest.events] == [2, 3, 4]


def test_max_bytes_still_consumes_a_record_larger_than_itself(wal_dir):
    """A hard byte bound would never advance past an oversized record."""
    big = dict(_event(0), blob="x" * 5000)
    wal.append_event(wal_dir, "s1", big)
    wal.append_event(wal_dir, "s1", _event(1))
    segment = wal_dir / wal.segment_name("s1", 0)

    first = wal.read_segment(segment, 0, max_bytes=64)
    assert [e["seq"] for e in first.events] == [0]
    assert first.new_offset == len(wal.encode_line(big))
    second = wal.read_segment(segment, first.new_offset, max_bytes=64)
    assert [e["seq"] for e in second.events] == [1]


def test_unbounded_read_unchanged_and_equal_to_a_chunked_drain(wal_dir):
    for i in range(9):
        wal.append_event(wal_dir, "s1", _event(i))
    segment = wal_dir / wal.segment_name("s1", 0)
    torn = b"deadbeef {\"type\":\"PostTool"
    with open(segment, "ab") as f:
        f.write(b"00000000 {\"crc\":\"wrong\"}\n")  # corrupt COMPLETE line
        f.write(torn)  # torn tail

    whole = wal.read_segment(segment)  # exactly as before this change
    assert [e["seq"] for e in whole.events] == list(range(9))
    assert whole.corrupt_lines == 1
    assert whole.new_offset == segment.stat().st_size - len(torn)

    events, corrupt, offset = [], 0, 0
    while True:
        chunk = wal.read_segment(segment, offset, max_events=2, max_bytes=40)
        assert len(chunk.events) <= 2
        if not chunk.events and chunk.new_offset == offset:
            break
        events += chunk.events
        corrupt += chunk.corrupt_lines
        offset = chunk.new_offset
    assert [e["seq"] for e in events] == [e["seq"] for e in whole.events]
    assert (offset, corrupt) == (whole.new_offset, whole.corrupt_lines)


def test_bounded_read_keeps_torn_tail_and_corrupt_line_semantics(wal_dir):
    wal.append_event(wal_dir, "s1", _event(0))
    segment = wal_dir / wal.segment_name("s1", 0)
    line = len(wal.encode_line(_event(0)))
    with open(segment, "ab") as f:
        # Torn tail carrying \r/\x0b/\x0c: splitting on anything but \n
        # would wedge the reader here forever.
        f.write(b"\rgarbage\x0b\x0cmore")
    torn = wal.read_segment(segment, 0, max_events=4, max_bytes=8)
    assert [e["seq"] for e in torn.events] == [0]
    assert torn.new_offset == line

    wal.append_event(wal_dir, "s1", _event(1))  # writer terminates the tail
    healed = wal.read_segment(segment, torn.new_offset, max_events=4, max_bytes=256)
    assert [e["seq"] for e in healed.events] == [1]
    assert healed.corrupt_lines == 1


def test_bounded_read_rejects_bounds_that_could_never_advance(wal_dir):
    wal.append_event(wal_dir, "s1", _event(0))
    segment = wal_dir / wal.segment_name("s1", 0)
    for kwargs in ({"max_events": 0}, {"max_bytes": 0}):
        with pytest.raises(ValueError):
            wal.read_segment(segment, 0, **kwargs)


def test_rotation_at_segment_cap(wal_dir, monkeypatch):
    monkeypatch.setattr(wal, "SEGMENT_MAX_BYTES", 200)
    for i in range(10):
        wal.append_event(wal_dir, "s1", _event(i))
    segments = sorted(p.name for p in wal_dir.glob("s1-*.jsonl"))
    assert len(segments) > 1
    # Every event lands exactly once across segments, in order per segment.
    seen = []
    for name in segments:
        seen += [e["seq"] for e in wal.read_segment(wal_dir / name).events]
    assert seen == list(range(10))


def test_offsets_roundtrip_atomic(tmp_path):
    path = tmp_path / "offsets.json"
    wal.save_offsets(path, {"a.jsonl": 42})
    assert wal.load_offsets(path) == {"a.jsonl": 42}
    assert not path.with_suffix(".tmp").exists()


def test_load_offsets_missing_or_garbage_is_empty(tmp_path):
    assert wal.load_offsets(tmp_path / "nope.json") == {}
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    assert wal.load_offsets(bad) == {}


def test_gc_removes_only_acked_cold_non_head_segments(wal_dir, monkeypatch):
    monkeypatch.setattr(wal, "SEGMENT_MAX_BYTES", 200)
    for i in range(10):
        wal.append_event(wal_dir, "s1", _event(i))
    segments = sorted(wal_dir.glob("s1-*.jsonl"))
    assert len(segments) >= 3
    old = segments[0]
    offsets = {old.name: old.stat().st_size}  # only the first is fully acked
    os.utime(old, (0, 0))  # make it cold

    removed = wal.gc_segments(wal_dir, offsets)
    assert removed == [old]
    assert not old.exists()
    # Head + unacked segments survive.
    assert all(p.exists() for p in segments[1:])


def test_gc_never_removes_head_even_if_acked_and_cold(wal_dir):
    wal.append_event(wal_dir, "s1", _event(0))
    head = wal_dir / wal.segment_name("s1", 0)
    os.utime(head, (0, 0))
    offsets = {head.name: head.stat().st_size}
    assert wal.gc_segments(wal_dir, offsets) == []
    assert head.exists()


def test_backlog_bytes(wal_dir):
    wal.append_event(wal_dir, "s1", _event(0))
    segment = wal_dir / wal.segment_name("s1", 0)
    size = segment.stat().st_size
    assert wal.backlog_bytes(wal_dir, {}) == size
    assert wal.backlog_bytes(wal_dir, {segment.name: size}) == 0


def test_concurrent_appends_from_forked_writers_all_survive(wal_dir):
    """20 processes hammer the same session; every record must be intact."""
    pids = []
    for i in range(20):
        pid = os.fork()
        if pid == 0:  # child
            try:
                wal.append_event(wal_dir, "s1", _event(i))
                os._exit(0)
            except BaseException:
                os._exit(1)
        pids.append(pid)
    assert all(os.waitpid(p, 0)[1] == 0 for p in pids)
    events = []
    for p in sorted(wal_dir.glob("s1-*.jsonl")):
        r = wal.read_segment(p)
        assert r.corrupt_lines == 0
        events += r.events
    assert sorted(e["seq"] for e in events) == list(range(20))
