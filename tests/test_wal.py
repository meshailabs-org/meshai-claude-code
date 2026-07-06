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
