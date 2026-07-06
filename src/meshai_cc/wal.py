"""Write-ahead log: the load-bearing durability primitive.

Outbox pattern: hooks fsync events HERE first, then (separately) nudge the
daemon. The daemon is a pure reader — hooks own all writes, including
rotation. Only disk failure can lose an acked event.

Line format: ``{crc32:08x} {json}\n`` — CRC over the JSON bytes detects torn
writes from a hook killed mid-append. Readers resync at the next newline and
count the damage; writers terminate a torn tail before appending so one
crash never corrupts the following record.

Durability details (locked design):
- macOS fsync does not reach the platter; use F_FULLFSYNC (D2 eng).
- New segment files are followed by a parent-directory fsync (T3.7) —
  otherwise the file itself can vanish on power loss.
- Rotation creates the next segment with O_CREAT|O_EXCL so two racing hooks
  cannot both create it (T3.6); losers fall back to opening the winner's.
- Appends hold an exclusive flock on the segment.
"""

import fcntl
import json
import logging
import os
import re
import sys
import time
import zlib
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger("meshai-cc")

SEGMENT_MAX_BYTES = 10 * 1024 * 1024
GC_MIN_AGE_SECONDS = 3600
_SEGMENT_RE = re.compile(r"^(?P<session>.+)-(?P<seq>\d{6})\.jsonl$")
_FILE_MODE = 0o600


def _fsync(fd: int) -> None:
    if sys.platform == "darwin":  # pragma: no cover — macOS only
        fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
    else:
        os.fsync(fd)


def fsync_dir(path: Path) -> None:
    """Persist a directory entry (new file) against power loss."""
    fd = os.open(path, os.O_RDONLY)
    try:
        _fsync(fd)
    finally:
        os.close(fd)


def encode_line(event: dict) -> bytes:
    payload = json.dumps(event, separators=(",", ":")).encode()
    return f"{zlib.crc32(payload):08x} ".encode() + payload + b"\n"


def decode_line(line: bytes) -> dict | None:
    """Return the event, or None if the line fails CRC/framing."""
    if len(line) < 10 or line[8:9] != b" ":
        return None
    try:
        expected = int(line[:8], 16)
    except ValueError:
        return None
    payload = line[9:]
    if zlib.crc32(payload) != expected:
        return None
    try:
        return json.loads(payload)
    except ValueError:
        return None


def segment_name(session_id: str, seq: int) -> str:
    return f"{session_id}-{seq:06d}.jsonl"


def _session_segments(wal_dir: Path, session_id: str) -> list[tuple[int, Path]]:
    out = []
    for p in wal_dir.glob(f"{session_id}-*.jsonl"):
        m = _SEGMENT_RE.match(p.name)
        if m and m.group("session") == session_id:
            out.append((int(m.group("seq")), p))
    return sorted(out)


def _create_exclusive(path: Path) -> bool:
    """Atomically create a new segment; False if another hook won the race."""
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, _FILE_MODE)
    except FileExistsError:
        return False
    os.close(fd)
    fsync_dir(path.parent)
    return True


def current_segment(wal_dir: Path, session_id: str) -> Path:
    """The segment new appends go to, rotating when the head is full."""
    segments = _session_segments(wal_dir, session_id)
    if not segments:
        first = wal_dir / segment_name(session_id, 0)
        _create_exclusive(first)  # a racing loser just uses the winner's file
        return first
    seq, head = segments[-1]
    try:
        if head.stat().st_size < SEGMENT_MAX_BYTES:
            return head
    except FileNotFoundError:  # GC'd between glob and stat
        pass
    nxt = wal_dir / segment_name(session_id, seq + 1)
    _create_exclusive(nxt)
    return nxt


def append_event(wal_dir: Path, session_id: str, event: dict) -> Path:
    """Durably append one event; returns the segment written.

    Raises on failure — the CALLER decides fail-open vs fail-closed
    (policy.fail_closed), because that decision belongs to the hook.
    """
    segment = current_segment(wal_dir, session_id)
    fd = os.open(segment, os.O_WRONLY | os.O_APPEND | os.O_CREAT, _FILE_MODE)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            # Terminate a torn tail (hook killed mid-append) so this record
            # starts on a fresh line and stays parseable.
            size = os.fstat(fd).st_size
            if size > 0:
                with open(segment, "rb") as check:
                    check.seek(size - 1)
                    if check.read(1) != b"\n":
                        os.write(fd, b"\n")
            os.write(fd, encode_line(event))
            _fsync(fd)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
    return segment


@dataclass(frozen=True)
class ReadResult:
    events: list[dict]
    new_offset: int
    corrupt_lines: int


def read_segment(path: Path, offset: int = 0) -> ReadResult:
    """Read complete, CRC-valid events from ``offset``.

    The offset only advances past newline-terminated lines, so a torn tail
    is re-read (and by then terminated by the next writer) rather than lost.
    Corrupt complete lines are skipped and counted.
    """
    events: list[dict] = []
    corrupt = 0
    with open(path, "rb") as f:
        f.seek(offset)
        data = f.read()
    # Split on \n ONLY. bytes.splitlines() also splits on \r/\x0b/\x0c,
    # and torn binary garbage containing one of those would wedge the
    # reader at this offset forever (found by Hypothesis).
    consumed = 0
    cursor = 0
    while True:
        newline = data.find(b"\n", cursor)
        if newline == -1:
            break  # torn tail — do not advance past it
        line = data[cursor:newline]
        cursor = newline + 1
        consumed = cursor
        event = decode_line(line)
        if event is None:
            if line:  # empty line = terminator injected over a torn tail
                corrupt += 1
            continue
        events.append(event)
    return ReadResult(events, offset + consumed, corrupt)


def gc_segments(
    wal_dir: Path,
    offsets: dict[str, int],
    min_age_seconds: float = GC_MIN_AGE_SECONDS,
    now: float | None = None,
) -> list[Path]:
    """Delete segments that are fully acked and cold. Never the newest
    segment of a session (it may still receive appends)."""
    now = time.time() if now is None else now
    removed = []
    by_session: dict[str, list[tuple[int, Path]]] = {}
    for p in wal_dir.glob("*.jsonl"):
        m = _SEGMENT_RE.match(p.name)
        if m:
            by_session.setdefault(m.group("session"), []).append(
                (int(m.group("seq")), p)
            )
    for segments in by_session.values():
        for _seq, path in sorted(segments)[:-1]:  # keep the head
            try:
                st = path.stat()
            except FileNotFoundError:
                continue
            acked = offsets.get(path.name, 0) >= st.st_size
            if acked and (now - st.st_mtime) >= min_age_seconds:
                path.unlink(missing_ok=True)
                removed.append(path)
    if removed:
        fsync_dir(wal_dir)
    return removed


def load_offsets(path: Path) -> dict[str, int]:
    try:
        return {str(k): int(v) for k, v in json.loads(path.read_text()).items()}
    except (FileNotFoundError, ValueError, AttributeError):
        return {}


def save_offsets(path: Path, offsets: dict[str, int]) -> None:
    """Atomic tmp+rename+fsync — a crash never leaves a half-written file."""
    tmp = path.with_suffix(".tmp")
    fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, _FILE_MODE)
    try:
        os.write(fd, json.dumps(offsets).encode())
        _fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    fsync_dir(path.parent)


def backlog_bytes(wal_dir: Path, offsets: dict[str, int]) -> int:
    total = 0
    for p in wal_dir.glob("*.jsonl"):
        try:
            total += max(0, p.stat().st_size - offsets.get(p.name, 0))
        except FileNotFoundError:
            continue
    return total
