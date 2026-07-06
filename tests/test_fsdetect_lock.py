"""Tests for filesystem refusal (T3.8) and the single-daemon PID lock (T3.9)."""

from pathlib import Path

import pytest

from meshai_cc.fsdetect import assert_wal_dir_safe, filesystem_type
from meshai_cc.lock import AlreadyRunningError, PidLock

MOUNTS = """\
/dev/sdc / ext4 rw,relatime 0 0
C:\\134 /mnt/c 9p rw,noatime 0 0
drvfs /mnt/d drvfs rw 0 0
nas:/vol /mnt/nas nfs4 rw 0 0
tmpfs /tmp tmpfs rw 0 0
"""


def test_longest_prefix_mount_wins():
    assert filesystem_type(Path("/mnt/c/Users/x"), MOUNTS) == "9p"
    assert filesystem_type(Path("/tmp/foo"), MOUNTS) == "tmpfs"
    assert filesystem_type(Path("/home/user/.local/state"), MOUNTS) == "ext4"


@pytest.mark.parametrize("path", ["/mnt/c/repo", "/mnt/d/repo", "/mnt/nas/x"])
def test_unsafe_filesystems_are_refused(path):
    with pytest.raises(RuntimeError, match="refusing to run"):
        assert_wal_dir_safe(Path(path), MOUNTS)


def test_safe_and_unknown_filesystems_allowed(tmp_path):
    assert assert_wal_dir_safe(Path("/home/u/.local/state"), MOUNTS) == "ext4"
    assert assert_wal_dir_safe(tmp_path, "") == "unknown"


def test_pid_lock_excludes_second_daemon(tmp_path):
    path = tmp_path / "daemon.pid"
    first = PidLock(path)
    first.acquire()
    second = PidLock(path)
    with pytest.raises(AlreadyRunningError):
        second.acquire()
    first.release()
    second.acquire()  # released lock is acquirable again
    second.release()
