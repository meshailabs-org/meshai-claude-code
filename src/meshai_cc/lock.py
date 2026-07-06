"""Single-daemon guarantee via PID file + exclusive flock (T3.9).

Two daemons tailing the same WAL would race offset commits and double-
publish (or worse, GC live segments). The flock is held for the process
lifetime; the PID inside is informational for `status`.
"""

import fcntl
import os
from pathlib import Path


class AlreadyRunningError(RuntimeError):
    pass


class PidLock:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._fd: int | None = None

    def acquire(self) -> None:
        fd = os.open(self._path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            existing = ""
            try:
                existing = os.read(fd, 32).decode(errors="ignore").strip()
            finally:
                os.close(fd)
            raise AlreadyRunningError(
                f"another meshai-cc daemon holds {self._path}"
                + (f" (pid {existing})" if existing else "")
            ) from None
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode())
        os.fsync(fd)
        self._fd = fd  # kept open: the flock lives exactly as long as we do

    def release(self) -> None:
        if self._fd is not None:
            try:
                os.unlink(self._path)
            except OSError:
                pass
            os.close(self._fd)
            self._fd = None
