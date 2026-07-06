"""Filesystem locations (XDG-style), all owner-only.

The WAL lives under the *state* dir — it is a durability buffer, not config.
Every path helper accepts an override root so tests never touch the real
home directory.
"""

import os
from pathlib import Path

_DIR_MODE = 0o700


def state_dir(root: Path | None = None) -> Path:
    base = root or Path(
        os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state")
    )
    return base / "meshai-cc"


def wal_dir(root: Path | None = None) -> Path:
    return state_dir(root) / "wal"


def offsets_path(root: Path | None = None) -> Path:
    return state_dir(root) / "offsets.json"


def status_path(root: Path | None = None) -> Path:
    return state_dir(root) / "status.json"


def socket_path(root: Path | None = None) -> Path:
    return state_dir(root) / "daemon.sock"


def pid_path(root: Path | None = None) -> Path:
    return state_dir(root) / "daemon.pid"


def config_dir(root: Path | None = None) -> Path:
    base = root or Path(
        os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")
    )
    return base / "meshai"


def ensure_dirs(root: Path | None = None) -> None:
    for d in (state_dir(root), wal_dir(root), config_dir(root)):
        d.mkdir(parents=True, exist_ok=True, mode=_DIR_MODE)
        os.chmod(d, _DIR_MODE)
