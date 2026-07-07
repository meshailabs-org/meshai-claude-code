"""Filesystem safety check for the WAL directory (T3.8).

fsync and flock are silent no-ops (or unreliable) on Windows-drive mounts
under WSL (DrvFS/9p) and on network filesystems; the WAL's durability
guarantee would be a lie there. The daemon refuses to operate rather than
pretend. Notably: a repo under /mnt/c on WSL is DrvFS, which is exactly why
the WAL lives under ~/.local/state (ext4) instead of the repo.
"""

import sys
from pathlib import Path

UNSAFE_FS = frozenset(
    {"9p", "v9fs", "drvfs", "nfs", "nfs4", "cifs", "smb2", "smbfs", "fuse.sshfs"}
)


def filesystem_type(path: Path, mounts_text: str | None = None) -> str:
    """Best-effort fs type of ``path`` (Linux/WSL via /proc/mounts).

    macOS has no /proc; local APFS/HFS+ honor F_FULLFSYNC, so it reports
    "apfs". Unknown parsing failures return "unknown" (allowed, logged by
    the caller); refusal is reserved for POSITIVELY identified unsafe types.
    """
    if sys.platform == "darwin":  # pragma: no cover; macOS only
        return "apfs"
    if mounts_text is None:
        try:
            mounts_text = Path("/proc/mounts").read_text()
        except OSError:
            return "unknown"
    resolved = str(path.resolve())
    best_len = -1
    best_type = "unknown"
    for line in mounts_text.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        mount_point = parts[1].replace("\\040", " ")
        fs_type = parts[2]
        if resolved == mount_point or resolved.startswith(
            mount_point.rstrip("/") + "/"
        ):
            if len(mount_point) > best_len:
                best_len = len(mount_point)
                best_type = fs_type
    return best_type.lower()


def assert_wal_dir_safe(path: Path, mounts_text: str | None = None) -> str:
    """Raise RuntimeError if the WAL directory sits on an unsafe filesystem."""
    fs = filesystem_type(path, mounts_text)
    if fs in UNSAFE_FS:
        raise RuntimeError(
            f"WAL directory {path} is on '{fs}' where fsync/flock do not hold; "
            "refusing to run. Move XDG_STATE_HOME to a local filesystem "
            "(on WSL: keep it under the Linux home, not /mnt/c)."
        )
    return fs
