"""CLI: install / uninstall / login / status (T7).

`install` registers the six hook commands in ~/.claude/settings.json.
Someone else's settings file is sacred: we back it up first, edit the
parsed JSON surgically (never touching unrelated keys), write via
tmp+rename, and roll back from the backup if anything goes wrong.
"""

import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path

from meshai_cc.config import save_api_key
from meshai_cc.events import HOOK_EVENTS
from meshai_cc.paths import offsets_path, status_path, wal_dir

_MARKER = "meshai-cc-hook"


def _settings_path(claude_dir: Path | None = None) -> Path:
    return (claude_dir or Path.home() / ".claude") / "settings.json"


def _hook_entry(event: str) -> dict:
    return {"hooks": [{"type": "command", "command": f"{_MARKER} {event}"}]}


def _is_ours(entry: dict) -> bool:
    return any(
        _MARKER in h.get("command", "")
        for h in entry.get("hooks", [])
        if isinstance(h, dict)
    )


def install(claude_dir: Path | None = None) -> str:
    path = _settings_path(claude_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    settings: dict = {}
    backup: Path | None = None
    if path.exists():
        backup = path.with_name(f"settings.json.meshai-backup-{int(time.time())}")
        shutil.copy2(path, backup)
        settings = json.loads(path.read_text())  # raises on corrupt input:
        # better to stop than to clobber a file we could not parse.
    hooks = settings.setdefault("hooks", {})
    added = []
    try:
        for event in HOOK_EVENTS:
            entries = hooks.setdefault(event, [])
            if any(_is_ours(e) for e in entries if isinstance(e, dict)):
                continue
            entries.append(_hook_entry(event))
            added.append(event)
        _atomic_write(path, settings)
    except Exception:
        if backup is not None:
            shutil.copy2(backup, path)  # roll back — never leave it half-edited
        raise
    return (
        f"registered hooks: {', '.join(added) or '(already installed)'}"
        + (f"\nbackup: {backup}" if backup else "")
    )


def uninstall(claude_dir: Path | None = None) -> str:
    path = _settings_path(claude_dir)
    if not path.exists():
        return "nothing to do"
    settings = json.loads(path.read_text())
    hooks = settings.get("hooks", {})
    removed = []
    for event in list(hooks):
        entries = hooks.get(event)
        if not isinstance(entries, list):
            continue
        kept = [e for e in entries if not (isinstance(e, dict) and _is_ours(e))]
        if len(kept) != len(entries):
            removed.append(event)
        if kept:
            hooks[event] = kept
        else:
            del hooks[event]
    _atomic_write(path, settings)
    return f"removed hooks: {', '.join(removed) or '(none found)'}"


def _atomic_write(path: Path, settings: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(settings, indent=2) + "\n")
    os.replace(tmp, path)


def login(api_key: str | None, root: Path | None = None) -> str:
    key = api_key or input("MeshAI API key (msh_...): ").strip()
    stored = save_api_key(key, root)
    return f"credentials stored at {stored} (0600)"


def status(root: Path | None = None) -> str:
    lines = []
    try:
        s = json.loads(status_path(root).read_text())
        age = time.time() - (s.get("last_flush_at") or 0)
        lines.append(f"daemon pid {s['pid']}: {s['exported_spans']} spans exported")
        lines.append(
            f"last flush: {int(age)}s ago" if s.get("last_flush_at") else
            "last flush: never"
        )
        lines.append(f"export failures: {s['export_failures']}")
        lines.append(f"corrupt WAL lines: {s['corrupt_lines']}")
        lines.append(f"WAL backlog: {s.get('wal_backlog_bytes', '?')} bytes")
    except (FileNotFoundError, ValueError, KeyError):
        lines.append("daemon: no status file (not running yet?)")
        try:
            from meshai_cc import wal  # noqa: PLC0415

            backlog = wal.backlog_bytes(
                wal_dir(root), wal.load_offsets(offsets_path(root))
            )
            lines.append(f"WAL backlog: {backlog} bytes")
        except OSError:
            pass
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:  # pragma: no cover — shim
    parser = argparse.ArgumentParser(prog="meshai-claude-code")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("install")
    sub.add_parser("uninstall")
    login_p = sub.add_parser("login")
    login_p.add_argument("--api-key")
    sub.add_parser("status")
    args = parser.parse_args(argv)
    try:
        if args.command == "install":
            print(install())
        elif args.command == "uninstall":
            print(uninstall())
        elif args.command == "login":
            print(login(args.api_key))
        elif args.command == "status":
            print(status())
    except Exception as exc:  # noqa: BLE001
        print(f"meshai-claude-code {args.command} failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
