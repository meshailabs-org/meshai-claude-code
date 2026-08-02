"""Policy and credential loading. Everything degrades to safe defaults."""

import json
import logging
import os
import socket
from dataclasses import dataclass
from pathlib import Path

from meshai_cc.paths import config_dir

logger = logging.getLogger("meshai-cc")


_DEFAULT_API_URL = "https://api.meshai.dev"
_DEFAULT_INGEST_URL = "https://ingest.meshai.dev"


@dataclass(frozen=True)
class Policy:
    fail_closed: bool = False
    heartbeat_on_session_start: bool = True
    auto_start_daemon: bool = True
    agent_name: str = ""
    base_url: str = "https://api.meshai.dev"
    # Telemetry goes to the collector gateway, not the API host. The gateway
    # absorbs large exports and re-chunks them before the API sees them, so a
    # batch the API would 413 is delivered instead. None = decide automatically.
    ingest_url: str | None = None

    def resolved_agent_name(self) -> str:
        return self.agent_name or f"claude-code-{socket.gethostname()}"

    def resolved_ingest_url(self) -> str:
        """Base URL for OTLP export.

        Explicit ``ingest_url`` wins. Otherwise only the DEFAULT production
        host is redirected: someone who pointed ``base_url`` at localhost or a
        self-hosted deployment must keep their telemetry there rather than
        having it silently shipped to MeshAI.
        """
        if self.ingest_url:
            return self.ingest_url
        if self.base_url == _DEFAULT_API_URL:
            return _DEFAULT_INGEST_URL
        return self.base_url


def load_policy(root: Path | None = None) -> Policy:
    """Read policy.yaml; any problem yields the (fail-open) defaults.

    Note the asymmetry with filters.yaml: a broken FILTER config must fail
    closed (deny content), but a broken POLICY file failing "closed" would
    mean blocking Claude Code; compliance mode is opt-in, never accidental.
    """
    path = config_dir(root) / "policy.yaml"
    if not path.exists():
        return Policy()
    try:
        import yaml  # noqa: PLC0415

        raw = yaml.safe_load(path.read_text()) or {}
        return Policy(
            fail_closed=bool(raw.get("fail_closed", False)),
            heartbeat_on_session_start=bool(
                raw.get("heartbeat_on_session_start", True)
            ),
            auto_start_daemon=bool(raw.get("auto_start_daemon", True)),
            agent_name=str(raw.get("agent_name", "") or ""),
            base_url=str(raw.get("base_url", _DEFAULT_API_URL)),
            ingest_url=(str(raw["ingest_url"]) if raw.get("ingest_url") else None),
        )
    except Exception:  # noqa: BLE001
        logger.warning("meshai-cc: unparseable %s; using defaults", path)
        return Policy()


def load_api_key(root: Path | None = None) -> str | None:
    """MESHAI_API_KEY env wins; else ~/.config/meshai/credentials.json."""
    env = os.environ.get("MESHAI_API_KEY", "").strip()
    if env:
        return env
    path = config_dir(root) / "credentials.json"
    try:
        key = json.loads(path.read_text()).get("api_key", "")
        return key.strip() or None
    except (FileNotFoundError, ValueError, AttributeError):
        return None


def save_api_key(api_key: str, root: Path | None = None) -> Path:
    if not api_key.startswith("msh_") or len(api_key) < 16:
        raise ValueError("API key must look like msh_...")
    path = config_dir(root) / "credentials.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"api_key": api_key}))
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)
    return path
