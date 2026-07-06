"""CLI tests: surgical settings.json edits, backup/rollback, login, status."""

import json

import pytest

from meshai_cc.cli import install, uninstall
from meshai_cc.config import load_api_key, save_api_key
from meshai_cc.events import HOOK_EVENTS


def test_install_into_empty_dir_registers_all_hooks(tmp_path):
    message = install(claude_dir=tmp_path)
    settings = json.loads((tmp_path / "settings.json").read_text())
    for event in HOOK_EVENTS:
        commands = [
            h["command"]
            for entry in settings["hooks"][event]
            for h in entry["hooks"]
        ]
        assert f"meshai-cc-hook {event}" in commands
    assert "registered hooks" in message


def test_install_preserves_unrelated_settings_and_hooks(tmp_path):
    existing = {
        "model": "opus",
        "permissions": {"allow": ["Bash(ls:*)"]},
        "hooks": {
            "PreToolUse": [
                {"matcher": "Bash", "hooks": [
                    {"type": "command", "command": "my-linter"}]}
            ]
        },
    }
    (tmp_path / "settings.json").write_text(json.dumps(existing))
    install(claude_dir=tmp_path)
    settings = json.loads((tmp_path / "settings.json").read_text())
    assert settings["model"] == "opus"
    assert settings["permissions"] == {"allow": ["Bash(ls:*)"]}
    pre = settings["hooks"]["PreToolUse"]
    assert any("my-linter" in json.dumps(e) for e in pre)  # theirs kept
    assert any("meshai-cc-hook" in json.dumps(e) for e in pre)  # ours added
    # A backup of the pre-edit file exists.
    assert list(tmp_path.glob("settings.json.meshai-backup-*"))


def test_install_is_idempotent(tmp_path):
    install(claude_dir=tmp_path)
    first = (tmp_path / "settings.json").read_text()
    message = install(claude_dir=tmp_path)
    assert (tmp_path / "settings.json").read_text() == first
    assert "already installed" in message


def test_install_refuses_to_clobber_corrupt_settings(tmp_path):
    (tmp_path / "settings.json").write_text("{corrupt")
    with pytest.raises(ValueError):
        install(claude_dir=tmp_path)
    assert (tmp_path / "settings.json").read_text() == "{corrupt"  # untouched


def test_uninstall_removes_only_ours(tmp_path):
    (tmp_path / "settings.json").write_text(json.dumps({
        "hooks": {
            "PreToolUse": [
                {"hooks": [{"type": "command", "command": "my-linter"}]},
            ]
        }
    }))
    install(claude_dir=tmp_path)
    uninstall(claude_dir=tmp_path)
    settings = json.loads((tmp_path / "settings.json").read_text())
    assert settings["hooks"]["PreToolUse"] == [
        {"hooks": [{"type": "command", "command": "my-linter"}]}
    ]
    assert "Stop" not in settings["hooks"]  # ours-only event fully removed


def test_login_roundtrip_and_permissions(tmp_path, monkeypatch):
    monkeypatch.delenv("MESHAI_API_KEY", raising=False)
    save_api_key("msh_" + "k" * 20, root=tmp_path)
    assert load_api_key(root=tmp_path) == "msh_" + "k" * 20
    creds = tmp_path / "meshai" / "credentials.json"
    assert oct(creds.stat().st_mode & 0o777) == "0o600"


def test_login_rejects_malformed_key(tmp_path):
    with pytest.raises(ValueError):
        save_api_key("sk-not-meshai", root=tmp_path)


def test_env_var_wins_over_credentials_file(tmp_path, monkeypatch):
    save_api_key("msh_" + "a" * 20, root=tmp_path)
    monkeypatch.setenv("MESHAI_API_KEY", "msh_" + "b" * 20)
    assert load_api_key(root=tmp_path) == "msh_" + "b" * 20
