# Changelog

## 0.1.0

First public release. OTel-native MeshAI connector for Claude Code:
durable, evidence-grade telemetry for AI coding agent activity, aimed at
EU AI Act Article 12 record-keeping.

- Hooks (`meshai-cc-hook`) fsync every Claude Code event to a local
  write-ahead log before anything else, so daemon crash, OOM, or network
  outage cannot lose events. Only disk failure can.
- Single daemon (`meshai-cc-daemon`) tails the WAL and publishes
  OTLP/HTTP protobuf spans to MeshAI; offsets advance only after a
  successful export and span ids are minted once at hook time, so
  at-least-once delivery plus server-side dedup yields exactly-once
  accounting.
- Per-turn token usage and cost are extracted from the session transcript
  on Stop; pricing is fetched from MeshAI with a bundled offline fallback.
- Default-deny content filtering (via `meshai-sdk[tracer]`): tool
  input/output is dropped unless allowlisted per tool, and allowlisted
  content is scrubbed of secrets, fail-closed.
- `meshai-claude-code` CLI: install/uninstall (surgical, backed-up
  `~/.claude/settings.json` edits), login, status.
- Platforms: macOS, Linux, WSL (WAL must live on a local filesystem; the
  daemon refuses DrvFS/9p/NFS/SMB).
