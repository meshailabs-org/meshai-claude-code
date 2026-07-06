# meshai-claude-code

[MeshAI](https://meshai.dev) &nbsp;·&nbsp; [Why durable telemetry](https://meshai.dev/blog/meshai-claude-code-durable-agent-telemetry) &nbsp;·&nbsp; [PyPI](https://pypi.org/project/meshai-claude-code/)

OTel-native MeshAI connector for Claude Code: durable, evidence-grade
telemetry for AI coding agent activity, aimed at EU AI Act Article 12
record-keeping. Every hook event is fsynced to a local write-ahead log
*before* anything else happens; a daemon publishes the WAL to MeshAI over
OTLP. Daemon crash, OOM, or network outage cannot lose events — only disk
failure can.

**Platforms (v1):** macOS, Linux, and WSL. On WSL the state directory must
live in the Linux filesystem (it does by default: `~/.local/state/meshai-cc`);
the daemon refuses to run against `/mnt/c` (DrvFS/9p), where fsync and file
locks do not hold. Native Windows support (TCP loopback) is v2.

## Install

```bash
pip install meshai-claude-code
meshai-claude-code login --api-key msh_...
meshai-claude-code install     # registers hooks in ~/.claude/settings.json
```

The daemon starts automatically on the next Claude Code session
(`auto_start_daemon: true`), or run `meshai-cc-daemon` yourself. Check
health with `meshai-claude-code status`.

## Architecture

```
                fsync                    tail              OTLP POST
  ┌──────┐  event ┌──────┐    nudge   ┌────────┐         ┌────────────┐
  │ hook │───────▶│ WAL  │◀───────────│ daemon │────────▶│ MeshAI API │
  └──────┘        └──────┘            └────┬───┘         └────────────┘
                                      1s poll backstop
```

- **Hooks** (`meshai-cc-hook <Event>`) are registered for SessionStart,
  UserPromptSubmit, PreToolUse, PostToolUse, PreCompact, and Stop. Each one
  appends a CRC-framed record to the WAL with a real fsync (`F_FULLFSYNC`
  on macOS) and exits — p99 under 50ms, CI-enforced.
- **The WAL** lives at `~/.local/state/meshai-cc/wal/` (owner-only). Hooks
  own writes and rotation; the daemon is a pure reader.
- **The daemon** (one per user, PID-file flock) converts events to
  OpenTelemetry spans and exports OTLP/HTTP protobuf to MeshAI. Offsets
  advance only after a successful export: delivery is at-least-once, and
  span ids are minted once at hook time, so MeshAI's ingest dedup makes
  accounting exactly-once.
- **Usage & cost**: on session Stop, the transcript is parsed for per-turn
  token usage, emitted with `gen_ai.*` attributes MeshAI turns into cost
  rows. Pricing comes from `GET /api/v1/pricing/anthropic` at daemon
  startup, with a bundled offline fallback.

## What leaves your machine (default: metadata only)

Tool content (`tool_input`/`tool_output`) is **dropped by default**.
Structural metadata (event type, tool name, timing, token counts) always
flows. Opt in per tool in `~/.config/meshai/filters.yaml`:

```yaml
tools:
  Bash:
    allow: [tool_input]
```

Allowlisted content passes through the MeshAI SDK's secret-redaction
pipeline (API keys, JWTs, private-key blocks, homoglyph and base64-wrapped
variants) and fails closed on any doubt. Filtering happens at emission in
the daemon; the WAL itself is owner-only local state.

## Compliance mode

```yaml
# ~/.config/meshai/policy.yaml
fail_closed: true    # WAL append failure blocks Claude Code (exit 2)
agent_name: my-cc    # registry identity; default claude-code-<hostname>
base_url: https://api.meshai.dev
```

With `fail_closed: true`, no evidence means no action — a tool call that
cannot be durably recorded does not run.

## Development

```bash
pip install -e ".[dev]"
pytest -q            # includes Hypothesis WAL property tests + latency gate
ruff check src/ tests/
```
