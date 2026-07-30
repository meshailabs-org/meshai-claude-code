# Changelog

## Unreleased

- Fix a permanent export wedge: the daemon read a segment's whole unexported
  tail and shipped it as ONE OTLP request, so once that tail exceeded the
  ingest body cap the request was rejected, the offset never advanced, and
  every tick retried a larger payload forever (no telemetry ever landed
  again). Reads are now bounded (`wal.read_segment(..., max_events=,
  max_bytes=)`, unbounded by default) and the daemon drains a segment in
  chunks, committing the offset after each one.
- The batch halves on every consecutive failure at the same offset (span
  fan-out per event is unbounded, so an acceptable size cannot be picked up
  front) and resets on any success.
- A single event whose spans can never be accepted no longer blocks the
  events behind it: after sustained failure on a batch backed off to one
  event (120 attempts AND 10 minutes, so an endpoint outage never
  qualifies) it is dropped, logged, and counted. `status` now reports
  `skipped events` and consecutive export failures.

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
