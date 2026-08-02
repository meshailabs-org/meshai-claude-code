# Changelog

## Unreleased

## 0.2.0 - 2026-08-02

Telemetry now exports to `https://ingest.meshai.dev` instead of
`api.meshai.dev`. Minor rather than patch: the default destination changes, so
behaviour differs for anyone who upgrades without reading.

The gateway is an OpenTelemetry collector that absorbs large exports and
re-chunks them into small batches before they reach the API. An export the API
would have rejected with 413 is delivered instead. This is the same class of
problem 0.1.1 fixed from the client side, now addressed on the server side too.

- Telemetry exports to the ingest gateway; `base_url` is unchanged and still
  serves rate-card fetches and heartbeats.
- New optional `ingest_url` in `policy.yaml` overrides the destination.
- Self-hosted and localhost `base_url` values keep their own telemetry: only the
  default production host is redirected, so a self-hoster's spans are never
  silently shipped to MeshAI.
- Stopped tracking compiled bytecode; `__pycache__` was never gitignored, so 23
  `.pyc` files were tracked in a package that publishes to PyPI.

## 0.1.1 - 2026-07-30

Fixes a production incident in which the daemon wedged permanently and sent
~500 rejected requests/hour at the ingest endpoint while exporting nothing.
Anyone still on 0.1.0 should upgrade: that version cannot recover on its own
once its backlog exceeds the ingest body cap.

- Size export batches by SERVER PROCESSING TIME, not payload size. 2,000 spans
  fits the 10 MiB body cap comfortably (~817 KB) but exceeded the OTLP
  exporter's 10s timeout, because ingest does per-span dedup, insert and policy
  evaluation. Measured against the live API with real spans (median): 100 ->
  1.48s, 250 -> 2.62s, 500 -> 5.28s, 1000 -> 11.16s. 250 keeps ~3x headroom and
  is the shipped value. Re-measure before raising it; the safe number is a
  property of the server's per-span cost, not of this package.

- Fix a permanent export wedge: the daemon read a segment's whole unexported
  tail and shipped it as ONE OTLP request, so once that tail exceeded the
  ingest body cap the request was rejected, the offset never advanced, and
  every tick retried a larger payload forever (no telemetry ever landed
  again). Reads are now bounded (`wal.read_segment(..., max_events=,
  max_bytes=)`, unbounded by default) and the daemon drains a segment in
  chunks, committing the offset after each one.
- Bound the request BODY at span level, not just the read: a read's spans are
  split across as many export calls as the 10 MiB ingest cap needs (250
  spans or ~4 MiB of estimated body per call), and the offset is committed
  only after every call of that read succeeded. Bounding raw WAL bytes alone
  was not enough - measured against the real backlog, 1 MiB of WAL became an
  81 MB body, because a Stop event re-reads the whole transcript and emits a
  usage span per turn, so ONE event can exceed the cap by itself. A partial
  failure is safe: those spans replay and the server dedups on (tenant_id,
  span_id).
- The read halves on every consecutive failure at the same offset and resets
  on any success. That is now a safety net rather than the mechanism; what it
  still buys is backing off to one event so a poison event can be isolated.
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
