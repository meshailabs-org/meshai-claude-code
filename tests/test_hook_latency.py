"""CI-enforced hook latency benchmark (T12.5): p99 < 50ms per D9 eng.

Measures the full run_hook path (policy load, event build, durable append,
socket nudge attempt) — what Claude Code actually waits on.
"""

import json
import time

from meshai_cc.hooks import run_hook

BUDGET_P99_MS = 50.0
N = 1000


def test_hook_append_p99_under_budget(tmp_path):
    payload = json.dumps({
        "session_id": "bench-session",
        "tool_name": "Bash",
        "tool_input": {"command": "git status --short && pytest -q"},
    })
    samples = []
    for _ in range(N):
        start = time.perf_counter()
        assert run_hook("PreToolUse", payload, root=tmp_path) == 0
        samples.append((time.perf_counter() - start) * 1000)
    samples.sort()
    p50 = samples[N // 2]
    p99 = samples[int(N * 0.99)]
    print(f"\nhook latency: p50={p50:.2f}ms p99={p99:.2f}ms")
    assert p99 < BUDGET_P99_MS, f"p99 {p99:.2f}ms exceeds {BUDGET_P99_MS}ms budget"
