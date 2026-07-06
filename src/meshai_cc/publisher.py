"""Convert WAL events to OTel spans and export them.

Spans are built as ReadableSpan objects (not the context-manager API)
because replay needs explicit historical timestamps and the DETERMINISTIC
ids minted at hook time — that determinism plus the server's UNIQUE
(tenant_id, span_id) is what turns at-least-once delivery into
exactly-once accounting. Export is synchronous and batched by us (not
BatchSpanProcessor): offsets advance only after a successful export.

Content filtering happens HERE, at the emission boundary, using the SDK's
FilterPipeline (D6 eng): nothing in this repo decides what is sensitive.
"""

import logging
from pathlib import Path

from meshai.tracer.filters import FilterConfig, FilterPipeline
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import SpanContext, SpanKind, Status, StatusCode, TraceFlags

from meshai_cc.events import root_span_id_for, trace_id_for, usage_span_id_for
from meshai_cc.pricing import Rates, estimate_cost_usd
from meshai_cc.transcript import extract_usage

logger = logging.getLogger("meshai-cc")

_SCOPE = InstrumentationScope("meshai-claude-code")

_SPAN_NAMES = {
    "SessionStart": "session.start",
    "UserPromptSubmit": "prompt",
    "PreToolUse": "tool.pre",
    "PostToolUse": "tool.post",
    "PreCompact": "session.compact",
    "Stop": "session.stop",
}


def _ctx(trace_id: str, span_id: str) -> SpanContext:
    return SpanContext(
        trace_id=int(trace_id, 16),
        span_id=int(span_id, 16),
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )


class Publisher:
    def __init__(
        self,
        exporter: SpanExporter,
        agent_name: str,
        filters: FilterConfig | None = None,
        rates: Rates | None = None,
    ) -> None:
        self._exporter = exporter
        self._resource = Resource.create(
            {
                "service.name": agent_name,
                "meshai.agent.framework": "claude-code",
            }
        )
        self._filters = FilterPipeline(
            filters if filters is not None else FilterConfig.load()
        )
        self._rates: Rates = rates or {}

    def spans_for_event(self, event: dict) -> list[ReadableSpan]:
        """One span per hook event; a Stop event also yields usage spans."""
        spans = [self._event_span(event)]
        if event.get("type") == "Stop" and event.get("transcript_path"):
            spans.extend(self._usage_spans(event))
        return spans

    def export(self, spans: list[ReadableSpan]) -> bool:
        if not spans:
            return True
        try:
            return self._exporter.export(spans) == SpanExportResult.SUCCESS
        except Exception:  # noqa: BLE001
            logger.warning("meshai-cc: span export raised", exc_info=True)
            return False

    def _event_span(self, event: dict) -> ReadableSpan:
        tool_name = event.get("tool_name") or ""
        name = _SPAN_NAMES.get(event["type"], event["type"])
        if tool_name:
            name = f"{name} {tool_name}"
        attributes: dict = {"meshai.session.id": event["session_id"]}
        if tool_name:
            attributes["gen_ai.tool.name"] = tool_name
            attributes["gen_ai.operation.name"] = "execute_tool"
        if event.get("cwd"):
            attributes["meshai.cwd"] = event["cwd"]
        for field, attr_key in (
            ("tool_input", "meshai.tool.input"),
            ("tool_output", "meshai.tool.output"),
        ):
            raw = event.get(field)
            if raw is None:
                continue
            filtered = self._filters.filter_content(tool_name, field, raw)
            if filtered is not None:
                attributes[attr_key] = filtered
        ts = int(event["ts_ns"])
        return ReadableSpan(
            name=name,
            context=_ctx(event["trace_id"], event["span_id"]),
            parent=_ctx(event["trace_id"], event["parent_span_id"]),
            resource=self._resource,
            attributes=attributes,
            kind=SpanKind.INTERNAL,
            instrumentation_scope=_SCOPE,
            status=Status(StatusCode.UNSET),
            start_time=ts,
            end_time=ts,
        )

    def _usage_spans(self, stop_event: dict) -> list[ReadableSpan]:
        session_id = stop_event["session_id"]
        trace_id = trace_id_for(session_id)
        parent = root_span_id_for(session_id)
        ts = int(stop_event["ts_ns"])
        spans = []
        for turn in extract_usage(Path(stop_event["transcript_path"])):
            attributes: dict = {
                "meshai.session.id": session_id,
                "gen_ai.system": "anthropic",
                "gen_ai.request.model": turn.model,
                "gen_ai.operation.name": "chat",
                "gen_ai.usage.input_tokens": turn.input_tokens,
                "gen_ai.usage.output_tokens": turn.output_tokens,
                "meshai.usage.cache_creation_tokens": turn.cache_creation_tokens,
                "meshai.usage.cache_read_tokens": turn.cache_read_tokens,
            }
            cost = estimate_cost_usd(
                self._rates, turn.model, turn.input_tokens, turn.output_tokens
            )
            if cost is not None:
                attributes["meshai.cost.estimate_usd"] = cost
            spans.append(
                ReadableSpan(
                    name=f"chat {turn.model}",
                    context=_ctx(
                        trace_id, usage_span_id_for(session_id, turn.message_id)
                    ),
                    parent=_ctx(trace_id, parent),
                    resource=self._resource,
                    attributes=attributes,
                    kind=SpanKind.CLIENT,
                    instrumentation_scope=_SCOPE,
                    status=Status(StatusCode.UNSET),
                    start_time=ts,
                    end_time=ts,
                )
            )
        return spans
