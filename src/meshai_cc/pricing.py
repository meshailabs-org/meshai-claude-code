"""Anthropic model pricing: fetched from MeshAI at daemon startup (D7 eng),
bundled fallback for offline. Centralized truth that updates without a pip
upgrade; the fallback keeps cost estimates working with no network."""

import logging
from decimal import Decimal
from pathlib import Path

import httpx

logger = logging.getLogger("meshai-cc")

_FALLBACK_PATH = Path(__file__).parent / "data" / "anthropic_pricing.yaml"

Rates = dict[str, tuple[Decimal, Decimal]]  # model -> (input, output) per 1K


def load_fallback() -> Rates:
    import yaml  # noqa: PLC0415

    raw = yaml.safe_load(_FALLBACK_PATH.read_text())
    return {
        model: (Decimal(str(v["input_price_per_1k"])),
                Decimal(str(v["output_price_per_1k"])))
        for model, v in raw["models"].items()
    }


def fetch_rates(base_url: str, api_key: str, timeout: float = 10.0) -> Rates:
    """GET /api/v1/pricing/anthropic; ANY failure falls back to bundled."""
    try:
        resp = httpx.get(
            f"{base_url}/api/v1/pricing/anthropic",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout,
        )
        resp.raise_for_status()
        models = resp.json()["data"]["models"]
        return {
            model: (Decimal(v["input_price_per_1k"]),
                    Decimal(v["output_price_per_1k"]))
            for model, v in models.items()
        }
    except Exception:  # noqa: BLE001
        logger.warning("meshai-cc: pricing fetch failed; using bundled fallback")
        return load_fallback()


def estimate_cost_usd(
    rates: Rates, model: str, input_tokens: int, output_tokens: int
) -> float | None:
    pair = rates.get(model)
    if pair is None:
        return None
    inp, out = pair
    cost = (Decimal(input_tokens) / 1000) * inp + (Decimal(output_tokens) / 1000) * out
    return float(cost.quantize(Decimal("0.000001")))
