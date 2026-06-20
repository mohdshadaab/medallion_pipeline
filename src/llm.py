import hashlib
import json
import warnings
from collections.abc import Mapping
from typing import Any

from openai import AsyncOpenAI, OpenAI
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field

from src.config import get_settings
from src.db import fetch_one, transaction


class ClassificationOutput(BaseModel):
    canonical_category: str = Field(description="Canonical support ticket category")
    confidence: float = Field(ge=0, le=1)
    rationale: str


def cache_key(normalized_input: str) -> str:
    settings = get_settings()
    raw = f"{settings.openai_model}|{settings.prompt_version}|{normalized_input}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _heuristic_classify(text: str) -> ClassificationOutput:
    lowered = text.lower()
    rules = [
        ("fire_safety", ["fire", "smoke", "sprinkler", "alarm", "exit sign"]),
        ("electrical", ["power", "outlet", "breaker", "electrical", "elec"]),
        ("hvac", ["temperature", "thermostat", "cold", "hot", "hvac", "a/c", "ac"]),
        ("plumbing", ["toilet", "faucet", "leak", "drip", "overflow", "plumbing"]),
        ("vertical_transport", ["elevator", "escalator", "lift"]),
        ("access", ["badge", "door", "lock", "access"]),
        ("it_network", ["printer", "network", "wifi", "internet"]),
        ("housekeeping", ["restroom", "clean", "trash", "housekeeping"]),
        ("pest_control", ["pest", "rodent", "insect"]),
    ]
    for category, needles in rules:
        if any(needle in lowered for needle in needles):
            return ClassificationOutput(canonical_category=category, confidence=0.85, rationale="matched deterministic keyword")
    return ClassificationOutput(canonical_category="unclassified", confidence=0.3, rationale="no confident deterministic match")


def _classification_prompt(normalized_input: str) -> str:
    return (
        "Classify this support ticket into one canonical category. "
        "Allowed categories: hvac, electrical, plumbing, fire_safety, access, "
        "it_network, vertical_transport, housekeeping, pest_control, other, unclassified. "
        "Return only JSON matching the schema.\n\n"
        f"Ticket text: {normalized_input}"
    )


def _to_mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "dict"):
        return value.dict()
    return {}


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _find_cost_usd(value: Any) -> float | None:
    data = _to_mapping(value)
    for key in ("cost_usd", "total_cost_usd", "total_cost", "cost"):
        raw = data.get(key)
        if isinstance(raw, Mapping):
            amount = _as_float(raw.get("usd") or raw.get("amount") or raw.get("total"))
        else:
            amount = _as_float(raw)
        if amount is not None:
            return amount
    for nested_key in ("usage", "billing", "cost_details"):
        nested = data.get(nested_key)
        if nested is not None:
            amount = _find_cost_usd(nested)
            if amount is not None:
                return amount
    return None


def _usage_token_count(usage: Any) -> int:
    data = _to_mapping(usage)
    return int(data.get("total_tokens") or 0)


def _estimated_cost_usd(usage: Any) -> float:
    settings = get_settings()
    data = _to_mapping(usage)
    input_tokens = int(data.get("input_tokens") or data.get("prompt_tokens") or 0)
    output_tokens = int(data.get("output_tokens") or data.get("completion_tokens") or 0)
    return (
        input_tokens * settings.openai_input_cost_per_1m_tokens
        + output_tokens * settings.openai_output_cost_per_1m_tokens
    ) / 1_000_000


def _response_cost_usd(response: Any) -> float:
    response_cost = _find_cost_usd(response)
    if response_cost is not None:
        return response_cost
    usage = getattr(response, "usage", None)
    usage_cost = _find_cost_usd(usage)
    if usage_cost is not None:
        return usage_cost
    # Official Responses docs expose usage tokens; use them only when no cost field is present.
    return _estimated_cost_usd(usage)


def build_async_client() -> AsyncOpenAI:
    settings = get_settings()
    return AsyncOpenAI(api_key=settings.openai_api_key, max_retries=settings.openai_max_retries)


async def aclassify_value(
    normalized_input: str, client: AsyncOpenAI | None = None
) -> tuple[ClassificationOutput, int, float]:
    """Compute-only async classification (no DB cache read/write).

    Returns (output, token_count, cost_usd). Degraded mode (no key) uses the
    deterministic heuristic. The batch caller (classify_agent) owns cache lookup,
    per-row method/cache_hit reconstruction, and batched persistence.
    """
    settings = get_settings()
    if not settings.openai_api_key:
        return _heuristic_classify(normalized_input), 0, 0.0
    client = client or build_async_client()
    with warnings.catch_warnings():
        # The Responses SDK emits benign Pydantic serializer warnings for its union-typed output.
        warnings.filterwarnings("ignore", message="Pydantic serializer warnings")
        response = await client.responses.parse(
            model=settings.openai_model,
            input=_classification_prompt(normalized_input),
            temperature=0,
            text_format=ClassificationOutput,
        )
        output = response.output_parsed
        usage = getattr(response, "usage", None)
        return output, _usage_token_count(usage), _response_cost_usd(response)


def classify_text(normalized_input: str) -> tuple[ClassificationOutput, bool, int, float]:
    settings = get_settings()
    key = cache_key(normalized_input)
    cached = fetch_one("SELECT output_json, confidence FROM meta.agent_cache WHERE cache_key = %s", (key,))
    if cached:
        with transaction() as conn:
            conn.execute("UPDATE meta.agent_cache SET last_used_at = now() WHERE cache_key = %s", (key,))
        return ClassificationOutput.model_validate(cached["output_json"]), True, 0, 0.0

    if not settings.openai_api_key:
        output = _heuristic_classify(normalized_input)
        token_count = 0
        cost_usd = 0.0
    else:
        client = OpenAI(api_key=settings.openai_api_key)
        prompt = _classification_prompt(normalized_input)
        with warnings.catch_warnings():
            # The Responses SDK emits benign Pydantic serializer warnings for its union-typed
            # output during parse/serialization; they do not affect the parsed result or cost.
            warnings.filterwarnings("ignore", message="Pydantic serializer warnings")
            response = client.responses.parse(
                model=settings.openai_model,
                input=prompt,
                temperature=0,
                text_format=ClassificationOutput,
            )
            output = response.output_parsed
            usage = getattr(response, "usage", None)
            token_count = _usage_token_count(usage)
            cost_usd = _response_cost_usd(response)

    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO meta.agent_cache (
                cache_key, model, prompt_version, normalized_input,
                output_json, confidence, token_count, cost_usd
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cache_key) DO UPDATE
            SET output_json = EXCLUDED.output_json,
                confidence = EXCLUDED.confidence,
                last_used_at = now()
            """,
            (
                key,
                settings.openai_model,
                settings.prompt_version,
                normalized_input,
                Jsonb(json.loads(output.model_dump_json())),
                output.confidence,
                token_count,
                cost_usd,
            ),
        )
    return output, False, token_count, cost_usd


def stable_input_hash(value: str) -> str:
    return cache_key(value)
