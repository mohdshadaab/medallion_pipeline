import asyncio
import json
import logging
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from src.config import get_settings
from src.db import fetch_all, transaction
from src.llm import ClassificationOutput, aclassify_value, build_async_client, stable_input_hash
from src.logging_config import log_event

logger = logging.getLogger(__name__)

KNOWN_CATEGORIES = {
    "hvac",
    "electrical",
    "plumbing",
    "fire_safety",
    "access",
    "it_network",
    "vertical_transport",
    "housekeeping",
    "pest_control",
    "other",
}

CONFIDENCE_THRESHOLD = 0.7


def _source_text(row: dict[str, Any]) -> str:
    return " ".join(filter(None, [row["category_raw"], row["description"]])) or row["category_normalized"]


async def _classify_misses(texts: list[str], settings) -> dict[str, tuple[ClassificationOutput, int, float]]:
    """Classify distinct unknown texts concurrently with bounded concurrency.

    Network calls happen here, outside any DB transaction. A failed call routes
    that value to `unclassified` (logged) rather than cancelling the batch.
    """
    if not texts:
        return {}
    semaphore = asyncio.Semaphore(settings.classify_concurrency)
    client = build_async_client() if settings.openai_api_key else None

    async def classify_one(text: str):
        async with semaphore:
            try:
                output, tokens, cost = await aclassify_value(text, client=client)
                return text, output, tokens, cost
            except Exception as exc:  # partial-failure isolation
                log_event(logger, "agent.classification.item_failed", logging.WARNING, error_message=str(exc))
                fallback = ClassificationOutput(
                    canonical_category="unclassified", confidence=0.0, rationale="classification failed"
                )
                return text, fallback, 0, 0.0

    results = await asyncio.gather(*(classify_one(t) for t in texts))
    return {text: (output, tokens, cost) for text, output, tokens, cost in results}


async def classify_batch(run_id: UUID, batch_id: str) -> dict[str, int | float]:
    settings = get_settings()
    rows = fetch_all(
        """
        SELECT silver_id, category_raw, category_normalized, description
        FROM silver.tickets
        WHERE batch_id = %s
        ORDER BY raw_line_no
        """,
        (batch_id,),
    )

    # Distinct unknown source texts -> cache key.
    distinct_unknown: dict[str, str] = {}
    for row in rows:
        if row["category_normalized"] not in KNOWN_CATEGORIES:
            text = _source_text(row)
            distinct_unknown.setdefault(text, stable_input_hash(text))

    # Bulk cache lookup for pre-existing entries (one query instead of one-per-row).
    cached: dict[str, ClassificationOutput] = {}
    if distinct_unknown:
        key_to_text = {key: text for text, key in distinct_unknown.items()}
        cache_rows = fetch_all(
            "SELECT cache_key, output_json FROM meta.agent_cache WHERE cache_key = ANY(%s)",
            (list(distinct_unknown.values()),),
        )
        for cache_row in cache_rows:
            text = key_to_text.get(cache_row["cache_key"])
            if text is not None:
                cached[text] = ClassificationOutput.model_validate(cache_row["output_json"])

    # Concurrently classify only the misses (distinct, not already cached).
    misses = [text for text in distinct_unknown if text not in cached]
    computed = await _classify_misses(misses, settings)

    compute_method = "llm" if settings.openai_api_key else "heuristic"

    # Reconstruct per-row method/cache_hit to preserve the original sequential semantics:
    # pre-existing cache -> "cache"; first compute occurrence -> compute method; later
    # identical occurrence within this batch -> "cache". Keeps gold.category_breakdown stable.
    computed_emitted: set[str] = set()
    ai_rows: list[tuple] = []
    cache_hits = cache_misses = token_count = 0
    cost_usd = 0.0

    for row in rows:
        category = row["category_normalized"]
        if category in KNOWN_CATEGORIES:
            ai_rows.append((row["silver_id"], stable_input_hash(category), category, category, 1.0, 0, 0.0, False, "dictionary"))
            continue
        text = _source_text(row)
        input_hash = distinct_unknown[text]
        if text in cached:
            output, tokens, cost, method, hit = cached[text], 0, 0.0, "cache", True
        elif text in computed_emitted:
            output, tokens, cost, method, hit = computed[text][0], 0, 0.0, "cache", True
        else:
            output, tokens, cost = computed[text]
            method, hit = compute_method, False
            computed_emitted.add(text)
        canonical = output.canonical_category if output.confidence >= CONFIDENCE_THRESHOLD else "unclassified"
        cache_hits += int(hit)
        cache_misses += int(not hit and method in {"llm", "heuristic"})
        token_count += tokens
        cost_usd += cost
        ai_rows.append(
            (row["silver_id"], input_hash, text, canonical, float(output.confidence), tokens, cost, hit, method)
        )

    # Persist everything in one short transaction (after all network calls).
    with transaction() as conn:
        conn.execute("DELETE FROM silver.tickets_ai WHERE batch_id = %s", (batch_id,))
        for (silver_id, input_hash, source_value, canonical, confidence, tokens, cost, hit, method) in ai_rows:
            conn.execute(
                """
                INSERT INTO silver.tickets_ai (
                    silver_id, run_id, batch_id, input_hash, source_column,
                    source_value, canonical_value, confidence, model,
                    prompt_version, token_count, cost_usd, cache_hit, method
                )
                VALUES (%s, %s, %s, %s, 'category', %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id, silver_id, source_column)
                DO UPDATE SET canonical_value = EXCLUDED.canonical_value,
                              confidence = EXCLUDED.confidence,
                              cache_hit = EXCLUDED.cache_hit,
                              method = EXCLUDED.method
                """,
                (
                    silver_id,
                    run_id,
                    batch_id,
                    input_hash,
                    source_value,
                    canonical,
                    confidence,
                    settings.openai_model,
                    settings.prompt_version,
                    tokens,
                    cost,
                    hit,
                    method,
                ),
            )
        # Persist newly computed distinct values to the cache for future runs.
        for text in computed_emitted:
            output, tokens, cost = computed[text]
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
                    distinct_unknown[text],
                    settings.openai_model,
                    settings.prompt_version,
                    text,
                    Jsonb(json.loads(output.model_dump_json())),
                    output.confidence,
                    tokens,
                    cost,
                ),
            )
        conn.execute(
            """
            INSERT INTO meta.agent_runs (
                run_id, batch_id, agent_name, status, prompt_version, model,
                output_json, token_count, cost_usd, cache_hits, cache_misses, finished_at
            )
            VALUES (%s, %s, 'semantic_classification', 'succeeded', %s, %s, %s, %s, %s, %s, %s, now())
            """,
            (
                run_id,
                batch_id,
                settings.prompt_version,
                settings.openai_model,
                Jsonb({"classified_rows": len(rows), "mode": "openai" if settings.openai_api_key else "degraded"}),
                token_count,
                cost_usd,
                cache_hits,
                cache_misses,
            ),
        )

    log_event(
        logger,
        "agent.classification.complete",
        run_id=run_id,
        batch_id=batch_id,
        mode="openai" if settings.openai_api_key else "degraded",
        classified=len(rows),
        cache_hits=cache_hits,
        cache_misses=cache_misses,
        token_count=token_count,
        cost_usd=f"{cost_usd:.6f}",
    )
    return {
        "classified": len(rows),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "token_count": token_count,
        "cost_usd": cost_usd,
    }


def classify_batch_sync(run_id: UUID, batch_id: str) -> dict[str, int | float]:
    """Synchronous entrypoint: runs the concurrent batch on one event loop.

    Called from the (synchronous) LangGraph classify node so the rest of the
    pipeline keeps its synchronous execution model.
    """
    return asyncio.run(classify_batch(run_id, batch_id))
