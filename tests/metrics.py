"""Shared metric collection for characterization/parity tests.

Both the baseline capture script and the parity test use these helpers so the
golden snapshot and the live comparison are computed identically.
"""
from typing import Any

from src.db import fetch_all, fetch_one


def latest_batch_id() -> str | None:
    row = fetch_one("SELECT batch_id FROM meta.pipeline_runs ORDER BY started_at DESC LIMIT 1")
    return row["batch_id"] if row else None


def collect_metrics(batch_id: str | None = None) -> dict[str, Any]:
    """Collect deterministic, behavior-defining metrics for one batch.

    Captures the values the async refactor must preserve: Silver reconciliation
    counts, Gold product row counts, the category breakdown including its
    classification_method dimension, agent cache accounting, and quality checks.
    """
    batch_id = batch_id or latest_batch_id()
    if not batch_id:
        raise SystemExit("No pipeline run found to collect metrics from.")

    run = fetch_one(
        """
        SELECT total_rows, clean_rows, quarantine_rows, dedup_rows, gold_ready
        FROM meta.pipeline_runs WHERE batch_id = %s
        """,
        (batch_id,),
    )

    gold_counts = {
        table: fetch_one(f"SELECT count(*) AS c FROM gold.{table} WHERE batch_id = %s", (batch_id,))["c"]
        for table in ("ticket_volume_daily", "resolution_sla_summary", "category_breakdown")
    }

    category_breakdown = fetch_all(
        """
        SELECT canonical_category, classification_method, ticket_count
        FROM gold.category_breakdown WHERE batch_id = %s
        ORDER BY canonical_category, classification_method
        """,
        (batch_id,),
    )

    quality = fetch_all(
        """
        SELECT stage_name, check_name, status
        FROM meta.quality_results WHERE batch_id = %s
        ORDER BY stage_name, check_name
        """,
        (batch_id,),
    )

    agent = fetch_one(
        """
        SELECT COALESCE(sum(cache_hits), 0) AS cache_hits,
               COALESCE(sum(cache_misses), 0) AS cache_misses
        FROM meta.agent_runs WHERE batch_id = %s AND agent_name = 'semantic_classification'
        """,
        (batch_id,),
    )

    ai_methods = fetch_all(
        """
        SELECT method, count(*) AS c
        FROM silver.tickets_ai WHERE batch_id = %s
        GROUP BY method ORDER BY method
        """,
        (batch_id,),
    )

    return {
        "rows": {k: run[k] for k in ("total_rows", "clean_rows", "quarantine_rows", "dedup_rows")},
        "gold_counts": gold_counts,
        "category_breakdown": [dict(r) for r in category_breakdown],
        "quality_results": [dict(r) for r in quality],
        "agent_cache": dict(agent),
        "ai_methods": [dict(r) for r in ai_methods],
    }
