import logging
from uuid import UUID

from src.db import transaction
from src.logging_config import log_event
from src.quality import record_lineage, record_quality_result


logger = logging.getLogger(__name__)


def build_gold(run_id: UUID, batch_id: str) -> dict[str, int]:
    with transaction() as conn:
        conn.execute("DELETE FROM gold.ticket_volume_daily WHERE batch_id = %s", (batch_id,))
        conn.execute("DELETE FROM gold.resolution_sla_summary WHERE batch_id = %s", (batch_id,))
        conn.execute("DELETE FROM gold.category_breakdown WHERE batch_id = %s", (batch_id,))

        volume = conn.execute(
            """
            INSERT INTO gold.ticket_volume_daily (
                batch_id, ticket_date, status_normalized, priority_normalized,
                category_normalized, assigned_to_normalized, ticket_count
            )
            SELECT batch_id,
                   created_at::date AS ticket_date,
                   status_normalized,
                   priority_normalized,
                   category_normalized,
                   assigned_to_normalized,
                   count(*)::int
            FROM silver.tickets
            WHERE batch_id = %s AND created_at IS NOT NULL
            GROUP BY batch_id, created_at::date, status_normalized,
                     priority_normalized, category_normalized, assigned_to_normalized
            """,
            (batch_id,),
        ).rowcount

        sla = conn.execute(
            """
            WITH base AS (
                SELECT *,
                       EXTRACT(EPOCH FROM (resolved_at - created_at)) / 3600.0 AS resolution_hours,
                       (
                           created_at IS NOT NULL
                           AND resolved_at IS NOT NULL
                           AND sla_hours IS NOT NULL
                           AND resolved_at >= created_at
                           AND NOT (validation_flags ? 'resolved_before_created')
                           AND NOT (sla_flags ? 'sentinel_sla')
                       ) AS eligible
                FROM silver.tickets
                WHERE batch_id = %s
            ),
            grouped AS (
                SELECT batch_id,
                       priority_normalized,
                       category_normalized,
                       assigned_to_normalized,
                       count(*)::int AS total_tickets,
                       count(*) FILTER (WHERE eligible)::int AS eligible_tickets,
                       count(*) FILTER (WHERE eligible AND resolution_hours <= sla_hours)::int AS sla_met_count,
                       count(*) FILTER (WHERE eligible AND resolution_hours > sla_hours)::int AS sla_breached_count,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY resolution_hours) FILTER (WHERE eligible) AS median_resolution_hours,
                       percentile_cont(0.95) WITHIN GROUP (ORDER BY resolution_hours) FILTER (WHERE eligible) AS p95_resolution_hours,
                       count(*) FILTER (WHERE NOT eligible)::int AS excluded_count
                FROM base
                GROUP BY batch_id, priority_normalized, category_normalized, assigned_to_normalized
            )
            INSERT INTO gold.resolution_sla_summary (
                batch_id, priority_normalized, category_normalized, assigned_to_normalized,
                total_tickets, eligible_tickets, sla_met_count, sla_breached_count,
                sla_met_rate, median_resolution_hours, p95_resolution_hours, excluded_count
            )
            SELECT batch_id,
                   priority_normalized,
                   category_normalized,
                   assigned_to_normalized,
                   total_tickets,
                   eligible_tickets,
                   sla_met_count,
                   sla_breached_count,
                   CASE WHEN eligible_tickets = 0 THEN 0 ELSE sla_met_count::numeric / eligible_tickets END,
                   median_resolution_hours,
                   p95_resolution_hours,
                   excluded_count
            FROM grouped
            """,
            (batch_id,),
        ).rowcount

        categories = conn.execute(
            """
            INSERT INTO gold.category_breakdown (
                batch_id, canonical_category, classification_method, ticket_count, avg_confidence
            )
            SELECT t.batch_id,
                   COALESCE(ai.canonical_value, t.category_normalized) AS canonical_category,
                   COALESCE(ai.method, 'dictionary') AS classification_method,
                   count(*)::int AS ticket_count,
                   avg(COALESCE(ai.confidence, 1.0)) AS avg_confidence
            FROM silver.tickets t
            LEFT JOIN silver.tickets_ai ai
              ON ai.silver_id = t.silver_id
             AND ai.batch_id = t.batch_id
             AND ai.source_column = 'category'
            WHERE t.batch_id = %s
            GROUP BY t.batch_id, COALESCE(ai.canonical_value, t.category_normalized), COALESCE(ai.method, 'dictionary')
            """,
            (batch_id,),
        ).rowcount

        conn.execute(
            """
            UPDATE meta.pipeline_runs
            SET status = 'completed', finished_at = now(), gold_ready = TRUE
            WHERE run_id = %s
            """,
            (run_id,),
        )

    record_quality_result(
        run_id,
        batch_id,
        "gold",
        "gold_products_built",
        "passed",
        observed_value=volume + sla + categories,
        details={"ticket_volume_daily": volume, "resolution_sla_summary": sla, "category_breakdown": categories},
    )
    record_lineage(run_id, batch_id, "silver.tickets", "gold.*", "gold_rebuild", volume + sla + categories)
    log_event(
        logger,
        "gold.build.complete",
        run_id=run_id,
        batch_id=batch_id,
        ticket_volume_daily=volume,
        resolution_sla_summary=sla,
        category_breakdown=categories,
        sla_invalid_inputs_excluded=True,
    )
    return {"ticket_volume_daily": volume, "resolution_sla_summary": sla, "category_breakdown": categories}
