from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from src.db import transaction


def record_quality_result(
    run_id: UUID,
    batch_id: str,
    stage_name: str,
    check_name: str,
    status: str,
    *,
    observed_value: float | None = None,
    expected_value: float | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO meta.quality_results (
                run_id, batch_id, stage_name, check_name, status,
                observed_value, expected_value, details
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, stage_name, check_name)
            DO UPDATE SET status = EXCLUDED.status,
                          observed_value = EXCLUDED.observed_value,
                          expected_value = EXCLUDED.expected_value,
                          details = EXCLUDED.details,
                          created_at = now()
            """,
            (
                run_id,
                batch_id,
                stage_name,
                check_name,
                status,
                observed_value,
                expected_value,
                Jsonb(details or {}),
            ),
        )


def record_lineage(
    run_id: UUID,
    batch_id: str,
    source_table: str,
    target_table: str,
    transform_name: str,
    row_count: int,
    metadata: dict[str, Any] | None = None,
) -> None:
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO meta.lineage (
                run_id, batch_id, source_table, target_table,
                transform_name, row_count, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, source_table, target_table, transform_name)
            DO UPDATE SET row_count = EXCLUDED.row_count,
                          metadata = EXCLUDED.metadata,
                          created_at = now()
            """,
            (run_id, batch_id, source_table, target_table, transform_name, row_count, Jsonb(metadata or {})),
        )
