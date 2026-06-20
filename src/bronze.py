import csv
import hashlib
import json
import logging
from pathlib import Path
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from src.db import fetch_one, transaction
from src.logging_config import log_event
from src.quality import record_lineage, record_quality_result


logger = logging.getLogger(__name__)

SOURCE_COLUMNS = [
    "ticket_id",
    "created_at",
    "resolved_at",
    "category",
    "priority",
    "status",
    "building",
    "description",
    "submitted_by",
    "assigned_to",
    "resolution_notes",
    "cost",
    "sla_hours",
]


def file_fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def batch_id_for(path: Path) -> str:
    return f"{path.stem}_{file_fingerprint(path)[:12]}"


def read_header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        return next(reader)


def count_data_rows(path: Path) -> int:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def record_source_schema(source_file: str, batch_id: str, header: list[str]) -> dict[str, Any]:
    header_hash = hashlib.sha256(json.dumps(header, sort_keys=True).encode("utf-8")).hexdigest()
    prior = fetch_one(
        """
        SELECT header, header_hash FROM meta.source_schemas
        WHERE source_file = %s AND batch_id <> %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (source_file, batch_id),
    )
    prior_header = prior["header"] if prior else []
    added_columns = sorted(set(header) - set(prior_header)) if prior else []
    removed_columns = sorted(set(prior_header) - set(header)) if prior else []
    common_columns = [column for column in header if column in set(prior_header)] if prior else []
    drift_status = "baseline"
    if prior and prior["header_hash"] != header_hash:
        drift_status = "changed"
    elif prior:
        drift_status = "unchanged"
    drift_details = {
        "previous_header": prior_header,
        "current_header": header,
        "added_columns": added_columns,
        "removed_columns": removed_columns,
        "common_columns": common_columns,
    }
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO meta.source_schemas (
                source_file, batch_id, header, header_hash, added_columns,
                removed_columns, common_columns, drift_details, drift_status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_file, batch_id)
            DO UPDATE SET header = EXCLUDED.header,
                          header_hash = EXCLUDED.header_hash,
                          added_columns = EXCLUDED.added_columns,
                          removed_columns = EXCLUDED.removed_columns,
                          common_columns = EXCLUDED.common_columns,
                          drift_details = EXCLUDED.drift_details,
                          drift_status = EXCLUDED.drift_status
            """,
            (
                source_file,
                batch_id,
                Jsonb(header),
                header_hash,
                Jsonb(added_columns),
                Jsonb(removed_columns),
                Jsonb(common_columns),
                Jsonb(drift_details),
                drift_status,
            ),
        )
    result = {
        "status": drift_status,
        "added_columns": added_columns,
        "removed_columns": removed_columns,
        "common_columns": common_columns,
    }
    log_event(
        logger,
        "bronze.schema_drift.recorded",
        source_file=source_file,
        batch_id=batch_id,
        drift_status=drift_status,
        added_columns=added_columns,
        removed_columns=removed_columns,
    )
    return result


def ensure_pipeline_run(run_id: UUID, batch_id: str, source_file: str, row_count: int, metadata: dict[str, Any]) -> UUID:
    existing = fetch_one("SELECT run_id FROM meta.pipeline_runs WHERE batch_id = %s", (batch_id,))
    if existing:
        run_id = existing["run_id"]
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO meta.pipeline_runs (run_id, batch_id, source_file, status, started_at, total_rows, metadata)
            VALUES (%s, %s, %s, 'running', now(), %s, %s)
            ON CONFLICT (batch_id)
            DO UPDATE SET status = 'running',
                          started_at = now(),
                          finished_at = NULL,
                          total_rows = EXCLUDED.total_rows,
                          gold_ready = FALSE,
                          error_message = NULL,
                          metadata = EXCLUDED.metadata
            """,
            (run_id, batch_id, source_file, row_count, Jsonb(metadata)),
        )
    return run_id


def _row_hash(row: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _clean_row(row: dict[str, Any]) -> tuple[dict[str, str | None], dict[str, Any]]:
    extra_fields: dict[str, Any] = {}
    if None in row:
        extra_fields["_extra_values"] = row.pop(None)
    known = {column: row.get(column) for column in SOURCE_COLUMNS}
    for key, value in row.items():
        if key not in SOURCE_COLUMNS:
            extra_fields[key] = value
    return known, extra_fields


def ingest_bronze(run_id: UUID, batch_id: str, source_path: Path, source_file: str) -> int:
    inserted_or_existing = 0
    with source_path.open(newline="", encoding="utf-8-sig") as handle, transaction() as conn:
        reader = csv.DictReader(handle)
        for raw_line_no, raw_row in enumerate(reader, start=2):
            raw_payload = dict(raw_row)
            known, extra_fields = _clean_row(dict(raw_row))
            result = conn.execute(
                """
                INSERT INTO bronze.raw_tickets (
                    source_file, raw_line_no, batch_id, content_hash,
                    raw_payload, extra_fields, ticket_id, created_at, resolved_at,
                    category, priority, status, building, description,
                    submitted_by, assigned_to, resolution_notes, cost, sla_hours
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (source_file, raw_line_no) DO UPDATE
                SET batch_id = EXCLUDED.batch_id,
                    content_hash = EXCLUDED.content_hash,
                    raw_payload = EXCLUDED.raw_payload,
                    extra_fields = EXCLUDED.extra_fields
                RETURNING bronze_id
                """,
                (
                    source_file,
                    raw_line_no,
                    batch_id,
                    _row_hash(raw_payload),
                    Jsonb(raw_payload),
                    Jsonb(extra_fields),
                    *[known[column] for column in SOURCE_COLUMNS],
                ),
            )
            if result.fetchone():
                inserted_or_existing += 1
    expected = count_data_rows(source_path)
    status = "passed" if inserted_or_existing == expected else "failed"
    record_quality_result(
        run_id,
        batch_id,
        "bronze",
        "bronze_row_count_matches_source",
        status,
        observed_value=inserted_or_existing,
        expected_value=expected,
    )
    record_lineage(run_id, batch_id, source_file, "bronze.raw_tickets", "bronze_ingest", inserted_or_existing)
    log_event(
        logger,
        "bronze.ingest.complete",
        run_id=run_id,
        batch_id=batch_id,
        source_file=source_file,
        rows=inserted_or_existing,
        expected_rows=expected,
        quality_status=status,
    )
    return inserted_or_existing
