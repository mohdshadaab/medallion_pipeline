import logging
import re
from decimal import Decimal, InvalidOperation
from uuid import UUID

from psycopg.types.json import Jsonb

from src.config import get_settings
from src.db import fetch_all, fetch_one, transaction
from src.logging_config import log_event


logger = logging.getLogger(__name__)


SAFE_TYPES = {"text", "numeric", "boolean"}
CORE_SOURCE_COLUMNS = {
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
}


def _sanitize_column(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", name.strip().lower()).strip("_")
    cleaned = re.sub(r"_+", "_", cleaned)
    if not cleaned:
        cleaned = "source_field"
    if cleaned[0].isdigit():
        cleaned = f"source_{cleaned}"
    return cleaned[:50]


def _infer_type(values: list[str]) -> str:
    non_empty = [value.strip() for value in values if value and value.strip()]
    if not non_empty:
        return "text"
    bool_values = {"true", "false", "t", "f", "yes", "no", "y", "n", "1", "0"}
    if all(value.lower() in bool_values for value in non_empty):
        return "boolean"
    try:
        for value in non_empty:
            Decimal(value.replace(",", ""))
        return "numeric"
    except (InvalidOperation, ValueError):
        return "text"


def _gold_impact(source_column: str, inferred_type: str, sample_values: list[str]) -> dict[str, str]:
    column = source_column.lower()
    distinct_sample_count = len({value for value in sample_values if value})
    if inferred_type == "numeric":
        return {
            "recommendation": "metric_candidate",
            "rationale": "Numeric fields may become Gold metrics only after business meaning is confirmed.",
        }
    if any(token in column for token in ["tier", "region", "segment", "team", "type"]) or distinct_sample_count <= 10:
        return {
            "recommendation": "dimension_candidate",
            "rationale": "Low-cardinality fields can be useful Gold dimensions after review.",
        }
    return {
        "recommendation": "no_gold_impact",
        "rationale": "Field does not look like a stable reporting dimension or metric.",
    }


def _suspected_pii(source_column: str) -> bool:
    column = source_column.lower()
    return any(token in column for token in ["email", "phone", "person", "name", "ssn"])


def _schema_exists(source_file: str, source_column: str) -> bool:
    mapping = fetch_one(
        """
        SELECT 1 FROM meta.schema_mappings
        WHERE source_file = %s AND source_column = %s
        LIMIT 1
        """,
        (source_file, source_column),
    )
    proposal = fetch_one(
        """
        SELECT 1 FROM meta.agent_proposals
        WHERE proposal_type = 'schema_mapping'
          AND proposal_json->>'source_column' = %s
          AND status = 'pending'
        LIMIT 1
        """,
        (source_column,),
    )
    return mapping is not None or proposal is not None


def _create_schema_mapping_proposals(run_id: UUID, batch_id: str) -> list[dict[str, object]]:
    schema = fetch_one(
        """
        SELECT source_file, added_columns
        FROM meta.source_schemas
        WHERE batch_id = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (batch_id,),
    )
    if not schema:
        return []
    source_file = schema["source_file"]
    added_columns = schema["added_columns"] or []
    proposals: list[dict[str, object]] = []
    for source_column in added_columns:
        if source_column in CORE_SOURCE_COLUMNS:
            continue
        if _schema_exists(source_file, source_column):
            continue
        samples = fetch_all(
            """
            SELECT extra_fields->>%s AS value
            FROM bronze.raw_tickets
            WHERE batch_id = %s AND extra_fields ? %s
            LIMIT 25
            """,
            (source_column, batch_id, source_column),
        )
        sample_values = [row["value"] for row in samples if row["value"] is not None]
        total = fetch_one(
            "SELECT count(*) AS count FROM bronze.raw_tickets WHERE batch_id = %s",
            (batch_id,),
        )["count"]
        present = fetch_one(
            "SELECT count(*) AS count FROM bronze.raw_tickets WHERE batch_id = %s AND extra_fields ? %s",
            (batch_id, source_column),
        )["count"]
        inferred_type = _infer_type(sample_values)
        silver_column = _sanitize_column(source_column)
        pii = _suspected_pii(source_column)
        gold_impact = _gold_impact(source_column, inferred_type, sample_values)
        proposal = {
            "source_file": source_file,
            "source_column": source_column,
            "silver_column": silver_column,
            "source_path": f"extra_fields.{source_column}",
            "inferred_type": inferred_type,
            "nullable": True,
            "null_rate": float(1 - (present / total)) if total else 0.0,
            "sample_values": sample_values[:10],
            "suspected_pii": pii,
            "recommended_action": "monitor" if pii else "promote",
            "gold_impact": gold_impact,
        }
        risk_level = "high" if pii else "low"
        with transaction() as conn:
            conn.execute(
                """
                INSERT INTO meta.agent_proposals (
                    run_id, batch_id, agent_name, proposal_type, proposal_json,
                    rationale, risk_level, confidence
                )
                VALUES (%s, %s, 'data_quality', 'schema_mapping', %s, %s, %s, %s)
                """,
                (
                    run_id,
                    batch_id,
                    Jsonb(proposal),
                    f"New Bronze column `{source_column}` was detected and can be governed into Silver.",
                    risk_level,
                    0.85,
                ),
            )
        log_event(
            logger,
            "agent.schema_mapping.proposed",
            run_id=run_id,
            batch_id=batch_id,
            source_column=source_column,
            silver_column=silver_column,
            inferred_type=inferred_type,
            risk_level=risk_level,
            gold_impact=gold_impact.get("recommendation"),
        )
        proposals.append(proposal)
    return proposals


def propose_quality_rules(run_id: UUID, batch_id: str) -> dict[str, object]:
    settings = get_settings()
    counts = fetch_one(
        """
        SELECT total_rows, clean_rows, quarantine_rows, dedup_rows, metadata
        FROM meta.pipeline_runs
        WHERE run_id = %s
        """,
        (run_id,),
    ) or {"total_rows": 0, "clean_rows": 0, "quarantine_rows": 0, "dedup_rows": 0, "metadata": {}}
    total = counts["total_rows"] or 0
    quarantine_rate = (counts["quarantine_rows"] or 0) / total if total else 0
    risk_level = "high" if quarantine_rate > 0.05 else "low"
    proposal = {
        "rules": [
            {
                "name": "exclude_invalid_ticket_sla_inputs",
                "description": "Exclude missing, sentinel, ambiguous, or reversed timestamp rows from SLA metrics.",
                "action": "already_applied_in_gold",
            },
            {
                "name": "monitor_quarantine_rate",
                "description": "Review source parsing when quarantine rate exceeds 5%.",
                "threshold": 0.05,
                "observed": quarantine_rate,
            },
        ],
        "schema_drift": (counts.get("metadata") or {}).get("schema_drift", "unknown"),
    }
    schema_mapping_proposals = _create_schema_mapping_proposals(run_id, batch_id)
    proposal["schema_mapping_proposals"] = schema_mapping_proposals
    status = "proposal_created" if risk_level == "high" else "observed"
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO meta.agent_runs (
                run_id, batch_id, agent_name, status, prompt_version, model,
                output_json, confidence, risk_level, finished_at
            )
            VALUES (%s, %s, 'data_quality', 'succeeded', %s, %s, %s, %s, %s, now())
            """,
            (
                run_id,
                batch_id,
                settings.prompt_version,
                settings.openai_model,
                Jsonb(proposal),
                0.9,
                risk_level,
            ),
        )
        existing = fetch_one(
            """
            SELECT 1 FROM meta.agent_proposals
            WHERE run_id = %s
              AND agent_name = 'data_quality'
              AND proposal_type = 'quality_rule'
              AND status = 'pending'
            LIMIT 1
            """,
            (run_id,),
        )
        if risk_level == "high" and not existing:
            conn.execute(
                """
                INSERT INTO meta.agent_proposals (
                    run_id, batch_id, agent_name, proposal_type, proposal_json,
                    rationale, risk_level, confidence
                )
                VALUES (%s, %s, 'data_quality', 'quality_rule', %s, %s, %s, %s)
                """,
                (
                    run_id,
                    batch_id,
                    Jsonb(proposal),
                    "Quarantine rate crossed the local review threshold.",
                    risk_level,
                    0.9,
                ),
            )
    log_event(
        logger,
        "agent.dq.complete",
        run_id=run_id,
        batch_id=batch_id,
        status=status,
        risk_level=risk_level,
        quarantine_rate=f"{quarantine_rate:.4f}",
        schema_mapping_proposals=len(schema_mapping_proposals),
    )
    return {"status": status, "risk_level": risk_level, "quarantine_rate": quarantine_rate}
