import hashlib
import logging
import re
from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

from dateutil import parser
from psycopg import sql
from psycopg.types.json import Jsonb

from src.db import fetch_all, transaction
from src.logging_config import log_event
from src.quality import record_lineage, record_quality_result


logger = logging.getLogger(__name__)

VALID_TICKET_ID = re.compile(r"^TKT-\d+$")

CATEGORY_MAP = {
    "a/c": "hvac",
    "ac": "hvac",
    "hvac": "hvac",
    "electrical": "electrical",
    "power issue": "electrical",
    "elec": "electrical",
    "plumbing": "plumbing",
    "fire safety": "fire_safety",
    "fire/safety": "fire_safety",
    "sprinkler": "fire_safety",
    "badge/access": "access",
    "access": "access",
    "it/network": "it_network",
    "network": "it_network",
    "vertical transport": "vertical_transport",
    "elevator/escalator": "vertical_transport",
    "housekeeping": "housekeeping",
    "pest": "pest_control",
    "other": "other",
}

PRIORITY_MAP = {
    "lo": "low",
    "low": "low",
    "normal": "medium",
    "med": "medium",
    "medium": "medium",
    "high": "high",
    "critical": "critical",
}

STATUS_MAP = {
    "open": "open",
    "in progress": "in_progress",
    "pending vendor": "pending_vendor",
    "escalated": "escalated",
    "resolved": "resolved",
    "closed": "resolved",
}


def _blank_to_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _norm_key(value: Any) -> str:
    text = _blank_to_none(value)
    return re.sub(r"\s+", " ", text.lower()) if text else ""


def normalize_category(value: Any) -> str:
    key = _norm_key(value)
    return CATEGORY_MAP.get(key, key.replace("/", "_").replace(" ", "_") or "unknown")


def normalize_priority(value: Any) -> str:
    return PRIORITY_MAP.get(_norm_key(value), "unknown")


def normalize_status(value: Any) -> str:
    return STATUS_MAP.get(_norm_key(value), "unknown")


def normalize_assignee(value: Any) -> str:
    key = _norm_key(value)
    if not key:
        return "unassigned"
    if "in-house" in key or "maintenance" in key or "overnight" in key:
        return "internal_team"
    if "vendor" in key or "services" in key or "electric" in key or "mechanical" in key or "pestpro" in key:
        return "vendor"
    return key.replace(" ", "_")


def parse_timestamp(value: Any) -> tuple[datetime | None, list[str]]:
    text = _blank_to_none(value)
    flags: list[str] = []
    if not text:
        return None, ["missing_date"]
    if re.match(r"^\d{1,2}/\d{1,2}/\d{4}", text):
        month, day, *_ = re.split(r"[/-]", text)
        if int(month) <= 12 and int(day) <= 12:
            flags.append("ambiguous_us_mm_dd")
    try:
        parsed = parser.parse(text, dayfirst=False, fuzzy=False)
    except (ValueError, TypeError, OverflowError):
        return None, ["invalid_date"]
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC), flags


def parse_cost(value: Any) -> tuple[Decimal | None, list[str]]:
    text = _blank_to_none(value)
    if not text:
        return None, ["missing_cost"]
    cleaned = text.replace("$", "").replace(",", "").strip()
    if cleaned.upper() in {"N/A", "NA", "TBD"}:
        return None, ["non_numeric_cost"]
    try:
        parsed = Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None, ["invalid_cost"]
    if parsed < 0:
        return None, ["negative_cost"]
    return parsed, []


def parse_sla(value: Any) -> tuple[int | None, list[str]]:
    text = _blank_to_none(value)
    if not text:
        return None, ["missing_sla"]
    try:
        parsed = int(float(text))
    except ValueError:
        return None, ["invalid_sla"]
    if parsed in {0, 999}:
        return None, ["sentinel_sla"]
    if parsed < 0:
        return None, ["negative_sla"]
    return parsed, []


def business_key(row: dict[str, Any], created_at: datetime | None) -> str:
    ticket_id = _blank_to_none(row.get("ticket_id"))
    if ticket_id and VALID_TICKET_ID.match(ticket_id):
        return ticket_id
    basis = "|".join(
        [
            created_at.date().isoformat() if created_at else "",
            _norm_key(row.get("building")),
            _norm_key(row.get("submitted_by")),
            _norm_key(row.get("description"))[:200],
        ]
    )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def transform_row(row: dict[str, Any]) -> dict[str, Any]:
    created_at, created_flags = parse_timestamp(row.get("created_at"))
    resolved_at, resolved_flags = parse_timestamp(row.get("resolved_at"))
    cost, cost_flags = parse_cost(row.get("cost"))
    sla_hours, sla_flags = parse_sla(row.get("sla_hours"))
    validation_flags: list[str] = []

    ticket_id = _blank_to_none(row.get("ticket_id"))
    if ticket_id and not VALID_TICKET_ID.match(ticket_id):
        validation_flags.append("invalid_ticket_id_format")
    if not created_at:
        validation_flags.append("missing_or_invalid_created_at")
    if resolved_at and created_at and resolved_at < created_at:
        validation_flags.append("resolved_before_created")
    if not _blank_to_none(row.get("description")) and not _blank_to_none(row.get("category")):
        validation_flags.append("missing_description_and_category")

    return {
        "bronze_id": row["bronze_id"],
        "source_file": row["source_file"],
        "raw_line_no": row["raw_line_no"],
        "business_key": business_key(row, created_at),
        "ticket_id": ticket_id,
        "created_at_raw": row.get("created_at"),
        "resolved_at_raw": row.get("resolved_at"),
        "created_at": created_at,
        "resolved_at": resolved_at,
        "date_flags": created_flags + resolved_flags,
        "category_raw": row.get("category"),
        "category_normalized": normalize_category(row.get("category")),
        "priority_raw": row.get("priority"),
        "priority_normalized": normalize_priority(row.get("priority")),
        "status_raw": row.get("status"),
        "status_normalized": normalize_status(row.get("status")),
        "building": _blank_to_none(row.get("building")),
        "description": _blank_to_none(row.get("description")),
        "submitted_by": _blank_to_none(row.get("submitted_by")),
        "assigned_to_raw": row.get("assigned_to"),
        "assigned_to_normalized": normalize_assignee(row.get("assigned_to")),
        "resolution_notes": _blank_to_none(row.get("resolution_notes")),
        "cost_raw": row.get("cost"),
        "cost": cost,
        "cost_flags": cost_flags,
        "sla_hours_raw": row.get("sla_hours"),
        "sla_hours": sla_hours,
        "sla_flags": sla_flags,
        "validation_flags": validation_flags,
        "raw_payload": row["raw_payload"],
    }


def _is_quarantine(row: dict[str, Any]) -> tuple[bool, str | None, str | None]:
    flags = set(row["validation_flags"])
    if "missing_or_invalid_created_at" in flags:
        return True, "invalid_created_at", "created_at could not be parsed"
    if "missing_description_and_category" in flags:
        return True, "missing_business_fields", "both description and category are missing"
    return False, None, None


def _dynamic_value_sql(inferred_type: str) -> tuple[sql.SQL, int]:
    if inferred_type == "numeric":
        return (
            sql.SQL(
                """
                CASE
                    WHEN (b.extra_fields ->> %s) ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    THEN (b.extra_fields ->> %s)::numeric
                    ELSE NULL
                END
                """
            ),
            2,
        )
    if inferred_type == "boolean":
        return (
            sql.SQL(
                """
                CASE lower(b.extra_fields ->> %s)
                    WHEN 'true' THEN TRUE
                    WHEN 't' THEN TRUE
                    WHEN 'yes' THEN TRUE
                    WHEN 'y' THEN TRUE
                    WHEN '1' THEN TRUE
                    WHEN 'false' THEN FALSE
                    WHEN 'f' THEN FALSE
                    WHEN 'no' THEN FALSE
                    WHEN 'n' THEN FALSE
                    WHEN '0' THEN FALSE
                    ELSE NULL
                END
                """
            ),
            1,
        )
    return sql.SQL("b.extra_fields ->> %s"), 1


def _populate_dynamic_columns(run_id: UUID, batch_id: str) -> int:
    mappings = fetch_all(
        """
        SELECT mapping_id, source_column, silver_column, inferred_type
        FROM meta.schema_mappings
        WHERE status = 'applied'
        ORDER BY created_at
        """
    )
    total_updates = 0
    for mapping in mappings:
        expression, source_param_count = _dynamic_value_sql(mapping["inferred_type"])
        query = (
            sql.SQL("UPDATE silver.tickets s SET {} = ").format(sql.Identifier(mapping["silver_column"]))
            + expression
            + sql.SQL(
                """
                FROM bronze.raw_tickets b
                WHERE s.bronze_id = b.bronze_id
                  AND s.batch_id = %s
                  AND b.extra_fields ? %s
                """
            )
        )
        params = [mapping["source_column"]] * source_param_count + [batch_id, mapping["source_column"]]
        with transaction() as conn:
            result = conn.execute(query, params)
            affected = result.rowcount or 0
            total_updates += affected
            conn.execute(
                """
                INSERT INTO meta.schema_mapping_applications (
                    mapping_id, run_id, batch_id, action, status, affected_rows
                )
                VALUES (%s, %s, %s, 'populate_silver_column', 'succeeded', %s)
                """,
                (mapping["mapping_id"], run_id, batch_id, affected),
            )
        log_event(
            logger,
            "silver.dynamic_mapping.applied",
            run_id=run_id,
            batch_id=batch_id,
            source_column=mapping["source_column"],
            silver_column=mapping["silver_column"],
            inferred_type=mapping["inferred_type"],
            affected_rows=affected,
        )
        record_quality_result(
            run_id,
            batch_id,
            "silver",
            f"dynamic_mapping_{mapping['silver_column']}",
            "passed",
            observed_value=affected,
            details={
                "source_column": mapping["source_column"],
                "silver_column": mapping["silver_column"],
                "inferred_type": mapping["inferred_type"],
            },
        )
    return total_updates


def rebuild_silver(run_id: UUID, batch_id: str) -> dict[str, int]:
    bronze_rows = fetch_all(
        "SELECT * FROM bronze.raw_tickets WHERE batch_id = %s ORDER BY raw_line_no",
        (batch_id,),
    )
    transformed = [transform_row(row) for row in bronze_rows]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in transformed:
        grouped[row["business_key"]].append(row)

    clean_count = quarantine_count = dup_count = 0
    with transaction() as conn:
        conn.execute("DELETE FROM silver.tickets_ai WHERE batch_id = %s", (batch_id,))
        conn.execute("DELETE FROM silver.tickets_dups WHERE batch_id = %s", (batch_id,))
        conn.execute("DELETE FROM silver.tickets_quarantine WHERE batch_id = %s", (batch_id,))
        conn.execute("DELETE FROM silver.tickets WHERE batch_id = %s", (batch_id,))
        for rows in grouped.values():
            canonical = rows[0]
            is_quarantine, reason_code, reason_detail = _is_quarantine(canonical)
            if is_quarantine:
                quarantine_count += len(rows)
                for row in rows:
                    conn.execute(
                        """
                        INSERT INTO silver.tickets_quarantine (
                            bronze_id, run_id, batch_id, source_file, raw_line_no,
                            reason_code, reason_detail, raw_payload
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (batch_id, source_file, raw_line_no) DO NOTHING
                        """,
                        (
                            row["bronze_id"],
                            run_id,
                            batch_id,
                            row["source_file"],
                            row["raw_line_no"],
                            reason_code,
                            reason_detail,
                            Jsonb(row["raw_payload"]),
                        ),
                    )
                continue

            clean_count += 1
            conn.execute(
                """
                INSERT INTO silver.tickets (
                    bronze_id, run_id, batch_id, source_file, raw_line_no, business_key,
                    ticket_id, created_at_raw, resolved_at_raw, created_at, resolved_at,
                    date_flags, category_raw, category_normalized, priority_raw,
                    priority_normalized, status_raw, status_normalized, building,
                    description, submitted_by, assigned_to_raw, assigned_to_normalized,
                    resolution_notes, cost_raw, cost, cost_flags, sla_hours_raw,
                    sla_hours, sla_flags, validation_flags
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    canonical["bronze_id"],
                    run_id,
                    batch_id,
                    canonical["source_file"],
                    canonical["raw_line_no"],
                    canonical["business_key"],
                    canonical["ticket_id"],
                    canonical["created_at_raw"],
                    canonical["resolved_at_raw"],
                    canonical["created_at"],
                    canonical["resolved_at"],
                    Jsonb(canonical["date_flags"]),
                    canonical["category_raw"],
                    canonical["category_normalized"],
                    canonical["priority_raw"],
                    canonical["priority_normalized"],
                    canonical["status_raw"],
                    canonical["status_normalized"],
                    canonical["building"],
                    canonical["description"],
                    canonical["submitted_by"],
                    canonical["assigned_to_raw"],
                    canonical["assigned_to_normalized"],
                    canonical["resolution_notes"],
                    canonical["cost_raw"],
                    canonical["cost"],
                    Jsonb(canonical["cost_flags"]),
                    canonical["sla_hours_raw"],
                    canonical["sla_hours"],
                    Jsonb(canonical["sla_flags"]),
                    Jsonb(canonical["validation_flags"]),
                ),
            )
            for duplicate in rows[1:]:
                dup_count += 1
                conn.execute(
                    """
                    INSERT INTO silver.tickets_dups (
                        bronze_id, run_id, batch_id, business_key,
                        canonical_bronze_id, reason_code, raw_payload
                    )
                    VALUES (%s, %s, %s, %s, %s, 'duplicate_business_key', %s)
                    ON CONFLICT (batch_id, bronze_id) DO NOTHING
                    """,
                    (
                        duplicate["bronze_id"],
                        run_id,
                        batch_id,
                        duplicate["business_key"],
                        canonical["bronze_id"],
                        Jsonb(duplicate["raw_payload"]),
                    ),
                )
        conn.execute(
            """
            UPDATE meta.pipeline_runs
            SET clean_rows = %s, quarantine_rows = %s, dedup_rows = %s
            WHERE run_id = %s
            """,
            (clean_count, quarantine_count, dup_count, run_id),
        )

    dynamic_updates = _populate_dynamic_columns(run_id, batch_id)
    bronze_count = len(bronze_rows)
    reconciled = clean_count + quarantine_count + dup_count
    record_quality_result(
        run_id,
        batch_id,
        "silver",
        "bronze_clean_quarantine_dedup_reconciliation",
        "passed" if bronze_count == reconciled else "failed",
        observed_value=reconciled,
        expected_value=bronze_count,
        details={"clean": clean_count, "quarantine": quarantine_count, "dedup_removed": dup_count},
    )
    reconciliation_status = "passed" if bronze_count == reconciled else "failed"
    log_event(
        logger,
        "silver.reconciliation",
        logging.INFO if reconciliation_status == "passed" else logging.ERROR,
        run_id=run_id,
        batch_id=batch_id,
        status=reconciliation_status,
        bronze=bronze_count,
        clean=clean_count,
        quarantine=quarantine_count,
        dedup_removed=dup_count,
        dynamic_column_updates=dynamic_updates,
    )
    record_lineage(
        run_id,
        batch_id,
        "bronze.raw_tickets",
        "silver.tickets",
        "silver_rebuild",
        clean_count,
        {"dynamic_column_updates": dynamic_updates},
    )
    return {"clean": clean_count, "quarantine": quarantine_count, "dedup_removed": dup_count, "bronze": bronze_count}
