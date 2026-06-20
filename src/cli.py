import argparse
import logging
import re

from psycopg import sql
from psycopg.types.json import Jsonb

from src.db import fetch_all, fetch_one, init_db, transaction
from src.events import EventType, new_event
from src.kafka_io import publish_recorded_event
from src.logging_config import configure_logging, log_event


SAFE_MAPPING_TYPES = {"text": "TEXT", "numeric": "NUMERIC", "boolean": "BOOLEAN"}
logger = logging.getLogger(__name__)


def _validate_identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z_][a-z0-9_]{0,49}", value):
        raise SystemExit(f"Unsafe Silver column name: {value}")
    return value


def _validate_type(value: str) -> str:
    if value not in SAFE_MAPPING_TYPES:
        raise SystemExit(f"Unsupported schema mapping type: {value}")
    return SAFE_MAPPING_TYPES[value]


def run_pipeline() -> None:
    init_db()
    from src.services.agent_worker import run_once as run_agent_once
    from src.services.bronze_worker import run_once as run_bronze_once
    from src.services.file_discovery import discover_file
    from src.services.gold_worker import handle_event as handle_gold_event
    from src.services.silver_worker import run_once as run_silver_once

    discovery = discover_file()
    log_event(
        logger,
        "pipeline.run.start",
        run_id=discovery.run_id,
        batch_id=discovery.batch_id,
        source_file=discovery.payload.get("source_file"),
        row_count=discovery.payload.get("row_count"),
    )
    publish_recorded_event(discovery)
    bronze = run_bronze_once()
    silver = run_silver_once()
    gate = run_agent_once()
    if gate.event_type == EventType.QUALITY_GATE_FAILED:
        log_event(logger, "pipeline.quality_gate.failed", logging.ERROR, run_id=gate.run_id, batch_id=gate.batch_id)
        print(f"pipeline stopped at quality gate batch={gate.batch_id}")
        return
    gold = handle_gold_event(gate)
    if gold:
        log_event(logger, "pipeline.run.complete", run_id=gold.run_id, batch_id=gold.batch_id, gold_ready=True)
        print(f"pipeline complete batch={gold.batch_id}")
    else:
        log_event(logger, "pipeline.run.already_complete", run_id=gate.run_id, batch_id=gate.batch_id)
        print(f"pipeline already complete batch={gate.batch_id}")


def print_status() -> None:
    run = fetch_one(
        """
        SELECT * FROM meta.pipeline_runs
        ORDER BY started_at DESC
        LIMIT 1
        """
    )
    if not run:
        print("No pipeline runs found. Run `make run` first.")
        return
    print(f"Run: {run['batch_id']} status={run['status']} gold_ready={run['gold_ready']}")
    print(
        f"Rows: total={run['total_rows']} clean={run['clean_rows']} "
        f"quarantine={run['quarantine_rows']} dedup_removed={run['dedup_rows']}"
    )
    stages = fetch_all(
        """
        SELECT stage_name, status, duration_ms, input_count, output_count, error_count, is_late, error_message
        FROM meta.stage_runs
        WHERE run_id = %s
        ORDER BY started_at
        """,
        (run["run_id"],),
    )
    print("\nStages:")
    for stage in stages:
        late = " late" if stage["is_late"] else ""
        error = f" error={stage['error_message']}" if stage["error_message"] else ""
        print(
            f"- {stage['stage_name']}: {stage['status']} duration_ms={stage['duration_ms']} "
            f"in={stage['input_count']} out={stage['output_count']}{late}{error}"
        )
    quality = fetch_all(
        """
        SELECT stage_name, check_name, status, observed_value, expected_value
        FROM meta.quality_results
        WHERE run_id = %s
        ORDER BY stage_name, check_name
        """,
        (run["run_id"],),
    )
    print("\nQuality:")
    for check in quality:
        print(
            f"- {check['stage_name']}.{check['check_name']}: {check['status']} "
            f"observed={check['observed_value']} expected={check['expected_value']}"
        )
    dlq = fetch_one("SELECT count(*) AS count FROM meta.dlq_events WHERE run_id = %s", (run["run_id"],))
    agent = fetch_one(
        """
        SELECT COALESCE(sum(cost_usd), 0) AS cost,
               COALESCE(sum(token_count), 0) AS tokens,
               COALESCE(sum(cache_hits), 0) AS cache_hits,
               COALESCE(sum(cache_misses), 0) AS cache_misses
        FROM meta.agent_runs
        WHERE run_id = %s
        """,
        (run["run_id"],),
    )
    pending = fetch_one(
        "SELECT count(*) AS count FROM meta.agent_proposals WHERE run_id = %s AND status = 'pending'",
        (run["run_id"],),
    )
    print(f"\nDLQ events: {dlq['count']}")
    print(
        f"Agent: tokens={agent['tokens']} cost_usd={agent['cost']} "
        f"cache_hits={agent['cache_hits']} cache_misses={agent['cache_misses']}"
    )
    print(f"Pending proposals: {pending['count']}")


def list_proposals() -> None:
    rows = fetch_all(
        """
        SELECT proposal_id, batch_id, agent_name, proposal_type, proposal_json,
               risk_level, confidence, status, rationale
        FROM meta.agent_proposals
        ORDER BY created_at DESC
        """
    )
    if not rows:
        print("No proposals found.")
        return
    for row in rows:
        details = ""
        if row["proposal_type"] == "schema_mapping":
            payload = row["proposal_json"]
            details = (
                f" source={payload.get('source_column')} silver={payload.get('silver_column')}"
                f" type={payload.get('inferred_type')} gold={payload.get('gold_impact', {}).get('recommendation')}"
            )
        print(
            f"{row['proposal_id']} batch={row['batch_id']} agent={row['agent_name']} "
            f"type={row['proposal_type']} risk={row['risk_level']} confidence={row['confidence']} "
            f"status={row['status']}{details} rationale={row['rationale']}"
        )


def _create_gold_impact_proposal(proposal: dict) -> None:
    payload = proposal["proposal_json"]
    gold_impact = payload.get("gold_impact", {})
    if gold_impact.get("recommendation") == "no_gold_impact":
        return
    existing = fetch_one(
        """
        SELECT 1 FROM meta.agent_proposals
        WHERE proposal_type = 'gold_impact'
          AND proposal_json->>'silver_column' = %s
          AND status = 'pending'
        LIMIT 1
        """,
        (payload["silver_column"],),
    )
    if existing:
        return
    impact_payload = {
        "silver_column": payload["silver_column"],
        "source_column": payload["source_column"],
        "inferred_type": payload["inferred_type"],
        "recommendation": gold_impact.get("recommendation"),
        "rationale": gold_impact.get("rationale"),
        "proposed_action": "review_for_gold_contract",
    }
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO meta.agent_proposals (
                run_id, batch_id, agent_name, proposal_type, proposal_json,
                rationale, risk_level, confidence
            )
            VALUES (%s, %s, 'gold_suggester', 'gold_impact', %s, %s, 'low', %s)
            """,
            (
                proposal["run_id"],
                proposal["batch_id"],
                Jsonb(impact_payload),
                f"Approved Silver column `{payload['silver_column']}` may be useful in Gold.",
                proposal["confidence"],
            ),
        )


def _apply_schema_mapping(proposal: dict) -> None:
    payload = proposal["proposal_json"]
    silver_column = _validate_identifier(payload["silver_column"])
    db_type = _validate_type(payload["inferred_type"])
    with transaction() as conn:
        conn.execute(
            sql.SQL("ALTER TABLE silver.tickets ADD COLUMN IF NOT EXISTS {} {}").format(
                sql.Identifier(silver_column),
                sql.SQL(db_type),
            )
        )
        mapping = conn.execute(
            """
            INSERT INTO meta.schema_mappings (
                proposal_id, source_file, source_column, silver_column,
                source_path, inferred_type, nullable, status, rationale,
                risk_level, confidence, gold_impact, approved_at, applied_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'applied', %s, %s, %s, %s, now(), now())
            ON CONFLICT (source_file, source_column, silver_column)
            DO UPDATE SET inferred_type = EXCLUDED.inferred_type,
                          nullable = EXCLUDED.nullable,
                          status = 'applied',
                          rationale = EXCLUDED.rationale,
                          risk_level = EXCLUDED.risk_level,
                          confidence = EXCLUDED.confidence,
                          gold_impact = EXCLUDED.gold_impact,
                          approved_at = COALESCE(meta.schema_mappings.approved_at, now()),
                          applied_at = now()
            RETURNING mapping_id
            """,
            (
                proposal["proposal_id"],
                payload["source_file"],
                payload["source_column"],
                silver_column,
                payload["source_path"],
                payload["inferred_type"],
                payload.get("nullable", True),
                proposal["rationale"],
                proposal["risk_level"],
                proposal["confidence"],
                Jsonb(payload.get("gold_impact", {})),
            ),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO meta.schema_mapping_applications (mapping_id, batch_id, action, status)
            VALUES (%s, %s, 'alter_silver_add_column', 'succeeded')
            """,
            (mapping["mapping_id"], proposal["batch_id"]),
        )
        conn.execute(
            """
            UPDATE meta.agent_proposals
            SET status = 'applied', decided_at = now(), decision_note = 'approved and applied via CLI'
            WHERE proposal_id = %s
            """,
            (proposal["proposal_id"],),
        )
    _create_gold_impact_proposal(proposal)


def decide_proposal(proposal_id: str, status: str) -> None:
    proposal = fetch_one("SELECT * FROM meta.agent_proposals WHERE proposal_id = %s", (proposal_id,))
    if not proposal:
        raise SystemExit(f"Proposal not found: {proposal_id}")
    if status == "approved" and proposal["proposal_type"] == "schema_mapping":
        _apply_schema_mapping(proposal)
    else:
        if status == "approved" and proposal["proposal_type"] == "gold_impact":
            status = "accepted"
        with transaction() as conn:
            conn.execute(
                """
                UPDATE meta.agent_proposals
                SET status = %s, decided_at = now(), decision_note = %s
                WHERE proposal_id = %s
                """,
                (status, f"{status} via CLI", proposal_id),
            )
    event_status = "approved" if status in {"approved", "accepted"} else status
    event_type = EventType.AGENT_PROPOSAL_APPROVED if event_status == "approved" else EventType.AGENT_PROPOSAL_REJECTED
    event = new_event(
        event_type,
        proposal["run_id"],
        proposal["batch_id"],
        {"proposal_id": proposal_id, "status": event_status, "proposal_type": proposal["proposal_type"]},
        idempotency_key=f"proposal:{event_status}:{proposal_id}",
    )
    publish_recorded_event(event)
    print(f"{event_status} proposal {proposal_id}")


def main() -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description="Local medallion pipeline CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("db-init")
    sub.add_parser("run")
    sub.add_parser("status")
    sub.add_parser("proposals")
    approve = sub.add_parser("approve")
    approve.add_argument("proposal_id")
    reject = sub.add_parser("reject")
    reject.add_argument("proposal_id")
    args = parser.parse_args()

    if args.command == "db-init":
        init_db()
        print("database initialized")
    elif args.command == "run":
        run_pipeline()
    elif args.command == "status":
        print_status()
    elif args.command == "proposals":
        list_proposals()
    elif args.command == "approve":
        decide_proposal(args.proposal_id, "approved")
    elif args.command == "reject":
        decide_proposal(args.proposal_id, "rejected")


if __name__ == "__main__":
    main()
