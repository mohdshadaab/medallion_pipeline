from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime
import logging
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from src.config import get_settings
from src.db import fetch_one, transaction
from src.events import EventType, PipelineEvent, Topic, new_event
from src.kafka_io import publish_recorded_event, record_dlq
from src.logging_config import log_event


logger = logging.getLogger(__name__)


def already_processed(event: PipelineEvent, consumer_name: str) -> bool:
    row = fetch_one(
        "SELECT 1 FROM meta.processed_events WHERE event_id = %s AND consumer_name = %s",
        (event.event_id, consumer_name),
    )
    return row is not None


def mark_processed(event: PipelineEvent, consumer_name: str) -> None:
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO meta.processed_events (event_id, event_type, consumer_name, run_id, batch_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (event.event_id, event.event_type.value, consumer_name, event.run_id, event.batch_id),
        )


@contextmanager
def stage_run(run_id: UUID, batch_id: str, stage_name: str, input_count: int = 0):
    started_at = datetime.now(UTC)
    log_event(logger, "stage.start", run_id=run_id, batch_id=batch_id, stage=stage_name, input_count=input_count)
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO meta.stage_runs (run_id, batch_id, stage_name, status, started_at, input_count)
            VALUES (%s, %s, %s, 'running', %s, %s)
            ON CONFLICT (run_id, stage_name)
            DO UPDATE SET status = 'running', started_at = EXCLUDED.started_at,
                          finished_at = NULL, error_message = NULL,
                          input_count = EXCLUDED.input_count
            """,
            (run_id, batch_id, stage_name, started_at, input_count),
        )
    try:
        yield
    except Exception as exc:
        finish_stage(run_id, stage_name, "failed", started_at, error_message=str(exc), error_count=1)
        raise


def finish_stage(
    run_id: UUID,
    stage_name: str,
    status: str,
    started_at: datetime,
    *,
    output_count: int = 0,
    error_count: int = 0,
    error_message: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    finished_at = datetime.now(UTC)
    duration_ms = int((finished_at - started_at).total_seconds() * 1000)
    is_late = duration_ms > get_settings().pipeline_stage_sla_seconds * 1000
    level = logging.ERROR if status == "failed" else logging.INFO
    log_event(
        logger,
        f"stage.{status}",
        level,
        run_id=run_id,
        stage=stage_name,
        duration_ms=duration_ms,
        output_count=output_count,
        error_count=error_count,
        is_late=is_late,
        error_message=error_message,
    )
    with transaction() as conn:
        conn.execute(
            """
            UPDATE meta.stage_runs
            SET status = %s, finished_at = %s, duration_ms = %s, output_count = %s,
                error_count = %s, error_message = %s, is_late = %s, metadata = %s
            WHERE run_id = %s AND stage_name = %s
            """,
            (
                status,
                finished_at,
                duration_ms,
                output_count,
                error_count,
                error_message,
                is_late,
                Jsonb(metadata or {}),
                run_id,
                stage_name,
            ),
        )


def run_stage(
    *,
    event: PipelineEvent,
    consumer_name: str,
    expected_type: EventType,
    stage_name: str,
    handler: Callable[[PipelineEvent], tuple[EventType | None, dict[str, Any], int]],
) -> PipelineEvent | None:
    if event.event_type != expected_type:
        log_event(
            logger,
            "event.ignored",
            consumer=consumer_name,
            expected_type=expected_type.value,
            event_type=event.event_type.value,
            event_id=event.event_id,
            batch_id=event.batch_id,
        )
        return None
    if already_processed(event, consumer_name):
        log_event(
            logger,
            "event.duplicate_skipped",
            consumer=consumer_name,
            event_type=event.event_type.value,
            event_id=event.event_id,
            batch_id=event.batch_id,
        )
        return None
    started_at = datetime.now(UTC)
    with stage_run(event.run_id, event.batch_id, stage_name):
        try:
            next_type, payload, output_count = handler(event)
            mark_processed(event, consumer_name)
            finish_stage(event.run_id, stage_name, "succeeded", started_at, output_count=output_count)
            if not next_type:
                return None
            next_event = new_event(
                next_type,
                event.run_id,
                event.batch_id,
                payload,
                correlation_id=event.correlation_id,
                idempotency_key=f"{stage_name}:{event.batch_id}:v1",
            )
            publish_recorded_event(next_event, Topic.EVENTS)
            return next_event
        except Exception as exc:
            fail_event = new_event(
                EventType.STAGE_FAILED,
                event.run_id,
                event.batch_id,
                {"stage": stage_name, "error": str(exc)},
                correlation_id=event.correlation_id,
                idempotency_key=f"{stage_name}:failed:{event.batch_id}:v1",
            )
            publish_recorded_event(fail_event, Topic.DLQ)
            record_dlq(event, Topic.DLQ.value, event.model_dump(mode="json"), str(exc), event.attempt)
            raise
