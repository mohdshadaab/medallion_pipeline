from pathlib import Path

from src.bronze import ingest_bronze
from src.config import get_settings
from src.db import fetch_one
from src.events import EventType, PipelineEvent, new_event
from src.kafka_io import consume_events
from src.logging_config import configure_logging
from src.services.file_discovery import discover_file
from src.workers import run_stage


def handle_event(event: PipelineEvent) -> PipelineEvent | None:
    def ingest(raw_event: PipelineEvent):
        source_path = Path(raw_event.payload.get("source_path") or get_settings().raw_tickets_path)
        source_file = raw_event.payload["source_file"]
        row_count = ingest_bronze(raw_event.run_id, raw_event.batch_id, source_path, source_file)
        return (
            EventType.BRONZE_BATCH_INGESTED,
            {
                "source_file": source_file,
                "row_count": row_count,
                "bronze_table": "bronze.raw_tickets",
            },
            row_count,
        )

    return run_stage(
        event=event,
        consumer_name="bronze-worker",
        expected_type=EventType.RAW_FILE_DISCOVERED,
        stage_name="bronze",
        handler=ingest,
    )


def run_once() -> PipelineEvent:
    latest = fetch_one(
        """
        SELECT payload FROM meta.outbox_events
        WHERE event_type = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (EventType.RAW_FILE_DISCOVERED.value,),
    )
    event = PipelineEvent.model_validate(latest["payload"]) if latest else discover_file()
    result = handle_event(event)
    if result:
        return result
    return new_event(
        EventType.BRONZE_BATCH_INGESTED,
        event.run_id,
        event.batch_id,
        {"source_file": event.payload["source_file"], "row_count": event.payload["row_count"]},
        correlation_id=event.correlation_id,
        idempotency_key=f"bronze:{event.batch_id}:v1",
    )


def main() -> None:
    configure_logging()
    if get_settings().kafka_required:
        consume_events("bronze-worker", handle_event)
    else:
        event = run_once()
        print(f"bronze complete batch={event.batch_id} rows={event.payload.get('row_count', 0)}")


if __name__ == "__main__":
    main()
