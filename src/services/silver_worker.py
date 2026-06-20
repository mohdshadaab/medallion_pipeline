from src.config import get_settings
from src.db import fetch_one
from src.events import EventType, PipelineEvent, new_event
from src.kafka_io import consume_events
from src.logging_config import configure_logging
from src.silver import rebuild_silver
from src.services.bronze_worker import run_once as run_bronze_once
from src.workers import run_stage


def handle_event(event: PipelineEvent) -> PipelineEvent | None:
    def build(raw_event: PipelineEvent):
        counts = rebuild_silver(raw_event.run_id, raw_event.batch_id)
        return (
            EventType.SILVER_BATCH_BUILT,
            {
                "silver_table": "silver.tickets",
                "clean_count": counts["clean"],
                "quarantine_count": counts["quarantine"],
                "dedup_removed": counts["dedup_removed"],
                "bronze_count": counts["bronze"],
            },
            counts["clean"],
        )

    return run_stage(
        event=event,
        consumer_name="silver-worker",
        expected_type=EventType.BRONZE_BATCH_INGESTED,
        stage_name="silver",
        handler=build,
    )


def run_once() -> PipelineEvent:
    latest = fetch_one(
        """
        SELECT payload FROM meta.outbox_events
        WHERE event_type = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (EventType.BRONZE_BATCH_INGESTED.value,),
    )
    event = PipelineEvent.model_validate(latest["payload"]) if latest else run_bronze_once()
    result = handle_event(event)
    return result or new_event(
        EventType.SILVER_BATCH_BUILT,
        event.run_id,
        event.batch_id,
        {"silver_table": "silver.tickets"},
        correlation_id=event.correlation_id,
        idempotency_key=f"silver:{event.batch_id}:v1",
    )


def main() -> None:
    configure_logging()
    if get_settings().kafka_required:
        consume_events("silver-worker", handle_event)
    else:
        event = run_once()
        print(f"silver complete batch={event.batch_id} clean={event.payload.get('clean_count', 0)}")


if __name__ == "__main__":
    main()
