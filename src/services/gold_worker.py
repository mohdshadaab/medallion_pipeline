from src.config import get_settings
from src.db import fetch_one
from src.events import EventType, PipelineEvent, new_event
from src.gold import build_gold
from src.kafka_io import consume_events, publish_recorded_event
from src.logging_config import configure_logging
from src.services.agent_worker import run_once as run_agent_once
from src.workers import run_stage


def handle_event(event: PipelineEvent) -> PipelineEvent | None:
    def build(raw_event: PipelineEvent):
        counts = build_gold(raw_event.run_id, raw_event.batch_id)
        completion = new_event(
            EventType.PIPELINE_RUN_COMPLETED,
            raw_event.run_id,
            raw_event.batch_id,
            {"gold_ready": True, "gold_counts": counts},
            correlation_id=raw_event.correlation_id,
            idempotency_key=f"pipeline-complete:{raw_event.batch_id}:v1",
        )
        publish_recorded_event(completion)
        return (
            EventType.GOLD_PRODUCTS_BUILT,
            {"gold_counts": counts},
            sum(counts.values()),
        )

    return run_stage(
        event=event,
        consumer_name="gold-worker",
        expected_type=EventType.QUALITY_GATE_PASSED,
        stage_name="gold",
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
        (EventType.QUALITY_GATE_PASSED.value,),
    )
    event = PipelineEvent.model_validate(latest["payload"]) if latest else run_agent_once()
    result = handle_event(event)
    return result or new_event(
        EventType.GOLD_PRODUCTS_BUILT,
        event.run_id,
        event.batch_id,
        {"gold_counts": {}},
        correlation_id=event.correlation_id,
        idempotency_key=f"gold:{event.batch_id}:v1",
    )


def main() -> None:
    configure_logging()
    if get_settings().kafka_required:
        consume_events("gold-worker", handle_event)
    else:
        event = run_once()
        print(f"gold complete batch={event.batch_id} counts={event.payload.get('gold_counts', {})}")


if __name__ == "__main__":
    main()
