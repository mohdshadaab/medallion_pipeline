from collections.abc import Callable
import logging
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer
from psycopg.types.json import Jsonb

from src.config import get_settings
from src.db import transaction
from src.events import PipelineEvent, Topic
from src.logging_config import log_event


logger = logging.getLogger(__name__)


def _producer() -> Producer:
    return Producer({"bootstrap.servers": get_settings().kafka_bootstrap_servers})


def publish_event(event: PipelineEvent, topic: Topic = Topic.EVENTS) -> bool:
    settings = get_settings()
    try:
        producer = _producer()
        producer.produce(topic.value, key=event.batch_id, value=event.to_json())
        producer.flush(10)
        mark_outbox_published(event.event_id)
        log_event(
            logger,
            "event.publish.succeeded",
            topic=topic.value,
            event_type=event.event_type.value,
            event_id=event.event_id,
            batch_id=event.batch_id,
        )
        return True
    except Exception as exc:
        if settings.kafka_required:
            log_event(
                logger,
                "event.publish.failed",
                logging.ERROR,
                topic=topic.value,
                event_type=event.event_type.value,
                event_id=event.event_id,
                batch_id=event.batch_id,
                error_message=str(exc),
            )
            raise
        log_event(
            logger,
            "event.publish.skipped",
            logging.WARNING,
            topic=topic.value,
            event_type=event.event_type.value,
            event_id=event.event_id,
            batch_id=event.batch_id,
            error_message=str(exc),
        )
        return False


def consume_events(consumer_name: str, event_handler: Callable[[PipelineEvent], None]) -> None:
    settings = get_settings()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": consumer_name,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([Topic.EVENTS.value])
    log_event(logger, "consumer.start", consumer=consumer_name, topic=Topic.EVENTS.value)
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            try:
                event = PipelineEvent.from_json(message.value())
                log_event(
                    logger,
                    "event.consume.received",
                    consumer=consumer_name,
                    event_type=event.event_type.value,
                    event_id=event.event_id,
                    batch_id=event.batch_id,
                )
                event_handler(event)
                consumer.commit(message)
            except Exception as exc:
                log_event(logger, "event.consume.failed", logging.ERROR, consumer=consumer_name, error_message=str(exc))
                record_dlq(None, Topic.DLQ.value, message.value(), str(exc))
                consumer.commit(message)
    finally:
        consumer.close()


def record_outbox(event: PipelineEvent, topic: Topic = Topic.EVENTS) -> None:
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO meta.stage_events (
                event_id, event_type, event_version, run_id, batch_id,
                correlation_id, idempotency_key, attempt, occurred_at, payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (idempotency_key) DO NOTHING
            """,
            (
                event.event_id,
                event.event_type.value,
                event.event_version,
                event.run_id,
                event.batch_id,
                event.correlation_id,
                event.idempotency_key,
                event.attempt,
                event.occurred_at,
                Jsonb(event.payload),
            ),
        )
        log_event(
            logger,
            "event.outbox.recorded",
            topic=topic.value,
            event_type=event.event_type.value,
            event_id=event.event_id,
            batch_id=event.batch_id,
        )
        conn.execute(
            """
            INSERT INTO meta.outbox_events (event_id, topic, event_type, run_id, batch_id, payload)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                event.event_id,
                topic.value,
                event.event_type.value,
                event.run_id,
                event.batch_id,
                Jsonb(event.model_dump(mode="json")),
            ),
        )


def mark_outbox_published(event_id: Any) -> None:
    with transaction() as conn:
        conn.execute(
            """
            UPDATE meta.outbox_events
            SET status = 'published', attempts = attempts + 1, published_at = now()
            WHERE event_id = %s
            """,
            (event_id,),
        )
        conn.execute(
            "UPDATE meta.stage_events SET published_at = now() WHERE event_id = %s",
            (event_id,),
        )


def record_dlq(event: PipelineEvent | None, topic: str, original_event: Any, error_message: str, attempts: int = 0) -> None:
    payload = original_event
    if isinstance(original_event, bytes):
        payload = original_event.decode("utf-8", errors="replace")
    if isinstance(original_event, str):
        payload = {"raw": original_event}
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO meta.dlq_events (
                event_id, event_type, topic, run_id, batch_id,
                original_event, error_message, attempts
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event.event_id if event else None,
                event.event_type.value if event else None,
                topic,
                event.run_id if event else None,
                event.batch_id if event else None,
                Jsonb(event.model_dump(mode="json") if event else payload),
                error_message,
                attempts,
            ),
        )
    log_event(
        logger,
        "event.dlq.recorded",
        logging.ERROR,
        topic=topic,
        event_type=event.event_type.value if event else None,
        event_id=event.event_id if event else None,
        batch_id=event.batch_id if event else None,
        attempts=attempts,
        error_message=error_message,
    )


def publish_recorded_event(event: PipelineEvent, topic: Topic = Topic.EVENTS) -> bool:
    record_outbox(event, topic)
    return publish_event(event, topic)
