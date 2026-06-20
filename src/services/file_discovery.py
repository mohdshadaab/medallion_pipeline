from pathlib import Path
from uuid import uuid4

from src.bronze import batch_id_for, count_data_rows, ensure_pipeline_run, read_header, record_source_schema
from src.config import get_settings
from src.events import EventType, PipelineEvent, new_event
from src.kafka_io import publish_recorded_event
from src.logging_config import configure_logging, log_event


import logging

logger = logging.getLogger(__name__)


def discover_file(path: Path | None = None) -> PipelineEvent:
    settings = get_settings()
    source_path = path or settings.raw_tickets_path
    source_file = source_path.name
    source_schema_name = settings.source_file_name or source_file
    batch_id = batch_id_for(source_path)
    row_count = count_data_rows(source_path)
    header = read_header(source_path)
    drift = record_source_schema(source_schema_name, batch_id, header)
    log_event(
        logger,
        "file.discovery.complete",
        source_file=source_file,
        source_schema_name=source_schema_name,
        batch_id=batch_id,
        row_count=row_count,
        schema_drift=drift["status"],
        added_columns=drift["added_columns"],
        removed_columns=drift["removed_columns"],
    )
    run_id = ensure_pipeline_run(
        uuid4(),
        batch_id,
        source_file,
        row_count,
        {
            "header": header,
            "source_schema_name": source_schema_name,
            "schema_drift": drift["status"],
            "schema_drift_details": drift,
        },
    )
    return new_event(
        EventType.RAW_FILE_DISCOVERED,
        run_id,
        batch_id,
        {
            "source_file": source_file,
            "source_schema_name": source_schema_name,
            "source_path": str(source_path),
            "row_count": row_count,
            "header": header,
            "schema_drift": drift["status"],
            "schema_drift_details": drift,
        },
        idempotency_key=f"discovery:{batch_id}:v1",
    )


def main() -> None:
    configure_logging()
    event = discover_file()
    publish_recorded_event(event)
    print(f"discovered {event.payload['source_file']} batch={event.batch_id} rows={event.payload['row_count']}")


if __name__ == "__main__":
    main()
