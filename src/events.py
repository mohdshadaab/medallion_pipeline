from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Topic(StrEnum):
    EVENTS = "pipeline.events"
    RETRY = "pipeline.retry"
    DLQ = "pipeline.dlq"


class EventType(StrEnum):
    RAW_FILE_DISCOVERED = "RawFileDiscovered"
    BRONZE_BATCH_INGESTED = "BronzeBatchIngested"
    SILVER_BATCH_BUILT = "SilverBatchBuilt"
    AGENT_PROPOSAL_CREATED = "AgentProposalCreated"
    AGENT_PROPOSAL_APPROVED = "AgentProposalApproved"
    AGENT_PROPOSAL_REJECTED = "AgentProposalRejected"
    CLASSIFICATION_COMPLETED = "ClassificationCompleted"
    QUALITY_GATE_PASSED = "QualityGatePassed"
    QUALITY_GATE_FAILED = "QualityGateFailed"
    GOLD_PRODUCTS_BUILT = "GoldProductsBuilt"
    PIPELINE_RUN_COMPLETED = "PipelineRunCompleted"
    STAGE_FAILED = "StageFailed"


class PipelineEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    event_version: str = "v1"
    run_id: UUID
    batch_id: str
    correlation_id: UUID
    idempotency_key: str
    attempt: int = 1
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    payload: dict[str, Any]

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, value: str | bytes) -> "PipelineEvent":
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        return cls.model_validate_json(value)


def new_event(
    event_type: EventType,
    run_id: UUID,
    batch_id: str,
    payload: dict[str, Any],
    *,
    correlation_id: UUID | None = None,
    idempotency_key: str | None = None,
    attempt: int = 1,
) -> PipelineEvent:
    correlation = correlation_id or uuid4()
    return PipelineEvent(
        event_type=event_type,
        run_id=run_id,
        batch_id=batch_id,
        correlation_id=correlation,
        idempotency_key=idempotency_key or f"{event_type.value}:{batch_id}:v1",
        attempt=attempt,
        payload=payload,
    )
