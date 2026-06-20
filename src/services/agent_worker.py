import logging
from typing import TypedDict

from langgraph.graph import END, StateGraph

from src.agents.classify_agent import classify_batch_sync
from src.agents.dq_agent import propose_quality_rules
from src.config import get_settings
from src.db import fetch_one
from src.events import EventType, PipelineEvent, new_event
from src.kafka_io import consume_events, publish_recorded_event
from src.logging_config import configure_logging, log_event
from src.services.silver_worker import run_once as run_silver_once
from src.workers import run_stage


logger = logging.getLogger(__name__)


class AgentState(TypedDict):
    event: PipelineEvent
    dq: dict[str, object]
    classification: dict[str, object]


def _run_langgraph(event: PipelineEvent) -> AgentState:
    def dq_node(state: AgentState) -> AgentState:
        state["dq"] = propose_quality_rules(event.run_id, event.batch_id)
        return state

    def classify_node(state: AgentState) -> AgentState:
        state["classification"] = classify_batch_sync(event.run_id, event.batch_id)
        return state

    graph = StateGraph(AgentState)
    graph.add_node("dq", dq_node)
    graph.add_node("classify", classify_node)
    graph.set_entry_point("dq")
    graph.add_edge("dq", "classify")
    graph.add_edge("classify", END)
    return graph.compile().invoke({"event": event, "dq": {}, "classification": {}})


def _quality_gate_passed(run_id, batch_id: str) -> bool:
    failed = fetch_one(
        """
        SELECT 1 FROM meta.quality_results
        WHERE run_id = %s AND batch_id = %s AND status = 'failed'
        LIMIT 1
        """,
        (run_id, batch_id),
    )
    return failed is None


def handle_event(event: PipelineEvent) -> PipelineEvent | None:
    def run_agents(raw_event: PipelineEvent):
        state = _run_langgraph(raw_event)
        classification = state["classification"]
        classification_event = new_event(
            EventType.CLASSIFICATION_COMPLETED,
            raw_event.run_id,
            raw_event.batch_id,
            classification,
            correlation_id=raw_event.correlation_id,
            idempotency_key=f"classification:{raw_event.batch_id}:v1",
        )
        publish_recorded_event(classification_event)
        gate_type = EventType.QUALITY_GATE_PASSED if _quality_gate_passed(raw_event.run_id, raw_event.batch_id) else EventType.QUALITY_GATE_FAILED
        log_event(
            logger,
            "agent.quality_gate.result",
            logging.INFO if gate_type == EventType.QUALITY_GATE_PASSED else logging.ERROR,
            run_id=raw_event.run_id,
            batch_id=raw_event.batch_id,
            gate=gate_type.value,
        )
        return (
            gate_type,
            {
                "dq": state["dq"],
                "classification": classification,
                "silver_table": "silver.tickets",
                "ai_table": "silver.tickets_ai",
            },
            int(classification.get("classified", 0)),
        )

    return run_stage(
        event=event,
        consumer_name="agent-worker",
        expected_type=EventType.SILVER_BATCH_BUILT,
        stage_name="agent",
        handler=run_agents,
    )


def run_once() -> PipelineEvent:
    latest = fetch_one(
        """
        SELECT payload FROM meta.outbox_events
        WHERE event_type = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (EventType.SILVER_BATCH_BUILT.value,),
    )
    event = PipelineEvent.model_validate(latest["payload"]) if latest else run_silver_once()
    result = handle_event(event)
    return result or new_event(
        EventType.QUALITY_GATE_PASSED,
        event.run_id,
        event.batch_id,
        {"silver_table": "silver.tickets", "ai_table": "silver.tickets_ai"},
        correlation_id=event.correlation_id,
        idempotency_key=f"quality:{event.batch_id}:v1",
    )


def main() -> None:
    configure_logging()
    if get_settings().kafka_required:
        consume_events("agent-worker", handle_event)
    else:
        event = run_once()
        print(f"agent complete batch={event.batch_id} gate={event.event_type.value}")


if __name__ == "__main__":
    main()
