# AI-Assisted Medallion Pipeline

Local event-driven medallion pipeline for messy support-ticket data. The implementation keeps the take-home scope lean: Docker Compose, PostgreSQL, Kafka/KRaft, Python workers, Pydantic JSON events, concurrent (asyncio) LLM classification, and LangGraph only inside the agent worker.

> For the full assignment write-up — requirement-by-requirement coverage, real run numbers, and the honest agent assessment — see [SOLUTION.md](SOLUTION.md). Design rationale lives in [plans/medallion_pipeline.md](plans/medallion_pipeline.md) (frozen local design), [plans/concurrent_classification.md](plans/concurrent_classification.md) (the concurrency refactor), and [plans/enterprise_scale.md](plans/enterprise_scale.md) (production scale).

## How To Run

**Prerequisite:** Docker + Docker Compose. An `OPENAI_API_KEY` is optional (see degraded mode below). Copy `.env.example` to `.env` first if you want to set one.

```bash
make up        # start postgres + kafka (KRaft)
make db-init   # create bronze / silver / gold / meta schemas
make run       # run the whole pipeline end-to-end
make status    # durable run health from the meta.* tables
```

`make run` is the single command for an end-to-end run.

### Two execution modes

The same stage logic runs in either of two topologies:

| Mode | Command | How stages advance | When to use |
|---|---|---|---|
| **In-process (default)** | `make run` | One process chains the stages directly ([src/cli.py](src/cli.py) `run_pipeline`). Events, outbox, `processed_events`, and idempotent writes are still recorded. | Deterministic, single-command end-to-end run. Recommended for grading/demo. |
| **Evented (Kafka consumers)** | set `KAFKA_REQUIRED=true` in `.env`, then `make run-evented` | Long-running worker containers consume Kafka events at-least-once with idempotent writes; `file-discovery` publishes the first event. | Demonstrates the real worker topology. |

> **Important:** the workers only become long-running Kafka consumers when `KAFKA_REQUIRED=true`. With the shipped default (`KAFKA_REQUIRED=false`), `make run-evented` starts each worker, runs it once, and exits — use `make run` for that single-process path.

### Optional commands

```bash
make build                       # rebuild the app image
make eval                        # classification accuracy + format-compliance harness
make run-drift                   # ingest the schema-drift fixture (adds customer_tier)
make proposals                   # list pending agent proposals (HITL)
make approve ID=<proposal_id>    # approve + apply a proposal
make reject ID=<proposal_id>     # reject a proposal
make logs                        # tail worker container logs
make clean                       # tear down containers + volumes
```

### Degraded mode (no API key)

The pipeline runs without `OPENAI_API_KEY`. In that mode, semantic classification falls back to deterministic keyword heuristics and still writes auditable agent/cache/cost metadata, so a full run completes offline.

## What A Run Produces

A clean degraded run over the provided `raw_tickets.csv` (pinned in [tests/baseline/golden.json](tests/baseline/golden.json)):

- **10,280** Bronze rows → **9,749 clean** / **531 quarantined** / **0 deduped** in Silver.
- Gold: `ticket_volume_daily` ≈ **9,701** rows, `resolution_sla_summary` ≈ **580** rows, `category_breakdown` = **30** rows.
- Reconciliation gate passes: `bronze = clean + quarantine + dedup_removed`.

(`dedup = 0` is expected for this dataset — see the Silver note below.)

## Architecture

```mermaid
flowchart TD
    RawCsv["raw_tickets.csv"] --> FileDiscovery["file-discovery"]
    FileDiscovery -->|"RawFileDiscovered"| KafkaEvents["Kafka: pipeline.events"]
    KafkaEvents --> BronzeWorker["bronze-worker"]
    BronzeWorker --> Bronze["bronze.raw_tickets"]
    BronzeWorker -->|"BronzeBatchIngested"| KafkaEvents
    KafkaEvents --> SilverWorker["silver-worker"]
    SilverWorker --> Silver["silver.tickets + quarantine + dups"]
    SilverWorker -->|"SilverBatchBuilt"| KafkaEvents
    KafkaEvents --> AgentWorker["agent-worker (LangGraph: DQ + classify)"]
    AgentWorker --> AgentMeta["meta.agent_runs + agent_cache"]
    AgentWorker --> AiTable["silver.tickets_ai"]
    AgentWorker -->|"QualityGatePassed"| KafkaEvents
    KafkaEvents --> GoldWorker["gold-worker"]
    GoldWorker --> Gold["gold.ticket_volume_daily + SLA + category_breakdown"]
    Cli["make status / approvals"] --> Meta["meta control tables"]
```

The diagram is the logical stage flow. `make run` executes it in one process; the evented mode runs each box as a Kafka consumer container. Either way, **Kafka carries batch/stage events, not row events**, and **Postgres is the durable source of truth** for Bronze, Silver, Gold, and metadata.

## Configuration

Settings load from `.env` (see [.env.example](.env.example)) into [src/config.py](src/config.py):

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgresql://medallion:medallion@postgres:5432/medallion` | Postgres connection |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka brokers |
| `KAFKA_REQUIRED` | `false` | `true` makes workers long-running Kafka consumers (see modes above) |
| `RAW_TICKETS_PATH` | `/app/raw_tickets.csv` | Source file ingested by Bronze |
| `OPENAI_API_KEY` | _(empty)_ | When set, classification uses the model; empty → heuristic degraded mode |
| `OPENAI_MODEL` | `gpt-4o-mini` | Classification model |
| `CLASSIFY_CONCURRENCY` | `8` | Bounded concurrency for the classification stage |
| `OPENAI_MAX_RETRIES` | `5` | SDK retry budget for 429s under concurrency |
| `PROMPT_VERSION` | `v1` | Part of the LLM cache key; bump to invalidate the cache |
| `PIPELINE_STAGE_SLA_SECONDS` | `300` | Threshold above which a stage is flagged `is_late` |
| `LOG_LEVEL` / `LOG_FORMAT` | `INFO` / `human` | Logging verbosity and format (`human` or `json`) |

## Event Contracts

Topics:

- `pipeline.events`: normal stage events and commands.
- `pipeline.retry`: reserved for transient retry events.
- `pipeline.dlq`: poison events with original payload and error metadata.

Every event includes:

- `event_id`
- `event_type`
- `event_version`
- `run_id`
- `batch_id`
- `correlation_id`
- `idempotency_key`
- `attempt`
- `occurred_at`
- `payload`

Consumers use at-least-once delivery and idempotent writes. A consumer records `meta.processed_events` only after its DB work succeeds.

## Medallion Layers

### Bronze

`bronze.raw_tickets` stores every physical CSV row with:

- All source columns as text.
- `source_file`, `raw_line_no`, `batch_id`, `ingested_at`.
- `content_hash` as a change fingerprint.
- Full `raw_payload` JSONB and `extra_fields` JSONB.

Bronze does not clean, normalize, or drop records. The identity is `(source_file, raw_line_no)` so reruns do not duplicate physical rows. `batch_id` is `<stem>_<sha256(file)[:12]>`, so the same file content always maps to the same batch.

Schema drift is captured automatically. When a new CSV column appears, Bronze stores it in `raw_payload` and `extra_fields`, and `meta.source_schemas` records added, removed, and common columns for the batch.

### Silver

`silver.tickets` contains typed, cleaned, deduplicated records. Field-level anomalies are kept as JSON flags. Structurally unusable rows go to `silver.tickets_quarantine`; duplicate losers go to `silver.tickets_dups`. Every transformation reconciles `bronze = clean + quarantine + dedup_removed` as a quality check.

Cleaning rules:

- Dates use a documented US `MM/DD` policy. Ambiguous and invalid dates are flagged and never silently feed SLA math.
- `resolved_at < created_at` is flagged and excluded from SLA metrics.
- Costs strip `$` and map `-1`, negatives, `N/A`, and `TBD` to NULL plus flags.
- SLA values `0`, `999`, invalid, and missing values are excluded from Gold SLA metrics.
- Dedup uses valid `ticket_id` (`^TKT-\d+$`) first, then a deterministic fallback fingerprint of cleaned date/building/submitter/description.

> **Dedup on this dataset:** dedup removes **0** rows. The logic is real and tested, but the provided data's duplicates are *semantic* (rows that say "Duplicate of ticket #…" in `resolution_notes` while carrying distinct `ticket_id`s and timestamps), not business-key collisions. Catching those would need fuzzy/embedding matching — noted as future work, not claimed.

### Gold

Gold products are deterministic ([src/gold.py](src/gold.py)):

- `gold.ticket_volume_daily`: ticket volume by day, status, priority, category, and assigned team.
- `gold.resolution_sla_summary`: SLA met rate, breach count, median/p95 resolution hours, and an explicit excluded-row count — computed only from eligible (valid, non-sentinel, non-reversed) rows.
- `gold.category_breakdown`: canonical category distribution with a `classification_method` dimension (`dictionary` / `llm` / `cache` / `heuristic`) so enrichment provenance is visible.

Gold tables are intentionally hand-written SQL, not agent-generated — Gold is a reporting contract and should not silently change under an agent.

## Agents

The pipeline follows one pattern: **agents propose → deterministic code validates and applies → humans approve high-risk changes**. Because the agents reason over the pipeline's own metadata (not free text), three of the four need no LLM — **only the Semantic Classification Agent calls a model.** This maps to the take-home's four options as follows:

| Option | Status | Uses LLM? | Where |
|---|---|---|---|
| (a) Schema Inference & Evolution | Built | No (deterministic profiler) | proposal in [src/agents/dq_agent.py](src/agents/dq_agent.py), DDL in [src/cli.py](src/cli.py), backfill in [src/silver.py](src/silver.py) |
| (b) Data Quality | Built | No | [src/agents/dq_agent.py](src/agents/dq_agent.py) |
| (c) Semantic Classification | Built | **Yes** (heuristic fallback) | [src/agents/classify_agent.py](src/agents/classify_agent.py) + [src/llm.py](src/llm.py) |
| (d) Gold Layer Design | Light | No | `gold_impact` proposals in [src/cli.py](src/cli.py); the static suggester in [src/agents/gold_suggester.py](src/agents/gold_suggester.py) is not wired into the run |

Full honest assessment (what saved real work, what didn't) is in [SOLUTION.md](SOLUTION.md).

### Semantic Classification Agent

Input: distinct dirty category/text values from Silver. Output: canonical category, confidence, method, model, prompt version, token/cost metadata, and cache status in `silver.tickets_ai`.

How it stays cheap and fast:

- A **dictionary/regex pre-pass** resolves known categories with zero model calls.
- The model is asked only about **distinct unknown** texts — never per row.
- A **bulk cache lookup** then **bounded-concurrency** `asyncio.gather` (`CLASSIFY_CONCURRENCY`) over the misses; results persist in one short transaction *after* the network calls (no DB transaction held across an API call).
- Results cache by `hash(model + prompt_version + normalized_input)`, so reruns re-hit cache with no repeat spend.
- Output uses OpenAI Structured Outputs + Pydantic at `temperature=0`; confidence `< 0.7` maps to `unclassified` so low-confidence guesses never reach Gold.

On the provided data this means ~3,785 distinct model calls cover 9,749 rows (45% are resolved by the dictionary alone).

### Data Quality + Schema Inference/Evolution Agent

Input: run counts, quarantine rate, schema-drift metadata. Output: auditable rule proposals and schema-mapping proposals in `meta.agent_runs` / `meta.agent_proposals`. It escalates a human proposal when the quarantine rate crosses 5%, and for each new Bronze column it infers a type, sanitizes a Silver column name, flags suspected PII, and proposes a governed mapping (see Schema Drift Promotion). Deterministic code applies the actual rules and DDL.

## Schema Drift Promotion

New source columns are not promoted silently. The pipeline uses a governed human-in-the-loop flow:

1. Bronze ingests the new column into `raw_payload` and `extra_fields`.
2. `meta.source_schemas` records drift details, including `added_columns`.
3. The Data Quality Agent samples the new field and creates a `schema_mapping` proposal.
4. A human reviews the proposal with `make proposals`.
5. Approval adds a real nullable column to `silver.tickets` and records the mapping in `meta.schema_mappings`.
6. The next Silver rebuild populates the approved column from Bronze.
7. If the field looks useful for reporting, the agent creates a separate `gold_impact` proposal. Gold tables are not changed automatically because they are reporting contracts.

Demo with the included drift fixture:

```bash
make clean
make up
make db-init
make run
make run-drift
make proposals
make approve ID=<schema_mapping_proposal_id>
make run-drift
```

After approval, inspect the promoted Silver column:

```bash
docker compose run --rm app python - <<'PY'
from src.db import fetch_all

rows = fetch_all(
    "SELECT ticket_id, customer_tier FROM silver.tickets WHERE customer_tier IS NOT NULL ORDER BY ticket_id"
)
for row in rows:
    print(row)
PY
```

Use `make proposals` again to review any `gold_impact` proposal. Approving a Gold impact proposal records the decision, but changing Gold tables still requires deterministic code changes.

## Observability And SLA

Local observability is metadata-first:

- `meta.pipeline_runs`: end-to-end run status and counts.
- `meta.stage_runs`: stage status, row counts, duration, late flag, and errors.
- `meta.quality_results`: reconciliation, parse, SLA exclusion, and Gold-readiness checks.
- `meta.lineage`: table-level lineage (source → target → transform → row count) written by every stage.
- `meta.outbox_events`, `meta.processed_events`, `meta.dlq_events`: event delivery and failure evidence.
- `meta.agent_runs`, `meta.agent_cache`: model, prompt version, confidence, cost, tokens, and cache behavior.

`make status` summarizes the latest run health, stage durations, quality failures, DLQ count, agent cost/cache stats, pending proposals, and Gold readiness.

Business SLA is measured only from clean Silver inputs. Pipeline SLA is represented by stage/run duration and late-stage flags (`is_late` vs `PIPELINE_STAGE_SLA_SECONDS`).

## Logging And Status

Runtime logs go to stdout/stderr so they appear in the terminal during `make run` and in Docker logs for worker containers. Logs use concise key-value messages and avoid secrets, raw payload JSON, full ticket descriptions, and prompt text.

Use runtime logs while the pipeline is running:

```bash
make run
```

Use the durable status view after a run:

```bash
make status
```

Use Docker logs for long-running worker services:

```bash
make logs
```

The logs show stage starts/completions, row counts, reconciliation results, schema drift details, agent mode, cache hits/misses, token/cost usage, event publish/outbox behavior, DLQ writes, and Gold product counts. `make status` remains the authoritative summary backed by the `meta.*` tables.

## Tests

```bash
docker compose run --rm app python -m pytest -q
```

The suite covers the async classification primitive, the concurrency bound and partial-failure isolation, and the synchronous DB helpers. The behavior-parity test ([tests/test_pipeline_parity.py](tests/test_pipeline_parity.py)) compares a fresh degraded run against the committed golden baseline ([tests/baseline/golden.json](tests/baseline/golden.json)) — including the per-row `classification_method` split — so run it after a clean degraded `make run`.

## Failure Handling

- Duplicate event: skipped by `meta.processed_events`.
- Consumer crash before offset commit: Kafka redelivers; database constraints keep writes idempotent.
- DB commit succeeds but Kafka publish fails: `meta.outbox_events` preserves the event for republish.
- Poison event: recorded in `meta.dlq_events`.
- Invalid agent output: recorded as agent failure and never mutates Silver or Gold.
- Quality gate failure: Gold does not build.

## What Changes At 100x Scale

- Move Bronze/Silver from Postgres to Delta Lake or Iceberg on object storage.
- Keep Postgres as the control plane for run state, approvals, idempotency, costs, and agent metadata.
- Add Schema Registry for event contracts.
- Use Debezium CDC for the outbox relay.
- Partition Kafka topics by tenant/source/domain.
- Use Spark, Flink, Dagster, dbt, or managed lakehouse pipelines for transforms.
- Add OpenTelemetry, Prometheus/Grafana, centralized logs, and alerting.
- Add a catalog/lineage system such as Unity Catalog, DataHub, Atlan, or OpenLineage.
- Use batch/asynchronous LLM enrichment (OpenAI Batch API) with tenant budgets and stronger eval gates.

Full production design is in [plans/enterprise_scale.md](plans/enterprise_scale.md).

## Why No FastAPI

The local assignment does not need an API server. Workers, Kafka, Postgres, and CLI commands cover the required pipeline, observability, and HITL approval flow. FastAPI would be a reasonable production addition for status, approvals, replay, or data-product APIs.
