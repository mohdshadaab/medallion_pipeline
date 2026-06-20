# Take-Home Solution — AI-Assisted Medallion Pipeline

**Role:** Senior AI Engineer · **Author:** Mohammad Shadaab

This document is the solution write-up for the take-home in [TAKE_HOME.md](TAKE_HOME.md). It maps every assignment requirement to concrete evidence in the repo, walks the design with the *real* numbers from a clean run, and gives an honest agent-by-agent assessment (what saved real work, what didn't, and why).

The repo itself is the primary artifact. This doc is the reviewer's map into it. For the operational README, see [README.md](README.md); the frozen design rationale lives in [plans/medallion_pipeline.md](plans/medallion_pipeline.md), the perf refactor in [plans/concurrent_classification.md](plans/concurrent_classification.md), and the production-scale design in [plans/enterprise_scale.md](plans/enterprise_scale.md).

---

## TL;DR

- **Bronze → Silver → Gold** in PostgreSQL with strict schema separation (`bronze`, `silver`, `gold`, `meta`), an event spine (Pydantic events + Kafka topics + transactional outbox), and idempotent, re-runnable stages.
- **One clean degraded run** (no API key) ingests **10,280 rows → 9,749 clean / 531 quarantined / 0 deduped**, then builds 3 Gold products. Numbers are pinned in [tests/baseline/golden.json](tests/baseline/golden.json) and re-asserted by a parity test.
- **Agents** implement the assignment's options **(a) Schema Inference & Evolution**, **(b) Data Quality**, and **(c) Semantic Classification** robustly, plus a light **(d) Gold Design** surface. The governing pattern throughout is *agents propose → deterministic code validates and applies → humans approve high-risk changes.*
- **Honest headline:** only the **Semantic Classification Agent** calls an LLM. The other agents are deterministic rule/inference engines that share the same proposal + HITL control plane. That is a deliberate choice (Gold and schema changes are reporting/structural contracts), and it's called out explicitly below rather than dressed up as "four LLM agents."

---

## How to run

```bash
make up        # postgres + kafka (KRaft)
make db-init   # create bronze/silver/gold/meta schemas
make run       # end-to-end pipeline (in-process stage chaining)
make status    # durable run health from meta.* tables
```

Runs **without** `OPENAI_API_KEY` — classification falls back to deterministic heuristics and still writes full agent/cache/cost metadata. Two execution paths exist (be precise about which does what):

| Path | Command | Topology | Event/outbox/idempotency trail |
|---|---|---|---|
| **Default** | `make run` | Stages chained **in-process** ([src/cli.py](src/cli.py) `run_pipeline`), `KAFKA_REQUIRED=false` | Still recorded (outbox, processed_events, stage_runs, idempotent writes) |
| **Evented** | `make run-evented` | Real Kafka **worker containers** ([docker-compose.yml](docker-compose.yml) `workers` profile) | At-least-once Kafka + idempotent consumers |

Optional: `make eval` (classification accuracy/format), `make run-drift` (schema-drift demo), `make proposals` / `make approve ID=…` / `make reject ID=…` (HITL).

---

## Requirement coverage map

### Part 1 — Medallion pipeline (50%)

| Requirement | Status | Evidence |
|---|---|---|
| Bronze: raw ingestion, schema-on-read, no data loss | ✅ | [src/bronze.py](src/bronze.py) `ingest_bronze`; all source columns as TEXT + `raw_payload`/`extra_fields` JSONB in [sql/schema.sql](sql/schema.sql) |
| Bronze lineage: source file, `ingested_at`, row hash | ✅ | `bronze.raw_tickets` columns `source_file`, `ingested_at`, `content_hash`, `batch_id`, `raw_line_no` |
| Silver: cleansed, deduplicated, typed, validated | ✅ | [src/silver.py](src/silver.py) `transform_row`, `rebuild_silver` |
| Silver: documented cleaning rules + rationale | ✅ | [Cleaning rules](#silver--cleaning-rules-with-rationale) table below; code in [src/silver.py](src/silver.py) |
| Gold: 2–3 business-ready aggregations + justification | ✅ | [src/gold.py](src/gold.py) → 3 products; [justification](#gold--three-products-and-why) below |
| Clear layer separation (schemas/naming) | ✅ | `bronze` / `silver` / `gold` / `meta` schemas in [sql/schema.sql](sql/schema.sql) |
| Idempotent and re-runnable | ✅ | Bronze `UNIQUE (source_file, raw_line_no)` + `ON CONFLICT`; batch-scoped truncate-and-rebuild; parity test [tests/test_pipeline_parity.py](tests/test_pipeline_parity.py) |
| Clear logging at each stage | ✅ | [src/logging_config.py](src/logging_config.py) `log_event`; stage events in [src/workers.py](src/workers.py); `make status` |
| Handle messiness without manual intervention | ✅ | Flags + quarantine + sentinel handling in [src/silver.py](src/silver.py); 531 rows quarantined automatically with reason codes |

### Part 2 — Agentic acceleration (50%) — "pick at least two"

| Option | Status | Where | LLM? |
|---|---|---|---|
| (a) Schema Inference & Evolution | ✅ Built | [src/agents/dq_agent.py](src/agents/dq_agent.py) `_create_schema_mapping_proposals` + DDL gen in [src/cli.py](src/cli.py) `_apply_schema_mapping` + backfill in [src/silver.py](src/silver.py) `_populate_dynamic_columns` | No (deterministic profiler) |
| (b) Data Quality | ✅ Built | [src/agents/dq_agent.py](src/agents/dq_agent.py) `propose_quality_rules` | No (deterministic) |
| (c) Semantic Classification | ✅ Built | [src/agents/classify_agent.py](src/agents/classify_agent.py) + [src/llm.py](src/llm.py) | **Yes** (heuristic fallback) |
| (d) Gold Layer Design | ⚠️ Light | static `suggest_gold_products()` in [src/agents/gold_suggester.py](src/agents/gold_suggester.py) (**not wired into the run**) + `gold_impact` proposals in [src/cli.py](src/cli.py) | No |

### Required-in-README items

| Item | Status | Evidence |
|---|---|---|
| Architecture diagram | ✅ | Mermaid in [README.md](README.md) and [plans/medallion_pipeline.md](plans/medallion_pipeline.md) |
| Agent assessment (what/sample I/O/honest take) | ✅ | [Part 2 section](#part-2--agentic-acceleration-detail--honest-assessment) below + README |
| What changes at 100x scale | ✅ | [plans/enterprise_scale.md](plans/enterprise_scale.md) + [scale section](#cost--scale-thinking-10m-rows) below |
| How to run (single command) | ✅ | `make run` (above) |

### Good-to-haves

| Touch | Status | Evidence |
|---|---|---|
| End-to-end data lineage | ✅ | `meta.lineage` (source→target→transform→row_count) via [src/quality.py](src/quality.py) `record_lineage`, written by every stage |
| Metadata auto-tagging at landing | ⚠️ Partial | PII hints + dimension/metric hints on **new** columns only ([src/agents/dq_agent.py](src/agents/dq_agent.py) `_suspected_pii`, `_gold_impact`); not all Bronze columns |
| Cross-source reconciliation + dim-model rec | ⚠️ Partial | Reconciliation gate `bronze = clean + quarantine + dedup` ([src/silver.py](src/silver.py)); dimensional model only sketched in plans (single source here) |
| Agent evaluation harness | ✅ | [src/eval/run_eval.py](src/eval/run_eval.py) + [src/eval/labeled_categories.csv](src/eval/labeled_categories.csv) → accuracy + format compliance |
| Human-in-the-loop approval gates | ✅ | `meta.agent_proposals` + `make proposals/approve/reject`; schema changes require approval before `ALTER`; Gold contracts never auto-changed |
| Incremental + backfill / replay | ⚠️ Partial | Deterministic `batch_id` by content fingerprint + idempotent rebuild + replay-by-batch; partition-level incremental is in the at-scale plan |
| Observability + SLA thinking | ✅ | `meta.pipeline_runs/stage_runs/quality_results/dlq_events/agent_runs`; `make status`; business-SLA vs pipeline-SLA split; `is_late` vs `PIPELINE_STAGE_SLA_SECONDS` |

---

## Part 1 — Medallion pipeline (detail)

### Layer separation

Four PostgreSQL schemas with one clear owner each ([sql/schema.sql](sql/schema.sql)):

- **`bronze`** — immutable evidence. `bronze.raw_tickets`.
- **`silver`** — trusted record-level truth. `silver.tickets` (+ `tickets_quarantine`, `tickets_dups`, `tickets_ai`).
- **`gold`** — business-serving products. `ticket_volume_daily`, `resolution_sla_summary`, `category_breakdown`.
- **`meta`** — the control plane. Runs, stages, events, outbox, DLQ, quality, lineage, agent runs/cache/proposals, schema mappings.

PostgreSQL (not a lakehouse) is the deliberate choice for a 10k-row take-home: schemas give clean medallion boundaries and readable lineage without standing up Spark/Delta. The at-scale plan explains the migration to Delta/Iceberg with Postgres demoted to control plane.

### Bronze — exact, auditable ingestion

- Every physical CSV line is stored. Known columns land as TEXT; the **full row** is preserved in `raw_payload` JSONB and any **unexpected** columns in `extra_fields` JSONB ([src/bronze.py](src/bronze.py) `_clean_row`). No cleaning, no dropping.
- **Lineage metadata:** `source_file`, `raw_line_no`, `ingested_at`, `batch_id`, `content_hash` (a change-fingerprint, not the identity).
- **Identity = `(source_file, raw_line_no)`** with `UNIQUE` + `ON CONFLICT DO UPDATE` → re-ingesting the same file never duplicates physical rows.
- **`batch_id` = `<stem>_<sha256(file)[:12]>`** ([src/bronze.py](src/bronze.py) `batch_id_for`) → deterministic per file *content*; the same file always maps to the same batch, which is what makes reruns safe.
- **Schema drift is captured at landing.** A new header column is recorded in `meta.source_schemas` (`added`/`removed`/`common` columns) and flows into `raw_payload`/`extra_fields` without breaking ingestion ([src/bronze.py](src/bronze.py) `record_source_schema`).

### Silver — cleaning rules with rationale

The raw data is genuinely messy: mixed date formats (`2024-10-24 14:32:10`, `12-Feb-2025 03:21`, `02/14/2025 08:16 AM`, `28-Feb-2024 17:03`), sentinel costs (`-1`), placeholder SLAs (`0`, `999`, `N/A`), category synonyms (`A/C`, `AC`, `hvac`, `power issue`, `Elec`), reversed timestamps, and rows whose category sits in the wrong column. Rules live in [src/silver.py](src/silver.py):

| Field | Rule | Rationale |
|---|---|---|
| Dates | Documented **US `MM/DD`** policy (`dateutil`, `dayfirst=False`). Flag `ambiguous_us_mm_dd` when both parts ≤ 12; `missing_date`/`invalid_date` otherwise. All normalized to UTC. | The data mixes ISO, US-slash, AM/PM and `DD-Mon-YYYY`. A deterministic policy is required, and **guessed dates must never silently feed SLA math** — so they're flagged, not invented. |
| `resolved_at < created_at` | Flag `resolved_before_created`; **excluded from SLA** in Gold. | Reversed timestamps are unrecoverable; excluding beats reporting negative resolution time. |
| Cost | Strip `$`/`,`; map `N/A`/`NA`/`TBD` → NULL+flag; `-1`/negatives → NULL+`negative_cost`. | `-1` and `TBD` are placeholders, not money. |
| SLA hours | Integer parse; `0` and `999` → NULL+`sentinel_sla`; negatives → NULL. **Excluded from Gold SLA.** | `0`/`999` are obvious sentinels that would wreck breach-rate metrics. |
| Category / priority / status / assignee | Normalize via explicit maps (`a/c`→`hvac`, `lo`→`low`, `closed`→`resolved`, `in-house`→`internal_team`, …). Unknowns kept, not dropped. | Operational labels are inconsistent; canonical values make Gold groupable. The raw value is always preserved alongside the normalized one. |
| Dedup | `business_key` = `ticket_id` when it matches `^TKT-\d+$`; else a deterministic fingerprint of `date\|building\|submitter\|description[:200]`. Losers → `silver.tickets_dups`. | Valid ticket IDs are the natural key; the fingerprint catches re-submits lacking a clean ID. |
| Quarantine | Unparseable `created_at` → `invalid_created_at`; missing **both** description and category → `missing_business_fields`. Whole key-group → `silver.tickets_quarantine`. | Structurally untrustworthy rows shouldn't pollute Silver, but they're **never discarded** — they're quarantined with a reason code. |

**Field-level anomalies are flagged in place** (JSON `*_flags` columns); only **structural** failures are quarantined. Every transformation is reconciled: `bronze = clean + quarantine + dedup_removed`, asserted as a quality check ([src/silver.py](src/silver.py) `rebuild_silver`).

> **Honest note on dedup:** the golden run shows **`dedup_rows = 0`**. The dedup logic is real and tested, but *this* dataset's duplicates are **semantic** — rows that say "Duplicate of ticket #3098" in `resolution_notes` while carrying distinct `ticket_id`s and timestamps. Those are not business-key collisions, so the deduper (correctly) removes nothing. I'm reporting that rather than implying dedup is doing heavy lifting here. Catching semantic duplicates would need fuzzy/embedding matching — noted as future work, not claimed.

### Gold — three products and why

Built deterministically from clean Silver ([src/gold.py](src/gold.py) `build_gold`); golden row counts in parentheses:

1. **`gold.ticket_volume_daily`** (9,701 rows) — volume by day × status × priority × category × assigned team. Answers the most basic ops question: *where is the workload and how is it trending?*
2. **`gold.resolution_sla_summary`** (580 rows) — SLA met-rate, breach count, **median & p95** resolution hours, and an explicit `excluded_count`, by priority × category × team. SLA is computed **only** from eligible rows (valid, non-sentinel, non-reversed) — the exclusion logic is inline in the `eligible` CTE, and the count of excluded rows is published beside the metric so consumers can judge trust.
3. **`gold.category_breakdown`** (30 rows) — canonical category distribution **with a `classification_method` dimension** (`dictionary` / `llm` / `cache` / `heuristic`). This makes enrichment **provenance** first-class: you can see exactly how much of each category came from the deterministic dictionary vs. the model.

Gold tables are intentionally hand-written SQL, not agent-generated — Gold is a **reporting contract**, and contracts shouldn't silently change under an agent (see [Gold Design assessment](#d-gold-layer-design-agent--light-honest)).

### Idempotency & re-runnability — the evidence

- Bronze `ON CONFLICT (source_file, raw_line_no) DO UPDATE` — reruns update, never duplicate.
- Silver/Gold **delete-by-`batch_id` then rebuild** — truncate-and-rebuild per batch keeps reruns exact.
- `meta.processed_events` (per-consumer) skips duplicate events; `meta.outbox_events` allows republish after a DB-commit/Kafka-publish split ([src/workers.py](src/workers.py), [src/kafka_io.py](src/kafka_io.py)).
- LLM results cached by `hash(model + prompt_version + normalized_input)` ([src/llm.py](src/llm.py) `cache_key`) → a rerun re-hits cache, **no repeat spend**.
- **Proof:** [tests/test_pipeline_parity.py](tests/test_pipeline_parity.py) asserts a fresh degraded run reproduces [tests/baseline/golden.json](tests/baseline/golden.json) exactly — including the per-row `classification_method` split that feeds `gold.category_breakdown`.

### Logging

Structured `key=value` events ([src/logging_config.py](src/logging_config.py) `log_event`) at every stage start/finish, with row counts, reconciliation result, schema-drift details, agent mode, cache hits/misses, token/cost, event publish/outbox, and DLQ writes. Secrets, raw payloads, full descriptions, and prompt text are deliberately kept out of logs. `make status` is the durable authoritative summary, backed by the `meta.*` tables.

---

## Part 2 — Agentic acceleration (detail & honest assessment)

**The pattern that ties them together:** agents operate on metadata/profiles/samples and **emit proposals**; deterministic code validates and applies; **high-risk changes wait for human approval** (`make approve`). This matches the best-practices guardrail "agents propose; pipeline code validates and applies," and it's why three of the four agents need no LLM — their job is structured reasoning over the pipeline's own metadata, not free-text understanding.

### (c) Semantic Classification Agent — the only LLM agent

**What it does:** normalizes the dirty `category`/free-text into a canonical taxonomy and writes results to `silver.tickets_ai` with confidence, model, prompt version, token/cost, cache status, and method ([src/agents/classify_agent.py](src/agents/classify_agent.py)).

**How it stays cheap (this is the real engineering):**
1. **Dictionary/regex pre-pass** resolves known categories with **zero** model calls.
2. The LLM is asked only about **distinct unknown** source texts — not per row.
3. **Bulk cache lookup** (`cache_key = ANY(...)`) then **bounded-concurrency** `asyncio.gather` over the misses only (`CLASSIFY_CONCURRENCY`, default 8); persistence is one short transaction *after* the network calls — never a DB transaction held across an API call.

**What that buys, in the golden run's own numbers (9,749 classifiable rows):**

| Method | Rows | Meaning |
|---|---|---|
| `dictionary` | 4,348 (45%) | Resolved deterministically — **never reached the model path** |
| `cache` | 1,616 | Repeated unknown texts reused within the batch |
| `llm`/`heuristic` | 3,785 | **Distinct** unknown texts actually computed |

So a keyed run needs ~**3,785 model calls for 9,749 rows** — distinct-value dedup + dictionary pre-pass cut model work by ~61%, and 45% of rows never touch the model at all.

**Sample input → output** (from [src/eval/labeled_categories.csv](src/eval/labeled_categories.csv)):

```
input : "Breaker keeps tripping in server room"
output: { canonical_category: "electrical", confidence: 0.85, rationale: "matched deterministic keyword" }
```

(Degraded/heuristic path shown; the keyed path returns the same category from the model with its own confidence, validated against the `ClassificationOutput` schema.)

**Honest take:** *Worth it — for adaptability and scale, not strictly for 10k rows.* On this dataset a hand-tuned dictionary already covers 45% and the heuristic mops up much of the rest, so the LLM's *marginal* value here is the long tail of genuinely ambiguous free-text (e.g. a category accidentally written into the description). The architecture (distinct-value + cache + confidence gate + provenance) is exactly what makes it pay off at 10M rows; at 10k it's more "demonstrably correct and cheap" than "indispensable." I'd rather say that than oversell it.

### (a) Schema Inference & Evolution Agent — strongest acceleration story

**What it does:** when a new source column appears (the [data/drift_raw_tickets.csv](data/drift_raw_tickets.csv) fixture adds `customer_tier`), it samples Bronze values, **infers a type** (`text`/`numeric`/`boolean`), sanitizes a safe Silver column name, estimates null-rate, flags PII suspicion, and emits a `schema_mapping` proposal ([src/agents/dq_agent.py](src/agents/dq_agent.py) `_create_schema_mapping_proposals`). On `make approve`, deterministic code generates and runs the **`ALTER TABLE silver.tickets ADD COLUMN …` DDL** (with identifier/type allow-listing) ([src/cli.py](src/cli.py) `_apply_schema_mapping`), and the next Silver rebuild **backfills** the column from Bronze JSON ([src/silver.py](src/silver.py) `_populate_dynamic_columns`).

**Sample output (proposal):**
```
source=customer_tier silver=customer_tier type=text gold=dimension_candidate
risk=low confidence=0.85  →  pending human approval
```

**Honest take:** *This saved the most real work.* The tedious, error-prone part of schema evolution is the migration plumbing — name sanitization, safe DDL, typed backfill, and an audit trail — and that's fully automated behind a one-line approval. The "inference" is a **deterministic profiler**, not an LLM; for this column shape that's more reliable and free, and I'm not going to call a regex an LLM. The governed flow (detect → propose → approve → versioned apply → backfill) is the part worth showing.

### (b) Data Quality Agent — deterministic, by design

**What it does:** profiles the run (quarantine rate, schema-drift status), proposes named rules with NL rationale, and **escalates to a human proposal when the quarantine rate crosses 5%** ([src/agents/dq_agent.py](src/agents/dq_agent.py) `propose_quality_rules`). It also owns the schema-mapping proposals above. Everything is written to `meta.agent_runs` with risk/confidence for audit.

**Honest take:** *Useful as a control-plane, modest as "AI" at this scale.* Its value is encoding **when a human should look** and producing an auditable trail — not magic. The rationales are **templated NL, not model-generated**, which I'm flagging plainly: an LLM here would add the most by *explaining novel anomalies* it hasn't seen a template for. At 10k rows the deterministic version is honestly the better cost/reliability trade; the LLM upgrade is in the at-scale plan.

### (d) Gold Layer Design Agent — light, honest

**What exists:** (1) a static `suggest_gold_products()` helper that returns the three Gold products and their purpose — but it is **not wired into the pipeline run** (verified: it has no caller; `gold_suggester` only appears elsewhere as an `agent_name` string literal). (2) A live `gold_impact` proposal: when a drifted column is promoted, the system flags whether it looks like a Gold **dimension** or **metric** candidate and routes it to human review ([src/cli.py](src/cli.py) `_create_gold_impact_proposal`) — Gold tables are **never** auto-altered.

**Honest take:** *This is the weakest of the four and I won't pretend otherwise.* It's the place a real LLM "given the Silver schema + business description, propose facts/dimensions and generate the SQL" agent would add the most, and it's the least built-out. The deliberate part is sound — Gold is a reporting contract, so I keep generation human-gated and the actual tables deterministic — but the "agent" here is a stub plus a routing heuristic, not a generator. If I extended the project, this is where I'd invest first.

---

## Prompt engineering

The classification prompt ([src/llm.py](src/llm.py) `_classification_prompt`) is deliberately small and constrained:

- **Closed label set** — the allowed categories are enumerated in the prompt; the model can't invent taxonomy.
- **Structured Outputs + Pydantic** — `responses.parse(..., text_format=ClassificationOutput)` forces typed JSON (`canonical_category`, `confidence`, `rationale`); no brittle string parsing.
- **`temperature=0`** — deterministic, cache-friendly outputs.
- **Guardrails:** confidence **< 0.7 → `unclassified`** ([src/agents/classify_agent.py](src/agents/classify_agent.py) `CONFIDENCE_THRESHOLD`), so low-confidence guesses never reach Gold; classify **distinct normalized values**, not raw rows; a failed call routes that value to `unclassified` and is logged rather than aborting the batch.

**Honest gap:** the current prompt sends `category_raw + description`, and `description`/`resolution_notes` can contain PII. The best-practices and at-scale docs call for redacting PII before prompting; the build tags suspected-PII columns but does **not yet redact free text before the call**. That's a real gap, listed below.

---

## Cost & scale thinking (10M rows)

**Already in place (the patterns that matter):** dictionary pre-pass (skip), distinct-value classification (dedup), cross-run cache keyed by `model+prompt_version+input` (skip unchanged), bounded concurrency, and a per-run token/cost ledger in `meta.agent_runs`.

**What changes at 100x** (full design in [plans/enterprise_scale.md](plans/enterprise_scale.md)):

- **Batch:** move enrichment to the **OpenAI Batch API** / async workers; enforce **per-tenant/per-job budgets**.
- **Cache & skip:** persist the distinct-value cache across runs; skip rows resolved by dictionary; skip unchanged records via `content_hash`; process **changed partitions only** with a replay window for late data.
- **Storage:** Bronze/Silver → **Delta Lake/Iceberg** on object storage; Gold → a warehouse (Snowflake/BigQuery/Databricks SQL); **Postgres stays as the control plane** for state, approvals, costs, and idempotency — not the analytical store.
- **Transport/governance:** Schema Registry for event contracts; **Debezium CDC** to replace outbox polling; topics partitioned by `tenant_id`/source; OpenTelemetry + Prometheus/Grafana + a lineage catalog (DataHub/Unity/OpenLineage).
- **Tenancy:** every table/event carries `tenant_id`, `schema_version`, `data_residency_region`; row-level security or per-tenant catalogs for large/regulated tenants.

---

## Tradeoff awareness — where agents helped vs. didn't

- **Real value:** schema-evolution plumbing (migration + typed backfill + audit) and semantic normalization of a dirty long-tail taxonomy with cheap, cached, provenance-tracked enrichment.
- **Limited value at this scale:** an LLM for data-quality rationale or for resolving categories a dictionary already handles — deterministic code is cheaper and more reliable for 10k rows. I kept those deterministic on purpose.
- **Deliberately *not* agentic:** Gold table definitions and the actual application of schema/Gold changes — these are contracts and structural mutations, so they stay deterministic and human-gated. Agents *propose*; they never silently mutate trusted tables.

---

## Honest limitations & what I'd do next

1. **Gold Design is a stub** — `suggest_gold_products()` isn't wired in; the only live Gold-design behavior is the `gold_impact` routing heuristic. First place I'd invest (a real schema→SQL generation agent behind HITL).
2. **No PII redaction before prompting** — suspected-PII columns are tagged, but free-text isn't scrubbed before the model call. Should redact/mask before enrichment.
3. **Dedup catches only business-key collisions** (`dedup_rows = 0` here) — semantic duplicates flagged in `resolution_notes` are not caught; would need fuzzy/embedding matching.
4. **Incremental loads are full-batch rebuilds** — fine at 10k; partition-level incremental + late-arrival replay is designed in the at-scale plan, not implemented.
5. **DQ rationale is templated, not generated** — the "explain *why*" is canned text; an LLM would help most on novel anomalies.
6. **Single source** — cross-source reconciliation and the dimensional-model recommendation are sketched, not built (only one input file exists).

---

## Repository map

| Path | Purpose |
|---|---|
| [sql/schema.sql](sql/schema.sql) | All `bronze`/`silver`/`gold`/`meta` DDL |
| [src/bronze.py](src/bronze.py) | Ingestion, lineage, schema-drift capture |
| [src/silver.py](src/silver.py) | Cleaning, typing, dedup, quarantine, reconciliation, dynamic columns |
| [src/gold.py](src/gold.py) | 3 deterministic Gold products + SLA exclusion logic |
| [src/llm.py](src/llm.py) | Async/sync OpenAI client, Structured Outputs, cache, cost/token extraction |
| [src/agents/classify_agent.py](src/agents/classify_agent.py) | Semantic Classification Agent (distinct-value, cached, concurrent) |
| [src/agents/dq_agent.py](src/agents/dq_agent.py) | Data Quality + Schema Inference/Evolution proposals |
| [src/agents/gold_suggester.py](src/agents/gold_suggester.py) | Static Gold suggester (not wired into the run) |
| [src/services/](src/services) | `file_discovery` + bronze/silver/agent/gold workers (LangGraph in `agent_worker`) |
| [src/workers.py](src/workers.py) | Stage runner, idempotency, outbox/DLQ orchestration |
| [src/cli.py](src/cli.py) | `db-init` / `run` / `status` / `proposals` / `approve` / `reject` |
| [tests/](tests) | Async/concurrency unit tests + golden-baseline parity test |
| [src/eval/](src/eval) | Classification accuracy/format-compliance harness |
| [plans/](plans) | Frozen local design, perf refactor, enterprise-scale design |
