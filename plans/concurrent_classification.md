---
name: Concurrent Classification (asyncio)
status: active
created: 2026-06-20
revised: 2026-06-20
type: refactor
depth: standard
---

# refactor: Concurrent LLM Classification with asyncio

> Revised after a compound-engineering document review (feasibility, coherence, scope-guardian, adversarial). Scope was narrowed from "convert the whole pipeline to async" to "make the classification stage concurrent," because ~99.9% of runtime is the sequential OpenAI calls and the DB stages total ~7s with no overlap to exploit. The full async-DB / async-entrypoint / Kafka-`to_thread` / LangGraph-`ainvoke` conversion is moved to Deferred.

## Summary

Speed up the only real bottleneck — the LLM classification stage (~3,800 sequential OpenAI calls, ~100 min) — by running the calls concurrently with `asyncio` (`AsyncOpenAI` + a bounded `Semaphore` + `gather`). Everything else (Bronze, Silver, Gold, event spine, Kafka, LangGraph, CLI, DB layer) stays synchronous. The concurrent work is contained behind a single `asyncio.run(...)` boundary at the classification call site, so blast radius is ~3 modules plus tests/docs.

This is behavior-preserving: Bronze/Silver/Gold counts, reconciliation, quarantine/dedup routing, `meta.quality_results`, `gold.category_breakdown` (including its `classification_method` dimension), and `meta.agent_runs` cache/token/cost accounting must match the pre-change baseline for the degraded path, and remain correct for the keyed path.

## Problem Frame

Measured per-stage cost on a real run: Bronze ~1.7s, Silver ~5.4s, Gold ~0.2s, classification ~99 min. The classification stage is ~99.9% of wall-clock because `classify_batch` (`medallion_pipeline/src/agents/classify_agent.py`) calls `classify_text` (`medallion_pipeline/src/llm.py`) once per row, sequentially, with the sync `OpenAI` client, and does one cache `SELECT` per row — all inside a single open DB transaction.

The fix is concurrency on the network calls, not a new execution model for the whole pipeline.

## Goals

- Run classification OpenAI calls concurrently with a bounded semaphore; cut the stage from ~100 min to single-digit minutes.
- Classify each distinct unknown value once; bulk cache lookup; batched writes; never hold a DB transaction across a network call.
- Preserve degraded mode (no `OPENAI_API_KEY` -> deterministic heuristics) and exact output parity, including per-row `method`/`cache_hit` semantics that feed `gold.category_breakdown`.
- Keep the change contained: synchronous DB and entrypoints unchanged.

## Scope Boundaries

In scope:
- Async, concurrent classification (`llm.py`, `classify_agent.py`) behind one `asyncio.run` boundary.
- Distinct-once classification, bulk cache lookup, batched writes, partial-failure handling.
- Optional synchronous connection pool in `db.py` (behind existing signatures, zero call-site churn) to remove the per-row connection storm.
- Config knobs, dependency pins, tests, and README.

### Deferred to Follow-Up Work
- Full async DB layer (`psycopg` async + `AsyncConnectionPool`) and converting Bronze/Silver/Gold/dq/quality/workers/kafka_io/services/cli to async. Justified only when concurrent/multi-consumer DB access exists (also deferred).
- Async entrypoints (`asyncio.run` per service), `confluent_kafka` via `asyncio.to_thread`, LangGraph `ainvoke`.
- Multi-consumer Kafka pipeline parallelism across batches/partitions.
- Bronze `COPY` and Silver `executemany` throughput wins (orthogonal; tiny stages).
- OpenAI Batch API for bulk non-urgent enrichment.

### Non-Goals
- No change to the medallion data model, cleaning rules, Gold definitions, or event contracts.
- No conversion of synchronous DB access to async in this plan.

## Key Technical Decisions

- **asyncio scoped to classification only.** `classify_batch` becomes a coroutine; the LangGraph classify node (sync) and `eval/run_eval.py` (sync) call a thin sync entrypoint that does exactly one `asyncio.run(...)`. No other module changes execution model. (Resolves the scope-mismatch and big-bang-cutover findings.)
- **Async OpenAI with bounded concurrency.** `AsyncOpenAI` + `asyncio.Semaphore(classify_concurrency)` + `asyncio.gather(..., return_exceptions=True)`. A failed call routes that value to `unclassified` (and is logged), never cancelling the batch.
- **Distinct-once + bulk cache + batched write.** Collect distinct unknown `source_text`; one cache lookup via `cache_key = ANY(%s)`; classify only misses concurrently; persist `silver.tickets_ai` / `meta.agent_cache` / `meta.agent_runs` in batched statements in one short transaction after `gather`. Network calls happen outside any transaction.
- **Behavior-preserving `method`/`cache_hit` reconstruction.** When writing per-row rows, reproduce the current semantics: a pre-existing cache entry -> `method="cache", cache_hit=True`; the first occurrence of a value computed this run -> `method="llm"|"heuristic", cache_hit=False`; later identical occurrences within the same batch -> `method="cache", cache_hit=True`. This keeps `gold.category_breakdown` (which GROUPs BY `classification_method`) and `meta.agent_runs` hit/miss counts identical. (Resolves the parity-vs-dedup finding.)
- **DB stays synchronous; optional sync pool.** Optionally add `psycopg_pool.ConnectionPool` inside `db.py` behind the existing `fetch_one`/`fetch_all`/`execute`/`transaction` signatures (preserving `row_factory=dict_row`), so there is zero call-site churn. This removes the per-row connection storm without async.
- **Retries.** Make `openai_max_retries` configurable (default higher than the SDK's 2) for `429`s under concurrency.
- **Terminology.** "classification stage" = the OpenAI/perf-critical work; reserve "agent worker" for the dq+classify orchestration. Code uses lowercase settings attributes (`classify_concurrency`); env vars use UPPER (`CLASSIFY_CONCURRENCY`).

## Implementation Units

---

### U0. Test Infra And Characterization Baseline (pre-change)

**Goal:** Stand up the test runner and capture a golden baseline from the current synchronous code before any change lands.

**Dependencies:** none (must run on unmodified `main`)

**Files:**
- `medallion_pipeline/requirements.txt` (add `pytest`, `pytest-asyncio`)
- `medallion_pipeline/pytest.ini` (or `pyproject`/`setup.cfg`) with `asyncio_mode = auto`
- `medallion_pipeline/tests/conftest.py`
- `medallion_pipeline/tests/baseline/` (committed golden fixtures)

**Approach:**
- Add pytest + pytest-asyncio and a minimal conftest.
- Run the current degraded pipeline once and record golden values: Silver clean/quarantine/dedup counts, `gold.ticket_volume_daily` / `gold.resolution_sla_summary` / `gold.category_breakdown` row counts and values, key `meta.quality_results`, and `meta.agent_runs` cache_hits/cache_misses. Commit as fixtures.

**Test scenarios:**
- `Test expectation: none -- infrastructure + baseline capture.` (Baseline is the artifact other units assert against.)

**Verification:** `pytest` runs; baseline fixtures exist and reflect current behavior.

---

### U1. Async OpenAI Client And Config Knobs

**Goal:** Provide an async, retry-configured classification primitive plus settings, keeping a sync wrapper for callers.

**Dependencies:** U0

**Files:**
- `medallion_pipeline/src/llm.py`
- `medallion_pipeline/src/config.py`
- `medallion_pipeline/.env.example`
- `medallion_pipeline/.env`
- `medallion_pipeline/requirements.txt`
- `medallion_pipeline/tests/test_llm_async.py`

**Approach:**
- Add an async `aclassify_text` using `AsyncOpenAI` with `max_retries=settings.openai_max_retries`; keep the existing cost/token extraction (`_response_cost_usd`) and the suppressed Pydantic serializer warnings.
- Keep a synchronous `classify_text` (used by `eval`) as a thin wrapper that runs the async path via `asyncio.run` for single calls, or retain the existing sync implementation — both must share cache-key logic `hash(model + prompt_version + normalized_text)`.
- Add settings: `classify_concurrency` (default 8), `openai_max_retries` (default 5). Add `CLASSIFY_CONCURRENCY`/`OPENAI_MAX_RETRIES` to env files. Raise the `openai` floor in requirements to a version that ships `responses.parse`.

**Test scenarios:**
- Degraded mode (no key): `aclassify_text` returns a heuristic result, `tokens=0`, `cost=0`, no network.
- Keyed mode (mocked `AsyncOpenAI`): returns parsed category + confidence; cost/token populated from usage.
- Cache hit path makes no API call.
- Below-threshold confidence maps to `unclassified`.

**Verification:** async unit tests pass; settings load from env.

---

### U2. Concurrent, Distinct-Once `classify_batch` (behavior-preserving)

**Goal:** Replace the sequential per-row loop with a bounded-concurrency batch that preserves all outputs.

**Dependencies:** U1

**Files:**
- `medallion_pipeline/src/agents/classify_agent.py`
- `medallion_pipeline/src/services/agent_worker.py`
- `medallion_pipeline/tests/test_classify_batch.py`

**Approach:**
- Make `classify_batch` a coroutine; expose a sync entrypoint (one `asyncio.run`) that the sync LangGraph classify node calls (LangGraph stays sync).
- Steps: load rows; compute each row's known-vs-unknown category; collect distinct unknown `source_text`; one bulk cache lookup (`cache_key = ANY(%s)`); `gather` with `Semaphore(classify_concurrency)` over misses using `return_exceptions=True`; map results back.
- Reconstruct per-row `method`/`cache_hit` per the Key Technical Decisions rule so `gold.category_breakdown` and `meta.agent_runs` accounting stay identical.
- Persist `silver.tickets_ai`, `meta.agent_cache`, and `meta.agent_runs` via batched statements in a single short transaction after `gather` (no transaction across network calls). Keep `ON CONFLICT` idempotency.
- A `gather` exception for a value -> that value classified `unclassified`, logged `agent.classification.item_failed`; the batch still completes.

**Test scenarios:**
- Degraded full batch reproduces the U0 golden `silver.tickets_ai` method/cache distribution and `meta.agent_runs` hit/miss counts exactly.
- Distinct values trigger exactly one classification each; duplicates within a batch reuse it (verified `method`/`cache_hit` reconstruction).
- Concurrency bound respected: never more than `classify_concurrency` in flight (instrumented fake client).
- One failing call (injected) yields `unclassified` for that value and does not abort the batch.
- Rerun is idempotent (same counts, no duplicate `silver.tickets_ai`).

**Verification:** keyed (mocked) and degraded paths both pass; agent stage wall-clock drops to single-digit minutes at concurrency 8-20.

---

### U3. Optional Synchronous Connection Pool

**Goal:** Remove the per-row connection storm without converting to async.

**Dependencies:** U0

**Files:**
- `medallion_pipeline/src/db.py`
- `medallion_pipeline/requirements.txt`
- `medallion_pipeline/tests/test_db_pool.py`

**Approach:**
- Introduce a module-level `psycopg_pool.ConnectionPool` (sync) used by `fetch_one`/`fetch_all`/`execute`/`transaction`, preserving `row_factory=dict_row` and the existing function signatures so no caller changes.
- Pin `psycopg-pool>=3.2` to match `psycopg>=3.2`. Size via `db_pool_max_size` (default small, e.g. 10); note pooling is governed by intra-stage write concurrency (~1-2), independent of `classify_concurrency`.

**Test scenarios:**
- Helpers return the same shapes (dict rows) as before.
- `transaction()` still commits on success and rolls back on exception.
- Pool is reused (no new connection per call).

**Verification:** existing flows unchanged; connection count per run drops sharply.

---

### U4. Parity + Concurrency Verification And Docs

**Goal:** Prove behavior preserved and concurrency correct; document the knobs.

**Dependencies:** U1, U2, U3

**Files:**
- `medallion_pipeline/tests/test_pipeline_parity.py`
- `medallion_pipeline/README.md`

**Approach:**
- Parity test asserts post-change degraded run matches U0 golden counts (Silver, Gold incl. `category_breakdown` + `classification_method`, reconciliation, `meta.agent_runs` hit/miss).
- Keyed correctness test (mocked `AsyncOpenAI`) asserts concurrency bound, distinct-once dedup, and partial-failure routing — covering the keyed path the degraded baseline cannot exercise.
- README: document `CLASSIFY_CONCURRENCY`, `OPENAI_MAX_RETRIES`, the single-`asyncio.run` boundary, rate-limit guidance, and that the broader async conversion is deferred.

**Test scenarios:**
- Degraded `make run` reproduces baseline counts; rerun twice identical; no duplicate Bronze rows.
- Concurrency cap never exceeded under a simulated burst.
- Injected API failure -> `unclassified`, batch completes.

**Verification:** parity + concurrency tests pass; `make run` (degraded) healthy; keyed smoke via `make eval` shows tokens/cost.

## Risks And Mitigations

- **Parity drift from dedup:** the `method`/`cache_hit` reconstruction (U2) is the explicit guard; U0 baseline + U4 parity test catch regressions.
- **Rate limits under concurrency:** bounded `Semaphore` + higher `openai_max_retries`; start `classify_concurrency` conservative (8).
- **`asyncio.run` reuse:** only one boundary at the classify call site; never call it from within an already-running loop (it is invoked from sync code only).
- **Partial failures:** `gather(return_exceptions=True)` + per-value `unclassified` routing.

## System-Wide Impact

- Touches ~3 source modules (`llm.py`, `classify_agent.py`, `agent_worker.py`) plus `db.py` (optional sync pool), config, env, requirements, README, and a new `tests/` tree.
- New deps: `pytest`, `pytest-asyncio`, `psycopg-pool` (if pool adopted); raised `openai` floor.
- Execution model is unchanged everywhere except the contained classification boundary; logging, idempotency, outbox/DLQ semantics untouched.

## Verification Strategy

- U0 golden baseline captured on current sync code before changes.
- `python -m compileall medallion_pipeline/src` clean.
- Degraded `make run` reproduces baseline; `make status` healthy; DLQ 0.
- Keyed `make eval` confirms async OpenAI + cost tracking.
- Timed keyed `make run` shows the classification stage dropping from ~100 min to single-digit minutes.
