# Medallion Architecture Best Practices

This document summarizes current medallion architecture guidance and translates it into a practical target design for the AI-assisted support-ticket pipeline in this assignment.

## Core Principle

Medallion architecture is a progressive data-quality pattern:

```text
Raw source file
  -> Bronze: exact, auditable ingestion
  -> Silver: validated, typed, deduplicated records
  -> Gold: business-ready metrics, models, and data products
```

Each layer should have a clear ownership boundary. Bronze preserves evidence, Silver creates a trusted record-level source of truth, and Gold serves business questions. Do not mix these responsibilities for convenience.

## Recommended Architecture

For this assignment, keep the architecture simple and explicit:

```text
data/raw_tickets.csv
        |
        v
bronze.raw_tickets
  - raw fields preserved
  - source_file
  - ingested_at
  - batch_id
  - row_hash
  - optional raw_payload
        |
        v
silver.tickets_clean
  - typed columns
  - normalized statuses, priorities, timestamps
  - deduplicated by stable row hash or natural key
  - validation flags or quarantine records
  - AI-enriched fields when useful
        |
        v
gold.*
  - ticket volume trends
  - SLA / resolution metrics
  - category, priority, or customer-impact summaries
  - materialized tables/views for reporting
```

Use separate schemas or namespaces for `bronze`, `silver`, and `gold`. If using PostgreSQL, schemas are enough. If using a lakehouse, map the same design to Delta tables and use catalog/schema boundaries for governance.

## Bronze Layer Rules

Bronze is the immutable evidence layer.

- Ingest the source exactly as received. Do not clean, normalize, or drop records here.
- Preserve every original field so Silver and Gold can be rebuilt without going back to the source.
- Add lineage metadata: `source_file`, `ingested_at`, `batch_id`, and `row_hash`.
- Prefer append-only writes. If a run is retried, make ingestion idempotent with `batch_id` and `row_hash`.
- Store malformed rows instead of failing the whole pipeline when possible.
- Document source assumptions, ingestion schedule, and retention policy.

For larger systems, partition Bronze by ingestion date and use incremental ingestion rather than full reloads.

## Silver Layer Rules

Silver is the trusted, record-level layer.

- Build Silver only from Bronze or other trusted Silver tables. Do not write directly from ingestion into Silver.
- Enforce schema, data types, naming conventions, and timestamp formats.
- Deduplicate using a deterministic rule, such as `row_hash`, source record ID, or a documented natural key.
- Normalize messy operational fields, such as status names, priority labels, teams, and timestamps.
- Add validation checks for required fields, accepted values, date ranges, uniqueness, and referential consistency.
- Never silently discard data. Route invalid records to a quarantine table or keep validation flags for investigation.
- Keep at least one non-aggregated, cleaned representation of each valid source record.

Silver should not encode dashboard-specific metrics. If a transformation needs business interpretation, it likely belongs in Gold.

## Gold Layer Rules

Gold is the business-serving layer.

- Model Gold tables around real questions, not around source-file structure.
- Prefer a few useful data products over many speculative aggregates.
- Use clear names that explain the business purpose, such as `gold.ticket_volume_daily` or `gold.resolution_sla_summary`.
- Precompute expensive or frequently used metrics.
- Document each metric definition and the Silver inputs behind it.
- Keep historical detail in Silver; Gold should be optimized for consumption.

For this ticket dataset, strong Gold candidates are:

- Daily or weekly ticket volume by status, priority, category, and assigned team.
- Resolution-time and SLA summaries by priority or category.
- AI-generated category or sentiment distributions, only if the enrichment is reliable enough to be useful.

## Data Quality Gates

Quality rules should become stricter as data moves downstream.

- Bronze gate: verify file readability, capture row counts, compute hashes, and record ingestion metadata.
- Bronze to Silver gate: validate schema, types, required fields, deduplication, null rates, accepted values, and timestamp parsing.
- Silver to Gold gate: validate metric definitions, aggregation logic, freshness, row-count reconciliation, and anomaly checks.

Every run should log:

- Input row count.
- Output row count.
- Duplicate count.
- Invalid or quarantined row count.
- Start and end timestamps.
- Batch ID.
- Failed validation rules.

## Idempotency and Reprocessing

The pipeline should be safe to rerun.

- Use deterministic `batch_id` generation or explicitly pass a batch ID.
- Use `row_hash` for deduplication and replay checks.
- Prefer replace-by-partition, merge/upsert, or truncate-and-rebuild for small assignment-scale tables.
- Make Gold rebuildable from Silver and Silver rebuildable from Bronze.
- Keep transformation logic in versioned code, not manual SQL edits.

For daily incremental loads at scale, process only changed partitions or changed records, and keep a replay window for late-arriving data.

## Governance, Lineage, and Security

Even in a small assignment, show the production shape.

- Track table-level lineage from Bronze to Silver to Gold.
- Track column-level lineage for important Gold metrics when possible.
- Keep ownership and purpose documented for each Gold table.
- Tag sensitive columns, especially free-text fields that might contain customer or personal information.
- Restrict direct analyst access to Bronze if raw data might contain sensitive content.
- Use views or masked columns for consumer-facing access when needed.

## Performance and Storage

Optimize each layer for its job.

- Bronze: optimize for fast, reliable writes and auditability.
- Silver: balance write performance, validation, and exploratory reads.
- Gold: optimize for frequent reads, dashboards, and BI queries.

At larger scale, use columnar formats such as Delta/Parquet, compact small files, cluster or partition by high-value query dimensions, and tune Gold tables for read latency.

## Agentic Acceleration Pattern

Agents should improve design speed and adaptability, but deterministic code should execute the pipeline.

Recommended agent responsibilities:

- Schema inference agent: inspect Bronze samples, propose a typed Silver schema, and generate draft DDL or transformation SQL.
- Data quality agent: profile Bronze, explain anomalies, and propose validation rules with rationale.
- Semantic classification agent: classify free-text tickets into structured categories, with batching, caching, and confidence thresholds.
- Gold design agent: propose useful aggregates from the Silver schema and business context.

Guardrails:

- Agents propose; pipeline code validates and applies.
- Store prompts, model inputs, outputs, and costs for auditability.
- Require human approval for schema migrations, destructive changes, or major classification-rule updates.
- Cache LLM results by stable text hash to control cost.
- Batch free-text enrichment and skip unchanged records.
- Evaluate agent output on a small repeatable test set before trusting it in Silver or Gold.

## Best Architecture for This Assignment

The best assignment architecture is intentionally modest:

- Use PostgreSQL schemas or local files to show strict `bronze`, `silver`, and `gold` separation.
- Implement deterministic pipeline steps with one command.
- Keep Bronze raw and auditable.
- Make Silver typed, deduplicated, and validated.
- Build two or three Gold outputs that answer obvious support-operations questions.
- Add agent assistance where it clearly saves work: schema proposal, data-quality rule proposal, semantic ticket classification, or Gold aggregate suggestions.
- Add honest README notes about where the agent helped and where deterministic logic was better.

This is stronger than overbuilding a distributed platform for a 10k-row dataset. The production-scale discussion can explain how the same design would move to Delta tables, incremental ingestion, catalog governance, lineage, monitoring, and backfill windows.

## Common Mistakes to Avoid

- Skipping Bronze and loading cleaned data directly.
- Cleaning or dropping records during Bronze ingestion.
- Letting Gold become a dumping ground for every transformation.
- Putting business-specific metric logic in Silver.
- Silently discarding invalid rows.
- Building many Gold tables without clear users or questions.
- Treating LLM output as trusted data without validation, caching, cost tracking, or review gates.

## Source References

- Databricks: [What is the medallion lakehouse architecture?](https://docs.databricks.com/aws/en/lakehouse/medallion)
- Databricks: [Design Delta Lake architecture](https://docs.databricks.com/aws/en/lakehouse-architecture/deployment-guide/delta-lake)
- Microsoft Fabric: [Understand medallion lakehouse architecture for Microsoft Fabric with OneLake](https://learn.microsoft.com/en-us/fabric/onelake/onelake-medallion-lakehouse-architecture)

