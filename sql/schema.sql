CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
    run_id UUID PRIMARY KEY,
    batch_id TEXT NOT NULL UNIQUE,
    source_file TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    total_rows INTEGER DEFAULT 0,
    clean_rows INTEGER DEFAULT 0,
    quarantine_rows INTEGER DEFAULT 0,
    dedup_rows INTEGER DEFAULT 0,
    gold_ready BOOLEAN NOT NULL DEFAULT FALSE,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS meta.stage_runs (
    stage_run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES meta.pipeline_runs(run_id),
    batch_id TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    duration_ms INTEGER,
    input_count INTEGER DEFAULT 0,
    output_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    error_message TEXT,
    is_late BOOLEAN NOT NULL DEFAULT FALSE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (run_id, stage_name)
);

CREATE TABLE IF NOT EXISTS meta.stage_events (
    event_id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_version TEXT NOT NULL,
    run_id UUID NOT NULL,
    batch_id TEXT NOT NULL,
    correlation_id UUID NOT NULL,
    idempotency_key TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ,
    UNIQUE (idempotency_key)
);

CREATE TABLE IF NOT EXISTS meta.outbox_events (
    outbox_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL UNIQUE,
    topic TEXT NOT NULL,
    event_type TEXT NOT NULL,
    run_id UUID NOT NULL,
    batch_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS meta.processed_events (
    event_id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    consumer_name TEXT NOT NULL,
    run_id UUID NOT NULL,
    batch_id TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.dlq_events (
    dlq_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID,
    event_type TEXT,
    topic TEXT NOT NULL DEFAULT 'pipeline.dlq',
    run_id UUID,
    batch_id TEXT,
    original_event JSONB NOT NULL,
    error_message TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.source_schemas (
    source_schema_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_file TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    header JSONB NOT NULL,
    header_hash TEXT NOT NULL,
    added_columns JSONB NOT NULL DEFAULT '[]'::jsonb,
    removed_columns JSONB NOT NULL DEFAULT '[]'::jsonb,
    common_columns JSONB NOT NULL DEFAULT '[]'::jsonb,
    drift_details JSONB NOT NULL DEFAULT '{}'::jsonb,
    drift_status TEXT NOT NULL DEFAULT 'baseline',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_file, batch_id)
);

ALTER TABLE meta.source_schemas ADD COLUMN IF NOT EXISTS added_columns JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE meta.source_schemas ADD COLUMN IF NOT EXISTS removed_columns JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE meta.source_schemas ADD COLUMN IF NOT EXISTS common_columns JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE meta.source_schemas ADD COLUMN IF NOT EXISTS drift_details JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS meta.schema_mappings (
    mapping_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proposal_id UUID,
    source_file TEXT NOT NULL,
    source_column TEXT NOT NULL,
    silver_column TEXT NOT NULL,
    source_path TEXT NOT NULL,
    inferred_type TEXT NOT NULL,
    nullable BOOLEAN NOT NULL DEFAULT TRUE,
    status TEXT NOT NULL DEFAULT 'approved',
    rationale TEXT NOT NULL,
    risk_level TEXT NOT NULL,
    confidence NUMERIC(5,4) NOT NULL,
    gold_impact JSONB NOT NULL DEFAULT '{}'::jsonb,
    approved_at TIMESTAMPTZ,
    applied_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_file, source_column, silver_column)
);

CREATE TABLE IF NOT EXISTS meta.schema_mapping_applications (
    application_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    mapping_id UUID NOT NULL REFERENCES meta.schema_mappings(mapping_id),
    run_id UUID,
    batch_id TEXT,
    action TEXT NOT NULL,
    status TEXT NOT NULL,
    affected_rows INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.raw_tickets (
    bronze_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_file TEXT NOT NULL,
    raw_line_no INTEGER NOT NULL,
    batch_id TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    content_hash TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    extra_fields JSONB NOT NULL DEFAULT '{}'::jsonb,
    ticket_id TEXT,
    created_at TEXT,
    resolved_at TEXT,
    category TEXT,
    priority TEXT,
    status TEXT,
    building TEXT,
    description TEXT,
    submitted_by TEXT,
    assigned_to TEXT,
    resolution_notes TEXT,
    cost TEXT,
    sla_hours TEXT,
    UNIQUE (source_file, raw_line_no)
);

CREATE INDEX IF NOT EXISTS idx_bronze_batch ON bronze.raw_tickets(batch_id);

CREATE TABLE IF NOT EXISTS silver.tickets (
    silver_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bronze_id UUID NOT NULL REFERENCES bronze.raw_tickets(bronze_id),
    run_id UUID NOT NULL,
    batch_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    raw_line_no INTEGER NOT NULL,
    business_key TEXT NOT NULL,
    ticket_id TEXT,
    created_at_raw TEXT,
    resolved_at_raw TEXT,
    created_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    date_policy TEXT NOT NULL DEFAULT 'US_MM_DD',
    date_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    category_raw TEXT,
    category_normalized TEXT,
    priority_raw TEXT,
    priority_normalized TEXT,
    status_raw TEXT,
    status_normalized TEXT,
    building TEXT,
    description TEXT,
    submitted_by TEXT,
    assigned_to_raw TEXT,
    assigned_to_normalized TEXT,
    resolution_notes TEXT,
    cost_raw TEXT,
    cost NUMERIC(12,2),
    cost_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    sla_hours_raw TEXT,
    sla_hours INTEGER,
    sla_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    validation_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    cleaned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, business_key)
);

CREATE INDEX IF NOT EXISTS idx_silver_batch ON silver.tickets(batch_id);
CREATE INDEX IF NOT EXISTS idx_silver_ticket ON silver.tickets(ticket_id);

CREATE TABLE IF NOT EXISTS silver.tickets_quarantine (
    quarantine_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bronze_id UUID REFERENCES bronze.raw_tickets(bronze_id),
    run_id UUID NOT NULL,
    batch_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    raw_line_no INTEGER NOT NULL,
    reason_code TEXT NOT NULL,
    reason_detail TEXT,
    raw_payload JSONB NOT NULL,
    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, source_file, raw_line_no)
);

CREATE TABLE IF NOT EXISTS silver.tickets_dups (
    dup_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bronze_id UUID NOT NULL REFERENCES bronze.raw_tickets(bronze_id),
    run_id UUID NOT NULL,
    batch_id TEXT NOT NULL,
    business_key TEXT NOT NULL,
    canonical_bronze_id UUID NOT NULL,
    reason_code TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, bronze_id)
);

CREATE TABLE IF NOT EXISTS silver.tickets_ai (
    ai_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    silver_id UUID REFERENCES silver.tickets(silver_id),
    run_id UUID NOT NULL,
    batch_id TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    source_column TEXT NOT NULL,
    source_value TEXT NOT NULL,
    canonical_value TEXT NOT NULL,
    confidence NUMERIC(5,4) NOT NULL,
    model TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    token_count INTEGER NOT NULL DEFAULT 0,
    cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
    cache_hit BOOLEAN NOT NULL DEFAULT FALSE,
    method TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, silver_id, source_column)
);

CREATE TABLE IF NOT EXISTS meta.agent_runs (
    agent_run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    batch_id TEXT NOT NULL,
    agent_name TEXT NOT NULL,
    status TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    model TEXT NOT NULL,
    input_hash TEXT,
    output_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence NUMERIC(5,4),
    risk_level TEXT,
    token_count INTEGER NOT NULL DEFAULT 0,
    cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
    cache_hits INTEGER NOT NULL DEFAULT 0,
    cache_misses INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS meta.agent_cache (
    cache_key TEXT PRIMARY KEY,
    model TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    normalized_input TEXT NOT NULL,
    output_json JSONB NOT NULL,
    confidence NUMERIC(5,4) NOT NULL,
    token_count INTEGER NOT NULL DEFAULT 0,
    cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.agent_proposals (
    proposal_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    batch_id TEXT NOT NULL,
    agent_name TEXT NOT NULL,
    proposal_type TEXT NOT NULL,
    proposal_json JSONB NOT NULL,
    rationale TEXT NOT NULL,
    risk_level TEXT NOT NULL,
    confidence NUMERIC(5,4) NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at TIMESTAMPTZ,
    decision_note TEXT
);

CREATE TABLE IF NOT EXISTS meta.quality_results (
    quality_result_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    batch_id TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    check_name TEXT NOT NULL,
    status TEXT NOT NULL,
    observed_value NUMERIC,
    expected_value NUMERIC,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, stage_name, check_name)
);

CREATE TABLE IF NOT EXISTS meta.lineage (
    lineage_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    batch_id TEXT NOT NULL,
    source_table TEXT NOT NULL,
    target_table TEXT NOT NULL,
    transform_name TEXT NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, source_table, target_table, transform_name)
);

CREATE TABLE IF NOT EXISTS gold.ticket_volume_daily (
    batch_id TEXT NOT NULL,
    ticket_date DATE NOT NULL,
    status_normalized TEXT NOT NULL,
    priority_normalized TEXT NOT NULL,
    category_normalized TEXT NOT NULL,
    assigned_to_normalized TEXT NOT NULL,
    ticket_count INTEGER NOT NULL,
    built_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, ticket_date, status_normalized, priority_normalized, category_normalized, assigned_to_normalized)
);

CREATE TABLE IF NOT EXISTS gold.resolution_sla_summary (
    batch_id TEXT NOT NULL,
    priority_normalized TEXT NOT NULL,
    category_normalized TEXT NOT NULL,
    assigned_to_normalized TEXT NOT NULL,
    total_tickets INTEGER NOT NULL,
    eligible_tickets INTEGER NOT NULL,
    sla_met_count INTEGER NOT NULL,
    sla_breached_count INTEGER NOT NULL,
    sla_met_rate NUMERIC(8,4) NOT NULL,
    median_resolution_hours NUMERIC(12,2),
    p95_resolution_hours NUMERIC(12,2),
    excluded_count INTEGER NOT NULL,
    built_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, priority_normalized, category_normalized, assigned_to_normalized)
);

CREATE TABLE IF NOT EXISTS gold.category_breakdown (
    batch_id TEXT NOT NULL,
    canonical_category TEXT NOT NULL,
    classification_method TEXT NOT NULL,
    ticket_count INTEGER NOT NULL,
    avg_confidence NUMERIC(5,4),
    built_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, canonical_category, classification_method)
);
