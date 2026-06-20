import {
  Callout,
  Card,
  CardBody,
  CardHeader,
  Grid,
  H1,
  H2,
  Pill,
  Stack,
  Stat,
  Table,
  Text,
} from "cursor/canvas";

function Status({ level }: { level: "done" | "partial" }) {
  const done = level === "done";
  return (
    <Pill tone={done ? "success" : "warning"} active size="sm">
      {done ? "Done" : "Partial"}
    </Pill>
  );
}

const part1Rows = [
  [
    <Text size="small" weight="medium">Bronze — raw ingestion, schema-on-read, no data loss + lineage (source file, ingested_at, row hash)</Text>,
    <Text size="small">Append-only `bronze.raw_tickets`; every physical row kept; known cols as TEXT, full row in `raw_payload`, unknown cols in `extra_fields`; `source_file`, `ingested_at`, `content_hash`.</Text>,
    <Text size="small">`src/bronze.py`, `sql/schema.sql`</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">Silver — cleansed, deduplicated, typed, validated + documented rules</Text>,
    <Text size="small">US MM/DD date policy with flags; cost `$`/`-1`/`N/A`→NULL; SLA `0`/`999` excluded; dedup by `ticket_id` else fingerprint; quarantine + validation flags; reconciliation `bronze = clean + quarantine + dedup`.</Text>,
    <Text size="small">`src/silver.py`, README cleaning rules</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">Gold — 2-3 business products + justification</Text>,
    <Text size="small">`ticket_volume_daily`, `resolution_sla_summary`, `category_breakdown`; choices justified in README.</Text>,
    <Text size="small">`src/gold.py`</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">PostgreSQL + clear layer separation</Text>,
    <Text size="small">Dedicated schemas `bronze` / `silver` / `gold` / `meta`; Postgres 16 via Docker Compose.</Text>,
    <Text size="small">`sql/schema.sql`, `docker-compose.yml`</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">Idempotent & re-runnable</Text>,
    <Text size="small">Deterministic `batch_id`; `(source_file, raw_line_no)` unique; Silver/Gold delete+rebuild; `meta.processed_events`; verified 2× run → identical counts.</Text>,
    <Text size="small">`src/bronze.py`, `src/silver.py`, `src/gold.py`</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">Clear logging at each stage</Text>,
    <Text size="small">Structured stage / event / layer logs to stdout + `make status` (run, stage durations, quality, DLQ, agent cost/cache).</Text>,
    <Text size="small">`src/logging_config.py`, `src/cli.py`</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">Handle messiness without manual intervention</Text>,
    <Text size="small">Quarantine + per-field flags + sentinel handling; raw CSV ingested as-is, never edited.</Text>,
    <Text size="small">`src/silver.py`, `src/bronze.py`</Text>,
    <Status level="done" />,
  ],
];

const part2Rows = [
  [
    <Text size="small" weight="medium">a) Schema Inference & Evolution</Text>,
    <Text size="small">Header drift detection (added/removed cols) in `meta.source_schemas`; DQ agent samples a new Bronze column, proposes a typed Silver mapping; HITL approve → `ALTER` + populate. Bonus drift + migration proposal covered.</Text>,
    <Text size="small">`src/agents/dq_agent.py`, `src/services/file_discovery.py`, `src/cli.py`</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">b) Data Quality Agent</Text>,
    <Text size="small">Profiles the batch (quarantine rate, drift, counts), proposes rules with rationale + risk level, stored in `meta.agent_proposals` and gated for review — explains why, not just flags.</Text>,
    <Text size="small">`src/agents/dq_agent.py`, `meta.agent_runs`</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">c) Semantic Classification Agent</Text>,
    <Text size="small">Dictionary/regex pre-pass + LLM for unknown distinct values → `silver.tickets_ai`; cache by `hash(model+prompt+text)`, async bounded concurrency, token/cost tracked, degraded fallback.</Text>,
    <Text size="small">`src/agents/classify_agent.py`, `src/llm.py`</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">d) Gold Layer Design Agent</Text>,
    <Text size="small">Design-time product suggester + `gold_impact` proposals for promoted fields. Final Gold tables stay deterministic in code (reporting contract) rather than agent-generated DDL.</Text>,
    <Text size="small">`src/agents/gold_suggester.py`, `src/cli.py`</Text>,
    <Status level="partial" />,
  ],
];

const readmeRows = [
  [
    <Text size="small" weight="medium">Architecture diagram</Text>,
    <Text size="small">Mermaid flow diagram in README.</Text>,
    <Text size="small">`README.md`</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">Agent assessment (what / sample I/O / honest take)</Text>,
    <Text size="small">Per-agent assessment with inputs, outputs, and honest tradeoffs.</Text>,
    <Text size="small">`README.md` (Agent Assessment)</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">What changes at 100x scale</Text>,
    <Text size="small">Scale section in README plus a full enterprise scale plan.</Text>,
    <Text size="small">`README.md`, `plans/medallion_ai_pipeline_enterprise_scale_plan.md`</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">How to run (single command up)</Text>,
    <Text size="small">`make up` + `make db-init` + `make run`, then `make status`.</Text>,
    <Text size="small">`README.md`, `Makefile`</Text>,
    <Status level="done" />,
  ],
];

const goodToHaveRows = [
  [
    <Text size="small" weight="medium">Data lineage (end-to-end)</Text>,
    <Text size="small">`meta.lineage` records Bronze→Silver→Gold transforms; run/stage metadata ties every row to a batch and run.</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">Metadata auto-tagging at landing (PII hints)</Text>,
    <Text size="small">Suspected-PII flag on new-column proposals; not yet a full automatic tagging pass over all columns at landing.</Text>,
    <Status level="partial" />,
  ],
  [
    <Text size="small" weight="medium">Cross-source reconciliation + dimensional model rec</Text>,
    <Text size="small">Batch reconciliation gate + dimensional sketch from the Gold suggester. Single source here, so cross-source reconciliation is N/A.</Text>,
    <Status level="partial" />,
  ],
  [
    <Text size="small" weight="medium">Agent evaluation harness</Text>,
    <Text size="small">Repeatable labeled set with accuracy + format-compliance scoring.</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">Human-in-the-loop approval gates</Text>,
    <Text size="small">`make proposals` / `approve` / `reject`; approval applies a real Silver `ALTER` and records the mapping.</Text>,
    <Status level="done" />,
  ],
  [
    <Text size="small" weight="medium">Incremental + backfill strategy</Text>,
    <Text size="small">Idempotent reruns + deterministic `batch_id` today; full incremental / late-arriving / replay-window handling designed in the scale plan, not implemented locally.</Text>,
    <Status level="partial" />,
  ],
  [
    <Text size="small" weight="medium">Observability & SLA thinking</Text>,
    <Text size="small">Structured logs, `meta.*` metric tables, `make status`, Gold SLA metrics, and late-stage flags.</Text>,
    <Status level="done" />,
  ],
];

export default function TakeHomeCoverage() {
  return (
    <Stack gap={20} style={{ padding: 24 }}>
      <Stack gap={6}>
        <H1>Take-Home Coverage — AI-Assisted Medallion Pipeline</H1>
        <Text tone="secondary">
          Every assignment line item from `TAKE_HOME.md` mapped to what we built, the evidence, and status. Source: `medallion_pipeline/`.
        </Text>
      </Stack>

      <Grid columns={4} gap={16}>
        <Stat value="Complete" label="Part 1 — Pipeline" tone="success" />
        <Stat value="4 built" label="Part 2 — Agents (2 required)" tone="success" />
        <Stat value="4 / 4" label="Required README items" tone="success" />
        <Stat value="7 passing" label="Automated tests" tone="success" />
      </Grid>

      <Stack gap={8}>
        <H2>Part 1 — Medallion Pipeline (50%)</H2>
        <Table
          headers={["Assignment line item", "What we did", "Evidence", "Status"]}
          columnAlign={["left", "left", "left", "center"]}
          rows={part1Rows}
        />
      </Stack>

      <Stack gap={8}>
        <H2>Part 2 — Agentic Acceleration (50%)</H2>
        <Callout tone="info" title="Requirement: pick at least two">
          We implemented all four options — b, c, and a fully; d as a design-time helper.
        </Callout>
        <Table
          headers={["Option", "What we built", "Evidence", "Status"]}
          columnAlign={["left", "left", "left", "center"]}
          rows={part2Rows}
          rowTone={[undefined, undefined, undefined, "warning"]}
        />
      </Stack>

      <Stack gap={8}>
        <H2>Required in README</H2>
        <Table
          headers={["Requirement", "What we did", "Evidence", "Status"]}
          columnAlign={["left", "left", "left", "center"]}
          rows={readmeRows}
        />
      </Stack>

      <Stack gap={8}>
        <H2>"Good to have" (optional)</H2>
        <Table
          headers={["Touch", "What we did", "Status"]}
          columnAlign={["left", "left", "center"]}
          rows={goodToHaveRows}
          rowTone={[undefined, "warning", "warning", undefined, undefined, "warning", undefined]}
        />
      </Stack>

      <Stack gap={8}>
        <H2>Verification snapshot</H2>
        <Grid columns={4} gap={16}>
          <Stat value="10,280" label="Bronze rows (no loss)" />
          <Stat value="9,749 / 531" label="Silver clean / quarantined" />
          <Stat value="0" label="DLQ events" tone="success" />
          <Stat value="~80s → ~2s" label="Classification stage" tone="success" />
        </Grid>
        <Text tone="secondary" size="small">
          Reconciliation `bronze = clean + quarantine + dedup` passed; re-running twice yields identical Silver/Gold; Gold = 9,701 daily-volume rows, 580 SLA rows, 30 category rows; concurrent classification preserves the golden baseline exactly (`cache_hits=1616`, `cache_misses=3785`).
        </Text>
      </Stack>

      <Card>
        <CardHeader>Beyond the brief (not required)</CardHeader>
        <CardBody>
          <Stack gap={6}>
            <Text size="small">Event-driven Kafka/KRaft spine carrying batch/stage events (Postgres stays source of truth).</Text>
            <Text size="small">Transactional outbox + DLQ + `meta.processed_events` for at-least-once delivery with idempotent consumers.</Text>
            <Text size="small">Governed schema-drift promotion: new Bronze column → proposal → human approval → real Silver column, with separate `gold_impact` proposals.</Text>
            <Text size="small">asyncio concurrency refactor of classification (~80s → ~2s degraded) behind a characterization golden-baseline parity test (7 tests passing).</Text>
            <Text size="small">Enterprise scale plan plus a compound-engineering multi-persona review that narrowed the async scope before implementation.</Text>
          </Stack>
        </CardBody>
      </Card>

      <Card>
        <CardHeader>Run & verify</CardHeader>
        <CardBody>
          <Stack gap={6}>
            <Text size="small">Start + run: `make up` → `make db-init` → `make run` → `make status`</Text>
            <Text size="small">Schema-drift HITL demo: `make run-drift` → `make proposals` → `make approve ID=...`</Text>
            <Text size="small">Parity (degraded, no API spend): `docker compose run --rm -e OPENAI_API_KEY= app python -m pytest -q`</Text>
          </Stack>
        </CardBody>
      </Card>

      <Callout tone="warning" title="Honest gaps & deviations">
        <Stack gap={6}>
          <Text size="small">Dataset lives at project root `raw_tickets.csv` (configurable via `RAW_TICKETS_PATH`), not `data/raw_tickets.csv`.</Text>
          <Text size="small">Schema "DDL generation" is a governed mapping + `ALTER` on approval, not free-form LLM-emitted DDL.</Text>
          <Text size="small">Gold design agent is a suggester; final Gold tables are deterministic code, by choice.</Text>
          <Text size="small">Full PII auto-tagging and incremental/late-arriving ingestion are designed in the scale plan, not implemented locally.</Text>
          <Text size="small">`.env` currently holds a real `OPENAI_API_KEY` in plaintext — rotate before sharing.</Text>
        </Stack>
      </Callout>
    </Stack>
  );
}
