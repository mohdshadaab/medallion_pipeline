"""U4: behavior parity vs the pre-change golden baseline (requires DB).

Run after a clean degraded pipeline run, e.g.:
    make clean && make up && make db-init
    docker compose run --rm -e OPENAI_API_KEY= app python -m src.cli run
    docker compose run --rm -e OPENAI_API_KEY= app python -m pytest tests/test_pipeline_parity.py -q

Asserts the async/concurrent classification reproduces the synchronous baseline
exactly, including the per-row method/cache_hit split that drives
gold.category_breakdown.
"""
from tests.metrics import collect_metrics


def test_degraded_run_matches_golden(golden_baseline):
    actual = collect_metrics()
    assert actual["rows"] == golden_baseline["rows"]
    assert actual["gold_counts"] == golden_baseline["gold_counts"]
    assert actual["agent_cache"] == golden_baseline["agent_cache"]
    assert actual["ai_methods"] == golden_baseline["ai_methods"]
    assert actual["category_breakdown"] == golden_baseline["category_breakdown"]
    assert actual["quality_results"] == golden_baseline["quality_results"]
