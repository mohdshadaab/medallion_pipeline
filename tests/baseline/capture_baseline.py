"""Capture the pre-change golden baseline from the current (synchronous) code.

Run AFTER a degraded `make run` against a clean DB, e.g.:
    docker compose run --rm -e OPENAI_API_KEY= app python -m tests.baseline.capture_baseline

Writes tests/baseline/golden.json, which the U4 parity test compares against.
"""
import json
from pathlib import Path

from tests.metrics import collect_metrics


def main() -> None:
    metrics = collect_metrics()
    out = Path(__file__).with_name("golden.json")
    out.write_text(json.dumps(metrics, indent=2, sort_keys=True, default=str))
    print(f"wrote baseline to {out}")
    print(json.dumps(metrics, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
