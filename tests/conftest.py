"""Pytest fixtures shared across the suite."""
import json
from pathlib import Path

import pytest


@pytest.fixture
def golden_baseline() -> dict:
    """Load the committed pre-change golden baseline (see U0 capture script)."""
    path = Path(__file__).parent / "baseline" / "golden.json"
    if not path.exists():
        pytest.skip("golden.json not captured yet; run tests/baseline/capture_baseline.py")
    return json.loads(path.read_text())
