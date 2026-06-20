"""U2: concurrency bound and partial-failure routing for classification.

These target the network-facing helper (`_classify_misses`) without a DB. Full
behavior-preservation (per-row method/cache_hit, gold parity) is covered by the
U4 parity test against the golden baseline.
"""
import asyncio
from dataclasses import dataclass

from src.agents import classify_agent
from src.llm import ClassificationOutput


@dataclass
class _Settings:
    classify_concurrency: int = 4
    openai_api_key: str = "test-key"


async def test_classify_misses_respects_concurrency_bound(monkeypatch):
    settings = _Settings(classify_concurrency=4)
    state = {"in_flight": 0, "peak": 0}

    async def fake_classify(text, client=None):
        state["in_flight"] += 1
        state["peak"] = max(state["peak"], state["in_flight"])
        await asyncio.sleep(0.01)
        state["in_flight"] -= 1
        return ClassificationOutput(canonical_category="hvac", confidence=0.95, rationale="m"), 5, 0.0

    monkeypatch.setattr(classify_agent, "aclassify_value", fake_classify)
    monkeypatch.setattr(classify_agent, "build_async_client", lambda: None)

    texts = [f"ticket text {i}" for i in range(20)]
    out = await classify_agent._classify_misses(texts, settings)

    assert len(out) == 20
    assert state["peak"] <= 4
    assert state["peak"] > 1  # actually ran concurrently


async def test_classify_misses_isolates_failures(monkeypatch):
    settings = _Settings(classify_concurrency=4)

    async def flaky_classify(text, client=None):
        if text == "boom":
            raise RuntimeError("api down")
        return ClassificationOutput(canonical_category="hvac", confidence=0.9, rationale="m"), 1, 0.0

    monkeypatch.setattr(classify_agent, "aclassify_value", flaky_classify)
    monkeypatch.setattr(classify_agent, "build_async_client", lambda: None)

    out = await classify_agent._classify_misses(["ok", "boom"], settings)

    assert out["ok"][0].canonical_category == "hvac"
    assert out["boom"][0].canonical_category == "unclassified"
    assert out["boom"][1] == 0  # no tokens charged for the failed call
