"""U1: async classification primitive (compute-only, no DB)."""
from dataclasses import dataclass

from src import llm
from src.llm import ClassificationOutput, aclassify_value


@dataclass
class _Settings:
    openai_api_key: str | None
    openai_model: str = "gpt-4o-mini"
    openai_max_retries: int = 2
    openai_input_cost_per_1m_tokens: float = 0.15
    openai_output_cost_per_1m_tokens: float = 0.60


class _FakeResponses:
    async def parse(self, **kwargs):
        class _Resp:
            output_parsed = ClassificationOutput(canonical_category="hvac", confidence=0.95, rationale="mock")
            usage = {"input_tokens": 100, "output_tokens": 20, "total_tokens": 120}
        return _Resp()


class _FakeClient:
    responses = _FakeResponses()


async def test_degraded_uses_heuristic_no_network(monkeypatch):
    monkeypatch.setattr(llm, "get_settings", lambda: _Settings(openai_api_key=None))
    out, tokens, cost = await aclassify_value("breaker keeps tripping in the server room")
    assert out.canonical_category == "electrical"
    assert tokens == 0
    assert cost == 0.0


async def test_keyed_calls_client_and_extracts_cost(monkeypatch):
    monkeypatch.setattr(llm, "get_settings", lambda: _Settings(openai_api_key="test-key"))
    out, tokens, cost = await aclassify_value("thermostat reads the wrong temperature", client=_FakeClient())
    assert out.canonical_category == "hvac"
    assert out.confidence == 0.95
    assert tokens == 120
    assert cost > 0  # estimated from usage tokens via fallback rates
