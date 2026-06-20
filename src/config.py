from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "postgresql://medallion:medallion@localhost:5432/medallion"
    kafka_bootstrap_servers: str = "localhost:9092"
    raw_tickets_path: Path = Path("raw_tickets.csv")
    source_file_name: str = "raw_tickets.csv"
    openai_api_key: str | None = None
    openai_model: str = "gpt-4o-mini"
    # Fallback per-1M-token rates, used only when the API response carries no explicit cost field.
    openai_input_cost_per_1m_tokens: float = 0.15
    openai_output_cost_per_1m_tokens: float = 0.60
    # Bounded concurrency for the classification stage and SDK retry budget for 429s under load.
    classify_concurrency: int = 8
    openai_max_retries: int = 5
    prompt_version: str = "v1"
    kafka_required: bool = False
    pipeline_stage_sla_seconds: int = 300
    log_level: str = "INFO"
    log_format: str = "human"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
