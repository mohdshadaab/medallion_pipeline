from collections.abc import Iterable
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from src.config import get_settings


def connect() -> psycopg.Connection:
    return psycopg.connect(get_settings().database_url, row_factory=dict_row)


def init_db() -> None:
    schema_path = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"
    with connect() as conn:
        conn.execute(schema_path.read_text())
        conn.commit()


@contextmanager
def transaction():
    with connect() as conn:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def fetch_one(query: str, params: Iterable[Any] | dict[str, Any] | None = None) -> dict[str, Any] | None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()


def fetch_all(query: str, params: Iterable[Any] | dict[str, Any] | None = None) -> list[dict[str, Any]]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return list(cur.fetchall())


def execute(query: str, params: Iterable[Any] | dict[str, Any] | None = None) -> None:
    with connect() as conn:
        conn.execute(query, params)
        conn.commit()
