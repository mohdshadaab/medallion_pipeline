"""U3: synchronous connection pool behind the existing db helpers (requires DB)."""
from src.db import execute, fetch_one, transaction


def test_pool_returns_dict_rows():
    assert fetch_one("SELECT 1 AS x") == {"x": 1}


def test_transaction_commits_and_rolls_back():
    execute("CREATE TABLE IF NOT EXISTS meta._pool_test (id int)")
    execute("DELETE FROM meta._pool_test")

    try:
        with transaction() as conn:
            conn.execute("INSERT INTO meta._pool_test (id) VALUES (1)")
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert fetch_one("SELECT count(*) AS c FROM meta._pool_test")["c"] == 0

    with transaction() as conn:
        conn.execute("INSERT INTO meta._pool_test (id) VALUES (2)")
    assert fetch_one("SELECT count(*) AS c FROM meta._pool_test")["c"] == 1

    execute("DROP TABLE meta._pool_test")
