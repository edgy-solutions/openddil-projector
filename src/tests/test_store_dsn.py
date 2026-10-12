"""Store connection source: PG* environment, DSN only as compose fallback."""
from __future__ import annotations

import asyncio

import asyncpg
import pytest

from persistence import postgres
from persistence.postgres import (
    PostgresPool,
    StoreNotConfigured,
    store_dsn_from_env,
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in ("POSTGRES_DSN", "PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"):
        monkeypatch.delenv(k, raising=False)


def test_dsn_set_is_used(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgres://u:pw-test-only@h:5432/d")
    monkeypatch.setenv("PGHOST", "ignored")
    assert store_dsn_from_env() == "postgres://u:pw-test-only@h:5432/d"


def test_pghost_only_gives_none_and_asyncpg_gets_none(monkeypatch):
    monkeypatch.setenv("PGHOST", "db.internal")
    monkeypatch.setenv("PGPASSWORD", "pw-test-only")
    dsn = store_dsn_from_env()
    assert dsn is None

    seen = {}

    async def fake_create_pool(*args, **kwargs):
        seen["args"] = args
        return object()

    monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)
    pool = PostgresPool(dsn)
    asyncio.run(pool.connect())
    assert seen["args"] == (None,)


def test_neither_raises(monkeypatch):
    with pytest.raises(StoreNotConfigured):
        store_dsn_from_env()


def test_log_names_source_never_password(monkeypatch, caplog):
    monkeypatch.setenv("PGHOST", "db.internal")
    monkeypatch.setenv("PGDATABASE", "openddil")
    monkeypatch.setenv("PGUSER", "svc")
    monkeypatch.setenv("PGPASSWORD", "pw-test-only")
    with caplog.at_level("INFO", logger="projector.postgres"):
        store_dsn_from_env()
    text = caplog.text
    assert "host=db.internal db=openddil user=svc" in text
    assert "pw-test-only" not in text
