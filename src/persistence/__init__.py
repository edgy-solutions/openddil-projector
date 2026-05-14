"""Postgres persistence layer.

`Write` is the value object a handler returns: which table, which mode
(upsert/append), the primary-key column(s), and the row dict.
`PostgresPool` turns a `Write` into SQL and executes it with retry.
"""
from .postgres import PostgresPool, Write

__all__ = ["PostgresPool", "Write"]
