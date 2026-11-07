from __future__ import annotations

from contextlib import contextmanager
from importlib import resources

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

J = Jsonb


class Database:
    def __init__(self, url: str, *, max_size: int = 10):
        self.url = url
        self.pool = ConnectionPool(url, min_size=1, max_size=max_size, open=True,
                                   kwargs={"row_factory": dict_row, "autocommit": True})

    @contextmanager
    def tx(self):
        """One transaction: commit on success, rollback on any exception."""
        with self.pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            yield cur

    def connect_raw(self):
        return psycopg.connect(self.url, autocommit=True, row_factory=dict_row)

    def close(self):
        self.pool.close()


def migrate(db: Database) -> None:
    sql = (resources.files("relay") / "schema.sql").read_text()
    with db.tx() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(727001)")
        cur.execute(sql)
