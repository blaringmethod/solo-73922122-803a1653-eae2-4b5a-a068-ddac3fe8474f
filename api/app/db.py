from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

from . import config


def primary(row_factory=None):
    """Connection to the always-on source database."""
    return psycopg.connect(
        config.PRIMARY_DSN, autocommit=True, connect_timeout=5, row_factory=row_factory
    )


@contextmanager
def primary_tx(row_factory=dict_row, *, repeatable_read=False, read_only=False):
    """Transactional connection to the source database.

    Commits when the block exits normally, rolls back on any exception. Used
    where bookkeeping and business rows must become visible atomically (the
    dual-source ingest) and where a query must see one consistent snapshot
    (``repeatable_read=True``).
    """
    conn = psycopg.connect(
        config.PRIMARY_DSN, connect_timeout=5, row_factory=row_factory
    )
    try:
        if repeatable_read:
            conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        if read_only:
            conn.read_only = True
        with conn:
            yield conn
    finally:
        conn.close()


def replica():
    """Read-only connection to the currently recovered replica."""
    return psycopg.connect(
        config.REPLICA_DSN, autocommit=True, connect_timeout=3, row_factory=dict_row
    )
