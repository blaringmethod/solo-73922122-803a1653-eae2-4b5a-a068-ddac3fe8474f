"""Test configuration.

The dual-source tests exercise real PostgreSQL (transactions, advisory locks,
triggers), so point them at a database before ``app.config`` is imported:

    # default is the docker-compose published port
    PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres \
    PGDATABASE=traffic PYTHONPATH=. python3 -m pytest tests -q

Any PG* variable already set in the environment wins. ``BACKUP_ON_STARTUP``
is forced off so the PITR base backup does not run during these tests, and
all file paths are redirected into /tmp so nothing touches the compose volumes.
"""

import os

os.environ.setdefault("PGHOST", "127.0.0.1")
os.environ.setdefault("PGPORT", "5432")
os.environ.setdefault("PGUSER", "postgres")
os.environ.setdefault("PGPASSWORD", "postgres")
os.environ.setdefault("PGDATABASE", "traffic")
os.environ["BACKUP_ON_STARTUP"] = "0"
os.environ.setdefault("STATE_DB", "/tmp/traffic-test-state/api.db")
os.environ.setdefault("RECOVERY_ROOT", "/tmp/traffic-test-state/recovery_work")
os.environ.setdefault("BACKUP_DIR", "/tmp/traffic-test-state/backups/base")
os.environ.setdefault("WAL_ARCHIVE_DIR", "/tmp/traffic-test-state/wal_archive")
