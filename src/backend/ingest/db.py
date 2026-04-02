"""Database connection utilities for ingestion scripts."""

import os
from contextlib import contextmanager
from typing import Generator, Any

import psycopg2
from psycopg2.extras import RealDictCursor


def get_connection_string() -> str:
    """Get database connection string from environment."""
    # Support both individual vars and full connection string
    conn_string = os.getenv("DATABASE_URL")
    if conn_string:
        return conn_string

    # Build from individual components
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "postgres")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "")

    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


@contextmanager
def get_db_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Get a database connection context manager."""
    conn = psycopg2.connect(get_connection_string())
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_db_cursor(
    commit: bool = True,
) -> Generator[psycopg2.extensions.cursor, None, None]:
    """Get a database cursor context manager."""
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def create_ingestion_run(source_name: str, source_version: str = None) -> int:
    """Create a new ingestion run record and return its ID."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO admin.ingestion_runs (source_name, source_version, status)
            VALUES (%s, %s, 'started')
            RETURNING ingestion_run_id
            """,
            (source_name, source_version),
        )
        result = cursor.fetchone()
        return result["ingestion_run_id"]


def complete_ingestion_run(
    ingestion_run_id: int,
    rows_loaded: int,
    rows_rejected: int = 0,
    error_notes: str = None,
    success: bool = True,
) -> None:
    """Mark an ingestion run as completed."""
    status = "succeeded" if success else "failed"
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            UPDATE admin.ingestion_runs
            SET status = %s,
                rows_loaded = %s,
                rows_rejected = %s,
                error_notes = %s,
                finished_at = now()
            WHERE ingestion_run_id = %s
            """,
            (status, rows_loaded, rows_rejected, error_notes, ingestion_run_id),
        )
