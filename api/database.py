"""
Database connection and utilities
"""
import psycopg
from psycopg.rows import dict_row
from contextlib import contextmanager
from typing import Generator

import os

# Database configuration (Aiven PostgreSQL or Render)
# Prioritize DATABASE_URL for Render deployment
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    # Render provides a full connection string: postgres://user:pass@host/db
    # psycopg.connect can handle this directly without parsing
    DB_CONFIG = {
        "conninfo": DATABASE_URL,
        "sslmode": "require" # Render usually requires SSL
    }
else:
    # Local development or individual vars
    DB_CONFIG = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "dbname": os.getenv("DB_NAME", "defaultdb"),
        "user": os.getenv("DB_USER", "dbuser"),
        "password": os.getenv("DB_PASSWORD", "dbpassword"),
        "sslmode": os.getenv("DB_SSLMODE", "prefer")
    }


@contextmanager
def get_db_connection() -> Generator:
    """
    Context manager for database connections

    Usage:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM datasets")
            results = cursor.fetchall()
    """
    conn = None
    try:
        conn = psycopg.connect(**DB_CONFIG, row_factory=dict_row)
        yield conn
    except psycopg.Error as e:
        if conn:
            conn.rollback()
        raise Exception(f"Database error: {str(e)}")
    finally:
        if conn:
            conn.close()


def execute_query(query: str, params: tuple = None) -> list:
    """
    Execute a SELECT query and return results as list of dicts

    Args:
        query: SQL query string
        params: Optional tuple of parameters for prepared statement

    Returns:
        List of dictionaries representing rows
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        return results


def execute_single(query: str, params: tuple = None, commit: bool = False) -> dict:
    """
    Execute a query and return single row as dict

    Args:
        query: SQL query string
        params: Optional tuple of parameters for prepared statement
        commit: Whether to commit the transaction (for INSERT/UPDATE/DELETE with RETURNING)

    Returns:
        Dictionary representing the row, or None if no results
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        result = cursor.fetchone()
        if commit:
            conn.commit()
        cursor.close()
        return result


def execute_insert(query: str, params: tuple = None):
    """
    Execute an INSERT/UPDATE/DELETE query that doesn't return results

    Args:
        query: SQL query string (INSERT/UPDATE/DELETE)
        params: Optional tuple of parameters for prepared statement

    Returns:
        None
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        cursor.close()


def test_connection() -> bool:
    """
    Test database connection

    Returns:
        True if connection successful, False otherwise
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False