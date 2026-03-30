"""Deterministic terminal grader for DB-ER.

Score (0.0 - 1.0) is computed by comparing the agent's final DB against the
hidden golden DB using:

  - ``PRAGMA table_info(<table>)``    -- schema structure (column names / types / nullability)
  - ``SELECT * FROM <table> ORDER BY rowid`` -- exact row data

For each core table the F1 score between the golden row-set and the agent
row-set is computed.  Tables are weighted equally.  A schema mismatch caps
the table score at 0.5.

This function is 100% deterministic (no hashing, no LLM, no randomness).
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Tuple

from server.fixtures import CORE_TABLES


#  Public API 

def compute_score(agent_conn: sqlite3.Connection, task_id: int) -> float:
    """Compare *agent_conn* against the golden DB and return a score in [0, 1].

    The golden DB is created fresh (read-only in-memory) each call to ensure
    it can never be contaminated by agent actions.
    """
    from server.fixtures import create_golden_db  # local import to avoid circular deps

    golden_conn = create_golden_db(task_id)
    try:
        table_scores = []
        for table in CORE_TABLES[task_id]:
            score = _score_table(agent_conn, golden_conn, table)
            table_scores.append(score)
        return round(sum(table_scores) / len(table_scores), 4) if table_scores else 0.0
    finally:
        golden_conn.close()


#  Per-table scoring 

def _score_table(
    agent_conn: sqlite3.Connection,
    golden_conn: sqlite3.Connection,
    table: str,
) -> float:
    """Return [0, 1] score for a single table."""

    # 1. Schema check
    schema_match = _schemas_match(agent_conn, golden_conn, table)

    # 2. Row data - fetch as frozensets of tuples for set comparison
    golden_rows = _fetch_rows(golden_conn, table)
    agent_rows = _fetch_rows(agent_conn, table)

    if not golden_rows and not agent_rows:
        return 1.0  # both empty - perfect

    if not golden_rows:
        return 0.0  # golden has no data but agent does - wrong

    golden_set = set(golden_rows)
    agent_set = set(agent_rows)

    matching = len(golden_set & agent_set)
    precision = matching / len(agent_set) if agent_set else 0.0
    recall = matching / len(golden_set)

    if precision + recall == 0:
        data_score = 0.0
    else:
        data_score = 2 * precision * recall / (precision + recall)  # F1

    # Schema mismatch: cap the maximum achievable score at 0.5
    if not schema_match:
        data_score = min(data_score, 0.5)

    return data_score


def _fetch_rows(conn: sqlite3.Connection, table: str) -> List[Tuple[Any, ...]]:
    """Fetch all rows ordered by rowid for deterministic comparison."""
    try:
        cursor = conn.execute(f"SELECT * FROM {table} ORDER BY rowid")
        return [tuple(row) for row in cursor.fetchall()]
    except sqlite3.OperationalError:
        return []


def _schemas_match(
    agent_conn: sqlite3.Connection,
    golden_conn: sqlite3.Connection,
    table: str,
) -> bool:
    """Compare PRAGMA table_info output for the given table."""
    golden_info = _table_info(golden_conn, table)
    agent_info = _table_info(agent_conn, table)
    return golden_info == agent_info


def _table_info(conn: sqlite3.Connection, table: str) -> List[Tuple]:
    """Return (name, type, notnull, pk) tuples from PRAGMA table_info."""
    try:
        cursor = conn.execute(f"PRAGMA table_info({table})")
        # We compare only name, type, notnull, pk (ignore cid and dflt_value for flexibility)
        return [(row[1], row[2].upper(), row[3], row[5]) for row in cursor.fetchall()]
    except sqlite3.OperationalError:
        return []


#  Violation counter (used for intermediate rewards) 

def compute_violations(conn: sqlite3.Connection, task_id: int) -> int:
    """Return the current violation count for the given task.

    Task 1: number of extra (duplicate) email rows.
    Task 2: number of FK violations in the purchases table.
    Task 3: number of unique employee IDs in employees_old not yet in employees_new.
    """
    try:
        if task_id == 1:
            row = conn.execute(
                """
                SELECT COUNT(*) FROM users
                WHERE id NOT IN (
                    SELECT MIN(id) FROM users GROUP BY email
                )
                """
            ).fetchone()
            return row[0]

        elif task_id == 2:
            conn.execute("PRAGMA foreign_keys = ON")
            rows = conn.execute("PRAGMA foreign_key_check(purchases)").fetchall()
            return len(rows)

        elif task_id == 3:
            row = conn.execute(
                """
                SELECT COUNT(DISTINCT id) FROM employees_old
                WHERE id NOT IN (SELECT id FROM employees_new)
                """
            ).fetchone()
            return row[0]

    except Exception:
        return 0

    return 0
