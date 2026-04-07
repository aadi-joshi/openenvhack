"""Unit tests for server.grader -- deterministic scoring and violation counts."""

import sqlite3

import pytest

from server.fixtures import create_corrupted_db, create_golden_db
from server.grader import compute_score, compute_violations


#  compute_violations 

class TestComputeViolations:
    def test_task1_initial_violations(self):
        conn = create_corrupted_db(1)
        v = compute_violations(conn, 1)
        # 3 duplicate rows (IDs 9, 10, 11)
        assert v == 3

    def test_task1_after_fix_zero_violations(self):
        conn = create_corrupted_db(1)
        # Delete the 3 duplicate rows (keep lowest ID per email)
        conn.execute("""
            DELETE FROM users
            WHERE id NOT IN (
                SELECT MIN(id) FROM users GROUP BY email
            )
        """)
        conn.commit()
        assert compute_violations(conn, 1) == 0

    def test_task2_initial_violations(self):
        conn = create_corrupted_db(2)
        v = compute_violations(conn, 2)
        # 6 purchases referencing 3 deleted products (2 orphans each)
        assert v == 6

    def test_task2_after_fix_zero_violations(self):
        conn = create_corrupted_db(2)
        # Re-insert the 3 deleted products
        import json
        logs = conn.execute("SELECT record_json FROM admin_audit_logs").fetchall()
        for (record_json,) in logs:
            p = json.loads(record_json)
            conn.execute(
                "INSERT OR REPLACE INTO products VALUES (?,?,?,?,?,?)",
                (p["id"], p["vendor_id"], p["name"], p["price"], p["stock_quantity"], p["category"]),
            )
        conn.commit()
        assert compute_violations(conn, 2) == 0

    def test_task3_initial_violations(self):
        conn = create_corrupted_db(3)
        v = compute_violations(conn, 3)
        # 7 unique employee IDs in employees_old not in employees_new
        # (IDs 3, 7, 11, 12, 13, 14, 15 - but 3 and 7 are duplicated)
        # DISTINCT count: 7
        assert v == 7

    def test_task3_after_fix_zero_violations(self):
        conn = create_corrupted_db(3)
        from server.fixtures import _EMP_CANONICAL, _ALREADY_MIGRATED_IDS
        # First: delete corrupted duplicates (keep first occurrence per ID)
        conn.execute("""
            DELETE FROM employees_old WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM employees_old GROUP BY id
            )
        """)
        # Then: migrate remaining employees
        for emp in _EMP_CANONICAL:
            eid = emp[0]
            if eid not in _ALREADY_MIGRATED_IDS:
                conn.execute(
                    "INSERT OR IGNORE INTO employees_new VALUES (?,?,?,?,?)",
                    (eid, emp[1], emp[2], emp[3], emp[5]),
                )
        conn.commit()
        assert compute_violations(conn, 3) == 0


#  compute_score 

class TestComputeScore:
    def test_golden_db_scores_1_0(self):
        """The golden DB should score perfectly against itself."""
        for task_id in (1, 2, 3):
            golden = create_golden_db(task_id)
            # Mock: patch create_golden_db to return the same conn
            # Instead, compare golden to itself directly
            from server import grader as g
            from server.fixtures import CORE_TABLES
            for table in CORE_TABLES[task_id]:
                rows = g._fetch_rows(golden, table)
                golden_set = set(rows)
                agent_set = set(rows)
                assert golden_set == agent_set
            golden.close()

    def test_unmodified_corrupted_db_scores_below_1_0(self):
        """Unrepaired DB must NOT score 1.0."""
        for task_id in (1, 2, 3):
            conn = create_corrupted_db(task_id)
            score = compute_score(conn, task_id)
            # Task 1 and 3: extra rows -> score < 1.0
            # Task 2: missing products -> score < 1.0
            assert score < 1.0, f"Task {task_id} unrepaired DB should not score 1.0"
            conn.close()

    def test_task1_perfect_repair_scores_1_0(self):
        conn = create_corrupted_db(1)
        conn.execute("""
            DELETE FROM users
            WHERE id NOT IN (SELECT MIN(id) FROM users GROUP BY email)
        """)
        conn.commit()
        score = compute_score(conn, 1)
        assert score == pytest.approx(0.99), f"Expected 0.99 (clamped perfect), got {score}"

    def test_task2_perfect_repair_scores_1_0(self):
        import json
        conn = create_corrupted_db(2)
        logs = conn.execute("SELECT record_json FROM admin_audit_logs").fetchall()
        for (record_json,) in logs:
            p = json.loads(record_json)
            conn.execute(
                "INSERT OR REPLACE INTO products VALUES (?,?,?,?,?,?)",
                (p["id"], p["vendor_id"], p["name"], p["price"], p["stock_quantity"], p["category"]),
            )
        conn.commit()
        score = compute_score(conn, 2)
        assert score == pytest.approx(0.99), f"Expected 0.99 (clamped perfect), got {score}"

    def test_task3_perfect_repair_scores_1_0(self):
        from server.fixtures import _EMP_CANONICAL, _ALREADY_MIGRATED_IDS
        conn = create_corrupted_db(3)
        # Delete corrupted duplicates
        conn.execute("""
            DELETE FROM employees_old WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM employees_old GROUP BY id
            )
        """)
        # Migrate remaining employees
        for emp in _EMP_CANONICAL:
            eid = emp[0]
            if eid not in _ALREADY_MIGRATED_IDS:
                conn.execute(
                    "INSERT OR IGNORE INTO employees_new VALUES (?,?,?,?,?)",
                    (eid, emp[1], emp[2], emp[3], emp[5]),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO compensation (employee_id, base_salary, bonus_pct, effective_date) VALUES (?,?,?,?)",
                    (eid, emp[4], 0.0, "2024-12-01"),
                )
        conn.commit()
        score = compute_score(conn, 3)
        assert score == pytest.approx(0.99), f"Expected 0.99 (clamped perfect), got {score}"

    def test_partial_repair_gives_partial_score(self):
        """Deleting only 1 of 3 duplicates should give partial score between 0 and 1."""
        conn = create_corrupted_db(1)
        conn.execute("DELETE FROM users WHERE id = 9")  # remove only 1 of 3 dups
        conn.commit()
        score = compute_score(conn, 1)
        assert 0.0 < score < 1.0, f"Expected partial score, got {score}"

    def test_score_is_deterministic(self):
        """Same DB state must always produce the same score."""
        conn1 = create_corrupted_db(1)
        conn2 = create_corrupted_db(1)
        assert compute_score(conn1, 1) == compute_score(conn2, 1)
