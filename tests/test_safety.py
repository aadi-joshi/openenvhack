"""Unit tests for server.safety -- pre/post-execution checks."""

import sqlite3

import pytest

from server.safety import (
    SafetyViolation,
    budget_cost,
    check_post_execution,
    check_pre_execution,
    classify_sql,
)

CORE_TABLES = ["users", "products", "vendors"]


#  classify_sql 

class TestClassifySql:
    def test_select(self):
        assert classify_sql("SELECT * FROM users") == "SELECT"

    def test_select_cte(self):
        assert classify_sql("WITH cte AS (SELECT 1) SELECT * FROM cte") == "SELECT"

    def test_pragma(self):
        assert classify_sql("PRAGMA table_info(users)") == "META"

    def test_insert(self):
        assert classify_sql("INSERT INTO users VALUES (1, 'a@b.com')") == "MUTATION"

    def test_update(self):
        assert classify_sql("UPDATE users SET status='inactive' WHERE id=1") == "MUTATION"

    def test_delete(self):
        assert classify_sql("DELETE FROM users WHERE id=9") == "MUTATION"

    def test_alter(self):
        assert classify_sql("ALTER TABLE users ADD COLUMN phone TEXT") == "MUTATION"

    def test_create(self):
        assert classify_sql("CREATE TABLE foo (id INTEGER PRIMARY KEY)") == "MUTATION"

    def test_leading_whitespace(self):
        assert classify_sql("   \n  SELECT id FROM users") == "SELECT"


#  budget_cost 

class TestBudgetCost:
    def test_select_costs_1(self):
        assert budget_cost("SELECT") == 1

    def test_meta_costs_1(self):
        assert budget_cost("META") == 1

    def test_mutation_costs_5(self):
        assert budget_cost("MUTATION") == 5


#  check_pre_execution 

class TestPreExecution:
    def test_safe_select_returns_none(self):
        assert check_pre_execution("SELECT * FROM users", CORE_TABLES) is None

    def test_drop_core_table_triggers_violation(self):
        v = check_pre_execution("DROP TABLE users", CORE_TABLES)
        assert v is not None
        assert v.kind == "SCHEMA_DESTRUCTION"
        assert "users" in v.message

    def test_drop_non_core_table_is_allowed(self):
        v = check_pre_execution("DROP TABLE temp_backup", CORE_TABLES)
        assert v is None

    def test_drop_table_if_exists_core(self):
        v = check_pre_execution("DROP TABLE IF EXISTS products", CORE_TABLES)
        assert v is not None
        assert v.kind == "SCHEMA_DESTRUCTION"

    def test_delete_with_where_is_safe(self):
        v = check_pre_execution(
            "DELETE FROM users WHERE id = 9", CORE_TABLES
        )
        assert v is None

    def test_delete_without_where_triggers_violation(self):
        v = check_pre_execution("DELETE FROM users", CORE_TABLES)
        assert v is not None
        assert v.kind == "TRUNCATION"

    def test_delete_from_with_newline_no_where(self):
        v = check_pre_execution("DELETE\nFROM\nusers", CORE_TABLES)
        assert v is not None
        assert v.kind == "TRUNCATION"

    def test_delete_with_subquery_where(self):
        sql = "DELETE FROM users WHERE id IN (SELECT id FROM temp_dups)"
        v = check_pre_execution(sql, CORE_TABLES)
        assert v is None

    def test_drop_core_table_case_insensitive(self):
        v = check_pre_execution("drop table USERS", CORE_TABLES)
        assert v is not None
        assert v.kind == "SCHEMA_DESTRUCTION"

    def test_pragma_is_safe(self):
        assert check_pre_execution("PRAGMA foreign_key_check", CORE_TABLES) is None

    def test_insert_is_safe(self):
        assert (
            check_pre_execution(
                "INSERT INTO users VALUES (99, 'x@y.com', 'x', '2024-01-01', 'active')",
                CORE_TABLES,
            )
            is None
        )


#  check_post_execution 

class TestPostExecution:
    def _make_conn_with_users(self, n: int) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
        for i in range(n):
            conn.execute("INSERT INTO users VALUES (?, ?)", (i + 1, f"u{i}@test.com"))
        conn.commit()
        return conn

    def test_no_loss_returns_none(self):
        conn = self._make_conn_with_users(10)
        initial = {"users": 10}
        assert check_post_execution(conn, initial) is None

    def test_small_loss_is_ok(self):
        conn = self._make_conn_with_users(10)
        conn.execute("DELETE FROM users WHERE id = 10")
        conn.commit()
        # floor = 10 (what we must protect); 9 >= 10*0.9 = 9 - OK
        floor = {"users": 10}
        assert check_post_execution(conn, floor) is None

    def test_catastrophic_loss_triggers_violation(self):
        conn = self._make_conn_with_users(10)
        conn.execute("DELETE FROM users WHERE id > 2")  # 8 deleted -> 2 remain (20%)
        conn.commit()
        floor = {"users": 10}
        v = check_post_execution(conn, floor)
        assert v is not None
        assert v.kind == "CATASTROPHIC_LOSS"
        assert "users" in v.message

    def test_zero_initial_is_ignored(self):
        conn = self._make_conn_with_users(0)
        initial = {"users": 0}
        assert check_post_execution(conn, initial) is None
