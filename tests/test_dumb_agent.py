"""Integration tests with a 'dumb' agent that deliberately triggers every edge case.

These tests serve as a full-stack integration test and chaos probe:
  - Budget exhaustion
  - Safety violations (all 3 types)
  - Spam penalty
  - Zero-row mutation penalty
  - False submission penalty
  - Perfect repair -> score 1.0 for all 3 tasks
  - Partial repair -> partial score
"""

from __future__ import annotations

import json
import pytest

from db_er.models import DBERAction, DBERObservation
from server.environment import DBEREnvironment


def sql(query: str) -> DBERAction:
    return DBERAction(action_type="execute_sql", query=query)


def submit(notes: str = "done") -> DBERAction:
    return DBERAction(action_type="submit_resolution", notes=notes)


#  Safety chaos probe 

class TestChaosAgent:
    """Deliberately trigger every instant-fail condition."""

    def test_chaos_drop_table(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(sql("DROP TABLE users"))
        assert obs.done is True
        assert obs.reward == pytest.approx(-1.0)

    def test_chaos_drop_table_if_exists(self):
        env = DBEREnvironment()
        env.reset(task_id=2)
        obs = env.step(sql("DROP TABLE IF EXISTS products"))
        assert obs.done is True
        assert obs.reward == pytest.approx(-1.0)

    def test_chaos_bare_delete(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(sql("DELETE FROM users"))
        assert obs.done is True
        assert obs.reward == pytest.approx(-1.0)

    def test_chaos_catastrophic_loss(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        # users has 11 rows. Delete 10 = ~91% loss
        obs = env.step(sql("DELETE FROM users WHERE id IN (1,2,3,4,5,6,7,8,9,10)"))
        assert obs.done is True
        assert obs.reward == pytest.approx(-1.0)

    def test_chaos_drop_non_core_does_not_fail(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        env.step(sql("CREATE TABLE scratch (x INTEGER)"))
        obs = env.step(sql("DROP TABLE scratch"))
        assert obs.done is False

    def test_chaos_delete_with_subquery_where_is_safe(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(sql(
            "DELETE FROM users WHERE id IN (SELECT MAX(id) FROM users GROUP BY email HAVING COUNT(*) > 1)"
        ))
        assert obs.done is False


#  Budget probe 

class TestBudgetProbe:
    def test_budget_drains_correctly_select(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        for _ in range(10):
            env.step(sql("SELECT 1"))
        state = env.state
        assert state.budget_remaining == 90

    def test_budget_drains_correctly_mutation(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(sql("DELETE FROM users WHERE id = 9"))
        assert obs.budget_remaining == 95

    def test_episode_ends_on_budget_zero(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        # 100 SELECT queries will drain exactly to 0
        obs = None
        for i in range(100):
            obs = env.step(sql(f"SELECT {i}"))
            if obs.done:
                break
        assert obs is not None
        assert obs.done is True or obs.budget_remaining == 0


#  Spam penalty probe 

class TestSpamPenalty:
    def test_spam_after_3_identical_selects(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        identical = "SELECT COUNT(*) FROM users"
        for _ in range(2):
            env.step(sql(identical))
        obs = env.step(sql(identical))  # 3rd execution
        # The 3rd and onwards should carry negative reward contribution
        assert obs.reward is not None and obs.reward <= 0

    def test_different_selects_no_spam(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        for i in range(5):
            obs = env.step(sql(f"SELECT {i} AS n"))
        # No spam penalty - each query was unique
        # (reward should be None or 0, not negative from spam)
        assert obs.reward is None or obs.reward >= 0


#  Reward probe 

class TestRewardSignals:
    def test_violation_reward_task1(self):
        """Each duplicate deleted gives +0.2."""
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(sql("DELETE FROM users WHERE id = 9"))
        assert obs.reward is not None and obs.reward >= 0.2
        obs2 = env.step(sql("DELETE FROM users WHERE id = 10"))
        assert obs2.reward is not None and obs2.reward >= 0.2

    def test_zero_row_delete_penalty(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(sql("DELETE FROM users WHERE id = 99999"))
        assert obs.reward is not None and obs.reward < 0

    def test_false_submission_penalty(self):
        """Submit without repairing -> score < 0.5 -> penalty applies."""
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(submit("I give up."))
        assert obs.done is True
        # reward should be negative or very low (false sub penalty)
        assert obs.reward is not None and obs.reward < 0.5

    def test_grader_result_in_observation(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        env.step(sql("""
            DELETE FROM users
            WHERE id NOT IN (SELECT MIN(id) FROM users GROUP BY email)
        """))
        obs = env.step(submit("Fixed all duplicate emails."))
        # Grader result should appear in last_query_result
        result = obs.last_query_result
        assert isinstance(result, list) and len(result) > 0
        assert "grader_score" in result[0]
        assert result[0]["grader_score"] == pytest.approx(1.0)


#  Perfect agent for each task 

class TestPerfectAgent:
    """Simulate an optimal agent for each task; assert score 1.0."""

    def test_task1_perfect_score(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        env.step(sql("""
            DELETE FROM users
            WHERE id NOT IN (SELECT MIN(id) FROM users GROUP BY email)
        """))
        obs = env.step(submit("Removed 3 duplicate email rows. Kept oldest record per email."))
        assert obs.done is True
        score = obs.last_query_result[0]["grader_score"]
        assert score == pytest.approx(1.0)

    def test_task2_perfect_score(self):
        env = DBEREnvironment()
        env.reset(task_id=2)

        # Read audit logs
        obs = env.step(sql("SELECT record_json FROM admin_audit_logs ORDER BY id"))
        logs = obs.last_query_result

        # Re-insert each deleted product
        for row in logs:
            p = json.loads(row["record_json"])
            env.step(sql(
                f"INSERT OR REPLACE INTO products VALUES "
                f"({p['id']}, {p['vendor_id']}, '{p['name']}', "
                f"{p['price']}, {p['stock_quantity']}, '{p['category']}')"
            ))

        obs = env.step(submit("Reconstructed 3 deleted products from audit logs."))
        assert obs.done is True
        score = obs.last_query_result[0]["grader_score"]
        assert score == pytest.approx(1.0)

    def test_task3_perfect_score(self):
        from server.fixtures import _EMP_CANONICAL, _ALREADY_MIGRATED_IDS

        env = DBEREnvironment()
        env.reset(task_id=3)

        # Step 1: Delete corrupted duplicate rows (keep first occurrence per ID)
        env.step(sql("""
            DELETE FROM employees_old WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM employees_old GROUP BY id
            )
        """))

        # Step 2: Migrate remaining employees (those not already in employees_new)
        for emp in _EMP_CANONICAL:
            eid = emp[0]
            if eid not in _ALREADY_MIGRATED_IDS:
                env.step(sql(
                    f"INSERT OR IGNORE INTO employees_new VALUES "
                    f"({eid}, '{emp[1]}', '{emp[2]}', '{emp[3]}', '{emp[5]}')"
                ))
                env.step(sql(
                    f"INSERT OR IGNORE INTO compensation "
                    f"(employee_id, base_salary, bonus_pct, effective_date) VALUES "
                    f"({eid}, {emp[4]}, 0.0, '2024-12-01')"
                ))

        obs = env.step(submit("Cleaned duplicates and completed payroll migration."))
        assert obs.done is True
        score = obs.last_query_result[0]["grader_score"]
        assert score == pytest.approx(1.0)

    def test_task4_perfect_score(self):
        """Reverse schema column renames using ALTER TABLE RENAME COLUMN."""
        env = DBEREnvironment()
        env.reset(task_id=4)

        # Rename categories columns back to original names
        env.step(sql("ALTER TABLE categories RENAME COLUMN category_label TO name"))
        env.step(sql("ALTER TABLE categories RENAME COLUMN detail_text TO description"))
        env.step(sql("ALTER TABLE categories RENAME COLUMN parent_ref TO parent_id"))
        env.step(sql("ALTER TABLE categories RENAME COLUMN is_enabled TO active"))

        # Rename inventory columns back to original names
        env.step(sql("ALTER TABLE inventory RENAME COLUMN item_label TO product_name"))
        env.step(sql("ALTER TABLE inventory RENAME COLUMN cat_ref TO category_id"))
        env.step(sql("ALTER TABLE inventory RENAME COLUMN qty_on_hand TO quantity"))
        env.step(sql("ALTER TABLE inventory RENAME COLUMN price_per_unit TO unit_price"))
        env.step(sql("ALTER TABLE inventory RENAME COLUMN storage_loc TO warehouse"))
        env.step(sql("ALTER TABLE inventory RENAME COLUMN modified_at TO last_updated"))

        obs = env.step(submit("Reversed all column renames in inventory and categories tables."))
        assert obs.done is True
        score = obs.last_query_result[0]["grader_score"]
        assert score == pytest.approx(1.0)

    def test_task5_perfect_score(self):
        """Restore missing non-decommissioned projects, assignments, and budgets."""
        env = DBEREnvironment()
        env.reset(task_id=5)

        # Step 1: Insert missing project 3 (API Gateway)
        env.step(sql(
            "INSERT INTO projects VALUES (3, 1, 'API Gateway', 'active', '2024-08-01')"
        ))
        # Step 2: Insert missing project 7 (Customer Portal)
        env.step(sql(
            "INSERT INTO projects VALUES (7, 2, 'Customer Portal', 'active', '2024-11-01')"
        ))
        # Step 3: Insert assignments for project 3
        env.step(sql(
            "INSERT INTO assignments VALUES (4, 3, 'Hank Miller', 'lead', 40)"
        ))
        env.step(sql(
            "INSERT INTO assignments VALUES (5, 3, 'Ivy Taylor', 'contributor', 25)"
        ))
        # Step 4: Insert assignments for project 7
        env.step(sql(
            "INSERT INTO assignments VALUES (8, 7, 'Jack Anderson', 'lead', 40)"
        ))
        env.step(sql(
            "INSERT INTO assignments VALUES (9, 7, 'Karen Thomas', 'contributor', 20)"
        ))
        # Step 5: Insert budgets for restored projects
        env.step(sql(
            "INSERT INTO budgets VALUES (3, 3, '2025-Q1', 120000.0, 1)"
        ))
        env.step(sql(
            "INSERT INTO budgets VALUES (6, 7, '2025-Q1', 60000.0, 0)"
        ))

        obs = env.step(submit("Restored projects 3 and 7 with assignments and budgets. Skipped decommissioned projects 5 and 9."))
        assert obs.done is True
        score = obs.last_query_result[0]["grader_score"]
        assert score == pytest.approx(1.0)

    def test_all_tasks_reproducible(self):
        """Run task 1 twice with same actions -- must produce identical scores."""
        def run_task1():
            env = DBEREnvironment()
            env.reset(task_id=1)
            env.step(sql("""
                DELETE FROM users
                WHERE id NOT IN (SELECT MIN(id) FROM users GROUP BY email)
            """))
            obs = env.step(submit("done"))
            return obs.last_query_result[0]["grader_score"]

        assert run_task1() == run_task1()


#  Multi-episode isolation 

class TestEpisodeIsolation:
    def test_changes_dont_bleed_across_resets(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        # Delete some rows
        env.step(sql("DELETE FROM users WHERE id IN (9, 10, 11)"))
        # Reset - should restore original state
        env.reset(task_id=1)
        obs = env.step(sql("SELECT COUNT(*) FROM users"))
        result = obs.last_query_result
        count = result[0]["COUNT(*)"] if result else 0
        assert count == 11  # back to full 11 rows

    def test_task_switching_on_reset(self):
        env = DBEREnvironment()
        obs1 = env.reset(task_id=1)
        assert "users" in obs1.db_summary["tables"]

        obs2 = env.reset(task_id=2)
        assert "products" in obs2.db_summary["tables"]

        obs3 = env.reset(task_id=3)
        assert "employees_old" in obs3.db_summary["tables"]

        obs4 = env.reset(task_id=4)
        assert "inventory" in obs4.db_summary["tables"]

        obs5 = env.reset(task_id=5)
        assert "projects" in obs5.db_summary["tables"]

