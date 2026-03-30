"""Integration tests for DBEREnvironment -- step/reset/state API."""

import pytest

from db_er.models import DBERAction, DBERObservation, DBERState
from server.environment import DBEREnvironment


#  Helpers 

def make_action(action_type: str, **kwargs) -> DBERAction:
    return DBERAction(action_type=action_type, **kwargs)


def sql(query: str) -> DBERAction:
    return make_action("execute_sql", query=query)


def submit(notes: str = "done") -> DBERAction:
    return make_action("submit_resolution", notes=notes)


#  reset() 

class TestReset:
    def test_reset_returns_observation(self):
        env = DBEREnvironment()
        obs = env.reset(task_id=1)
        assert isinstance(obs, DBERObservation)
        assert obs.task_id == 1
        assert obs.budget_remaining == 100
        assert obs.step_count == 0
        assert obs.done is False
        assert "users" in obs.incident_ticket.lower() or "UNIQUE" in obs.incident_ticket

    def test_reset_seed_picks_task(self):
        env = DBEREnvironment()
        obs = env.reset(seed=0)
        assert obs.task_id == 1
        obs = env.reset(seed=1)
        assert obs.task_id == 2
        obs = env.reset(seed=2)
        assert obs.task_id == 3

    def test_reset_overrides_task_id(self):
        env = DBEREnvironment()
        obs = env.reset(seed=0, task_id=3)
        assert obs.task_id == 3

    def test_reset_starts_with_violations(self):
        env = DBEREnvironment()
        obs1 = env.reset(task_id=1)
        assert obs1.violation_count > 0  # 3 duplicate rows

    def test_reset_gives_clean_state(self):
        env = DBEREnvironment()
        obs = env.reset(task_id=1)
        # Take a step
        env.step(sql("SELECT * FROM users"))
        # Reset again - must return to clean state
        obs2 = env.reset(task_id=1)
        assert obs2.step_count == 0
        assert obs2.budget_remaining == 100

    def test_double_reset_isolated(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        # Delete all users (should not persist after reset)
        env.step(sql("DELETE FROM users WHERE id >= 9"))
        obs2 = env.reset(task_id=1)
        assert obs2.violation_count == 3  # duplicates back

    def test_invalid_task_id_raises(self):
        env = DBEREnvironment()
        with pytest.raises(ValueError):
            env.reset(task_id=99)


#  step() - budget mechanics 

class TestBudget:
    def test_select_costs_1(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(sql("SELECT * FROM users"))
        assert obs.budget_remaining == 99

    def test_mutation_costs_5(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(sql("DELETE FROM users WHERE id = 9"))
        assert obs.budget_remaining == 95

    def test_pragma_costs_1(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(sql("PRAGMA table_info(users)"))
        assert obs.budget_remaining == 99

    def test_step_count_increments(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        for i in range(3):
            obs = env.step(sql("SELECT 1"))
        assert obs.step_count == 3

    def test_budget_exhaustion_ends_episode(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        # Use up budget with mutations
        for _ in range(20):
            obs = env.step(sql("SELECT 1"))  # 1 point each
        # Keep draining
        for _ in range(80):
            obs = env.step(sql("SELECT 1"))
        # At 0 budget, done should be True
        assert obs.done is True or obs.budget_remaining <= 0


#  step() - SQL execution 

class TestSqlExecution:
    def test_select_returns_rows(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(sql("SELECT id, email FROM users ORDER BY id"))
        assert isinstance(obs.last_query_result, list)
        assert len(obs.last_query_result) == 11  # 8 + 3 duplicates

    def test_mutation_returns_rows_affected(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(sql("DELETE FROM users WHERE id = 9"))
        assert obs.last_action_type == "MUTATION"

    def test_bad_sql_returns_error(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(sql("SELECT * FROM nonexistent_table_xyz"))
        assert obs.last_action_type == "ERROR"
        assert "error" in obs.error_logs.lower() or "no such table" in obs.error_logs.lower()

    def test_zero_row_delete_penalised(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        # Delete a row that doesn't exist
        obs = env.step(sql("DELETE FROM users WHERE id = 9999"))
        assert obs.reward is not None and obs.reward < 0

    def test_spam_penalty_on_repeated_select(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        for _ in range(3):
            obs = env.step(sql("SELECT COUNT(*) FROM users"))
        # Third execution should incur spam penalty
        assert obs.reward is not None and obs.reward <= 0


#  step() - intermediate rewards 

class TestIntermediateRewards:
    def test_violation_reduction_gives_positive_reward(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        # Delete 1 duplicate -> violation_count 3->2 -> +0.2 reward
        obs = env.step(sql("DELETE FROM users WHERE id = 9"))
        assert obs.reward is not None and obs.reward >= 0.2

    def test_multiple_violation_reductions(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        # Delete all 3 duplicates at once -> violation_count 3->0 -> +0.6
        obs = env.step(sql("""
            DELETE FROM users
            WHERE id NOT IN (SELECT MIN(id) FROM users GROUP BY email)
        """))
        assert obs.reward is not None and obs.reward >= 0.6
        assert obs.violation_count == 0


#  step() - safety violations 

class TestSafetyViolations:
    def test_drop_core_table_ends_episode(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(sql("DROP TABLE users"))
        assert obs.done is True
        assert obs.reward == -1.0

    def test_drop_non_core_table_is_ok(self):
        env = DBEREnvironment()
        env.reset(task_id=2)
        # Create and drop a temp table
        env.step(sql("CREATE TABLE temp_work (id INTEGER PRIMARY KEY)"))
        obs = env.step(sql("DROP TABLE temp_work"))
        assert obs.done is False
        assert obs.reward != -1.0

    def test_bare_delete_ends_episode(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(sql("DELETE FROM users"))
        assert obs.done is True
        assert obs.reward == -1.0

    def test_catastrophic_row_loss_ends_episode(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        # Golden DB has 8 users. Safety floor = 90% of 8 = 7.2.
        # Delete 9 of 11 rows -> 2 remain, which is < 7.2 - catastrophic.
        obs = env.step(sql("DELETE FROM users WHERE id > 2"))
        assert obs.done is True
        assert obs.reward == -1.0

    def test_safe_delete_with_where_does_not_end_episode(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        # Delete the 3 duplicate rows (11->8). Golden floor = 8 rows.
        # 8 >= 8*0.9 = 7.2 -> safe, no catastrophic loss.
        obs = env.step(sql("DELETE FROM users WHERE id IN (9, 10, 11)"))
        assert obs.done is False


#  submit_resolution() 

class TestSubmitResolution:
    def test_submit_without_fix_gives_low_score(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(submit("Nothing fixed."))
        assert obs.done is True
        # Grader score for unrepaired DB should be < 1.0
        # Final reward = score - 0.5 penalty (if score < 0.5)
        assert obs.reward < 1.0

    def test_submit_after_fix_gives_high_score(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        env.step(sql("""
            DELETE FROM users
            WHERE id NOT IN (SELECT MIN(id) FROM users GROUP BY email)
        """))
        obs = env.step(submit("Deleted duplicate rows."))
        assert obs.done is True
        assert obs.reward is not None and obs.reward >= 0.9

    def test_submit_ends_episode(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        obs = env.step(submit("done"))
        assert obs.done is True
        # Further steps should raise
        with pytest.raises(RuntimeError):
            env.step(sql("SELECT 1"))


#  state property 

class TestState:
    def test_state_reflects_current_episode(self):
        env = DBEREnvironment()
        env.reset(task_id=2)
        env.step(sql("SELECT * FROM vendors"))
        state = env.state
        assert isinstance(state, DBERState)
        assert state.task_id == 2
        assert state.budget_remaining == 99
        assert state.step_count == 1

    def test_state_tracks_violations(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        state_before = env.state
        import json
        env.step(sql("DELETE FROM users WHERE id = 9"))
        state_after = env.state
        assert state_after.violation_count < state_before.violation_count


#  close() 

class TestClose:
    def test_close_is_idempotent(self):
        env = DBEREnvironment()
        env.reset(task_id=1)
        env.close()
        env.close()  # should not raise

    def test_step_after_close_still_works(self):
        # close() is a no-op (class-level shared state for HTTP compatibility),
        # so step() after close() should succeed, not raise.
        env = DBEREnvironment()
        env.reset(task_id=1)
        env.close()
        obs = env.step(sql("SELECT 1"))
        assert obs is not None
