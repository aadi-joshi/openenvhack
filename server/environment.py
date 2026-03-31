"""DBEREnvironment -- the core OpenEnv Environment implementation.

This class wires together:
  - fixtures  (corrupted DB per task, golden DB for grading)
  - safety    (pre/post-execution guards)
  - grader    (terminal score 0.0-1.0)
  - reward shaping (intermediate signals)

Episode lifecycle:
  reset(seed, task_id)  -> fresh DB, clean state, first observation
  step(action)          -> execute SQL or submit; returns updated observation
  state                 -> server-side state snapshot

See PROMPT.md sec2 for the full specification.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any, Dict, List, Optional

from openenv.core.env_server.interfaces import Environment, EnvironmentMetadata

from db_er.models import DBERAction, DBERObservation, DBERState
from server.fixtures import (
    CORE_TABLES,
    GOLDEN_ROW_COUNTS,
    TASK_TICKETS,
    create_corrupted_db,
    get_initial_row_counts,
)
from server.grader import compute_score, compute_violations
from server.safety import (
    SafetyViolation,
    budget_cost,
    check_post_execution,
    check_pre_execution,
    classify_sql,
)


#  Module-level shared episode state 
# openenv-core creates a NEW DBEREnvironment() instance per HTTP request
# (reset_handler and step_handler each call self._env_factory()).  All state
# must therefore live outside any instance - in this module-level object whose
# attributes are mutated in-place so every DBEREnvironment instance sees them.

class _EpisodeState:
    def __init__(self) -> None:
        self.conn: Optional[sqlite3.Connection] = None
        self.task_id: int = 1
        self.budget: int = 100
        self.violation_count: int = 0
        self.initial_row_counts: Dict[str, int] = {}
        self.safety_row_counts: Dict[str, int] = {}
        self.query_history: Dict[str, int] = {}
        self.step_count: int = 0
        self.episode_id: Optional[str] = None
        self.done: bool = False
        self.last_obs: Optional[DBERObservation] = None


_S = _EpisodeState()  # singleton - shared by all DBEREnvironment instances


class DBEREnvironment(Environment[DBERAction, DBERObservation, DBERState]):
    """Database Emergency Response OpenEnv environment.

    Supports 5 tasks of increasing difficulty:
      1 -- Phantom Duplicates  (Easy)
      2 -- Cascading Failure   (Medium)
      3 -- Payroll Black Hole  (Medium-Hard)
      4 -- Schema Drift        (Hard)
      5 -- Referential Maze    (Very Hard)
    """

    SUPPORTS_CONCURRENT_SESSIONS = False

    def get_metadata(self) -> EnvironmentMetadata:
        return EnvironmentMetadata(
            name="DB-ER: Database Emergency Response",
            description=(
                "An OpenEnv benchmark where an AI agent is paged into a production "
                "database incident and must diagnose and repair corruption using raw "
                "SQL queries under a strict action budget. Features 5 tasks spanning "
                "duplicate cleanup, FK restoration, payroll migration, schema repair, "
                "and multi-table referential integrity recovery. Graded deterministically "
                "via F1 score on exact row sets against a hidden golden database."
            ),
            version="1.0.0",
            author="Team maxout (Aadi Joshi, Kavya Bhand)",
            documentation_url="https://huggingface.co/spaces/aadi-joshi/openenvhack",
        )

    #  OpenEnv API 

    def reset(
        self,
        seed: Optional[int] = None,
        episode_id: Optional[str] = None,
        task_id: Optional[int] = None,
        **kwargs,
    ) -> DBERObservation:
        """Start a new episode.

        Args:
            seed:       Controls task selection when *task_id* is not given.
                        seed % 3 -> 0=task 1, 1=task 2, 2=task 3.
            episode_id: Opaque identifier stored in state.
            task_id:    Override task selection (1, 2 or 3).
        """
        #  1. Pick task 
        if task_id is not None:
            if task_id not in (1, 2, 3, 4, 5):
                raise ValueError(f"task_id must be 1, 2, 3, 4, or 5 -- got {task_id!r}")
            _S.task_id = task_id
        elif seed is not None:
            _S.task_id = (seed % 5) + 1
        else:
            _S.task_id = 1

        #  2. Spin up a fresh corrupted DB 
        if _S.conn is not None:
            try:
                _S.conn.close()
            except Exception:
                pass

        _S.conn = create_corrupted_db(_S.task_id)
        _S.initial_row_counts = get_initial_row_counts(_S.conn, _S.task_id)
        # Safety floor = min(corrupted_initial, golden) per table.
        # - Task 1 (users 11->8 fix): min(11,8)=8  -> can delete 3 dupes, can't go below 8
        # - Task 2 (products 9->12 fix): min(9,12)=9 -> can INSERT more, can't delete any
        # - Task 3 (employees_old 18->15 fix): min(18,15)=15 -> can delete 3 dupes
        golden = GOLDEN_ROW_COUNTS[_S.task_id]
        _S.safety_row_counts = {
            table: min(_S.initial_row_counts.get(table, 0), golden.get(table, 0))
            for table in CORE_TABLES[_S.task_id]
        }

        #  3. Reset episode bookkeeping 
        _S.budget = 100
        _S.violation_count = compute_violations(_S.conn, _S.task_id)
        _S.query_history = {}
        _S.step_count = 0
        _S.episode_id = episode_id
        _S.done = False

        #  4. Build and return initial observation 
        obs = self._build_observation(
            last_action_type="NONE",
            last_query_result=[],
            error_logs="",
            reward=None,
            done=False,
        )
        _S.last_obs = obs
        return obs

    def step(
        self,
        action: DBERAction,
        timeout_s: Optional[float] = None,
        **kwargs,
    ) -> DBERObservation:
        """Execute one action and return the resulting observation."""
        if _S.conn is None or _S.done:
            raise RuntimeError(
                "Environment not initialised or episode already done. Call reset() first."
            )

        _S.step_count += 1

        if action.action_type == "execute_sql":
            return self._handle_execute_sql(action)
        elif action.action_type == "submit_resolution":
            return self._handle_submit_resolution(action)
        else:
            # Unknown action type - count as an error
            obs = self._build_observation(
                last_action_type="ERROR",
                last_query_result=[],
                error_logs=f"Unknown action_type: {action.action_type!r}",
                reward=-0.05,
                done=False,
            )
            _S.last_obs = obs
            return obs

    @property
    def state(self) -> DBERState:
        """Server-side state snapshot (for GET /state)."""
        return DBERState(
            task_id=_S.task_id,
            budget_remaining=_S.budget,
            violation_count=_S.violation_count,
            initial_row_counts=_S.initial_row_counts,
            query_history=dict(_S.query_history),
            episode_id=_S.episode_id,
            step_count=_S.step_count,
        )

    def close(self) -> None:
        # No-op: openenv-core calls close() after each HTTP request, but the
        # SQLite connection must persist across requests in _S (the singleton).
        pass

    #  Private helpers 

    def _handle_execute_sql(self, action: DBERAction) -> DBERObservation:
        query = (action.query or "").strip()
        if not query:
            return self._build_observation(
                last_action_type="ERROR",
                last_query_result=[],
                error_logs="execute_sql action requires a non-empty 'query' field.",
                reward=-0.05,
                done=False,
            )

        sql_type = classify_sql(query)
        core_tables = CORE_TABLES[_S.task_id]

        #  Pre-execution safety check 
        violation = check_pre_execution(query, core_tables)
        if violation:
            _S.done = True
            obs = self._build_observation(
                last_action_type="ERROR",
                last_query_result=[],
                error_logs=violation.message,
                reward=-1.0,
                done=True,
            )
            _S.last_obs = obs
            return obs

        #  Budget check 
        cost = budget_cost(sql_type)
        if _S.budget < cost:
            _S.done = True
            obs = self._build_observation(
                last_action_type="ERROR",
                last_query_result=[],
                error_logs=(
                    f"Budget exhausted. Remaining={_S.budget}, "
                    f"required={cost} for {sql_type}. Episode ended."
                ),
                reward=-0.5,
                done=True,
            )
            _S.last_obs = obs
            return obs

        #  Spam penalty 
        spam_penalty = 0.0
        if sql_type in ("SELECT", "META"):
            norm = _normalise_query(query)
            count = _S.query_history.get(norm, 0) + 1
            _S.query_history[norm] = count
            if count >= 3:
                spam_penalty = -0.05

        #  Execute the SQL 
        _S.budget -= cost

        prev_violation_count = _S.violation_count
        result_rows: List[Dict[str, Any]] = []
        error_msg = ""
        action_type_label = sql_type

        try:
            if sql_type == "MUTATION":
                # Use a savepoint so we can rollback on post-execution safety failure.
                # IMPORTANT: do NOT call conn.commit() before releasing the savepoint --
                # commit() would persist the changes before we can check row counts.
                _S.conn.execute("SAVEPOINT before_mutation")
                try:
                    cursor = _S.conn.execute(query)
                    affected = cursor.rowcount

                    #  Post-execution safety check (pre-commit) 
                    # Use golden row counts as floor so agents can delete corrupted rows
                    post_violation = check_post_execution(
                        _S.conn, _S.safety_row_counts
                    )
                    if post_violation:
                        # Roll back everything to before the mutation
                        _S.conn.execute("ROLLBACK TO before_mutation")
                        _S.conn.execute("RELEASE before_mutation")
                        _S.done = True
                        obs = self._build_observation(
                            last_action_type="ERROR",
                            last_query_result=[],
                            error_logs=post_violation.message,
                            reward=-1.0,
                            done=True,
                        )
                        _S.last_obs = obs
                        return obs

                    # Safe: commit by releasing the savepoint
                    _S.conn.execute("RELEASE before_mutation")

                    # Intermediate reward for 0-row mutations
                    if affected == 0:
                        spam_penalty += -0.1  # -0.1 for 0-row UPDATE/DELETE

                    result_rows = [{"rows_affected": affected}]
                    action_type_label = "MUTATION"

                except sqlite3.Error as exc:
                    try:
                        _S.conn.execute("ROLLBACK TO before_mutation")
                        _S.conn.execute("RELEASE before_mutation")
                    except Exception:
                        pass
                    error_msg = f"SQLite error: {exc}"
                    action_type_label = "ERROR"

            else:
                # SELECT / PRAGMA / META
                cursor = _S.conn.execute(query)
                rows = cursor.fetchall()
                if rows:
                    cols = [d[0] for d in cursor.description or []]
                    result_rows = [dict(zip(cols, row)) for row in rows]
                action_type_label = sql_type

        except sqlite3.Error as exc:
            error_msg = f"SQLite error: {exc}"
            action_type_label = "ERROR"
        except Exception as exc:
            error_msg = f"Unexpected error: {exc}"
            action_type_label = "ERROR"

        #  Recompute violations 
        _S.violation_count = compute_violations(_S.conn, _S.task_id)

        # Intermediate reward for reducing violations
        violation_reward = 0.0
        if _S.violation_count < prev_violation_count:
            reduction = prev_violation_count - _S.violation_count
            violation_reward = 0.2 * reduction

        total_reward = violation_reward + spam_penalty

        #  Budget exhausted mid-episode? 
        if _S.budget <= 0:
            _S.done = True

        obs = self._build_observation(
            last_action_type=action_type_label,
            last_query_result=result_rows if not error_msg else error_msg,
            error_logs=error_msg,
            reward=total_reward if total_reward != 0 else None,
            done=_S.done,
        )
        _S.last_obs = obs
        return obs

    def _handle_submit_resolution(self, action: DBERAction) -> DBERObservation:
        """Trigger the terminal grader and end the episode."""
        _S.done = True

        # Run the deterministic grader
        terminal_score = compute_score(_S.conn, _S.task_id)

        # False submission penalty - if the DB still has unresolved violations,
        # the agent submitted too early.  Apply -0.5 regardless of grader score.
        current_violations = compute_violations(_S.conn, _S.task_id)
        if current_violations > 0:
            final_reward = terminal_score - 0.5
        else:
            final_reward = terminal_score

        obs = self._build_observation(
            last_action_type="META",
            last_query_result=[
                {
                    "grader_score": terminal_score,
                    "message": (
                        f"Episode complete. Grader score: {terminal_score:.4f}. "
                        f"Notes: {action.notes or '(none)'}"
                    ),
                }
            ],
            error_logs="",
            reward=final_reward,
            done=True,
        )
        _S.last_obs = obs
        return obs

    #  Observation builder 

    def _build_observation(
        self,
        last_action_type: str,
        last_query_result,
        error_logs: str,
        reward: Optional[float],
        done: bool,
    ) -> DBERObservation:
        db_summary = _get_db_summary(_S.conn) if _S.conn else {}
        return DBERObservation(
            incident_ticket=TASK_TICKETS[_S.task_id],
            task_id=_S.task_id,
            last_action_type=last_action_type,
            last_query_result=last_query_result,
            error_logs=error_logs,
            db_summary=db_summary,
            violation_count=_S.violation_count,
            budget_remaining=_S.budget,
            step_count=_S.step_count,
            done=done,
            reward=reward,
        )


#  Utilities 

def _normalise_query(query: str) -> str:
    """Normalise a SQL query for spam detection (case-fold + collapse whitespace)."""
    q = query.upper()
    q = re.sub(r"\s+", " ", q).strip()
    return q


def _get_db_summary(conn: Optional[sqlite3.Connection]) -> Dict[str, Any]:
    """Return {"tables": [...], "row_counts": {...}} from the live connection."""
    if conn is None:
        return {"tables": [], "row_counts": {}}
    try:
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        ]
        row_counts = {}
        for table in tables:
            row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            row_counts[table] = row[0]
        return {"tables": tables, "row_counts": row_counts}
    except Exception:
        return {"tables": [], "row_counts": {}}
