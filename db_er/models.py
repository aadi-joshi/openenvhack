"""Pydantic models for DB-ER actions, observations, and state."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import Field

from openenv.core.env_server.types import Action, Observation, State


class DBERAction(Action):
    """Actions the agent can take inside the DB-ER environment.

    Two action types:
    - ``execute_sql``      - run a raw SQL query against the live database.
    - ``submit_resolution``- declare the episode finished; triggers the grader.
    """

    action_type: Literal["execute_sql", "submit_resolution"] = Field(
        ..., description="Type of action to perform"
    )
    query: Optional[str] = Field(
        None,
        description=(
            "SQL query to execute. Required when action_type='execute_sql'. "
            "Budget cost: -1 for SELECT/PRAGMA, -5 for INSERT/UPDATE/DELETE/ALTER."
        ),
    )
    notes: Optional[str] = Field(
        None,
        description=(
            "Human-readable resolution notes. Required when action_type='submit_resolution'. "
            "Should describe what was diagnosed and fixed."
        ),
    )

    model_config = {"extra": "forbid"}


class DBERObservation(Observation):
    """Full observation returned after every reset() and step().

    All fields are JSON-serialisable so the agent can reason over them directly.
    """

    #  Incident context 
    incident_ticket: str = Field(
        ..., description="The initial incident alert / pager message"
    )
    task_id: int = Field(
        1, description="Active task ID: 1=Easy, 2=Medium, 3=Medium-Hard, 4=Hard, 5=Very Hard"
    )

    #  Last action result 
    last_action_type: str = Field(
        "NONE",
        description="Category of the last executed action: SELECT | MUTATION | META | ERROR | NONE",
    )
    last_query_result: Union[List[Dict[str, Any]], str] = Field(
        default_factory=list,
        description="Rows returned by the last SELECT, or a status string for mutations.",
    )
    error_logs: str = Field(
        "", description="stderr / exception message from the last action (empty if no error)"
    )

    #  Environment state 
    db_summary: Dict[str, Any] = Field(
        default_factory=dict,
        description='Live DB snapshot: {"tables": [...], "row_counts": {...}}',
    )
    violation_count: int = Field(
        0,
        description=(
            "Current count of broken constraints (FK violations + data integrity issues). "
            "Decreasing this gives +0.2 intermediate reward."
        ),
    )
    budget_remaining: int = Field(
        100,
        description="Remaining action budget. SELECT/PRAGMA costs 1; mutations cost 5. Hitting 0 ends the episode.",
    )
    step_count: int = Field(0, description="Number of steps taken in the current episode")

    model_config = {"extra": "forbid"}


class DBERState(State):
    """Internal server-side state (returned by GET /state)."""

    task_id: int = Field(1)
    budget_remaining: int = Field(100)
    violation_count: int = Field(0)
    initial_row_counts: Dict[str, int] = Field(default_factory=dict)
    query_history: Dict[str, int] = Field(
        default_factory=dict,
        description="Normalised query -> execution count (for spam detection)",
    )
