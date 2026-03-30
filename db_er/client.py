"""Typed HTTP client for the DB-ER environment."""

from __future__ import annotations

from typing import Any, Dict, Optional

from openenv.core import EnvClient
from openenv.core.client_types import StepResult

from db_er.models import DBERAction, DBERObservation, DBERState


class DBERClient(EnvClient[DBERAction, DBERObservation, DBERState]):
    """Async client that connects to a running DB-ER server.

    Usage (async)::

        async with DBERClient(base_url="http://localhost:8000") as client:
            result = await client.reset()
            print(result.observation.incident_ticket)

    Usage (sync)::

        with DBERClient(base_url="http://localhost:8000").sync() as client:
            result = client.reset()
            obs = result.observation

    The ``reset()`` method accepts an optional ``task_id`` keyword argument
    (1=Easy, 2=Medium, 3=Hard).  If omitted the server cycles tasks based on
    the provided ``seed``.
    """

    def _step_payload(self, action: DBERAction) -> Dict[str, Any]:
        return action.model_dump(exclude_none=True)

    def _parse_result(self, payload: Dict[str, Any]) -> StepResult[DBERObservation]:
        obs_data = payload.get("observation", {})
        reward = payload.get("reward")
        done = payload.get("done", False)
        obs = DBERObservation(**obs_data)
        return StepResult(observation=obs, reward=reward, done=done)

    def _parse_state(self, payload: Dict[str, Any]) -> DBERState:
        return DBERState(**payload)
