"""FastAPI application entrypoint for DB-ER.

Wires together:
  openenv.core.env_server.http_server.create_app(...)

Exposes the standard OpenEnv HTTP API:
  POST /reset  POST /step  GET /state  GET /schema  GET /metadata  GET /health
  POST /mcp    WS   /ws    GET /openapi.json  GET /docs

Run locally:
  uvicorn server.app:app --host 0.0.0.0 --port 8000 --reload
  # or via pyproject.toml script:
  uv run server
"""

from __future__ import annotations

import os

from openenv.core.env_server.http_server import create_app

from db_er.models import DBERAction, DBERObservation
from server.environment import DBEREnvironment

#  FastAPI app (used by openenv.yaml app = server.app:app) 

app = create_app(
    env=DBEREnvironment,
    action_cls=DBERAction,
    observation_cls=DBERObservation,
    env_name="db_er",
    max_concurrent_envs=1,
)


#  Entrypoint (used by pyproject.toml [project.scripts] server) 

def main(host: str = "0.0.0.0", port: int = 8000) -> None:
    import uvicorn

    port = int(os.environ.get("PORT", port))
    uvicorn.run(
        "server.app:app",
        host=host,
        port=port,
        log_level="info",
        reload=False,
    )


if __name__ == "__main__":
    main()
