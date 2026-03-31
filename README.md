---
title: DB-ER Database Emergency Response
emoji: "\U0001F6A8"
colorFrom: red
colorTo: indigo
sdk: docker
app_port: 7860
short_description: OpenEnv benchmark for AI database repair agents
tags:
  - openenv
  - reinforcement-learning
  - sql
  - agent-benchmark
  - sre
---

# DB-ER: Database Emergency Response

An OpenEnv environment where an AI agent gets paged into a production database incident and has to diagnose and fix the corruption before things get worse.

[![HF Space](https://img.shields.io/badge/HF%20Space-Live-orange)](https://huggingface.co/spaces/aadi-joshi/openenvhack)
[![Python](https://img.shields.io/badge/python-3.11+-blue)](https://python.org)
[![OpenEnv](https://img.shields.io/badge/OpenEnv-v0.2-green)](https://github.com/meta-pytorch/OpenEnv)

---

## What is DB-ER?

DB-ER puts an agent in the role of an on-call database engineer responding to a production incident. The agent receives an incident ticket, has access to a live SQLite database, and must figure out what went wrong and fix it using raw SQL queries, all within a limited action budget.

The catch: the incident logs are sometimes misleading, the database schema has no documentation, and every destructive action is permanent within the episode.

## The Five Tasks

**Task 1 - Phantom Duplicates (Easy)**
A bulk import script ran twice. The `users` table has duplicate email rows that are causing constraint failures across the app. Find them and remove the duplicates while keeping the original records.

**Task 2 - Cascading Failure (Medium)**
Someone ran a cleanup script that deleted three products still referenced by active purchases. Every checkout is now broken. The `admin_audit_logs` table has the deleted records stored as JSON. Restore them.

**Task 3 - Payroll Black Hole (Medium-Hard)**
A migration script died partway through. The incident log blames a timeout, but that is a red herring. The real problem is corrupted duplicate employee IDs in the source table that caused the crash. Clean the corruption, then finish the migration and preserve every salary mapping correctly.

**Task 4 - Schema Drift (Hard)**
A junior DBA ran a schema standardization script that renamed columns in the `inventory` and `categories` tables. The application code expects the original column names and is now broken. The `schema_changelog` table has a record of every rename. Reverse the column renames while preserving all data. Requires the SQLite CREATE-INSERT-DROP-RENAME pattern since ALTER TABLE column rename support is limited.

**Task 5 - Referential Maze (Very Hard)**
A cross-database sync job crashed partway through, leaving orphaned FK references and missing records across four interrelated tables (`departments`, `projects`, `assignments`, `budgets`). The incident ticket blames a network timeout, but the real problem is scattered across all four tables. Not all missing records should be restored: some projects were deliberately decommissioned last quarter. The agent must check `decommission_log` before re-inserting anything and use `sync_audit` to find the data for legitimately missing records. Restoring a decommissioned project is penalized.

---

## How Scoring Works

Each episode ends when the agent calls `submit_resolution` or runs out of budget. The score compares the agent's final database against a hidden golden database using F1 score on exact row sets. A perfect repair gives 1.0.

**Intermediate rewards give signal throughout the episode:**
- +0.2 each time `violation_count` drops
- -0.1 for mutations that affect zero rows
- -0.05 for repeating the same SELECT three or more times
- -0.5 penalty on submission if violations are still present

**Instant episode fail (reward -1.0):**
- DROP TABLE on any core table
- DELETE without a WHERE clause
- Removing more than 10% of rows from a core table in a single mutation

---

## Running Locally

```bash
pip install -r requirements.txt
uvicorn server.app:app --host 0.0.0.0 --port 7860
```

Run the tests:

```bash
pytest tests/ -v
```

Run the baseline agent (requires API credentials):

```bash
export API_BASE_URL=https://api-inference.huggingface.co/v1
export MODEL_NAME=meta-llama/Llama-3.3-70B-Instruct
export HF_TOKEN=your_token_here
python inference.py
```

---

## HTTP API

Follows the standard OpenEnv spec:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Liveness check |
| `/reset` | POST | Start a new episode. Pass `task_id` (1-5) or `seed` |
| `/step` | POST | Execute one action |
| `/state` | GET | Current episode state |
| `/schema` | GET | Action and observation schemas |
| `/metadata` | GET | Environment metadata |
| `/docs` | GET | Swagger UI |

Start an episode:

```bash
curl -X POST https://aadi-joshi-openenvhack.hf.space/reset \
  -H "Content-Type: application/json" \
  -d '{"task_id": 1}'
```

Run a query:

```bash
curl -X POST https://aadi-joshi-openenvhack.hf.space/step \
  -H "Content-Type: application/json" \
  -d '{"action": {"action_type": "execute_sql", "query": "SELECT * FROM users"}}'
```

---

## Project Layout

```
db_er/          Pydantic models (action, observation, state) and HTTP client
server/         Environment logic, fixtures, grader, and safety guards
tests/          Unit and integration tests
inference.py    Baseline agent using the OpenAI client
openenv.yaml    OpenEnv manifest
Dockerfile      Container config for HF Spaces (port 7860)
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `API_BASE_URL` | OpenAI-compatible endpoint (required for inference) |
| `MODEL_NAME` | Model identifier (required for inference) |
| `HF_TOKEN` | API key (required for inference) |
| `ENV_BASE_URL` | Point at a remote server instead of running in-process |
| `TASK_IDS` | Which tasks to run, e.g. `1,2,3,4,5` |

---

Built for the Meta x Hugging Face OpenEnv Hackathon 2026 by team maxout (Aadi Joshi, Kavya Bhand).
