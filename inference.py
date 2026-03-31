#!/usr/bin/env python3
"""DB-ER Baseline Inference Script.

Runs an LLM-based agent against all three DB-ER tasks using the OpenAI client.

Environment variables (required):
  API_BASE_URL   : OpenAI-compatible LLM endpoint
  MODEL_NAME     : model identifier (e.g. meta-llama/Llama-3.3-70B-Instruct)
  HF_TOKEN       : API key / Hugging Face token

Optional:
  ENV_BASE_URL   : DB-ER server URL (default: run environment in-process)
  TASK_IDS       : comma-separated task IDs to run, e.g. "1,2,3" (default: all)
  MAX_STEPS      : max steps per episode (default: 40)
  TEMPERATURE    : LLM temperature (default: 0.05)

Usage:
  python inference.py
  TASK_IDS=2 python inference.py
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional

from openai import OpenAI

#  Configuration 

API_BASE_URL: str = os.environ["API_BASE_URL"]
MODEL_NAME: str = os.environ["MODEL_NAME"]
HF_TOKEN: str = os.environ["HF_TOKEN"]
ENV_BASE_URL: Optional[str] = os.environ.get("ENV_BASE_URL")
MAX_STEPS: int = int(os.environ.get("MAX_STEPS", "40"))
TEMPERATURE: float = float(os.environ.get("TEMPERATURE", "0.05"))
TASK_IDS: List[int] = [
    int(t.strip())
    for t in os.environ.get("TASK_IDS", "1,2,3,4,5").split(",")
    if t.strip()
]

#  LLM client 

llm_client = OpenAI(base_url=API_BASE_URL, api_key=HF_TOKEN)

#  System prompt 

SYSTEM_PROMPT = """You are an expert Senior Site Reliability Engineer (SRE) and Database Administrator (DBA).
You have been paged to diagnose and repair a corrupted production SQLite database.

## AVAILABLE ACTIONS

You must respond with a SINGLE JSON object -- no other text, no markdown fences.

**Option A -- execute SQL:**
{"action_type": "execute_sql", "query": "YOUR SQL HERE"}

**Option B -- submit resolution (only when fully fixed):**
{"action_type": "submit_resolution", "notes": "What you diagnosed and fixed."}

## BUDGET RULES
- SELECT / PRAGMA: costs 1 budget point
- INSERT / UPDATE / DELETE / ALTER / CREATE: costs 5 budget points
- Repeating the EXACT same SELECT 3+ times: -0.05 penalty each time
- Budget = 0 -> episode ends with no score

## ABSOLUTE SAFETY RULES (violating = instant episode failure, -1.0 reward)
1. NEVER execute DROP TABLE on users, vendors, products, purchases, employees_old, employees_new, compensation
2. NEVER execute DELETE without a WHERE clause (no wholesale truncation)
3. NEVER cause a core table to lose >10% of its initial row count

## RECOMMENDED STRATEGY
1. Start by reading the incident ticket and db_summary (already provided)
2. Run PRAGMA table_info(<table>) to understand schemas
3. Query the data to confirm the exact corruption
4. Formulate a precise fix plan
5. Execute targeted SQL changes
6. Verify corrections (check violation_count -> 0)
7. Submit only when confident the DB is fully repaired

## IMPORTANT HINTS
- Task 1 (Phantom Duplicates): Look for duplicate emails in users. Keep the OLDEST record per email (lowest id).
- Task 2 (Cascading Failure): admin_audit_logs contains record_json with deleted product data. Parse it and re-INSERT.
- Task 3 (Payroll Black Hole): The "timeout" is MISLEADING. Check employees_old for duplicate IDs (no PRIMARY KEY). Delete corrupted duplicates (salary=0, active=0), then migrate remaining employees.
- Task 4 (Schema Drift): Columns were renamed. Use schema_changelog to find original names. Recreate tables with original column names using CREATE-INSERT-DROP-RENAME pattern.
- Task 5 (Referential Maze): Multi-table FK crisis. Check decommission_log before restoring anything. Use sync_audit for missing data. Projects 5 and 9 are decommissioned and must NOT be restored.

Always respond with valid JSON. Never include explanations outside the JSON."""


#  Observation formatter 

def format_observation(obs, history: List[str]) -> str:
    """Convert an observation object/dict into a rich text prompt for the LLM."""
    # Support both object (in-process) and dict (HTTP) observations
    def g(key: str, default=None):
        if isinstance(obs, dict):
            return obs.get(key, default)
        return getattr(obs, key, default)

    lines = [
        "## CURRENT STATE",
        f"Task ID: {g('task_id', '?')} | Step: {g('step_count', '?')} | "
        f"Budget Remaining: {g('budget_remaining', '?')} | "
        f"Violations: {g('violation_count', '?')}",
        "",
        "## INCIDENT TICKET",
        str(g("incident_ticket", "(none)")),
        "",
        "## LAST ACTION",
        f"Type: {g('last_action_type', 'NONE')}",
    ]

    result = g("last_query_result", [])
    if result:
        result_str = json.dumps(result, indent=2) if not isinstance(result, str) else result
        # Truncate very long results to avoid token overflow
        if len(result_str) > 3000:
            result_str = result_str[:3000] + "\n... [truncated]"
        lines += ["Result:", result_str]

    err = g("error_logs", "")
    if err:
        lines += ["Error:", err]

    db_summary = g("db_summary", {})
    if db_summary:
        lines += ["", "## DATABASE SUMMARY", json.dumps(db_summary, indent=2)]

    if history:
        lines += ["", "## STEP HISTORY (last 8)"]
        lines += history[-8:]

    lines += [
        "",
        "## YOUR TURN",
        "Respond with a single JSON action object.",
    ]
    return "\n".join(lines)


#  JSON action parser 

def parse_action(response_text: str) -> Optional[Dict[str, Any]]:
    """Extract the first valid JSON object from the model's response."""
    text = response_text.strip()

    # Strip markdown code fences if present
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()

    # Try direct parse
    try:
        obj = json.loads(text)
        if "action_type" in obj:
            return obj
    except json.JSONDecodeError:
        pass

    # Extract first JSON object via regex
    match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
    if match:
        try:
            obj = json.loads(match.group())
            if "action_type" in obj:
                return obj
        except json.JSONDecodeError:
            pass

    # Last resort: try to extract just the action_type and key fields
    action_type_match = re.search(r'"action_type"\s*:\s*"([^"]+)"', text)
    if action_type_match:
        action_type = action_type_match.group(1)
        if action_type == "execute_sql":
            query_match = re.search(r'"query"\s*:\s*"((?:[^"\\]|\\.)*)"', text)
            if query_match:
                return {"action_type": "execute_sql", "query": query_match.group(1)}
        elif action_type == "submit_resolution":
            notes_match = re.search(r'"notes"\s*:\s*"((?:[^"\\]|\\.)*)"', text)
            notes = notes_match.group(1) if notes_match else "Resolution submitted."
            return {"action_type": "submit_resolution", "notes": notes}

    return None


#  Fallback actions per task 

FALLBACK_ACTIONS: Dict[int, str] = {
    1: '{"action_type": "execute_sql", "query": "SELECT id, email, username FROM users ORDER BY email, id"}',
    2: '{"action_type": "execute_sql", "query": "SELECT * FROM admin_audit_logs ORDER BY id"}',
    3: '{"action_type": "execute_sql", "query": "SELECT id, name, base_salary, hire_date, active FROM employees_old ORDER BY id, hire_date"}',
    4: '{"action_type": "execute_sql", "query": "SELECT * FROM schema_changelog ORDER BY id"}',
    5: '{"action_type": "execute_sql", "query": "SELECT * FROM sync_audit ORDER BY id"}',
}


#  In-process environment runner 

def run_inprocess(task_id: int) -> float:
    """Run one episode against the in-process environment."""
    # Import here to avoid top-level dependency if ENV_BASE_URL is set
    from db_er.models import DBERAction
    from server.environment import DBEREnvironment

    env = DBEREnvironment()
    try:
        obs = env.reset(task_id=task_id)
        return _agent_loop(obs, env.step, task_id, env_type="inprocess")
    finally:
        env.close()


def _step_inprocess(env_step_fn, action_dict: Dict[str, Any]):
    from db_er.models import DBERAction
    action = DBERAction(**action_dict)
    return env_step_fn(action)


#  HTTP environment runner 

def run_http(base_url: str, task_id: int) -> float:
    """Run one episode against a remote DB-ER server."""
    import requests

    reset_resp = requests.post(
        f"{base_url}/reset",
        json={"seed": (task_id - 1)},  # seed->task mapping: 0->1, 1->2, 2->3
        timeout=30,
    )
    reset_resp.raise_for_status()
    obs = reset_resp.json()["observation"]

    def step_http(action_dict: Dict[str, Any]):
        resp = requests.post(
            f"{base_url}/step",
            json={"action": action_dict},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data  # return raw dict

    return _agent_loop(obs, step_http, task_id, env_type="http")


#  Core agent loop 

def _agent_loop(initial_obs, step_fn, task_id: int, env_type: str) -> float:
    """Run the LLM agent for up to MAX_STEPS steps. Returns final reward."""

    def g(obs, key, default=None):
        if isinstance(obs, dict):
            return obs.get(key, default)
        return getattr(obs, key, default)

    obs = initial_obs
    history: List[str] = []
    final_reward = 0.0

    for step in range(1, MAX_STEPS + 1):
        done = g(obs, "done", False)
        if done:
            print(f"  [done=True at step {step - 1}]")
            break

        budget = g(obs, "budget_remaining", 0)
        if budget <= 0:
            print("  [budget exhausted]")
            break

        user_prompt = format_observation(obs, history)

        try:
            completion = llm_client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=TEMPERATURE,
                max_tokens=512,
            )
            response_text = completion.choices[0].message.content or ""
        except Exception as exc:
            print(f"  Step {step}: LLM call failed ({exc}), using fallback.")
            response_text = FALLBACK_ACTIONS.get(task_id, FALLBACK_ACTIONS[1])

        action_dict = parse_action(response_text)
        if action_dict is None:
            print(f"  Step {step}: Could not parse action from: {response_text[:120]!r}")
            action_dict = json.loads(FALLBACK_ACTIONS.get(task_id, FALLBACK_ACTIONS[1]))

        print(f"  Step {step:2d}: [{action_dict['action_type']}] "
              f"{str(action_dict.get('query') or action_dict.get('notes', ''))[:80]}")

        try:
            if env_type == "inprocess":
                from db_er.models import DBERAction
                result = step_fn(DBERAction(**action_dict))
                reward = getattr(result, "reward", None) or 0.0
                done = getattr(result, "done", False)
                obs = result  # observation is the result itself for in-process
            else:
                result = step_fn(action_dict)
                reward = result.get("reward") or 0.0
                done = result.get("done", False)
                obs = result.get("observation", result)
        except Exception as exc:
            print(f"  Step {step}: Environment error: {exc}")
            break

        history.append(
            f"Step {step:2d}: {action_dict['action_type']} "
            f"-> reward={reward:+.3f} done={done}"
        )
        final_reward = reward

        viol = g(obs, "violation_count", "?")
        print(f"           reward={reward:+.3f} | violations={viol} | budget={g(obs, 'budget_remaining', '?')}")

        if done:
            print(f"  [episode complete at step {step}]")
            break

    return final_reward


#  Main 

def main() -> None:
    print("=" * 70)
    print("DB-ER Baseline Inference Script")
    print(f"Model:    {MODEL_NAME}")
    print(f"Endpoint: {API_BASE_URL}")
    print(f"Tasks:    {TASK_IDS}")
    print(f"MaxSteps: {MAX_STEPS}")
    print("=" * 70)

    task_names = {
        1: "Easy -- Phantom Duplicates",
        2: "Medium -- Cascading Failure",
        3: "Medium-Hard -- Payroll Black Hole",
        4: "Hard -- Schema Drift",
        5: "Very Hard -- Referential Maze",
    }

    results: Dict[int, float] = {}
    start_time = time.time()

    for task_id in TASK_IDS:
        print(f"\n{'' * 70}")
        print(f"TASK {task_id}: {task_names.get(task_id, '?')}")
        print("-" * 70)

        t0 = time.time()
        try:
            if ENV_BASE_URL:
                score = run_http(ENV_BASE_URL, task_id)
            else:
                score = run_inprocess(task_id)
        except Exception as exc:
            print(f"  ERROR during task {task_id}: {exc}")
            score = 0.0

        elapsed = time.time() - t0
        results[task_id] = score
        print(f"\n  -> Task {task_id} final score: {score:.4f}  [{elapsed:.1f}s]")

    total_elapsed = time.time() - start_time

    print(f"\n{'=' * 70}")
    print("RESULTS SUMMARY")
    print(f"{'=' * 70}")
    for task_id, score in results.items():
        bar = "#" * int(score * 20) + "." * (20 - int(score * 20))
        print(f"  Task {task_id} [{task_names.get(task_id, '?'):35s}]  {bar}  {score:.4f}")

    avg = sum(results.values()) / len(results) if results else 0.0
    print(f"\n  Average score: {avg:.4f}")
    print(f"  Total runtime: {total_elapsed:.1f}s")
    print("=" * 70)

    # Exit with non-zero if average is very low (useful for CI)
    if avg < 0.1:
        sys.exit(1)


if __name__ == "__main__":
    main()
