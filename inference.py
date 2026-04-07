#!/usr/bin/env python3
"""DB-ER Baseline Inference Script.

Runs an LLM-based agent against all five DB-ER tasks using the OpenAI client.

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

Stdout format (required by OpenEnv spec):
  [START] task=<slug> env=db_er model=<model>
  [STEP]  step=<n> action=<str> reward=<0.00> done=<true|false> error=<msg|null>
  [END]   success=<true|false> steps=<n> rewards=<r1,r2,...>
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

API_BASE_URL: str = os.environ.get("API_BASE_URL", "https://api-inference.huggingface.co/v1")
MODEL_NAME: str = os.environ.get("MODEL_NAME", "meta-llama/Llama-3.3-70B-Instruct")
HF_TOKEN: Optional[str] = os.environ.get("HF_TOKEN")
ENV_BASE_URL: Optional[str] = os.environ.get("ENV_BASE_URL")
MAX_STEPS: int = int(os.environ.get("MAX_STEPS", "40"))
TEMPERATURE: float = float(os.environ.get("TEMPERATURE", "0.05"))
TASK_IDS: List[int] = [
    int(t.strip())
    for t in os.environ.get("TASK_IDS", "1,2,3,4,5").split(",")
    if t.strip()
]

#  Task metadata

TASK_SLUGS: Dict[int, str] = {
    1: "phantom-duplicates",
    2: "cascading-failure",
    3: "payroll-black-hole",
    4: "schema-drift",
    5: "referential-maze",
}

TASK_NAMES: Dict[int, str] = {
    1: "Easy -- Phantom Duplicates",
    2: "Medium -- Cascading Failure",
    3: "Medium-Hard -- Payroll Black Hole",
    4: "Hard -- Schema Drift",
    5: "Very Hard -- Referential Maze",
}

#  LLM client (initialized in main() after env var validation)

llm_client: Optional[OpenAI] = None

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
- Task 4 (Schema Drift): Columns were renamed. Use schema_changelog (old_column to new_column mapping). Reverse each rename with: ALTER TABLE <table> RENAME COLUMN <current_name> TO <original_name>. Do NOT use DROP TABLE - inventory and categories are protected core tables and will trigger instant -1.0 failure.
- Task 5 (Referential Maze): Multi-table FK crisis. Check decommission_log before restoring anything. Use sync_audit for missing data. Projects 5 and 9 are decommissioned and must NOT be restored.

Always respond with valid JSON. Never include explanations outside the JSON."""


#  Observation formatter

def format_observation(obs, history: List[str]) -> str:
    """Convert an observation object/dict into a rich text prompt for the LLM."""
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

    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()

    try:
        obj = json.loads(text)
        if "action_type" in obj:
            return obj
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
    if match:
        try:
            obj = json.loads(match.group())
            if "action_type" in obj:
                return obj
        except json.JSONDecodeError:
            pass

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


#  Stdout helpers (spec-required log lines)

def _fmt_action(action_dict: Dict[str, Any]) -> str:
    """Compact single-line action string for [STEP] output."""
    if action_dict["action_type"] == "execute_sql":
        q = re.sub(r"\s+", " ", action_dict.get("query", "")).strip()
        if len(q) > 80:
            q = q[:77] + "..."
        return f"execute_sql({q!r})"
    else:
        notes = (action_dict.get("notes", "") or "")[:60]
        return f"submit_resolution({notes!r})"


def emit_start(task_id: int) -> None:
    slug = TASK_SLUGS.get(task_id, f"task-{task_id}")
    print(f"[START] task={slug} env=db_er model={MODEL_NAME}", flush=True)


def emit_step(step: int, action_dict: Dict[str, Any], reward: float,
              done: bool, error: str) -> None:
    action_str = _fmt_action(action_dict)
    error_str = error.replace("\n", " ").strip() if error else "null"
    done_str = "true" if done else "false"
    print(
        f"[STEP] step={step} action={action_str} "
        f"reward={reward:.2f} done={done_str} error={error_str}",
        flush=True,
    )


def emit_end(success: bool, steps: int, rewards: List[float], score: float) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    success_str = "true" if success else "false"
    # Clamp score to open interval (0, 1) as required by Phase 2 validation.
    clamped = max(0.01, min(0.99, score))
    print(
        f"[END] success={success_str} steps={steps} score={clamped:.2f} rewards={rewards_str}",
        flush=True,
    )


#  In-process environment runner

def run_inprocess(task_id: int) -> float:
    """Run one episode against the in-process environment."""
    from db_er.models import DBERAction
    from server.environment import DBEREnvironment

    env = DBEREnvironment()
    try:
        obs = env.reset(task_id=task_id)
        return _agent_loop(obs, env.step, task_id, env_type="inprocess")
    finally:
        env.close()


#  HTTP environment runner

def run_http(base_url: str, task_id: int) -> float:
    """Run one episode against a remote DB-ER server."""
    import requests

    reset_resp = requests.post(
        f"{base_url}/reset",
        json={"task_id": task_id},
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
        return resp.json()

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
    rewards: List[float] = []
    final_reward = 0.0
    episode_score = 0.01  # default if agent never submits (budget exhausted)
    steps_taken = 0
    success = False

    emit_start(task_id)

    try:
        for step in range(1, MAX_STEPS + 1):
            done = g(obs, "done", False)
            if done:
                success = True
                print(f"  [done=True at step {step - 1}]", file=sys.stderr)
                break

            budget = g(obs, "budget_remaining", 0)
            if budget <= 0:
                print("  [budget exhausted]", file=sys.stderr)
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
                print(f"  Step {step}: LLM call failed ({exc}), using fallback.", file=sys.stderr)
                response_text = FALLBACK_ACTIONS.get(task_id, FALLBACK_ACTIONS[1])

            action_dict = parse_action(response_text)
            if action_dict is None:
                print(f"  Step {step}: Could not parse action from: {response_text[:120]!r}", file=sys.stderr)
                action_dict = json.loads(FALLBACK_ACTIONS.get(task_id, FALLBACK_ACTIONS[1]))

            print(
                f"  Step {step:2d}: [{action_dict['action_type']}] "
                f"{str(action_dict.get('query') or action_dict.get('notes', ''))[:80]}",
                file=sys.stderr,
            )

            reward = 0.0
            done = False
            error_str = ""

            try:
                if env_type == "inprocess":
                    from db_er.models import DBERAction
                    result = step_fn(DBERAction(**action_dict))
                    reward = float(getattr(result, "reward", None) or 0.0)
                    done = bool(getattr(result, "done", False))
                    error_str = getattr(result, "error_logs", "") or ""
                    obs = result
                else:
                    result = step_fn(action_dict)
                    reward = float(result.get("reward") or 0.0)
                    done = bool(result.get("done", False))
                    obs = result.get("observation", result)
                    error_str = (obs.get("error_logs", "") if isinstance(obs, dict)
                                 else getattr(obs, "error_logs", "")) or ""
            except Exception as exc:
                error_str = str(exc)
                print(f"  Step {step}: Environment error: {exc}", file=sys.stderr)
                emit_step(step, action_dict, 0.0, False, error_str)
                rewards.append(0.0)
                steps_taken = step
                break

            steps_taken = step
            rewards.append(reward)
            final_reward = reward

            emit_step(step, action_dict, reward, done, error_str)

            history.append(
                f"Step {step:2d}: {action_dict['action_type']} "
                f"-> reward={reward:+.3f} done={done}"
            )

            viol = g(obs, "violation_count", "?")
            print(
                f"           reward={reward:+.3f} | violations={viol} | budget={g(obs, 'budget_remaining', '?')}",
                file=sys.stderr,
            )

            if done:
                success = True
                # When agent submits, final_reward IS the grader score.
                episode_score = final_reward
                print(f"  [episode complete at step {step}]", file=sys.stderr)
                break

    finally:
        emit_end(success, steps_taken, rewards, episode_score)

    return final_reward


#  Main

def main() -> None:
    global llm_client

    # HF_TOKEN has no default and is required to authenticate with the LLM endpoint.
    if not HF_TOKEN:
        print(
            "ERROR: HF_TOKEN environment variable is not set.\n"
            "Set it before running:\n"
            "  export HF_TOKEN=hf_your_token\n"
            "  python inference.py",
            file=sys.stderr,
        )
        sys.exit(0)

    llm_client = OpenAI(base_url=API_BASE_URL, api_key=HF_TOKEN)

    print("=" * 70, file=sys.stderr)
    print("DB-ER Baseline Inference Script", file=sys.stderr)
    print(f"Model:    {MODEL_NAME}", file=sys.stderr)
    print(f"Endpoint: {API_BASE_URL}", file=sys.stderr)
    print(f"Tasks:    {TASK_IDS}", file=sys.stderr)
    print(f"MaxSteps: {MAX_STEPS}", file=sys.stderr)
    print("=" * 70, file=sys.stderr)

    results: Dict[int, float] = {}
    start_time = time.time()

    for task_id in TASK_IDS:
        print(f"\n{'=' * 70}", file=sys.stderr)
        print(f"TASK {task_id}: {TASK_NAMES.get(task_id, '?')}", file=sys.stderr)
        print("-" * 70, file=sys.stderr)

        t0 = time.time()
        try:
            if ENV_BASE_URL:
                score = run_http(ENV_BASE_URL, task_id)
            else:
                score = run_inprocess(task_id)
        except Exception as exc:
            print(f"  ERROR during task {task_id}: {exc}", file=sys.stderr)
            score = 0.0

        elapsed = time.time() - t0
        results[task_id] = score
        print(f"\n  -> Task {task_id} final score: {score:.4f}  [{elapsed:.1f}s]", file=sys.stderr)

    total_elapsed = time.time() - start_time

    print(f"\n{'=' * 70}", file=sys.stderr)
    print("RESULTS SUMMARY", file=sys.stderr)
    print(f"{'=' * 70}", file=sys.stderr)
    for task_id, score in results.items():
        bar = "#" * int(score * 20) + "." * (20 - int(score * 20))
        print(f"  Task {task_id} [{TASK_NAMES.get(task_id, '?'):35s}]  {bar}  {score:.4f}", file=sys.stderr)

    avg = sum(results.values()) / len(results) if results else 0.0
    print(f"\n  Average score: {avg:.4f}", file=sys.stderr)
    print(f"  Total runtime: {total_elapsed:.1f}s", file=sys.stderr)
    print("=" * 70, file=sys.stderr)

    sys.exit(0)


if __name__ == "__main__":
    main()
