"""Wave-1 source agent (plan.md §7.2).

One agent, one database, one slice of the incident. It sees only its own db_id
and symptoms[] — never truth.json, never another agent's db_id or output.

The loop is written by hand rather than handed to the SDK tool runner because
every query has to be counted against a budget and timed into telemetry; the
budget is the experiment's control (§10), so it cannot be approximate.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any

from agents.hotdata_scope import SLICES, BudgetExceeded, ScopedDB, query
from agents.llm import (
    FAST_MODEL,
    Usage,
    append_assistant,
    append_tool_results,
    call,
    load_prompt,
    prompt_hash,
    validated_call,
)
from agents.schemas import AgentName, HypothesisSet
from telemetry.emit import Emitter

MAX_TURNS = 24
ROWS_PREVIEW = 40  # rows returned to the model per query; it must aggregate in SQL

# Full-text and vector search syntax is not documented in the SDK (VERIFY item 6);
# these templates are the current best guess and are overridable from the
# environment so they can be corrected at the workshop without a code change.
FTS_SQL = os.getenv(
    "HOTDATA_FTS_SQL",
    "SELECT * FROM {table} WHERE search({column}, '{q}') LIMIT {limit}",
)
VECTOR_SQL = os.getenv(
    "HOTDATA_VECTOR_SQL",
    "SELECT * FROM {table} ORDER BY distance({column}, '{q}') LIMIT {limit}",
)

TOOL_SQL = {
    "name": "run_sql",
    "description": (
        "Run one read-only SQL query against your database. Aggregate in SQL — "
        "never select raw rows in bulk. Returns at most "
        f"{ROWS_PREVIEW} rows."
    ),
    "input_schema": {
        "type": "object",
        "properties": {"sql": {"type": "string", "description": "A single SELECT statement."}},
        "required": ["sql"],
        "additionalProperties": False,
    },
}
TOOL_FTS = {
    "name": "search_text",
    "description": ("Full-text search the indexed text column of your table. Use this to "
                    "find error signatures and distinctive phrases."),
    "input_schema": {
        "type": "object",
        "properties": {"q": {"type": "string", "description": "Search phrase."},
                       "limit": {"type": "integer", "description": "Max rows (default 20)."}},
        "required": ["q"],
        "additionalProperties": False,
    },
}
TOOL_VECTOR = {
    "name": "search_similar",
    "description": ("Semantic search over past postmortems. Search the SYMPTOM pattern, "
                    "not a guessed cause."),
    "input_schema": {
        "type": "object",
        "properties": {"q": {"type": "string", "description": "Symptom description."},
                       "limit": {"type": "integer", "description": "Max rows (default 10)."}},
        "required": ["q"],
        "additionalProperties": False,
    },
}

INDEXED_COLUMN = {"logs": "msg", "k8s_events": "message", "postmortems": "body"}

SCHEMA_HINT = {
    "logs": "logs(ts TIMESTAMP, service, level, trace_id, msg, attrs JSON-string)",
    "metrics": ("metrics(ts TIMESTAMP, service, rps, p99_ms, err_rate, cpu_pct, mem_mb, "
                "db_conns) — one row per service per minute, 120 minutes"),
    "changes": "changes(event_id, ts TIMESTAMP, kind, service, detail, author)",
    "k8s_events": "k8s_events(event_id, ts TIMESTAMP, service, pod, reason, message)",
    "postmortems": "postmortems(pm_id, title, service, fault_type, body)",
}


def tools_for(agent: str) -> list[dict]:
    kinds = SLICES[agent]["kinds"]
    tools = [TOOL_SQL] if "sql" in kinds else []
    if "fts" in kinds:
        tools.append(TOOL_FTS)
    if "vector" in kinds:
        tools.append(TOOL_VECTOR)
    return tools


def _run_tool(name: str, args: dict, scope: ScopedDB, em: Emitter) -> str:
    """Execute one tool call against the agent's own DB. Returns a text result.

    Errors come back to the model as text, not exceptions: a malformed query is
    the agent's problem to recover from, and burning a budget unit on it is the
    correct cost.
    """
    table = SLICES[scope.agent]["table"]
    if name == "run_sql":
        sql, kind = args["sql"], "sql"
    elif name == "search_text":
        kind = "fts"
        sql = FTS_SQL.format(table=table, column=INDEXED_COLUMN[table],
                             q=str(args["q"]).replace("'", "''"),
                             limit=int(args.get("limit", 20)))
    elif name == "search_similar":
        kind = "vector"
        sql = VECTOR_SQL.format(table=table, column=INDEXED_COLUMN[table],
                                q=str(args["q"]).replace("'", "''"),
                                limit=int(args.get("limit", 10)))
    else:
        return f"unknown tool {name}"

    try:
        rows = query(scope.db_id, sql, agent=scope.agent, kind=kind, em=em,
                     scope=scope, client_=scope.client)
    except BudgetExceeded:
        raise
    except Exception as exc:  # noqa: BLE001 — hand the error back to the model
        hint = ""
        if kind in ("fts", "vector"):
            hint = ("\nThe search index may be unavailable. Fall back to run_sql with a "
                    "LIKE filter on the text column.")
        return f"QUERY FAILED: {str(exc)[:400]}{hint}"

    if not rows:
        return "0 rows."
    shown = rows[:ROWS_PREVIEW]
    out = json.dumps(shown, default=str)
    if len(rows) > ROWS_PREVIEW:
        out += f"\n({len(rows)} rows total, {ROWS_PREVIEW} shown — aggregate in SQL.)"
    return out


@dataclass
class AgentOutcome:
    """What wave 1 produced, WITHOUT the agent_end row.

    The row is emitted by the pipeline instead, after truth-derived hit_service /
    hit_fault are known. Emitting here and amending later produced two agent_end
    rows per agent, which silently halves Q1's hit_rate and doubles Q7's agent
    count — the aggregation convention in §9.1 assumes exactly one.
    """

    hset: HypothesisSet
    usage: Usage
    duration_ms: float
    queries: int
    db_id: str
    model: str


def run_agent(agent: str, scope: ScopedDB, symptoms: list[dict], em: Emitter, *,
              model: str = FAST_MODEL) -> AgentOutcome:
    """Run one wave-1 agent to a validated HypothesisSet.

    Never raises: a failed agent returns an empty HypothesisSet so the correlator
    still gets four opinions instead of none.
    """
    usage = Usage()
    t0 = time.monotonic()
    table = SLICES[agent]["table"]
    em.event("agent_start", agent=agent, wave=1, db_id=scope.db_id,
             prompt_hash=prompt_hash(agent), model=model)

    system = load_prompt(agent)
    opening = (
        f"Your database contains one table: {SCHEMA_HINT[table]}\n\n"
        f"Wave-0 triage found these symptoms (services >= 3x their own first-40-minute "
        f"baseline, earliest first):\n{json.dumps(symptoms, indent=2, default=str)}\n\n"
        f"You may make at most {scope.budget} queries. Investigate, then return your "
        f"HypothesisSet as bare JSON."
    )
    convo: list[dict] = [{"role": "user", "content": opening}]
    tools = tools_for(agent)

    try:
        for _ in range(MAX_TURNS):
            if scope.remaining <= 0:
                convo.append({"role": "user", "content":
                              "Query budget exhausted. Return your HypothesisSet JSON now."})
                break
            resp = call(model=model, system=system, messages=convo, tools=tools,
                        em=em, agent=agent, wave=1, usage=usage, prompt_name=agent)
            append_assistant(convo, resp)

            if not resp.wants_tools:
                break

            # Every issued tool call must get a result back, or the next request
            # is rejected for an unanswered call — including budget refusals.
            results: list[tuple[str, str]] = []
            for tc in resp.tool_calls:
                try:
                    out = _run_tool(tc.name, tc.args, scope, em)
                except BudgetExceeded:
                    out = "Budget exhausted. Return your HypothesisSet JSON now."
                results.append((tc.id, out[:20000]))
            append_tool_results(convo, results)

        hset = validated_call(
            model=model, system=system,
            messages=convo + [{"role": "user", "content":
                               "Return ONLY your HypothesisSet JSON now. No prose."}],
            schema=HypothesisSet, em=em, agent=agent, wave=1, usage=usage,
            prompt_name=agent)
    except Exception as exc:  # noqa: BLE001 — one agent must not kill the run
        em.event("error", agent=agent, wave=1, success=False, error_msg=str(exc)[:500])
        hset = None

    if hset is None:
        hset = HypothesisSet.empty(agent)
    hset.queries_used = scope.used
    hset.agent = AgentName(agent)  # trust the caller, not the model's self-label

    return AgentOutcome(hset=hset, usage=usage,
                        duration_ms=(time.monotonic() - t0) * 1000,
                        queries=scope.used, db_id=scope.db_id, model=model)
