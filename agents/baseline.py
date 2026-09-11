"""Single-agent baseline (plan.md §3) — the control arm.

One DB holding all five tables and all three indexes. One fast-model agent with a
query budget equal to the sum of the five parallel agents' budgets (110). It
finishes with one strong-model synthesis call using the SAME correlator prompt.

The only variable changed is parallel vs sequential. This file must not be tuned
to lose, and must not be tuned to win.
"""

from __future__ import annotations

import json
import time
from typing import Any

from agents.agent import INDEXED_COLUMN, ROWS_PREVIEW, SCHEMA_HINT, MAX_TURNS
from agents.hotdata_scope import SINGLE_BUDGET, BudgetExceeded, ScopedDB, query
from agents.llm import (
    FAST_MODEL,
    STRONG_MODEL,
    Usage,
    append_assistant,
    append_tool_results,
    call,
    load_prompt,
    prompt_hash,
    validated_call,
)
from agents.schemas import RCAReport
from telemetry.emit import Emitter

SEARCHABLE = tuple(INDEXED_COLUMN)  # logs, k8s_events, postmortems

TOOL_SQL = {
    "name": "run_sql",
    "description": ("Run one read-only SQL query against the incident database. "
                    "Aggregate in SQL — never select raw rows in bulk. Returns at most "
                    f"{ROWS_PREVIEW} rows."),
    "input_schema": {
        "type": "object",
        "properties": {"sql": {"type": "string"}},
        "required": ["sql"], "additionalProperties": False,
    },
}
TOOL_FTS = {
    "name": "search_text",
    "description": "Full-text search an indexed text column (logs.msg or k8s_events.message).",
    "input_schema": {
        "type": "object",
        "properties": {"table": {"type": "string", "enum": ["logs", "k8s_events"]},
                       "q": {"type": "string"},
                       "limit": {"type": "integer"}},
        "required": ["table", "q"], "additionalProperties": False,
    },
}
TOOL_VECTOR = {
    "name": "search_similar",
    "description": "Semantic search over past postmortems. Search the symptom pattern.",
    "input_schema": {
        "type": "object",
        "properties": {"q": {"type": "string"}, "limit": {"type": "integer"}},
        "required": ["q"], "additionalProperties": False,
    },
}


def _run_tool(name: str, args: dict, scope: ScopedDB, em: Emitter) -> str:
    from agents.agent import FTS_SQL, VECTOR_SQL

    if name == "run_sql":
        sql, kind = args["sql"], "sql"
    elif name == "search_text":
        table = args["table"]
        kind = "fts"
        sql = FTS_SQL.format(table=table, column=INDEXED_COLUMN[table],
                             q=str(args["q"]).replace("'", "''"),
                             limit=int(args.get("limit", 20)))
    elif name == "search_similar":
        kind = "vector"
        sql = VECTOR_SQL.format(table="postmortems", column="body",
                                q=str(args["q"]).replace("'", "''"),
                                limit=int(args.get("limit", 10)))
    else:
        return f"unknown tool {name}"

    try:
        rows = query(scope.db_id, sql, agent="single", kind=kind, em=em,
                     scope=scope, client_=scope.client)
    except BudgetExceeded:
        raise
    except Exception as exc:  # noqa: BLE001
        hint = ("\nThe search index may be unavailable. Fall back to run_sql with a LIKE "
                "filter.") if kind in ("fts", "vector") else ""
        return f"QUERY FAILED: {str(exc)[:400]}{hint}"

    if not rows:
        return "0 rows."
    out = json.dumps(rows[:ROWS_PREVIEW], default=str)
    if len(rows) > ROWS_PREVIEW:
        out += f"\n({len(rows)} rows total, {ROWS_PREVIEW} shown — aggregate in SQL.)"
    return out


def run_baseline(scope: ScopedDB, symptoms: list[dict], em: Emitter, *,
                 fast_model: str = FAST_MODEL,
                 strong_model: str = STRONG_MODEL) -> tuple[RCAReport, Usage]:
    """Investigate every source sequentially, then synthesize with the correlator
    prompt. Never raises."""
    usage = Usage()
    t0 = time.monotonic()
    em.event("agent_start", agent="single", wave=1, db_id=scope.db_id,
             model=fast_model, prompt_hash=prompt_hash("single"))

    schema_block = "\n".join(f"  {SCHEMA_HINT[t]}" for t in SCHEMA_HINT)
    opening = (
        f"The incident database contains all five tables:\n{schema_block}\n\n"
        f"Wave-0 triage found these symptoms (services >= 3x their own first-40-minute "
        f"baseline, earliest first):\n{json.dumps(symptoms, indent=2, default=str)}\n\n"
        f"You may make at most {scope.budget} queries across all five sources. "
        f"Investigate, then return your RCAReport as bare JSON."
    )
    convo: list[dict] = [{"role": "user", "content": opening}]
    tools = [TOOL_SQL, TOOL_FTS, TOOL_VECTOR]

    try:
        for _ in range(MAX_TURNS * 2):  # more turns: 5 sources and 110 queries
            if scope.remaining <= 0:
                convo.append({"role": "user", "content":
                              "Query budget exhausted. Produce your RCAReport now."})
                break
            resp = call(model=fast_model, system=load_prompt("single"), messages=convo,
                        tools=tools, em=em, agent="single", wave=1, usage=usage,
                        prompt_name="single")
            append_assistant(convo, resp)
            if not resp.wants_tools:
                break
            results: list[tuple[str, str]] = []
            for tc in resp.tool_calls:
                try:
                    out = _run_tool(tc.name, tc.args, scope, em)
                except BudgetExceeded:
                    out = "Budget exhausted. Produce your RCAReport now."
                results.append((tc.id, out[:20000]))
            append_tool_results(convo, results)
    except Exception as exc:  # noqa: BLE001
        em.event("error", agent="single", wave=1, success=False, error_msg=str(exc)[:500])

    investigation = "\n\n".join(
        m["content"] for m in convo
        if m["role"] == "assistant" and isinstance(m.get("content"), str) and m["content"]
    )

    em.event("agent_end", agent="single", wave=1, db_id=scope.db_id, model=fast_model,
             duration_ms=(time.monotonic() - t0) * 1000, query_count=scope.used,
             tokens_in=usage.tokens_in, tokens_out=usage.tokens_out,
             cost_usd=usage.cost_usd, retry_count=usage.retries,
             prompt_hash=prompt_hash("single"), success=True)

    # Synthesis: strong model, correlator prompt — identical to the parallel arm.
    syn_t0 = time.monotonic()
    syn = Usage()
    em.event("agent_start", agent="correlator", wave=2, model=strong_model,
             prompt_hash=prompt_hash("correlator"))
    user = ("A single agent investigated every data source for this incident. Its findings "
            f"follow.\n\nWave-0 triage symptoms:\n{json.dumps(symptoms, indent=2, default=str)}\n\n"
            f"Investigation notes:\n{investigation[:40000]}\n\nReturn ONLY the RCAReport JSON.")
    report = validated_call(model=strong_model, system=load_prompt("correlator"),
                            messages=[{"role": "user", "content": user}],
                            schema=RCAReport, em=em, agent="correlator", wave=2,
                            usage=syn, prompt_name="correlator")
    if report is None:
        from agents.correlator import _fallback_report
        report = _fallback_report([], symptoms)

    em.event("agent_end", agent="correlator", wave=2, model=strong_model,
             duration_ms=(time.monotonic() - syn_t0) * 1000, tokens_in=syn.tokens_in,
             tokens_out=syn.tokens_out, cost_usd=syn.cost_usd, retry_count=syn.retries,
             prompt_hash=prompt_hash("correlator"), success=True,
             payload={"rca": report.model_dump(mode="json")})

    total = Usage(tokens_in=usage.tokens_in + syn.tokens_in,
                  tokens_out=usage.tokens_out + syn.tokens_out,
                  cost_usd=usage.cost_usd + syn.cost_usd,
                  calls=usage.calls + syn.calls, retries=usage.retries + syn.retries)
    return report, total
