"""Hotdata lifecycle: provision / load / index / scoped tools / destroy / sweep
(plan.md §8).

Isolation rule (§3): each wave-1 agent sees ONLY its own db_id plus symptoms[].
Agents never receive truth.json and never see another agent's db_id or output
before wave 2.

Create -> query -> destroy is explicit and every step emits telemetry; destroy is
always reached through a try/finally in wave 3, with sweep() as the backstop.

The SDK calls below are the §8 VERIFY list — resolve them in the 10:00-11:00
workshops before wiring.
"""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from telemetry.emit import Emitter

DB_PREFIX = os.getenv("DB_PREFIX", "isw")
ORPHAN_MAX_AGE_S = 15 * 60

# Per-agent slice: agent -> (table, parquet stem, query kinds, budget) — §7.2.
SLICES: dict[str, dict[str, Any]] = {
    "logs":     {"table": "logs",        "kinds": ("sql", "fts"),    "budget": 40},
    "metrics":  {"table": "metrics",     "kinds": ("sql",),          "budget": 30},
    "changes":  {"table": "changes",     "kinds": ("sql",),          "budget": 15},
    "infra":    {"table": "k8s_events",  "kinds": ("sql", "fts"),    "budget": 15},
    "history":  {"table": "postmortems", "kinds": ("vector",),       "budget": 10},
}

# The single-agent baseline gets one DB with all five tables and all three
# indexes, and a budget equal to the sum of the parallel agents' budgets (§3).
SINGLE_BUDGET = sum(s["budget"] for s in SLICES.values())  # 110

INDEX_SPECS = (
    ("logs", "msg", "fts"),
    ("k8s_events", "message", "fts"),
    ("postmortems", "body", "vector"),
)


class BudgetExceeded(RuntimeError):
    """Raised when an agent exhausts its query budget."""


@dataclass
class ScopedDB:
    """What a wave-1 agent is handed: one db_id and nothing else."""

    agent: str
    db_id: str
    budget: int
    used: int = 0


def provision(run_id: str, n: int, em: Emitter) -> list[str]:
    """Bulk-create n instant DBs from one template. Address by ID — names are not
    unique. Emits db_create x n with duration.

    TODO(VERIFY item 4, §8): bulk-create response is sync vs async batch polling,
    and time-to-ready. If >10s, pre-warm a DB pool per batch and report
    provisioning separately in Q7 (§13).
    """
    raise NotImplementedError("wire to Hotdata bulk-create after VERIFY item 4")


def load(db_id: str, table: str, parquet: Path, em: Emitter) -> int:
    """Upload + load parquet into `table`. Returns row count; emits db_load.

    Parquet, not inline CSV — inline caps at 2 MB and logs.parquet is ~200k rows.
    """
    raise NotImplementedError("wire to Hotdata load after VERIFY item 3")


def index(db_id: str, specs: tuple = INDEX_SPECS, *, em: Emitter) -> None:
    """FTS on logs.msg and k8s_events.message; vector on postmortems.body.
    Emits db_load with kind=index.

    TODO(VERIFY item 6, §8): do indexes refresh after new loads into an existing
    table? Affects FTS on the telemetry error_msg index.
    """
    raise NotImplementedError("wire to Hotdata index after VERIFY item 6")


def tools(db_id: str, budget: int, agent: str, em: Emitter) -> list[Any]:
    """Scoped SQL/search tools for one agent. Enforces the budget and times every
    call, emitting a `query` row per call (kind, text truncated, rows, duration).

    Preferred: hotdata-langchain tools, if RocketRide accepts LangChain tools.
    Fallback: thin wrappers over the Python SDK.

    TODO(VERIFY items 1-2, §8): how a RocketRide agent node calls custom Python /
    LangChain tools inside a wave, and whether waves can be parameterized with
    runtime values (wave-0 db_ids -> wave-1 agent config). Fallback for item 1 is
    agents-as-tools under a parent agent node (GATE 1, 11:45).
    """
    raise NotImplementedError("wire tool binding after VERIFY items 1-2")


def destroy(db_ids: list[str], em: Emitter) -> None:
    """Delete DBs. ALWAYS reached through try/finally in wave 3. Emits db_destroy x n.

    Never raises: a teardown failure must not mask a run's result. Failures are
    emitted with success=False and swept later.

    TODO(VERIFY item 5, §8): delete-DB call and its rate limits.
    """
    raise NotImplementedError("wire to Hotdata delete after VERIFY item 5")


def sweep(prefix: str = DB_PREFIX, max_age_s: int = ORPHAN_MAX_AGE_S, *, em: Emitter) -> int:
    """Delete orphaned `{prefix}-*` DBs older than max_age_s. Run at batch start
    and end. Emits db_destroy with agent='sweeper'. Returns the count reaped.

    Zero orphan DBs is a submission checklist item (§15).
    """
    raise NotImplementedError("wire to Hotdata list+delete after VERIFY item 5")


@contextmanager
def run_scope(run_id: str, n: int, em: Emitter) -> Iterator[list[str]]:
    """Provision n DBs and guarantee teardown.

    with run_scope(run_id, 5, em) as db_ids:
        ...  # waves 1-2
    # wave 3 teardown happens here, even on exception
    """
    db_ids: list[str] = []
    try:
        db_ids = provision(run_id, n, em)
        yield db_ids
    finally:
        if db_ids:
            try:
                destroy(db_ids, em)
            except Exception:  # noqa: BLE001 — teardown must never mask the real error
                em.event("error", agent="pipeline", wave=3, success=False,
                         error_msg="teardown failed; left to sweeper")
