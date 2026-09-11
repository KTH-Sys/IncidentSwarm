"""Waves 0-3, end to end (plan.md §3).

    wave 0  provision + load + index + triage        (deterministic, no LLM)
    wave 1  fan-out: five agents, concurrently       (parallel mode)
            or one agent over all five tables        (single mode)
    wave 2  fan-in: correlator -> RCAReport
    wave 3  teardown (always) + score + flush

Isolation (§3): agents never receive truth.json and never see another agent's
db_id or output before wave 2. Truth is loaded HERE, by the runner, and used only
after wave 2 to score and to stamp telemetry.
"""

from __future__ import annotations

import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agents.agent import run_agent
from agents.baseline import run_baseline
from agents.correlator import run_correlator
from agents.hotdata_scope import (
    FANOUT,
    INDEX_SPECS,
    SINGLE_BUDGET,
    SLICES,
    ScopedDB,
    client,
    destroy,
    index,
    load,
    provision,
)
from agents.llm import Usage
from agents.schemas import HypothesisSet, RCAReport
from agents.wave0 import Wave0Result, run_wave0, slice_path, triage
from bench.score import load_truth, score_agent, score_report
from telemetry.emit import Emitter, RunContext


@dataclass
class RunResult:
    run_id: str
    scenario_id: str
    mode: str
    pipeline_version: str
    score: int
    components: dict
    report: RCAReport
    wall_ms: float
    cost_usd: float
    tokens_in: int = 0
    tokens_out: int = 0
    queries: int = 0


def _history_enabled() -> bool:
    """HistoryAgent needs a vector index, which needs an embedding provider
    (VERIFY item 6). Without one it is skipped — which is §12 cut-list item 1."""
    return bool(os.getenv("HOTDATA_EMBEDDING_PROVIDER_ID"))


def run_parallel(run_id: str, scenario_id: str, em: Emitter, *, data_dir: Path = Path("data"),
                 client_: Any = None) -> tuple[RCAReport, dict]:
    """Waves 0-2 for the parallel arm. Teardown is the caller's finally path."""
    c = client_ or client()
    agents = [a for a in FANOUT if a != "history" or _history_enabled()]

    w0 = run_wave0(run_id, scenario_id, em, data_dir=data_dir, client_=c, n=len(agents))
    symptoms = [s.as_agent_context() for s in w0.symptoms]

    # WAVE 1 — fan-out. This ThreadPoolExecutor is the claim being tested.
    t1 = time.monotonic()
    em.event("wave_start", agent="pipeline", wave=1)
    hsets: dict[str, HypothesisSet] = {}
    usages: list[Usage] = []
    with ThreadPoolExecutor(max_workers=len(agents)) as pool:
        futures = {pool.submit(run_agent, a, w0.scoped[a], symptoms, em): a for a in agents}
        for fut in as_completed(futures):
            a = futures[fut]
            try:
                hset, usage = fut.result()
            except Exception as exc:  # noqa: BLE001 — one agent must not kill the run
                em.event("error", agent=a, wave=1, success=False, error_msg=str(exc)[:500])
                hset, usage = HypothesisSet.empty(a), Usage()
            hsets[a], _ = hset, usages.append(usage)
    em.event("wave_end", agent="pipeline", wave=1,
             duration_ms=(time.monotonic() - t1) * 1000, success=True)

    # WAVE 2 — fan-in.
    t2 = time.monotonic()
    em.event("wave_start", agent="pipeline", wave=2)
    report, corr_usage = run_correlator([hsets[a] for a in agents], symptoms, em)
    em.event("wave_end", agent="pipeline", wave=2,
             duration_ms=(time.monotonic() - t2) * 1000, success=True)

    usages.append(corr_usage)
    return report, {"w0": w0, "hsets": hsets, "usages": usages, "symptoms": symptoms}


def run_single(run_id: str, scenario_id: str, em: Emitter, *, data_dir: Path = Path("data"),
               client_: Any = None) -> tuple[RCAReport, dict]:
    """The baseline arm: one DB, all five tables, all three indexes, budget 110."""
    c = client_ or client()
    t0 = time.monotonic()
    em.event("wave_start", agent="pipeline", wave=0)

    prov = provision(run_id, 1, em, client_=c)
    db_id = prov.db_ids[0]
    for agent in FANOUT:
        load(db_id, SLICES[agent]["table"], slice_path(scenario_id, agent, data_dir),
             em, client_=c)
    index(db_id, INDEX_SPECS, em=em, client_=c)

    symptoms_objs = triage(db_id, em, client_=c)
    symptoms = [s.as_agent_context() for s in symptoms_objs]
    em.event("wave_end", agent="pipeline", wave=0,
             duration_ms=(time.monotonic() - t0) * 1000, success=True,
             payload={"symptoms": symptoms, "db_ids": [db_id], "batch_id": prov.batch_id})

    scope = ScopedDB(agent="single", db_id=db_id, budget=SINGLE_BUDGET, client=c, em=em)
    report, usage = run_baseline(scope, symptoms, em)
    return report, {"prov": prov, "scope": scope, "usages": [usage], "symptoms": symptoms}


def run_once(scenario_id: str, mode: str, pipeline_version: str, *,
             data_dir: Path = Path("data"), sink: Any = None,
             client_: Any = None) -> RunResult:
    """One scored run, waves 0-3, with teardown guaranteed.

    Truth is loaded here and never enters agent context; it is used only to score
    and to stamp fault_type / hit_* onto telemetry (§3).
    """
    truth = load_truth(scenario_id, data_dir)
    run_id = f"{scenario_id}-{mode}-{pipeline_version}-{uuid.uuid4().hex[:6]}"
    ctx = RunContext(run_id=run_id, pipeline_version=pipeline_version, mode=mode,
                     scenario_id=scenario_id, fault_type=truth.fault_type.value)
    em = Emitter(ctx, sink=sink, load_only=False)  # row inserts are supported
    c = client_ or client()

    t0 = time.monotonic()
    em.event("run_start", agent="pipeline", wave=0)
    state: dict = {}
    batch_id = None
    db_ids: list[str] = []
    try:
        if mode == "parallel":
            report, state = run_parallel(run_id, scenario_id, em, data_dir=data_dir, client_=c)
            batch_id, db_ids = state["w0"].prov.batch_id, state["w0"].db_ids
        else:
            report, state = run_single(run_id, scenario_id, em, data_dir=data_dir, client_=c)
            batch_id, db_ids = state["prov"].batch_id, state["prov"].db_ids
    finally:
        # WAVE 3 — teardown, always.
        if batch_id:
            em.event("wave_start", agent="pipeline", wave=3)
            destroy(batch_id, em, db_ids=db_ids, client_=c)

    scored = score_report(report, truth)
    usages = state.get("usages", [])
    cost = sum(u.cost_usd for u in usages)
    tin = sum(u.tokens_in for u in usages)
    tout = sum(u.tokens_out for u in usages)

    # Per-agent hits are stamped after the fact, outside agent context.
    for agent, hset in state.get("hsets", {}).items():
        hits = score_agent(hset, truth)
        em.event("agent_end", agent=agent, wave=1, success=True,
                 hit_service=hits["hit_service"], hit_fault=hits["hit_fault"],
                 payload={"scored_after_the_fact": True})

    wall = (time.monotonic() - t0) * 1000
    em.event("run_end", agent="pipeline", wave=3, duration_ms=wall, score=scored["score"],
             cost_usd=cost, tokens_in=tin, tokens_out=tout,
             query_count=sum(s.used for s in _scopes(state)),
             hit_service=scored["components"]["service"],
             hit_fault=scored["components"]["fault_type"], success=True,
             payload={"components": scored["components"], "rca": scored["predicted"],
                      "telemetry_dropped": em.dropped})
    em.flush(final=True)

    return RunResult(run_id=run_id, scenario_id=scenario_id, mode=mode,
                     pipeline_version=pipeline_version, score=scored["score"],
                     components=scored["components"], report=report, wall_ms=wall,
                     cost_usd=cost, tokens_in=tin, tokens_out=tout,
                     queries=sum(s.used for s in _scopes(state)))


def _scopes(state: dict) -> list[ScopedDB]:
    if "w0" in state:
        return list(state["w0"].scoped.values())
    return [state["scope"]] if "scope" in state else []
