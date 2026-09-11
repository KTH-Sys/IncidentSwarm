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
from agents.llm import STRONG_MODEL, Usage, prompt_hash, warm
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
                 client_: Any = None, state: dict | None = None) -> tuple[RCAReport, dict]:
    """Waves 0-2 for the parallel arm. Teardown is the caller's finally path,
    driven by `state["prov"]`, which wave 0 sets as soon as the DBs exist."""
    c = client_ or client()
    state = state if state is not None else {}
    agents = [a for a in FANOUT if a != "history" or _history_enabled()]

    # Learn each model's parameter quirks once, before five agents discover the
    # same 400 concurrently.
    from agents.llm import FAST_MODEL
    warm(FAST_MODEL, em)
    warm(STRONG_MODEL, em)

    w0 = run_wave0(run_id, scenario_id, em, data_dir=data_dir, client_=c, n=len(agents),
                   state=state)
    state["w0"] = w0
    symptoms = [s.as_agent_context() for s in w0.symptoms]

    # WAVE 1 — fan-out. This ThreadPoolExecutor is the claim being tested.
    t1 = time.monotonic()
    em.event("wave_start", agent="pipeline", wave=1)
    hsets: dict[str, HypothesisSet] = {}
    usages: list[Usage] = []
    outcomes: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=len(agents)) as pool:
        futures = {pool.submit(run_agent, a, w0.scoped[a], symptoms, em): a for a in agents}
        for fut in as_completed(futures):
            a = futures[fut]
            try:
                outcome = fut.result()
                hsets[a], outcomes[a] = outcome.hset, outcome
                usages.append(outcome.usage)
            except Exception as exc:  # noqa: BLE001 — one agent must not kill the run
                em.event("error", agent=a, wave=1, success=False, error_msg=str(exc)[:500])
                hsets[a] = HypothesisSet.empty(a)
                usages.append(Usage())
    state["outcomes"] = outcomes
    em.event("wave_end", agent="pipeline", wave=1,
             duration_ms=(time.monotonic() - t1) * 1000, success=True)

    # WAVE 2 — fan-in.
    t2 = time.monotonic()
    em.event("wave_start", agent="pipeline", wave=2)
    report, corr_usage = run_correlator([hsets[a] for a in agents], symptoms, em)
    em.event("wave_end", agent="pipeline", wave=2,
             duration_ms=(time.monotonic() - t2) * 1000, success=True)

    usages.append(corr_usage)
    state.update(hsets=hsets, usages=usages, symptoms=symptoms)
    return report, state


def run_single(run_id: str, scenario_id: str, em: Emitter, *, data_dir: Path = Path("data"),
               client_: Any = None, state: dict | None = None) -> tuple[RCAReport, dict]:
    """The baseline arm: one DB, all five tables, all three indexes, budget 110."""
    c = client_ or client()
    state = state if state is not None else {}
    t0 = time.monotonic()
    em.event("wave_start", agent="pipeline", wave=0)

    prov = provision(run_id, 1, em, client_=c)
    state["prov"] = prov  # register before anything that can fail
    db_id = prov.db_ids[0]
    connection_id = None
    for agent in FANOUT:
        loaded = load(db_id, SLICES[agent]["table"],
                      slice_path(scenario_id, agent, data_dir), em, client_=c)
        connection_id = connection_id or loaded.connection_id
    index(connection_id, INDEX_SPECS, em=em, client_=c, db_id=db_id)

    symptoms_objs = triage(db_id, em, client_=c)
    symptoms = [s.as_agent_context() for s in symptoms_objs]
    em.event("wave_end", agent="pipeline", wave=0,
             duration_ms=(time.monotonic() - t0) * 1000, success=True,
             payload={"symptoms": symptoms, "db_ids": [db_id], "batch_id": prov.batch_id})

    scope = ScopedDB(agent="single", db_id=db_id, budget=SINGLE_BUDGET, client=c, em=em)
    report, usage = run_baseline(scope, symptoms, em)
    state.update(scope=scope, usages=[usage], symptoms=symptoms)
    return report, state


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
    em = Emitter(ctx, sink=sink, load_only=True)  # micro-batch: each flush is an upload
    c = client_ or client()

    t0 = time.monotonic()
    em.event("run_start", agent="pipeline", wave=0)
    # `state` is populated as the run proceeds, NOT returned at the end, so the
    # finally path can tear down databases even when the step that failed came
    # after provisioning. Binding teardown to the return value meant a failure
    # anywhere in waves 0-2 leaked every DB the run had created.
    state: dict = {}
    try:
        runner = run_parallel if mode == "parallel" else run_single
        report, state = runner(run_id, scenario_id, em, data_dir=data_dir,
                               client_=c, state=state)
    finally:
        # WAVE 3 — teardown, always.
        prov = state.get("prov")
        if prov is not None:
            em.event("wave_start", agent="pipeline", wave=3)
            destroy(prov.batch_id, em, db_ids=prov.db_ids, client_=c)

    scored = score_report(report, truth)
    usages = state.get("usages", [])
    cost = sum(u.cost_usd for u in usages)
    tin = sum(u.tokens_in for u in usages)
    tout = sum(u.tokens_out for u in usages)

    # ONE agent_end per agent (§9.1), emitted here because hit_service and
    # hit_fault derive from truth and must be computed outside agent context.
    for agent, outcome in state.get("outcomes", {}).items():
        hits = score_agent(outcome.hset, truth)
        em.event("agent_end", agent=agent, wave=1, db_id=outcome.db_id,
                 model=outcome.model, duration_ms=outcome.duration_ms,
                 query_count=outcome.queries, tokens_in=outcome.usage.tokens_in,
                 tokens_out=outcome.usage.tokens_out, cost_usd=outcome.usage.cost_usd,
                 retry_count=outcome.usage.retries, prompt_hash=prompt_hash(agent),
                 success=bool(outcome.hset.hypotheses),
                 hit_service=hits["hit_service"], hit_fault=hits["hit_fault"],
                 payload={"hypotheses": outcome.hset.model_dump(mode="json")["hypotheses"],
                          "notes": outcome.hset.notes})

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
