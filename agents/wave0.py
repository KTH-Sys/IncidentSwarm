"""Wave 0 — provision + triage (plan.md §3). Deterministic, no LLM.

    bulk-create 5 DBs -> load each with ONLY its slice -> index -> triage

Triage produces `symptoms[]`: services whose err_rate or p99 is >= 3x their own
first-40-minute baseline, with the onset minute for each. That list is the only
cross-source context a wave-1 agent gets; everything else it must find itself.

The SQL below avoids interval arithmetic and casts on purpose (§9.3 dialect
safety) — it ranks rows per service instead of slicing by timestamp, so it works
wherever window functions do.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agents.hotdata_scope import (
    FANOUT,
    INDEX_SPECS,
    SLICES,
    Provisioned,
    ScopedDB,
    client,
    index,
    load,
    provision,
    query,
)
from telemetry.emit import Emitter

BASELINE_MINUTES = 40
SYMPTOM_MULTIPLIER = 3.0

MIN_SUSTAINED_MIN = 3   # a breach shorter than this is a blip, not an incident
ONGOING_SLACK_MIN = 2   # the incident must still be live at the end of the window

TRIAGE_SQL = f"""
WITH ranked AS (
  SELECT service, ts, err_rate, p99_ms,
         ROW_NUMBER() OVER (PARTITION BY service ORDER BY ts) AS rn
  FROM metrics
),
base AS (
  SELECT service,
         AVG(err_rate) AS base_err,
         AVG(p99_ms)   AS base_p99
  FROM ranked
  WHERE rn <= {BASELINE_MINUTES}
  GROUP BY service
),
horizon AS (
  SELECT MAX(rn) AS last_rn FROM ranked
),
later AS (
  SELECT r.service, r.ts, r.rn, r.err_rate, r.p99_ms, b.base_err, b.base_p99
  FROM ranked r
  JOIN base b ON b.service = r.service
  WHERE r.rn > {BASELINE_MINUTES}
),
breaches AS (
  SELECT service, ts, rn
  FROM later
  WHERE err_rate >= {SYMPTOM_MULTIPLIER} * base_err
     OR p99_ms   >= {SYMPTOM_MULTIPLIER} * base_p99
),
islands AS (
  SELECT service, ts, rn,
         rn - ROW_NUMBER() OVER (PARTITION BY service ORDER BY rn) AS grp
  FROM breaches
),
runs AS (
  SELECT service, grp,
         MIN(ts) AS onset_ts,
         MIN(rn) AS start_rn,
         MAX(rn) AS end_rn,
         COUNT(*) AS breach_minutes
  FROM islands
  GROUP BY service, grp
),
ongoing AS (
  SELECT service, onset_ts, start_rn, breach_minutes
  FROM runs
  WHERE breach_minutes >= {MIN_SUSTAINED_MIN}
    AND end_rn >= (SELECT last_rn FROM horizon) - {ONGOING_SLACK_MIN}
)
SELECT o.service,
       o.onset_ts        AS onset_ts,
       o.breach_minutes  AS breach_minutes,
       MAX(l.err_rate)   AS peak_err_rate,
       MAX(l.p99_ms)     AS peak_p99_ms,
       MIN(l.base_err)   AS base_err_rate,
       MIN(l.base_p99)   AS base_p99_ms
FROM ongoing o
JOIN later l
  ON l.service = o.service AND l.rn >= o.start_rn
GROUP BY o.service, o.onset_ts, o.breach_minutes
ORDER BY o.onset_ts
"""


@dataclass
class Symptom:
    """One anomalous service. Onset ordering is the point — the earliest service
    is the root-cause candidate, not the loudest (§7.2 correlator heuristics)."""

    service: str
    onset_ts: str
    peak_err_rate: float | None = None
    peak_p99_ms: float | None = None
    base_err_rate: float | None = None
    base_p99_ms: float | None = None
    err_breach_minutes: int = 0
    p99_breach_minutes: int = 0

    def as_agent_context(self) -> dict[str, Any]:
        """What an agent is allowed to see. No truth, no db_ids, no other agent."""
        return {
            "service": self.service,
            "onset_ts": str(self.onset_ts),
            "peak_err_rate": self.peak_err_rate,
            "peak_p99_ms": self.peak_p99_ms,
            "baseline_err_rate": self.base_err_rate,
            "baseline_p99_ms": self.base_p99_ms,
        }


@dataclass
class Wave0Result:
    prov: Provisioned
    symptoms: list[Symptom]
    scoped: dict[str, ScopedDB] = field(default_factory=dict)

    @property
    def db_ids(self) -> list[str]:
        return self.prov.db_ids

    def context_for(self, agent: str) -> dict[str, Any]:
        """The complete wave-1 input for one agent: its own db_id, its budget,
        and symptoms[]. Nothing else crosses the boundary."""
        s = self.scoped[agent]
        return {
            "agent": agent,
            "db_id": s.db_id,
            "budget": s.budget,
            "symptoms": [x.as_agent_context() for x in self.symptoms],
        }


def slice_path(scenario_id: str, agent: str, data_dir: Path) -> Path:
    return Path(data_dir) / scenario_id / f"{SLICES[agent]['table']}.parquet"


def run_wave0(run_id: str, scenario_id: str, em: Emitter, *, data_dir: Path = Path("data"),
              client_: Any = None, agents: list[str] | None = None,
              state: dict | None = None) -> Wave0Result:
    """Provision, load slices, index, triage. Returns db_ids + symptoms[].

    Caller owns teardown — wrap this in hotdata_scope.run_scope, or call
    destroy(result.prov.batch_id) from a finally path (§8).
    """
    c = client_ or client()
    agents = list(agents if agents is not None else FANOUT)
    t0 = time.monotonic()
    em.event("wave_start", agent="pipeline", wave=0)

    prov = provision(run_id, len(agents), em, client_=c)
    # Register the moment the DBs exist, so a failure in load or index still
    # reaches teardown. Anything set after this point is too late.
    if state is not None:
        state["prov"] = prov

    # Each DB gets exactly one slice. FANOUT order maps agent -> db_ids[i].
    scoped: dict[str, ScopedDB] = {}
    for i, agent in enumerate(agents):
        db_id = prov.db_ids[i]
        table = SLICES[agent]["table"]
        loaded = load(db_id, table, slice_path(scenario_id, agent, data_dir), em, client_=c)
        index(loaded.connection_id, INDEX_SPECS, em=em, client_=c, only_table=table,
              db_id=db_id)
        scoped[agent] = ScopedDB(agent=agent, db_id=db_id,
                                 budget=SLICES[agent]["budget"], client=c, em=em)

    # Triage always runs on the metrics table, even when MetricsAgent is not
    # spawned (v2): the metrics SIGNAL is what symptoms[] is made of. When the
    # agent is dropped, wave 0 loads metrics into the first DB purely to triage
    # it, so the deterministic signal survives the agent's removal.
    if "metrics" in scoped:
        metrics_db = scoped["metrics"].db_id
    else:
        metrics_db = prov.db_ids[0]
        load(metrics_db, "metrics", slice_path(scenario_id, "metrics", data_dir),
             em, client_=c)
    symptoms = triage(metrics_db, em, client_=c)

    em.event("wave_end", agent="pipeline", wave=0,
             duration_ms=(time.monotonic() - t0) * 1000, success=True,
             payload={"symptoms": [s.as_agent_context() for s in symptoms],
                      "db_ids": prov.db_ids, "batch_id": prov.batch_id})
    return Wave0Result(prov=prov, symptoms=symptoms, scoped=scoped)


def triage(metrics_db_id: str, em: Emitter, *, client_: Any = None) -> list[Symptom]:
    """Deterministic SQL triage on the metrics DB. No LLM, no budget charge —
    this is pipeline work, so it is emitted as agent='pipeline', wave=0."""
    rows = query(metrics_db_id, TRIAGE_SQL, agent="pipeline", kind="sql",
                 em=em, client_=client_)
    return [
        Symptom(
            service=r["service"],
            onset_ts=r["onset_ts"],
            peak_err_rate=r.get("peak_err_rate"),
            peak_p99_ms=r.get("peak_p99_ms"),
            base_err_rate=r.get("base_err_rate"),
            base_p99_ms=r.get("base_p99_ms"),
            err_breach_minutes=int(r.get("err_breach_minutes") or 0),
            p99_breach_minutes=int(r.get("p99_breach_minutes") or 0),
        )
        for r in rows
    ]
