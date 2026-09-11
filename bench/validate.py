"""Benchmark validity check.

    python -m bench.validate

Runs the REAL wave-0 triage SQL (agents/wave0.TRIAGE_SQL) against the generated
parquet with DuckDB, offline, and asserts that every scenario is actually
solvable from its own data:

  - the true root service is the EARLIEST sustained onset, so "earliest onset
    beats largest magnitude" is a winnable heuristic rather than a coin flip;
  - the cascade arrives in dependency order, never before the service it follows;
  - the trigger event named in truth.json exists in changes or k8s_events
    (or is legitimately null for third_party_latency);
  - the root's log signature is present after t0;
  - each scenario carries exactly 2 red herrings.

A score is only meaningful if the answer was reachable. Run this after
regenerating data and before trusting any batch.
"""

from __future__ import annotations

import glob
import json
import sys
from pathlib import Path

import config  # noqa: F401  — loads .env
import duckdb
import pandas as pd

from agents.wave0 import TRIAGE_SQL
from gen.faults import DEPENDENCIES

TABLES = ("metrics", "logs", "changes", "k8s_events")
EXPECTED_RED_HERRINGS = 2


def _depth_from(root: str) -> dict[str, int]:
    """Hops upstream from `root`. Cascade onsets must respect this ordering."""
    depth, frontier = {root: 0}, [root]
    while frontier:
        svc = frontier.pop(0)
        for caller in (s for s, deps in DEPENDENCIES.items() if svc in deps):
            if caller not in depth:
                depth[caller] = depth[svc] + 1
                frontier.append(caller)
    return depth


def check(d: Path) -> tuple[bool, str]:
    truth = json.loads((d / "truth.json").read_text())
    con = duckdb.connect()
    for t in TABLES:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{d / f'{t}.parquet'}')")

    triage = con.execute(TRIAGE_SQL).df()
    order = list(triage.service)
    root = truth["service"]
    problems: list[str] = []

    if not order:
        problems.append("triage found no symptoms")
    elif order[0] != root:
        problems.append(f"root {root} is not earliest onset (got {order[0]})")

    # Cascade must not overtake the service it follows.
    depth = _depth_from(root)
    ranks = {s: i for i, s in enumerate(order)}
    for svc in order:
        if svc in depth and depth[svc] > 0:
            nearer = [o for o in order if o in depth and depth[o] < depth[svc]]
            if any(ranks[n] > ranks[svc] for n in nearer):
                problems.append(f"{svc} onset precedes a service closer to the root")

    trigger = truth["trigger_event_id"]
    if trigger is not None:
        ids = set(con.execute("SELECT event_id FROM changes").df().event_id) | set(
            con.execute("SELECT event_id FROM k8s_events").df().event_id)
        if trigger not in ids:
            problems.append(f"trigger {trigger} missing from changes/k8s_events")

    start = pd.Timestamp(truth["start_ts"])
    n_sig = con.execute(
        "SELECT COUNT(*) AS n FROM logs WHERE service = ? AND ts >= ? "
        "AND level IN ('ERROR','WARN')", [root, start]).df().n[0]
    if n_sig == 0:
        problems.append("no error/warn log signature for the root after t0")

    if len(truth["red_herrings"]) != EXPECTED_RED_HERRINGS:
        problems.append(f"{len(truth['red_herrings'])} red herrings, expected {EXPECTED_RED_HERRINGS}")

    return not problems, " -> ".join(order) if not problems else "; ".join(problems)


def main(data_dir: str = "data") -> int:
    dirs = sorted(Path(p) for p in glob.glob(f"{data_dir}/s*") if Path(p).is_dir())
    if not dirs:
        print(f"no scenarios in {data_dir}/ — run: python -m gen.scenarios --seeds 1-12 99")
        return 1

    failures = 0
    print(f"{'sid':6s} {'fault':21s} {'root':10s} detail")
    for d in dirs:
        truth = json.loads((d / "truth.json").read_text())
        ok, detail = check(d)
        failures += not ok
        flag = "" if ok else "  <-- FAIL"
        print(f"{truth['scenario_id']:6s} {truth['fault_type']:21s} "
              f"{truth['service']:10s} {detail}{flag}")

    print()
    if failures:
        print(f"{failures}/{len(dirs)} scenarios are NOT solvable from their data")
    else:
        print(f"all {len(dirs)} scenarios valid — "
              "root is the earliest sustained onset, cascade follows the dependency chain")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
