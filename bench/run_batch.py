"""Batch runner (plan.md §10).

    python -m bench.run_batch --mode both --version v1 --seeds 1-12 --concurrency 3

Controls (§10): same models, same total query budget, same correlator prompt.
B1 and B2 are interleaved (P, S, P, S ...) so both modes meet the same rate-limit
conditions. Scenario-level concurrency is 3 for both modes.

Timebox (§10): if the median run exceeds 3 minutes, cut to seeds 1-6 and still
cover each fault type once.
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import config  # noqa: F401  — loads .env
from agents.hotdata_scope import client, sweep
from agents.pipeline import RunResult, run_once
from gen.scenarios import parse_seeds, scenario_id
from telemetry.emit import Emitter, HotdataSink, ParquetSink, RunContext, replay_backlog

DEFAULT_CONCURRENCY = 3
MEDIAN_RUN_BUDGET_S = 180


def interleaved(seeds: list[int], modes: list[str]) -> list[tuple[int, str]]:
    """P, S, P, S ... so both modes see the same rate-limit conditions."""
    return [(seed, mode) for seed in seeds for mode in modes]


def make_sink(local: bool):
    return ParquetSink() if local else HotdataSink()


def main() -> int:
    ap = argparse.ArgumentParser(description="Run a benchmark batch.")
    ap.add_argument("--mode", choices=("parallel", "single", "both"), default="parallel")
    ap.add_argument("--version", required=True, help="pipeline_version tag: v1, v2, single-v1")
    ap.add_argument("--seeds", nargs="+", required=True, help="e.g. 1-12")
    ap.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    ap.add_argument("--data", type=Path, default=Path("data"))
    ap.add_argument("--local-telemetry", action="store_true",
                    help="write telemetry to parquet instead of the telemetry DB")
    ap.add_argument("--no-sweep", action="store_true", help="skip the orphan sweep")
    ap.add_argument("--skip-agents", default="",
                    help="comma-separated wave-1 agents to drop (v2 adaptive fan-out)")
    args = ap.parse_args()

    if args.concurrency < 1:
        ap.error("--concurrency must be at least 1")

    skip = tuple(a.strip() for a in args.skip_agents.split(",") if a.strip())
    try:
        seeds = parse_seeds(args.seeds)
    except ValueError as exc:
        ap.error(str(exc))
    modes = ["parallel", "single"] if args.mode == "both" else [args.mode]
    jobs = interleaved(seeds, modes)
    missing = [scenario_id(seed) for seed in seeds
               if not (args.data / scenario_id(seed) / "truth.json").is_file()]
    if missing:
        ap.error(f"Missing scenarios: {', '.join(missing)}. Run python -m gen.scenarios first.")
    sink = make_sink(args.local_telemetry)
    c = client()

    sweeper = Emitter(RunContext("sweep", args.version, "parallel", "-"), sink=sink,
                      load_only=False)
    if not args.no_sweep:
        print(f"swept {sweep(em=sweeper, client_=c)} orphan DBs at batch start")

    print(f"{len(jobs)} runs: seeds={seeds} modes={modes} version={args.version} "
          f"concurrency={args.concurrency}" + (f" skip={list(skip)}" if skip else ""))

    results: list[RunResult] = []
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = {
            pool.submit(run_once, scenario_id(seed), mode, _tag(args.version, mode),
                        data_dir=args.data, sink=sink, client_=c,
                        skip_agents=skip): (seed, mode)
            for seed, mode in jobs
        }
        for fut in as_completed(futures):
            seed, mode = futures[fut]
            try:
                r = fut.result()
                results.append(r)
                print(f"  {r.scenario_id} {mode:8s} score={r.score}/4 "
                      f"wall={r.wall_ms/1000:5.1f}s ${r.cost_usd:.4f} q={r.queries}")
            except Exception as exc:  # noqa: BLE001 — one bad run must not kill the batch
                print(f"  FAILED seed={seed} mode={mode}: {exc}")

    if not args.no_sweep:
        print(f"swept {sweep(em=sweeper, client_=c)} orphan DBs at batch end")
    sweeper.flush(final=True)

    if not args.local_telemetry:
        if replayed := replay_backlog(sink):
            print(f"replayed {replayed} backlogged telemetry rows")

    _summarize(results, time.monotonic() - t0)
    failed = len(jobs) - len(results)
    if failed:
        print(f"\n{failed}/{len(jobs)} runs failed; inspect the dashboard's failure view.")
    return 1 if failed else 0


def _tag(version: str, mode: str) -> str:
    """The baseline is tagged single-v1 so Q4 separates it from the parallel arms."""
    return version if mode == "parallel" else (
        version if version.startswith("single") else f"single-{version}")


def _summarize(results: list[RunResult], elapsed: float) -> None:
    print(f"\ndone: {len(results)} runs in {elapsed:.0f}s")
    if not results:
        return
    for mode in ("parallel", "single"):
        rs = [r for r in results if r.mode == mode]
        if not rs:
            continue
        walls = [r.wall_ms / 1000 for r in rs]
        print(f"  {mode:8s} n={len(rs):2d} "
              f"score={statistics.mean(r.score for r in rs):.2f}/4 "
              f"median_wall={statistics.median(walls):5.1f}s "
              f"cost=${statistics.mean(r.cost_usd for r in rs):.4f}")
    med = statistics.median([r.wall_ms / 1000 for r in results])
    if med > MEDIAN_RUN_BUDGET_S:
        print(f"  TIMEBOX: median run {med:.0f}s exceeds {MEDIAN_RUN_BUDGET_S}s — "
              "cut to seeds 1-6 (plan.md §10)")


if __name__ == "__main__":
    sys.exit(main())
