"""Batch runner (plan.md §10).

    python -m bench.run_batch --mode parallel --version v1 --seeds 1-12 --concurrency 3

Fires one webhook per run at RocketRide with {run_id, scenario_id, mode,
pipeline_version} and waits for the run to land.

Controls (§10): same models, temperature 0, same total query budget, same
correlator prompt. Interleave B1/B2 (P, S, P, S ...) with --interleave so both
modes meet the same rate-limit conditions. Concurrency 3 for both modes.

Timebox (§10): if the median run exceeds 3 min, cut to seeds 1-6 and still cover
each fault type once.
"""

from __future__ import annotations

import argparse
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from bench.score import load_truth
from gen.scenarios import parse_seeds, scenario_id
from telemetry.emit import Emitter, ParquetSink, RunContext, replay_backlog

DEFAULT_CONCURRENCY = 3
MEDIAN_RUN_BUDGET_S = 180  # §10 timebox trigger


def make_run_id(scenario: str, mode: str, version: str) -> str:
    return f"{scenario}-{mode}-{version}-{uuid.uuid4().hex[:6]}"


def fire(scenario: str, mode: str, version: str, data_dir: Path) -> dict:
    """Fire one run and wait for it. Returns the run_end summary.

    The runner — not the agents — attaches truth-derived columns (fault_type,
    hit_*, score) to telemetry, outside agent context (§3 isolation rule).

    TODO(VERIFY item 2, §8): webhook payload shape, and whether waves can be
    parameterized with runtime values.
    """
    truth = load_truth(scenario, data_dir)
    ctx = RunContext(
        run_id=make_run_id(scenario, mode, version),
        pipeline_version=version,
        mode=mode,
        scenario_id=scenario,
        fault_type=truth.fault_type.value,
    )
    raise NotImplementedError("wire the RocketRide webhook after VERIFY item 2")


def interleaved(seeds: list[int], modes: list[str]) -> list[tuple[int, str]]:
    """P, S, P, S ... so both modes see the same rate-limit conditions."""
    return [(seed, mode) for seed in seeds for mode in modes]


def main() -> None:
    ap = argparse.ArgumentParser(description="Run a benchmark batch.")
    ap.add_argument("--mode", choices=("parallel", "single", "both"), default="parallel")
    ap.add_argument("--version", required=True, help="pipeline_version tag: v1, v2, single-v1")
    ap.add_argument("--seeds", nargs="+", required=True, help="e.g. 1-12")
    ap.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    ap.add_argument("--data", type=Path, default=Path("data"))
    ap.add_argument("--sweep", action="store_true",
                    help="sweep orphan DBs at batch start and end (§8)")
    args = ap.parse_args()

    seeds = parse_seeds(args.seeds)
    modes = ["parallel", "single"] if args.mode == "both" else [args.mode]
    jobs = interleaved(seeds, modes)

    print(f"{len(jobs)} runs: seeds={seeds} modes={modes} "
          f"version={args.version} concurrency={args.concurrency}")

    results = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = {
            pool.submit(fire, scenario_id(seed), mode, args.version, args.data): (seed, mode)
            for seed, mode in jobs
        }
        for fut in as_completed(futures):
            seed, mode = futures[fut]
            try:
                results.append(fut.result())
            except Exception as exc:  # noqa: BLE001 — one bad run must not kill the batch
                print(f"  FAILED seed={seed} mode={mode}: {exc}")

    # Replay anything the emitter had to spill to the local backlog (§9.2).
    if replayed := replay_backlog(ParquetSink()):
        print(f"replayed {replayed} backlogged telemetry rows")

    print(f"done: {len(results)}/{len(jobs)} runs landed")


if __name__ == "__main__":
    main()
