"""Scenario generator CLI (plan.md §5).

    python -m gen.scenarios --seeds 1-12 99

Writes, per scenario, into data/<scenario_id>/:
    logs.parquet  metrics.parquet  changes.parquet  k8s_events.parquet
    postmortems.parquet  truth.json

truth.json is NEVER loaded into any DB (§3 isolation rule).

Seeds 1-12 are the batch set, two per fault type, round-robin. Seed 99 is the
hold-out for the live demo, with a fault type chosen at demo time.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from gen.faults import CATALOG, PRIORITY

DATA_DIR = Path("data")
DEMO_SEED = 99

TABLES = ("logs", "metrics", "changes", "k8s_events", "postmortems")


def parse_seeds(tokens: list[str]) -> list[int]:
    """Accepts '1-12', '99', or a mix: --seeds 1-12 99"""
    seeds: list[int] = []
    for tok in tokens:
        if "-" in tok:
            lo, hi = tok.split("-", 1)
            seeds.extend(range(int(lo), int(hi) + 1))
        else:
            seeds.append(int(tok))
    return sorted(dict.fromkeys(seeds))


def scenario_id(seed: int) -> str:
    return f"s{seed:02d}"


def fault_for_seed(seed: int, catalog_keys: tuple[str, ...]) -> str:
    """Round-robin so seeds 1-12 cover each fault type twice (§5.1).

    If only the PRIORITY three are built, rotate over those instead (§12 cut 2).
    """
    return catalog_keys[(seed - 1) % len(catalog_keys)]


def generate(seed: int, out_dir: Path, fault_type: str | None = None) -> dict:
    """Generate one scenario. Deterministic in `seed`.

    TODO: wire gen.noise baseline -> gen.faults.inject -> gen.noise.apply_cascade
    -> gen.noise.add_red_herrings -> write parquet + truth.json.
    """
    raise NotImplementedError


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate synthetic incident scenarios.")
    ap.add_argument("--seeds", nargs="+", required=True, help="e.g. 1-12 99")
    ap.add_argument("--out", type=Path, default=DATA_DIR)
    ap.add_argument("--fault", help="force a fault type (used for the seed-99 demo)")
    ap.add_argument("--only-priority", action="store_true",
                    help="rotate over the 3 PRIORITY fault types only (§12 cut list)")
    args = ap.parse_args()

    keys = PRIORITY if args.only_priority else tuple(CATALOG)
    for seed in parse_seeds(args.seeds):
        ft = args.fault or fault_for_seed(seed, keys)
        sid = scenario_id(seed)
        out = args.out / sid
        out.mkdir(parents=True, exist_ok=True)
        truth = generate(seed, out, ft)
        (out / "truth.json").write_text(json.dumps(truth, indent=2, default=str))
        print(f"{sid}: {ft} -> {out}")


if __name__ == "__main__":
    main()
