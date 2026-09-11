"""Scenario generator CLI (plan.md §5).

    python -m gen.scenarios --seeds 1-12 99

Writes, per scenario, into data/<scenario_id>/:
    logs.parquet  metrics.parquet  changes.parquet  k8s_events.parquet
    postmortems.parquet  truth.json

truth.json is NEVER loaded into any DB (§3 isolation rule).

Seeds 1-12 are the batch set, two per fault type, round-robin. Seed 99 is the
hold-out for the live demo, with a fault type chosen at demo time via --fault.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import config  # noqa: F401  — loads .env

from gen import noise
from gen.faults import CATALOG, PRIORITY, T0_RANGE_MIN, WINDOW_MIN
from gen.postmortems import build_corpus

DATA_DIR = Path("data")
DEMO_SEED = 99
WINDOW_START = datetime(2026, 9, 11, 13, 0, tzinfo=timezone.utc)

TABLES = ("logs", "metrics", "changes", "k8s_events", "postmortems")

GENERIC_ERRORS = (
    "request failed: connection reset by peer",
    "downstream returned 503",
    "deadline exceeded while calling downstream",
)
INFO_MSGS = ("request completed", "handled request", "cache hit", "cache miss")
WARN_MSGS = ("retrying downstream call", "slow query detected", "elevated queue depth")
ROUTES = ("/checkout", "/charge", "/cart", "/inventory/reserve", "/health")


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
    """Round-robin so seeds 1-12 cover each fault type twice (§5.1)."""
    return catalog_keys[(seed - 1) % len(catalog_keys)]


def _ts(minute: int) -> datetime:
    return WINDOW_START + timedelta(minutes=int(minute))


def _build_logs(world: dict, root: str, t0: int, inj, rng: np.random.Generator) -> list[dict]:
    """Sampled application logs. Error volume tracks each service's err_rate, so
    the log signal and the metric signal agree without being copies."""
    services = list(world)
    shares = np.array([noise.RPS_SHARE[s] for s in services])
    shares = shares / shares.sum()
    fault_msgs = (inj.log_msg,) + tuple(inj.extra_log_msgs)
    rows: list[dict] = []

    for minute in range(WINDOW_MIN):
        traces = [f"tr_{rng.integers(0, 2**31):08x}" for _ in range(60)]
        for svc, share in zip(services, shares):
            n = max(1, int(noise.LOGS_PER_MINUTE_TOTAL * share))
            err_rate = float(world[svc]["err_rate"][minute])
            n_err = int(round(n * err_rate))
            p99 = float(world[svc]["p99_ms"][minute])

            for k in range(n):
                is_err = k < n_err
                if is_err:
                    if svc == root and minute >= t0:
                        msg = str(rng.choice(fault_msgs))
                        level = inj.log_level
                    else:
                        msg = str(rng.choice(GENERIC_ERRORS))
                        level = "ERROR"
                    status = 500
                elif rng.random() < 0.04:
                    level, msg, status = "WARN", str(rng.choice(WARN_MSGS)), 200
                else:
                    level, msg, status = "INFO", str(rng.choice(INFO_MSGS)), 200

                latency = max(1.0, float(rng.normal(p99 * 0.35, p99 * 0.2)))
                rows.append({
                    "ts": _ts(minute) + timedelta(seconds=float(rng.uniform(0, 60))),
                    "service": svc,
                    "level": level,
                    "trace_id": traces[int(rng.integers(0, len(traces)))],
                    "msg": msg,
                    "attrs": json.dumps({"status": status, "latency_ms": round(latency, 1),
                                         "route": str(rng.choice(ROUTES))}),
                })
    if len(rows) > noise.LOGS_CAP:
        idx = rng.choice(len(rows), size=noise.LOGS_CAP, replace=False)
        rows = [rows[i] for i in sorted(idx)]
    return rows


def generate(seed: int, out_dir: Path, fault_type: str) -> dict:
    """Generate one scenario. Deterministic in `seed`. Returns the truth dict."""
    rng = np.random.default_rng(seed)
    fault = CATALOG[fault_type]
    t0 = int(rng.integers(T0_RANGE_MIN[0], T0_RANGE_MIN[1] + 1))

    world = noise.baseline_metrics(rng)
    changes: list[dict] = []
    k8s: list[dict] = []
    chg_n, k8s_n = [0], [0]

    def next_chg() -> str:
        chg_n[0] += 1
        return f"chg_{chg_n[0]:04d}"

    def next_k8s() -> str:
        k8s_n[0] += 1
        return f"k8s_{k8s_n[0]:04d}"

    noise.routine_k8s_noise(k8s, rng, next_k8s)
    inj = fault.inject(world, changes, k8s, t0, rng, next_chg, next_k8s)
    noise.apply_cascade(world, fault.root_service, inj.err_elevation, rng)
    herrings = noise.add_red_herrings(world, changes, k8s, t0, rng, next_chg, next_k8s)

    logs = _build_logs(world, fault.root_service, t0, inj, rng)

    metrics = [
        {"ts": _ts(m), "service": svc,
         "rps": float(world[svc]["rps"][m]),
         "p99_ms": float(world[svc]["p99_ms"][m]),
         "err_rate": float(world[svc]["err_rate"][m]),
         "cpu_pct": float(world[svc]["cpu_pct"][m]),
         "mem_mb": float(world[svc]["mem_mb"][m]),
         "db_conns": (None if np.isnan(world[svc]["db_conns"][m])
                      else int(world[svc]["db_conns"][m]))}
        for svc in world for m in range(WINDOW_MIN)
    ]

    changes_rows = sorted(
        ({"event_id": c["event_id"], "ts": _ts(c["minute"]), "kind": c["kind"],
          "service": c["service"], "detail": c["detail"], "author": c["author"]}
         for c in changes), key=lambda r: r["ts"])
    k8s_rows = sorted(
        ({"event_id": e["event_id"], "ts": _ts(e["minute"]), "service": e["service"],
          "pod": e["pod"], "reason": e["reason"], "message": e["message"]}
         for e in k8s), key=lambda r: r["ts"])

    out_dir.mkdir(parents=True, exist_ok=True)
    _write(out_dir / "logs.parquet", logs)
    _write(out_dir / "metrics.parquet", metrics)
    _write(out_dir / "changes.parquet", changes_rows)
    _write(out_dir / "k8s_events.parquet", k8s_rows)
    _write(out_dir / "postmortems.parquet", build_corpus(np.random.default_rng(7)))

    return {
        "scenario_id": scenario_id(seed),
        "service": fault.root_service,
        "fault_type": fault_type,
        "trigger_event_id": inj.trigger_event_id,
        "start_ts": _ts(t0).isoformat().replace("+00:00", "Z"),
        "red_herrings": herrings,
    }


def _write(path: Path, rows: list[dict]) -> None:
    pq.write_table(pa.Table.from_pylist(rows), path)


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
        truth = generate(seed, out, ft)
        (out / "truth.json").write_text(json.dumps(truth, indent=2))
        print(f"{sid}: {ft:20s} root={truth['service']:9s} "
              f"trigger={truth['trigger_event_id']} t0={truth['start_ts']}")


if __name__ == "__main__":
    main()
