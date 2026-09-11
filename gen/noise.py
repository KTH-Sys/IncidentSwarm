"""Baseline traffic, cascade, and red herrings (plan.md §5.1, §5.3).

The baseline has to look boring and real: the fault signature is only findable
because it breaks these levels, and the red herrings are what a naive agent
latches onto instead.
"""

from __future__ import annotations

import numpy as np

from gen.faults import (
    CASCADE_FRACTION,
    CASCADE_LAG_MIN,
    DEPENDENCIES,
    METRIC_SERVICES,
    WINDOW_MIN,
)

# --- Baseline levels (§5.1) ---
GATEWAY_RPS = 30.0
RPS_SINUSOID_AMPLITUDE = 0.20  # +/-20%
RPS_PERIOD_MIN = 45.0
ERR_RATE_RANGE = (0.001, 0.005)  # 0.1-0.5%
P99_MS_BY_SERVICE = {
    "gateway": (80, 140),
    "checkout": (120, 200),
    "payments": (150, 250),
    "inventory": (90, 160),
    "postgres": (80, 130),
}
RPS_SHARE = {"gateway": 1.0, "checkout": 0.85, "payments": 0.55,
             "inventory": 0.40, "postgres": 0.55}
CPU_BASE = {"gateway": 32, "checkout": 41, "payments": 38, "inventory": 45, "postgres": 50}
MEM_BASE = {"gateway": 512, "checkout": 760, "payments": 690, "inventory": 580, "postgres": 1450}
DB_CONNS_SERVICES = ("payments", "postgres")
DB_CONNS_BASE = {"payments": 14, "postgres": 22}

LOGS_PER_MINUTE_TOTAL = 1650  # ~200k rows over the 120-min window
LOGS_CAP = 250_000

RED_HERRINGS = (
    "deploy_to_inventory",
    "gateway_err_spike",
    "unrelated_flag_flip",
    "routine_rollout_noise",
)
RED_HERRING_COUNT = 2
RED_HERRING_OFFSETS_MIN = {
    "deploy_to_inventory": (-45, -35),
    "gateway_err_spike": (-35, -25),
    "unrelated_flag_flip": (-50, -20),
    "routine_rollout_noise": (-60, -5),
}


def upstream_of(service: str) -> list[str]:
    """Services that depend on `service`, transitively — these show the cascade."""
    parents = [s for s, deps in DEPENDENCIES.items() if service in deps]
    out = list(parents)
    for p in parents:
        out.extend(upstream_of(p))
    return list(dict.fromkeys(out))


def baseline_metrics(rng: np.random.Generator, n_min: int = WINDOW_MIN) -> dict[str, dict]:
    """Per-service metric series. Returns {service: {metric: ndarray}}."""
    t = np.arange(n_min)
    world: dict[str, dict] = {}
    phase = rng.uniform(0, 2 * np.pi)

    for svc in METRIC_SERVICES:
        wave = 1 + RPS_SINUSOID_AMPLITUDE * np.sin(2 * np.pi * t / RPS_PERIOD_MIN + phase)
        rps = GATEWAY_RPS * RPS_SHARE[svc] * wave * rng.normal(1.0, 0.03, n_min)

        lo, hi = ERR_RATE_RANGE
        err = np.clip(rng.uniform(lo, hi) + rng.normal(0, 0.0004, n_min), 0.0, 1.0)

        p_lo, p_hi = P99_MS_BY_SERVICE[svc]
        p99 = rng.uniform(p_lo, p_hi) + rng.normal(0, (p_hi - p_lo) * 0.06, n_min)

        cpu = np.clip(CPU_BASE[svc] + rng.normal(0, 4, n_min) + 6 * (wave - 1), 1, 100)
        mem = MEM_BASE[svc] + rng.normal(0, 18, n_min) + np.linspace(0, 12, n_min)

        conns = (DB_CONNS_BASE[svc] + rng.normal(0, 2.0, n_min)
                 if svc in DB_CONNS_SERVICES else np.full(n_min, np.nan))

        world[svc] = {
            "rps": np.maximum(rps, 0.1),
            "err_rate": err,
            "p99_ms": np.maximum(p99, 1.0),
            "cpu_pct": cpu,
            "mem_mb": np.maximum(mem, 1.0),
            "db_conns": conns,
        }
    return world


def apply_cascade(world: dict[str, dict], root: str, err_elevation: np.ndarray,
                  rng: np.random.Generator) -> None:
    """Propagate the root's error elevation upstream, one hop at a time.

    Each hop adds its own 1-2 min lag and takes ~50% of what the hop below it
    saw, so the cascade arrives in dependency order: a caller can never light up
    before the callee it is waiting on. That ordering is the whole benchmark —
    it is what separates "earliest onset" from "largest magnitude", and it is
    what fools a naive agent into blaming `gateway`.
    """
    n = len(err_elevation)
    frontier = [(root, err_elevation, 0)]
    seen = {root}

    while frontier:
        svc, signal, depth = frontier.pop(0)
        for caller in (s for s, deps in DEPENDENCIES.items() if svc in deps):
            if caller in seen:
                continue
            seen.add(caller)
            lag = int(rng.integers(CASCADE_LAG_MIN[0], CASCADE_LAG_MIN[1] + 1))
            shifted = np.zeros(n)
            shifted[lag:] = signal[: n - lag] * CASCADE_FRACTION
            world[caller]["err_rate"] = np.clip(world[caller]["err_rate"] + shifted, 0.0, 1.0)
            # Latency propagates too, but weaker and noisier than the error signal.
            world[caller]["p99_ms"] = world[caller]["p99_ms"] * (1 + 0.25 * (shifted > 0))
            frontier.append((caller, shifted, depth + 1))


def add_red_herrings(world: dict[str, dict], changes: list[dict], k8s: list[dict],
                     t0: int, rng: np.random.Generator, next_chg, next_k8s) -> list[str]:
    """Sample RED_HERRING_COUNT herrings, write them, return ids for truth.json."""
    picks = rng.choice(len(RED_HERRINGS), size=RED_HERRING_COUNT, replace=False)
    ids: list[str] = []

    for i in picks:
        kind = RED_HERRINGS[int(i)]
        lo, hi = RED_HERRING_OFFSETS_MIN[kind]
        at = max(0, t0 + int(rng.integers(lo, hi + 1)))

        if kind == "deploy_to_inventory":
            eid = next_chg()
            changes.append({"event_id": eid, "minute": at, "kind": "deploy",
                            "service": "inventory", "detail": "inventory v1.8.2->v1.8.3",
                            "author": rng.choice(AUTHORS)})
            ids.append(eid)

        elif kind == "gateway_err_spike":
            # 5 minutes of elevated gateway errors that resolve on their own.
            span = slice(at, min(at + 5, len(world["gateway"]["err_rate"])))
            world["gateway"]["err_rate"][span] += rng.uniform(0.02, 0.035)
            ids.append(f"gateway_spike@{at}")

        elif kind == "unrelated_flag_flip":
            eid = next_chg()
            changes.append({"event_id": eid, "minute": at, "kind": "flag",
                            "service": "gateway", "detail": "gateway.request_logging=on",
                            "author": rng.choice(AUTHORS)})
            ids.append(eid)

        elif kind == "routine_rollout_noise":
            svc = str(rng.choice(["gateway", "checkout", "inventory"]))
            pod = f"{svc}-{rng.integers(1000, 9999)}"
            for off, reason, msg in (
                (0, "Killing", f"Stopping container {svc}"),
                (1, "Scheduled", f"Successfully assigned default/{pod} to node-2"),
            ):
                k8s.append({"event_id": next_k8s(), "minute": at + off, "service": svc,
                            "pod": pod, "reason": reason, "message": msg})
            ids.append(f"rollout@{at}")

    return ids


AUTHORS = ("apatel", "jlin", "mkowalski", "sdiaz", "twong")


def routine_k8s_noise(k8s: list[dict], rng: np.random.Generator, next_k8s,
                      n_min: int = WINDOW_MIN) -> None:
    """Background Scheduled/Killing churn so non-routine reasons have to be
    separated from noise rather than simply being the only rows present."""
    for _ in range(int(rng.integers(6, 12))):
        svc = str(rng.choice(METRIC_SERVICES[:4]))
        at = int(rng.integers(0, n_min - 2))
        pod = f"{svc}-{rng.integers(1000, 9999)}"
        k8s.append({"event_id": next_k8s(), "minute": at, "service": svc, "pod": pod,
                    "reason": "Scheduled",
                    "message": f"Successfully assigned default/{pod} to node-{rng.integers(1,4)}"})
