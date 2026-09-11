"""Baseline traffic, red herrings, and cascade (plan.md §5.1, §5.3).

Baseline must look boring and real: the fault signature is only findable because
it breaks these levels, and the red herrings are what a naive agent latches onto.
"""

from __future__ import annotations

from dataclasses import dataclass

from gen.faults import CASCADE_FRACTION, CASCADE_LAG_MIN, DEPENDENCIES

# --- Baseline levels (§5.1) ---
GATEWAY_RPS = 30.0
RPS_SINUSOID_AMPLITUDE = 0.20  # +/-20%
ERR_RATE_RANGE = (0.001, 0.005)  # 0.1-0.5%
P99_MS_BY_SERVICE = {
    "gateway": (80, 140),
    "checkout": (120, 200),
    "payments": (150, 250),
    "inventory": (90, 160),
    "postgres": (80, 130),
}

# Downstream services get rps proportional to gateway.
RPS_SHARE = {"gateway": 1.0, "checkout": 0.85, "payments": 0.55, "inventory": 0.40,
             "postgres": 0.55}

# Red herrings (§5.3): each scenario gets 2, sampled from these.
RED_HERRINGS = (
    "deploy_to_inventory",       # 35-45 min before t0
    "gateway_err_spike",         # 5-min spike, 25-35 min before t0
    "unrelated_flag_flip",       # on gateway
    "routine_rollout_noise",     # Scheduled / Killing k8s events
)
RED_HERRING_COUNT = 2

RED_HERRING_OFFSETS_MIN = {
    "deploy_to_inventory": (-45, -35),
    "gateway_err_spike": (-35, -25),
    "unrelated_flag_flip": (-50, -20),
    "routine_rollout_noise": (-60, 0),
}


def upstream_of(service: str) -> list[str]:
    """Services that depend on `service`, transitively — these show the cascade."""
    parents = [s for s, deps in DEPENDENCIES.items() if service in deps]
    out = list(parents)
    for p in parents:
        out.extend(upstream_of(p))
    return list(dict.fromkeys(out))


def baseline_metrics(n_minutes: int, rng) -> list[dict]:
    """120 x 5 = 600 rows. Sinusoidal rps, Gaussian jitter on err_rate and p99."""
    raise NotImplementedError


def baseline_logs(metrics: list[dict], rng) -> list[dict]:
    """~200k rows, capped at 250k. trace_id shared across services per request."""
    raise NotImplementedError


def apply_cascade(metrics: list[dict], root_service: str, t0_min: int, rng) -> None:
    """Upstream services get ~50% of the root's error elevation, lagged 1-2 min.

    The correlator's job is to discount this (upstream errors lagging downstream
    are cascade, not cause). Getting the lag right is what makes the benchmark
    non-trivial.
    """
    raise NotImplementedError


def add_red_herrings(tables: dict, t0_min: int, rng) -> list[str]:
    """Sample RED_HERRING_COUNT herrings, write them, return their event ids for
    truth.json."""
    raise NotImplementedError
