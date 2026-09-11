"""Fault catalog -> signal injectors (plan.md §5.3).

Each fault has a root service, a trigger event, and a signature across metrics,
logs, and k8s events. An injector writes that signature on top of the baseline
from gen/noise.py and returns the error elevation it caused, so the cascade can
be derived from it rather than hand-tuned per fault.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np

# --- World (§5.1) ---
# gateway -> checkout -> {payments, inventory}; payments -> postgres;
# payments -> stripe-api (external).
SERVICES = ("gateway", "checkout", "payments", "inventory", "postgres")
METRIC_SERVICES = SERVICES
DEPENDENCIES = {
    "gateway": ("checkout",),
    "checkout": ("payments", "inventory"),
    "payments": ("postgres", "stripe-api"),
    "inventory": (),
    "postgres": (),
}
EXTERNAL = ("stripe-api",)

WINDOW_MIN = 120
METRIC_INTERVAL_MIN = 1
T0_RANGE_MIN = (50, 80)

CASCADE_FRACTION = 0.5
CASCADE_LAG_MIN = (1, 2)


@dataclass
class Injection:
    """What an injector produced, for truth.json and downstream generation."""

    trigger_event_id: str | None
    err_elevation: np.ndarray          # per-minute error elevation on the root
    log_msg: str                       # the error signature agents search for
    log_level: str = "ERROR"
    extra_log_msgs: tuple[str, ...] = ()


@dataclass
class Fault:
    fault_type: str
    root_service: str
    trigger_kind: str | None  # deploy / flag / config / k8s / None (external)
    trigger_detail: str | None
    metrics_signature: str
    logs_signature: str
    k8s_signature: str | None
    inject: Callable | None = None


def _ramp(n: int, t0: int, length: int, peak: float) -> np.ndarray:
    """Linear ramp to `peak` over `length` minutes starting at t0, then flat."""
    out = np.zeros(n)
    for i in range(t0, n):
        out[i] = peak * min(1.0, (i - t0 + 1) / max(length, 1))
    return out


def _step(n: int, t0: int, value: float, delay: int = 0) -> np.ndarray:
    out = np.zeros(n)
    out[min(t0 + delay, n):] = value
    return out


# --- Injectors -------------------------------------------------------------

def inject_bad_deploy(world, changes, k8s, t0, rng, next_chg, next_k8s) -> Injection:
    n = WINDOW_MIN
    svc = "payments"
    elev = _step(n, t0, float(rng.uniform(0.08, 0.15)), delay=1)  # err 8-15% from t0+1
    world[svc]["err_rate"] = np.clip(world[svc]["err_rate"] + elev, 0, 1)
    world[svc]["p99_ms"] *= 1 + 0.6 * (elev > 0)
    eid = next_chg()
    changes.append({"event_id": eid, "minute": t0, "kind": "deploy", "service": svc,
                    "detail": "payments v2.3.0->v2.3.1", "author": str(rng.choice(AUTHORS))})
    return Injection(eid, elev, "NullPointerException in ChargeHandler",
                     extra_log_msgs=("HTTP 500 returned from /charge",))


def inject_flag_flip(world, changes, k8s, t0, rng, next_chg, next_k8s) -> Injection:
    n = WINDOW_MIN
    svc = "checkout"
    elev = _step(n, t0, float(rng.uniform(0.015, 0.025)))  # err ~2%
    world[svc]["err_rate"] = np.clip(world[svc]["err_rate"] + elev, 0, 1)
    world[svc]["p99_ms"] *= 1 + 2.0 * (elev > 0)  # p99 x3
    eid = next_chg()
    changes.append({"event_id": eid, "minute": t0, "kind": "flag", "service": svc,
                    "detail": "checkout.new_pricing=on", "author": str(rng.choice(AUTHORS))})
    return Injection(eid, elev, "pricing fallback after timeout", log_level="ERROR")


def inject_pool_exhaustion(world, changes, k8s, t0, rng, next_chg, next_k8s) -> Injection:
    n = WINDOW_MIN
    svc = "payments"
    elev = _step(n, t0, float(rng.uniform(0.04, 0.07)))
    world[svc]["err_rate"] = np.clip(world[svc]["err_rate"] + elev, 0, 1)
    world[svc]["p99_ms"] *= 1 + 9.0 * (elev > 0)  # p99 x10
    world[svc]["db_conns"][t0:] = 10.0             # pinned flat at the new ceiling
    world["postgres"]["db_conns"][t0:] = 10.0
    eid = next_chg()
    changes.append({"event_id": eid, "minute": t0, "kind": "config", "service": svc,
                    "detail": "db_pool_max 50->10", "author": str(rng.choice(AUTHORS))})
    return Injection(eid, elev, "timeout acquiring connection from pool")


def inject_cpu_throttle(world, changes, k8s, t0, rng, next_chg, next_k8s) -> Injection:
    n = WINDOW_MIN
    svc = "inventory"
    elev = _ramp(n, t0, 4, float(rng.uniform(0.02, 0.04)))
    world[svc]["err_rate"] = np.clip(world[svc]["err_rate"] + elev, 0, 1)
    world[svc]["p99_ms"] *= 1 + 3.0 * (elev > 0)   # p99 x4
    world[svc]["cpu_pct"][t0:] = np.clip(rng.normal(98, 1.0, n - t0), 90, 100)
    eid = next_k8s()
    pod = f"{svc}-{rng.integers(1000, 9999)}"
    k8s.append({"event_id": eid, "minute": t0, "service": svc, "pod": pod,
                "reason": "Unhealthy", "message": "Unhealthy: readiness probe failed"})
    for off in (3, 7):
        k8s.append({"event_id": next_k8s(), "minute": t0 + off, "service": svc, "pod": pod,
                    "reason": "Unhealthy", "message": "Unhealthy: readiness probe failed"})
    return Injection(eid, elev, "slow request", log_level="WARN")


def inject_third_party_latency(world, changes, k8s, t0, rng, next_chg, next_k8s) -> Injection:
    n = WINDOW_MIN
    svc = "payments"
    elev = _ramp(n, t0, 10, float(rng.uniform(0.04, 0.06)))  # err ~5%
    world[svc]["err_rate"] = np.clip(world[svc]["err_rate"] + elev, 0, 1)
    world[svc]["p99_ms"] *= 1 + 5.0 * _ramp(n, t0, 10, 1.0)  # p99 ramps x6 over 10 min
    # No change, no k8s event — trigger_event_id stays null (§5.3).
    return Injection(None, elev, "stripe-api upstream timeout 504")


def inject_memory_leak(world, changes, k8s, t0, rng, next_chg, next_k8s) -> Injection:
    n = WINDOW_MIN
    svc = "checkout"
    leak_start = max(0, t0 - 30)  # mem ramps from t0-30
    mem = world[svc]["mem_mb"]
    mem[leak_start:] += np.linspace(0, 1400, n - leak_start)
    elev = _step(n, t0, float(rng.uniform(0.03, 0.06)))
    world[svc]["err_rate"] = np.clip(world[svc]["err_rate"] + elev, 0, 1)
    world[svc]["p99_ms"] *= 1 + 1.5 * (elev > 0)

    pod = f"{svc}-{rng.integers(1000, 9999)}"
    eid = next_k8s()
    k8s.append({"event_id": eid, "minute": t0, "service": svc, "pod": pod,
                "reason": "OOMKilled", "message": "Container checkout OOMKilled"})
    # Sawtooth: each restart drops memory, then it climbs again.
    for i, off in enumerate((8, 17)):
        at = t0 + off
        if at >= n:
            break
        mem[at:] -= mem[at] - (700 + 60 * i)
        mem[at:] += np.linspace(0, 900, n - at)
        k8s.append({"event_id": next_k8s(), "minute": at, "service": svc, "pod": pod,
                    "reason": "BackOff", "message": "Back-off restarting failed container"})
    world[svc]["mem_mb"] = np.maximum(mem, 100.0)
    return Injection(eid, elev, "GC overhead limit exceeded")


AUTHORS = ("apatel", "jlin", "mkowalski", "sdiaz", "twong")

CATALOG: dict[str, Fault] = {
    "bad_deploy": Fault("bad_deploy", "payments", "deploy", "v2.3.0->v2.3.1",
                        "err_rate 8-15% from t0+1",
                        "NullPointerException in ChargeHandler, HTTP 500", None,
                        inject_bad_deploy),
    "flag_flip": Fault("flag_flip", "checkout", "flag", "checkout.new_pricing=on",
                       "p99 x3, err_rate ~2%", "pricing fallback after timeout", None,
                       inject_flag_flip),
    "pool_exhaustion": Fault("pool_exhaustion", "payments", "config", "db_pool_max 50->10",
                             "payments db_conns pinned at 10, p99 x10",
                             "timeout acquiring connection from pool", None,
                             inject_pool_exhaustion),
    "cpu_throttle": Fault("cpu_throttle", "inventory", "k8s", "Unhealthy: readiness probe failed",
                          "cpu_pct >= 97%, p99 x4", "sparse 'slow request' WARNs",
                          "Unhealthy: readiness probe failed", inject_cpu_throttle),
    "third_party_latency": Fault("third_party_latency", "payments", None, None,
                                 "p99 ramps x6 over 10 min, err_rate ~5%",
                                 "stripe-api upstream timeout 504", None,
                                 inject_third_party_latency),
    "memory_leak": Fault("memory_leak", "checkout", "k8s", "OOMKilled",
                         "mem_mb linear ramp from t0-30, sawtooth after restarts",
                         "GC overhead limit", "OOMKilled, BackOff", inject_memory_leak),
}

PRIORITY = ("bad_deploy", "pool_exhaustion", "memory_leak")
