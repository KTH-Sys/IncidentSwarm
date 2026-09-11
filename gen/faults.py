"""Fault catalog -> signal injectors (plan.md §5.3).

Each fault has a root service, a trigger event, and a signature across metrics,
logs, and k8s events. The injectors write the fault's signal on top of the
baseline traffic produced by gen/noise.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

# --- World (§5.1) ---
# gateway -> checkout -> {payments, inventory}; payments -> postgres;
# payments -> stripe-api (external).
SERVICES = ("gateway", "checkout", "payments", "inventory", "postgres")
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
T0_RANGE_MIN = (50, 80)  # fault onset drawn uniformly from this range

# Cascade rule (§5.1): services upstream of the root cause get ~50% of its error
# elevation, lagged 1-2 min. This is what fools naive agents into blaming gateway.
CASCADE_FRACTION = 0.5
CASCADE_LAG_MIN = (1, 2)


@dataclass
class Fault:
    fault_type: str
    root_service: str
    trigger_kind: str | None  # deploy / flag / config / k8s / None (external)
    trigger_detail: str | None
    metrics_signature: str
    logs_signature: str
    k8s_signature: str | None
    inject: Callable | None = field(default=None, repr=False)


CATALOG: dict[str, Fault] = {
    "bad_deploy": Fault(
        fault_type="bad_deploy",
        root_service="payments",
        trigger_kind="deploy",
        trigger_detail="v2.3.0->v2.3.1",
        metrics_signature="err_rate 8-15% from t0+1",
        logs_signature="NullPointerException in ChargeHandler, HTTP 500",
        k8s_signature=None,
    ),
    "flag_flip": Fault(
        fault_type="flag_flip",
        root_service="checkout",
        trigger_kind="flag",
        trigger_detail="checkout.new_pricing=on",
        metrics_signature="p99 x3, err_rate ~2%",
        logs_signature="pricing fallback after timeout",
        k8s_signature=None,
    ),
    "pool_exhaustion": Fault(
        fault_type="pool_exhaustion",
        root_service="payments",
        trigger_kind="config",
        trigger_detail="db_pool_max 50->10",
        metrics_signature="payments db_conns pinned at 10, p99 x10",
        logs_signature="timeout acquiring connection from pool",
        k8s_signature=None,
    ),
    "cpu_throttle": Fault(
        fault_type="cpu_throttle",
        root_service="inventory",
        trigger_kind="k8s",  # trigger_event_id is the first Unhealthy event
        trigger_detail="Unhealthy: readiness probe failed",
        metrics_signature="cpu_pct >= 97%, p99 x4",
        logs_signature="sparse 'slow request' WARNs",
        k8s_signature="Unhealthy: readiness probe failed",
    ),
    "third_party_latency": Fault(
        fault_type="third_party_latency",
        root_service="payments",
        trigger_kind=None,  # external: trigger_event_id is null
        trigger_detail=None,
        metrics_signature="p99 ramps x6 over 10 min, err_rate ~5%",
        logs_signature="stripe-api upstream timeout 504",
        k8s_signature=None,
    ),
    "memory_leak": Fault(
        fault_type="memory_leak",
        root_service="checkout",
        trigger_kind="k8s",  # trigger_event_id is the first OOMKilled event
        trigger_detail="OOMKilled",
        metrics_signature="mem_mb linear ramp from t0-30, sawtooth after restarts",
        logs_signature="GC overhead limit",
        k8s_signature="OOMKilled, BackOff",
    ),
}

# Build order when behind schedule (§11, 11:45-12:30): these three first.
PRIORITY = ("bad_deploy", "pool_exhaustion", "memory_leak")


def inject(fault_type: str, tables: dict, t0_min: int, rng) -> dict:
    """Write `fault_type`'s signature into the baseline tables in place.

    TODO: one injector per catalog entry. Build PRIORITY first (§12 cut list
    item 2 allows dropping to 3 fault types).
    """
    raise NotImplementedError(f"injector for {fault_type}")
