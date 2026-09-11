"""Templated corpus of 20 past postmortems (plan.md §5.2).

Shared across scenarios, vector-indexed on `body`. 12 map to catalog fault types,
8 are distractors — so HistoryAgent cannot win by always matching, and a
confident answer from it is sometimes wrong on purpose.

Bodies describe SYMPTOMS in natural incident language rather than naming the
fault type, so vector search is a fair test instead of a keyword lookup.
"""

from __future__ import annotations

import numpy as np

CORPUS_SIZE = 20
MATCHING = 12
DISTRACTORS = 8

BODY_TEMPLATE = """\
## Summary
On {date}, {service} experienced {symptom} lasting {duration} minutes.

## Impact
{impact}

## Root cause
{root_cause}

## Detection
{detection}

## Resolution
{resolution}

## Lessons
{lessons}
"""

# fault_type -> (title, service, symptom, root_cause, detection, resolution, lessons)
MATCHED = [
    ("bad_deploy", "Checkout errors after payments release", "payments",
     "a sharp jump in 5xx responses from the charge endpoint",
     "a release shipped an unguarded null dereference in the charge handler",
     "error-rate alert fired two minutes after the rollout completed",
     "rolled back to the previous release; errors cleared within a minute",
     "tie deploy markers to error-rate panels so the correlation is visible at a glance"),
    ("bad_deploy", "Regression in order submission path", "checkout",
     "elevated server errors immediately following a version bump",
     "a code change removed a defensive check on an optional field",
     "on-call noticed the error step change aligned exactly with the release time",
     "reverted the release",
     "a step change that starts at a release time is a release problem until proven otherwise"),
    ("flag_flip", "Latency tripled after pricing experiment enabled", "checkout",
     "p99 latency roughly tripling with a small rise in errors",
     "a feature flag enabled a pricing path that called a slow downstream synchronously",
     "latency alert; the flag audit log showed the toggle at the same minute",
     "disabled the flag",
     "flag flips deserve the same scrutiny as deploys; they are changes too"),
    ("flag_flip", "Timeout fallbacks on new checkout path", "checkout",
     "intermittent fallback responses and rising tail latency",
     "an experiment routed a fraction of traffic through an unoptimized code path",
     "fallback log volume rose sharply",
     "turned the experiment off and re-enabled it behind a cache",
     "ramp experiments gradually and watch tail latency, not just averages"),
    ("pool_exhaustion", "Connection pool starvation in payments", "payments",
     "requests queueing on connection acquisition with p99 an order of magnitude up",
     "a configuration change lowered the maximum pool size well below peak concurrency",
     "connection count sat pinned flat at a ceiling while latency climbed",
     "restored the previous pool maximum",
     "a flat-topped metric is a limit being hit, not a plateau in demand"),
    ("pool_exhaustion", "Database saturation during peak", "postgres",
     "timeouts acquiring connections and severe latency amplification",
     "pool capacity was reduced during a config cleanup and not load tested",
     "timeout log messages naming the pool",
     "raised the pool ceiling and added an alert on pool utilization",
     "alert on saturation of every bounded resource, not only CPU and memory"),
    ("cpu_throttle", "Readiness probes failing under CPU pressure", "inventory",
     "CPU pinned near its limit with probe failures and degraded latency",
     "a workload increase pushed the service past its CPU allocation",
     "readiness probe failures appeared in cluster events",
     "raised the CPU limit and added horizontal capacity",
     "probe failures are often a symptom of resource starvation, not a bad probe"),
    ("cpu_throttle", "Throttling after traffic shift", "inventory",
     "sustained high CPU with slow request warnings",
     "traffic was shifted onto a pool with a lower CPU allocation",
     "cluster events showed repeated readiness failures on the same pods",
     "shifted traffic back and resized the pool",
     "capacity assumptions must follow traffic when it moves"),
    ("third_party_latency", "Payment provider degradation", "payments",
     "tail latency ramping steadily over ten minutes with a moderate error rise",
     "an external payment provider degraded; nothing changed on our side",
     "upstream timeout messages naming the provider host",
     "enabled a circuit breaker and queued retries until the provider recovered",
     "when no internal change lines up, look outward before blaming the last deploy"),
    ("third_party_latency", "Upstream 504s from external API", "payments",
     "gateway timeouts from a third-party dependency with a gradual latency ramp",
     "the external service was rate limiting us during an incident on their side",
     "logs showed 504s naming an external hostname",
     "backed off and reduced concurrency to the provider",
     "a gradual ramp with no deploy nearby points at a dependency, not a release"),
    ("memory_leak", "Gradual memory growth ending in restarts", "checkout",
     "memory climbing steadily for half an hour before the first container kill",
     "an unbounded cache retained request objects across requests",
     "container kill events, preceded by a long linear memory climb",
     "patched the cache to bound its size",
     "the onset of a leak is where memory starts climbing, not where the pod dies"),
    ("memory_leak", "Repeated container restarts under load", "checkout",
     "a sawtooth memory pattern with repeated restarts and backoff",
     "a leak filled the heap; each restart reset it and it climbed again",
     "garbage collection overhead warnings, then kill and backoff events",
     "raised the limit as a stopgap, then fixed the retention bug",
     "sawtooth memory is a leak plus restarts; read back to the first climb"),
]

DISTRACTORS_DATA = [
    ("dns_failure", "Resolution failures after DNS migration", "gateway",
     "widespread connection failures across every downstream",
     "a DNS migration left stale records for internal services"),
    ("cert_expiry", "TLS handshake failures", "gateway",
     "a total stop in traffic to one downstream",
     "an expired intermediate certificate was not rotated"),
    ("disk_full", "Write failures on log volume", "postgres",
     "write errors and a read-only database",
     "log rotation stopped and the volume filled"),
    ("cache_stampede", "Thundering herd after cache flush", "inventory",
     "a brief but extreme load spike on the database",
     "a full cache flush sent every request to the origin at once"),
    ("rate_limit_upstream", "Throttled by partner API", "payments",
     "a burst of 429 responses from a partner",
     "a batch job consumed the shared rate limit budget"),
    ("clock_skew", "Token validation failures", "gateway",
     "authentication failures across a subset of hosts",
     "NTP drift on two hosts pushed them outside the token validity window"),
    ("queue_backlog", "Consumer lag during backfill", "checkout",
     "growing processing delay with no error rise",
     "a backfill job outpaced consumer capacity"),
    ("noisy_neighbor", "Degradation from co-tenant workload", "inventory",
     "erratic latency with normal CPU on our own processes",
     "a co-tenant workload saturated shared disk bandwidth"),
]


def build_corpus(rng: np.random.Generator) -> list[dict]:
    """Return CORPUS_SIZE postmortem rows. Deterministic given `rng`."""
    rows: list[dict] = []

    for i, (ft, title, svc, symptom, cause, detect, fix, lesson) in enumerate(MATCHED):
        rows.append({
            "pm_id": f"pm_{i + 1:03d}",
            "title": title,
            "service": svc,
            "fault_type": ft,
            "body": BODY_TEMPLATE.format(
                date=f"2026-0{int(rng.integers(1, 9))}-{int(rng.integers(10, 28))}",
                service=svc, symptom=symptom, duration=int(rng.integers(12, 90)),
                impact="Checkout conversion dropped for the duration of the incident.",
                root_cause=cause, detection=detect, resolution=fix, lessons=lesson),
        })

    for j, (ft, title, svc, symptom, cause) in enumerate(DISTRACTORS_DATA):
        rows.append({
            "pm_id": f"pm_{len(MATCHED) + j + 1:03d}",
            "title": title,
            "service": svc,
            "fault_type": ft,
            "body": BODY_TEMPLATE.format(
                date=f"2026-0{int(rng.integers(1, 9))}-{int(rng.integers(10, 28))}",
                service=svc, symptom=symptom, duration=int(rng.integers(8, 60)),
                impact="Partial degradation for a subset of requests.",
                root_cause=cause,
                detection="Alerting caught it within a few minutes.",
                resolution="Mitigated once the cause was identified.",
                lessons="Check the dependency chain before assuming an application bug."),
        })

    assert len(rows) == CORPUS_SIZE, f"corpus is {len(rows)}, expected {CORPUS_SIZE}"
    return rows
