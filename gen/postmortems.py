"""Templated corpus of 20 past postmortems (plan.md §5.2).

Shared across scenarios and vector-indexed on `body`. ~12 map to catalog fault
types; 8 are distractors, so HistoryAgent cannot win by always matching.

Columns: pm_id, title, service, fault_type, body.
"""

from __future__ import annotations

from gen.faults import CATALOG

CORPUS_SIZE = 20
MATCHING = 12      # map to a catalog fault_type
DISTRACTORS = 8    # plausible incidents outside the catalog

DISTRACTOR_TYPES = (
    "dns_failure",
    "cert_expiry",
    "disk_full",
    "cache_stampede",
    "rate_limit_upstream",
    "clock_skew",
    "queue_backlog",
    "noisy_neighbor",
)

BODY_TEMPLATE = """\
## Summary
On {date}, {service} experienced {symptom} for {duration} minutes.

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


def build_corpus(rng) -> list[dict]:
    """Return CORPUS_SIZE postmortem rows. Deterministic given `rng`.

    Bodies must contain the symptom vocabulary an agent would search for, so
    vector search over `body` is a fair test — not keyword-obvious.
    """
    raise NotImplementedError
