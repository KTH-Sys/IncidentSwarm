# Single-agent baseline — db: all 5 tables | queries: sql + fts + vector | budget: 110

This is the control in the experiment (plan.md §3). It exists to be compared
against, and the comparison is only fair if the ONLY variable is parallel vs
sequential. Do not tune this prompt to lose, and do not tune it to win.

Your database has all five tables and all three indexes:

| table | columns | index |
|---|---|---|
| `logs` | ts, service, level, trace_id, msg, attrs | fts on `msg` |
| `metrics` | ts, service, rps, p99_ms, err_rate, cpu_pct, mem_mb, db_conns | — |
| `changes` | event_id, ts, kind, service, detail, author | — |
| `k8s_events` | event_id, ts, service, pod, reason, message | fts on `message` |
| `postmortems` | pm_id, title, service, fault_type, body | vector on `body` |

You are given `symptoms[]` from wave-0 triage. Your query budget is 110 — the sum
of the five parallel agents' budgets. Spend it across all five sources.

## Method

Work through each source, then synthesize:

1. **metrics** — change points vs. each service's first-40-minute baseline; find
   the earliest anomalous service and the cascade direction.
2. **logs** — error signatures via full-text search; first occurrence per service;
   per-minute error counts.
3. **changes** — rank changes by proximity to onset and blast radius.
4. **k8s_events** — non-routine reasons (OOMKilled / BackOff / Unhealthy) and
   their onset, separated from rollout noise.
5. **postmortems** — vector search on the symptom pattern for precedent.

Dependency graph: `gateway -> checkout -> {payments, inventory}`,
`payments -> postgres`, `payments -> stripe-api` (external).

## Synthesis

Finish with a single synthesis step using the SAME heuristics the correlator uses:

1. Earliest onset beats largest magnitude.
2. A change within 10 min *before* onset on the root or its dependency is the
   prime trigger candidate.
3. Upstream errors lagging downstream are cascade, not cause.
4. Changes more than 20 min before onset with no matching symptoms are red herrings.
5. No change and no k8s trigger, but logs name an external host ->
   `third_party_latency` with a null trigger.

Return ONE JSON object matching `RCAReport` (agents/schemas.py), identical in
shape to the correlator's output. No prose outside it.

```json
{
  "root_cause": {"service": "", "fault_type": "", "trigger_event_id": null,
                 "start_ts": "", "confidence": 0.0},
  "timeline": [{"ts": "", "source": "changes|metrics|logs|k8s|history", "event": ""}],
  "ruled_out": [{"candidate": "", "reason": ""}],
  "summary": "<= 3 sentences"
}
```
